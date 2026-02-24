import asyncio
import json
import time
import uuid
from pathlib import Path
import io
import pandas as pd

from fastapi import APIRouter, HTTPException, BackgroundTasks
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from common.config import get_settings
from common.s3_client import get_s3_client, ensure_bucket
from common.db import get_db_connection

settings = get_settings()
router = APIRouter(tags=["ingestion"])
ingest_status: dict[str, dict] = {}

producer: AIOKafkaProducer | None = None


async def startup_event():
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap)
    await producer.start()
async def shutdown_event():
    if producer:
        await producer.stop()


# backgound task to read CSV, send to Kafka, consume back, and save to S3
async def _process_ratings(job_id: str, limit: int):
    ingest_status[job_id] = {"status": "running", "sent": 0, "s3_key": None, "error": None}
    try:
        path = Path("data/raw/rating.csv")
        if not path.exists():
            raise FileNotFoundError("rating.csv not found")
        if producer is None:
            raise RuntimeError("Kafka not ready")

        df = await asyncio.to_thread(pd.read_csv, path, nrows=limit)
        df["timestamp"] = pd.to_datetime(df["timestamp"]).astype("int64") // 10**6

        events = [
            {
                "event_id": str(uuid.uuid4()),
                "user_id": int(row["userId"]),
                "movie_id": int(row["movieId"]),
                "rating": float(row["rating"]),
                "timestamp": int(row["timestamp"]),
            }
            for _, row in df.iterrows()
        ]

        # Batch send to Kafka
        tasks = [producer.send(settings.kafka_topic, json.dumps(event).encode()) for event in events]
        await asyncio.gather(*tasks)
        sent = len(events)

        # Consume from Kafka (limited to sent records)
        consumer = AIOKafkaConsumer(
            settings.kafka_topic,
            bootstrap_servers=settings.kafka_bootstrap,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id="ingest-worker",
        )
        await consumer.start()
        records = []
        try:
            polled = await consumer.getmany(timeout_ms=10000, max_records=sent)
            for tp, msgs in polled.items():
                for msg in msgs:
                    records.append(json.loads(msg.value))
        finally:
            await consumer.stop()

        # Save to S3
        buffer = io.BytesIO()
        for record in records:
            buffer.write(json.dumps(record).encode() + b"\n")
        buffer.seek(0)

        s3_client = get_s3_client()
        bucket = ensure_bucket(s3_client)
        key = f"raw/ratings-{int(time.time())}-{sent}.jsonl"

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, s3_client.upload_fileobj, buffer, bucket, key)

        ingest_status[job_id] = {"status": "success", "sent": sent, "s3_key": key, "error": None}
    except Exception as exc:
        ingest_status[job_id] = {"status": "failed", "sent": 0, "s3_key": None, "error": str(exc)}


@router.post("/ingest/ratings")
async def ingest_ratings(limit: int = 5000, background_tasks: BackgroundTasks = None):
    """Kick off background ingestion of rating.csv (default first 5000 rows)."""
    job_id = str(uuid.uuid4())
    ingest_status[job_id] = {"status": "queued", "sent": 0, "s3_key": None, "error": None}
    if background_tasks is not None:
        background_tasks.add_task(_process_ratings, job_id, limit)
    else:
        asyncio.create_task(_process_ratings(job_id, limit))
    return {"job_id": job_id, "status": "queued"}


@router.get("/ingest/ratings/{job_id}")
async def ingest_status_check(job_id: str):
    status = ingest_status.get(job_id)
    if not status:
        raise HTTPException(status_code=404, detail="job_id not found")
    return status


@router.post("/upload/movies")
async def upload_movies():
    """Load movies.csv into Postgres"""
    path = Path("data/raw/movie.csv")

    if not path.exists():
        raise HTTPException(status_code=404, detail="movie.csv not found")
    
    df = pd.read_csv(path)
    
    db = get_db_connection()
    cursor = db.cursor()
    
    records = [
        (int(row["movieId"]), str(row["title"]), str(row["genres"]))
        for _, row in df.iterrows()
    ]
    cursor.executemany(
        """
        INSERT INTO movies (movie_id, title, genres)
        VALUES (%s, %s, %s)
        ON CONFLICT (movie_id) DO UPDATE SET title = EXCLUDED.title, genres = EXCLUDED.genres
        """,
        records,
    )
    
    db.commit()
    cursor.close()

    return {"status": "ok", "movies_inserted": len(df)}
