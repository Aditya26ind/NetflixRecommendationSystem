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
    """Initialize kafka producer with simple retries so API startup doesn't crash while Kafka boots/DNS settles."""
    global producer
    attempts = 5
    delay = 3
    for i in range(1, attempts + 1):
        try:
            producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap)
            await producer.start()
            break
        except Exception as exc:
            producer = None
            if i == attempts:
                raise
            print(f"[event_api] Kafka connect failed ({exc}); retrying in {delay}s ({i}/{attempts})")
            await asyncio.sleep(delay)


async def shutdown_event():
    global producer
    if producer:
        await producer.stop()



# backgound task to read CSV, send to Kafka, consume back, and save to S3
async def _process_ratings(job_id: str,  csv_row_number: int = 0):
    ingest_status[job_id] = {"status": "running", "sent": 0, "s3_key": None, "error": None}
    try:
        s3_client = get_s3_client()

        buffer = io.BytesIO()
        s3_client.download_fileobj(ensure_bucket(s3_client), "raw/rating.csv", buffer)
        buffer.seek(0)
        
        df = pd.read_csv(
            buffer,
            skiprows=range(1, csv_row_number + 1),  
            nrows=1                                   
        )
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
        ingest_status[job_id]["sent"] = sent

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
                    print(msg.value)
                    records.append(json.loads(msg.value))
            await consumer.commit()
        finally:
            await consumer.stop()

        if not records:
            raise ValueError("No records consumed from Kafka")

        # Convert consumed records back to DataFrame
        consumed_df = pd.DataFrame(records)

        # Download existing rating.csv from S3
        existing_buffer = io.BytesIO()
        s3_client.download_fileobj(ensure_bucket(s3_client), "raw/rating.csv", existing_buffer)
        existing_buffer.seek(0)
        existing_df = pd.read_csv(existing_buffer)

        # Append new data
        merged_df = pd.concat([existing_df, consumed_df], ignore_index=True)

        # Upload back to S3 (overwrite rating.csv)
        out_buffer = io.BytesIO()
        merged_df.to_csv(out_buffer, index=False)
        out_buffer.seek(0)

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, s3_client.upload_fileobj, out_buffer, ensure_bucket(s3_client), "raw/rating.csv")

        ingest_status[job_id] = {"status": "success", "sent": sent, "s3_key": "raw/rating.csv", "error": None}

    except Exception as exc:
        ingest_status[job_id] = {"status": "failed", "sent": 0, "s3_key": None, "error": str(exc)}


@router.post("/event/rating")
async def ingest_ratings(csv_row_number=0, background_tasks: BackgroundTasks = None):
    """Kick off background ingestion of rating.csv (default first 5000 rows)."""
    job_id = str(uuid.uuid4())
    ingest_status[job_id] = {"status": "queued", "sent": 0, "s3_key": None, "error": None}
    if background_tasks is not None:
        background_tasks.add_task(_process_ratings, job_id, csv_row_number)
    else:
        asyncio.create_task(_process_ratings(job_id, csv_row_number))
    return {"job_id": job_id, "status": "queued"}
