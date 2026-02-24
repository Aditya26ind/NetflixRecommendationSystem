import asyncio
from pathlib import Path
import pandas as pd

from common.db import get_db_connection
from common.s3_client import ensure_bucket, get_s3_client
from fastapi import FastAPI
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from . import event_api, recommendation_api
from common.config import get_settings

settings = get_settings()
producer: AIOKafkaProducer | None = None  # local producer for this module

app = FastAPI(title="Unified API", version="0.1.0")

app.include_router(event_api.router)
app.include_router(recommendation_api.router)

async def startup_event():
    """Seed DB + S3, then start Kafka producer (and event_api producer)."""
    global producer

    loop = asyncio.get_running_loop()

    # Load CSVs once
    # data directory lives at /app/data/raw inside the image
    data_root = Path(__file__).resolve().parent.parent / "data" / "raw"
    movie_path = data_root / "movie.csv"
    rating_path = data_root / "rating.csv"

    movie_df = pd.read_csv(movie_path)

    #  Insert movies to DB (run in executor to not block event loop) ---
    async def insert_movies():
        records = [
            (int(row[0]), str(row[1]), str(row[2]))
            for row in movie_df[["movieId", "title", "genres"]].itertuples(index=False)
        ]
        await loop.run_in_executor(None, _insert_movies_sync, records)

    def _insert_movies_sync(records):
        db = get_db_connection()
        cursor = db.cursor()
        try:
            cursor.executemany(
                """
                INSERT INTO movies (movie_id, title, genres)
                VALUES (%s, %s, %s)
                ON CONFLICT (movie_id) DO UPDATE SET title = EXCLUDED.title, genres = EXCLUDED.genres
                """,
                records,
            )
            db.commit()
        finally:
            cursor.close()
            db.close()  # always close connection

    # ---  Upload rating.csv to S3 if not already there ---
    async def upload_ratings():
        client = get_s3_client()
        bucket = ensure_bucket(client)
        s3_key = "raw/rating.csv"
        try:
            await loop.run_in_executor(
                None,
                lambda: client.head_object(Bucket=bucket, Key=s3_key)  # correct way to check existence
            )
            print(f"{s3_key} already exists in S3, skipping upload.")
        except client.exceptions.ClientError:
            if rating_path.exists():
                await loop.run_in_executor(
                    None,
                    lambda: client.upload_file(
                        Filename=str(rating_path),
                        Bucket=bucket,
                        Key=s3_key
                    )
                )
                print(f"Uploaded {rating_path} to s3://{bucket}/{s3_key}")
            else:
                print("Local rating.csv not found, skipping upload.")

    # --- Run DB insert + S3 upload concurrently ---
    await asyncio.gather(insert_movies(), upload_ratings())

    # Start Kafka Producer for this module
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap)
    await producer.start()
    print("Kafka producer started.")

    # Also start the producer used inside event_api (so its routes work)
    await event_api.startup_event()
    
    
async def shutdown_event():
    if producer:
        await producer.stop()
    # stop producer managed by event_api
    await event_api.shutdown_event()

# attach lifecycle handlers (our own wrapper also calls event_api's)
app.add_event_handler("startup", startup_event)
app.add_event_handler("shutdown", shutdown_event)

@app.get("/health")
async def health():
    return {"status": "ok"}
