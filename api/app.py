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

app = FastAPI(title="Unified API", version="0.1.0")

app.include_router(event_api.router)
app.include_router(recommendation_api.router)

app.add_event_handler("startup", event_api.startup_event)
app.add_event_handler("shutdown", event_api.shutdown_event)

async def startup_event():
    global producer

    loop = asyncio.get_running_loop()

    # Load CSVs once
    movie_path = Path(__file__).resolve().parent / "data" / "raw" / "movie.csv"
    rating_path = Path(__file__).resolve().parent / "data" / "raw" / "rating.csv"

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
        s3_key = "raw/rating.csv"
        try:
            await loop.run_in_executor(
                None,
                lambda: client.head_object(Bucket=ensure_bucket(client), Key=s3_key)  # correct way to check existence
            )
            print(f"{s3_key} already exists in S3, skipping upload.")
        except client.exceptions.ClientError:
            if rating_path.exists():
                await loop.run_in_executor(
                    None,
                    lambda: client.upload_file(
                        Filename=str(rating_path),
                        Bucket=ensure_bucket(client),
                        Key=s3_key
                    )
                )
                print(f"Uploaded {rating_path} to s3://{settings.s3_bucket}/{s3_key}")
            else:
                print("Local rating.csv not found, skipping upload.")

    # --- Run DB insert + S3 upload concurrently ---
    await asyncio.gather(insert_movies(), upload_ratings())

    # Start Kafka Producer
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap)
    await producer.start()
    print("Kafka producer started.")
    
    
async def shutdown_event():
    if producer:
        await producer.stop()

@app.get("/health")
async def health():
    return {"status": "ok"}
