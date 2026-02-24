"""Batch aggregates: read raw ratings from S3 (MinIO), write user_aggregates and movie_popularity to Postgres."""
import json
from collections import Counter
import pandas as pd
import io

from common import logging
from common.config import get_settings
from common.db import db_cursor, init_pool
from common.s3_client import get_s3_client, ensure_bucket

settings = get_settings()


def load_events_from_s3(prefix: str = "raw/"):
    s3 = get_s3_client()
    bucket = ensure_bucket(s3)
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    events = []
    for obj in resp.get("Contents", []):
        body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode()
        # logging.info(f"Loaded {obj['Key']} from S3, size: {len(body)} bytes")
        for line in body.splitlines():
            events.append(json.loads(line))
    return events


def aggregate_user_features(df: pd.DataFrame):
    results = []
    for user_id in df["user_id"].unique():
        user_df = df[df["user_id"] == user_id]
        top_movies = user_df.nlargest(5, "rating")["movie_id"].tolist()
        results.append((int(user_id), top_movies))
    return results


def aggregate_movie_popularity(df: pd.DataFrame):
    grouped = df.groupby("movie_id").agg(event_count=("rating", "count"), avg_rating=("rating", "mean")).reset_index()
    return [(int(row.movie_id), int(row.event_count), float(row.avg_rating)) for row in grouped.itertuples(index=False)]


def main():
    init_pool()
    events = load_events_from_s3()
    if not events:
        print("no data in s3 raw/")
        return
    df = pd.DataFrame(events)

    users = aggregate_user_features(df)
    movies = aggregate_movie_popularity(df)

    with db_cursor() as cur:
        
        # for optimization used executemany as it do bulk.
        cur.executemany(
            """
            INSERT INTO user_aggregates (user_id, top_movies)
            VALUES (%s, %s)
            ON CONFLICT (user_id)
            DO UPDATE SET top_movies = EXCLUDED.top_movies
            """,
            users,
        )
        
        # same
        cur.executemany(
            """
            INSERT INTO movie_popularity (movie_id, event_count, avg_rating)
            VALUES (%s, %s, %s)
            ON CONFLICT (movie_id)
            DO UPDATE SET event_count = EXCLUDED.event_count, avg_rating = EXCLUDED.avg_rating
            """,
            movies,
        )
    print(f"users updated: {len(users)}, movies updated: {len(movies)}")


if __name__ == "__main__":
    main()
