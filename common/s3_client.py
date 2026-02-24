import boto3
from functools import lru_cache
from .config import get_settings


@lru_cache(maxsize=1)
def get_s3_client():
    settings = get_settings()
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
    )

# s3 bucket management
def ensure_bucket(client=None):
    client = client or get_s3_client()
    settings = get_settings()
    buckets = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    if settings.minio_bucket not in buckets:
        client.create_bucket(Bucket=settings.minio_bucket)
    return settings.minio_bucket
