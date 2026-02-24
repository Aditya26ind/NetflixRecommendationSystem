import os
from functools import lru_cache

import os

class Settings:
    kafka_bootstrap: str
    kafka_topic: str
    redis_url: str
    s3_raw: str
    s3_aggregates: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    openai_api_key: str
    openai_model: str
    openai_api_key: str
    openai_model: str
    
    # Database settings
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    def __init__(self):
        # Kafka
        self.kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
        self.kafka_topic = os.getenv("KAFKA_TOPIC", "user-events")

        # Redis
        self.redis_url = os.getenv("REDIS_URL", "redis://redis:6379")

        # S3
        self.s3_raw = os.getenv("S3_RAW", "s3a://datalake/raw/user_events")
        self.s3_aggregates = os.getenv("S3_AGG", "s3a://datalake/aggregates")

        # MinIO
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minio")
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minio123")
        self.minio_bucket = os.getenv("MINIO_BUCKET", "datalake")

        # OpenAI
        self.openai_api_key = os.getenv("OPENAI_API_KEY", "")
        self.openai_model = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")

        # AI
        self.openai_api_key = os.getenv("OPENAI_API_KEY", "")
        self.openai_model = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")

        # Database
        self.db_host = os.getenv("DB_HOST", "localhost")
        self.db_port = int(os.getenv("DB_PORT", "5432"))
        self.db_name = os.getenv("DB_NAME", "movie_recommendations")
        self.db_user = os.getenv("DB_USER", "postgres")
        self.db_password = os.getenv("DB_PASSWORD", "password")



@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
