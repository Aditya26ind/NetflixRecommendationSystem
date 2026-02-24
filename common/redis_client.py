import redis
from functools import lru_cache
from .config import get_settings

# get redis client
@lru_cache(maxsize=1)
def get_redis_client():
    settings = get_settings()
    return redis.Redis.from_url(settings.redis_url)
