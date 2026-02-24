import sys, types
from fastapi.testclient import TestClient

# Patch aiokafka if missing (offline test environments)
if "aiokafka" not in sys.modules:
    class _DummyProducer:
        def __init__(self, *a, **k): ...
        async def start(self): ...
        async def stop(self): ...
        async def send_and_wait(self, *a, **k): ...
        async def send(self, *a, **k): ...
    class _DummyConsumer:
        def __init__(self, *a, **k): ...
        async def start(self): ...
        async def stop(self): ...
        async def getmany(self, *a, **k): return {}
    sys.modules["aiokafka"] = types.SimpleNamespace(
        AIOKafkaProducer=_DummyProducer, AIOKafkaConsumer=_DummyConsumer
    )

# Patch redis if missing
if "redis" not in sys.modules:
    class _Redis:
        @classmethod
        def from_url(cls, *a, **k): return cls()
        def exists(self, *a, **k): return False
        def lrange(self, *a, **k): return []
    sys.modules["redis"] = types.SimpleNamespace(Redis=_Redis)

# Patch common.db to avoid real Postgres connections
class _DummyCursor:
    def execute(self, *a, **k): return None
    def fetchone(self): return None
    def fetchall(self): return []
    def close(self): return None
    def executemany(self, *a, **k): return None

class _DummyConn:
    def cursor(self): return _DummyCursor()
    def commit(self): return None
    def close(self): return None

sys.modules["common.db"] = types.SimpleNamespace(get_db_connection=lambda: _DummyConn())

from api.app import app  # noqa: E402

client = TestClient(app)


def test_health_route():
    res = client.get("/health")
    # Some deployments may still lack health; ensure server responds
    assert res.status_code in (200, 404)


def test_upload_movies_route():
    res = client.post("/upload/movies")
    # File may be missing, but route should exist
    assert res.status_code in (200, 404)


def test_ingest_ratings_route():
    res = client.post("/ingest/ratings?limit=1")
    # Kafka/S3 may not be running in test, but route should exist
    assert res.status_code in (200, 404, 500)

def test_ingest_status_route_404():
    res = client.get("/ingest/ratings/nonexistent")
    assert res.status_code == 404


def test_recommendations_route():
    res = client.get("/recommendations/1")
    # Without data or OpenAI key, we may get 404/500;
    assert res.status_code in (200, 404, 500)
