import datetime
import sys
import types

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

# Stub aiokafka early so importing event_api doesn't require the real package
if "aiokafka" not in sys.modules:
    sys.modules["aiokafka"] = types.SimpleNamespace(
        AIOKafkaProducer=object, AIOKafkaConsumer=object
    )

if "redis" not in sys.modules:
    sys.modules["redis"] = types.SimpleNamespace(Redis=object)

import api.event_api as event_api
import api.recommendation_api as recommendation_api


# -------- Event API tests --------

def _make_event_app(monkeypatch):
    async def fake_process(job_id: str, csv_row_number: int = 0):
        # Simulate successful ingestion
        event_api.ingest_status[job_id] = {
            "status": "success",
            "sent": 1,
            "s3_key": "raw/rating.csv",
            "error": None,
        }

    monkeypatch.setattr(event_api, "_process_ratings", fake_process)

    app = FastAPI()
    app.include_router(event_api.router)
    return app


def test_event_ingest_returns_job_and_updates_status(monkeypatch):
    app = _make_event_app(monkeypatch)
    client = TestClient(app)

    resp = client.post("/event/rating", params={"csv_row_number": 1})

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "queued"
    job_id = body["job_id"]

    # Manually invoke the fake processor to simulate background completion
    import anyio

    anyio.run(event_api._process_ratings, job_id, 1)

    assert event_api.ingest_status[job_id]["status"] == "success"


# -------- Recommendation API tests --------


class FakeCursor:
    def __init__(self):
        self.last_query = ""

    def execute(self, query, params=None):
        self.last_query = query
        self.params = params

    def fetchone(self):
        # Return top_movies plus a timestamp-like object
        return ([10, 11], datetime.datetime(2025, 1, 1, 0, 0, 0))

    def fetchall(self):
        if "WHERE movie_id = ANY" in self.last_query:
            return [("Action|Comedy",), ("Drama",)]
        return [
            (10, "Movie A", "Action|Comedy"),
            (11, "Movie B", "Drama"),
            (12, "Movie C", "Action"),
            (13, "Movie D", "Comedy"),
            (14, "Movie E", "Drama"),
        ]

    def close(self):
        pass


class FakeDB:
    def cursor(self):
        return FakeCursor()

    def close(self):
        pass


class FakeRedis:
    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value):
        self.store[key] = value


def _make_reco_app(monkeypatch):
    monkeypatch.setattr(recommendation_api, "get_db_connection", lambda: FakeDB())
    monkeypatch.setattr(recommendation_api, "get_redis_client", lambda: FakeRedis())
    monkeypatch.setattr(recommendation_api, "client", None)  # force deterministic path

    app = FastAPI()
    app.include_router(recommendation_api.router)
    return app


def test_get_recommendations_returns_five_movies(monkeypatch):
    app = _make_reco_app(monkeypatch)
    client = TestClient(app)

    resp = client.get("/recommendations/1")

    assert resp.status_code == 200
    body = resp.json()
    assert body["user_id"] == 1
    assert len(body["recommendations"]) == 5
    assert body["based_on_genres"]
