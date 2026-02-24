import json
from typing import Any

# converts Python dicts to JSON bytes and back, for Kafka payloads
def to_json_bytes(payload: Any) -> bytes:
    return json.dumps(payload).encode("utf-8")

# converts JSON bytes back to Python dicts
def from_json_bytes(data: bytes):
    return json.loads(data.decode("utf-8"))
