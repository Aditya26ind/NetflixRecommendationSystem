# """CSV → Kafka producer for local testing."""
# import asyncio
# import csv
# import json
# import uuid
# from aiokafka import AIOKafkaProducer
# from .config import KAFKA_BOOTSTRAP, TOPIC_EVENTS

# CSV_PATH = "data/raw/kaggle_events.csv"
# async def send_rows():
#     producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
#     await producer.start()
#     try:
#         with open(CSV_PATH, newline="") as f:
#             reader = csv.DictReader(f)
#             for row in reader:
#                 if "event_id" not in row or not row["event_id"]:
#                     row["event_id"] = str(uuid.uuid4())
#                 payload = json.dumps(row).encode()
#                 await producer.send_and_wait(TOPIC_EVENTS, payload)
#                 print(f"Sent {row['event_id']}")
#     finally:
#         await producer.stop()


# if __name__ == "__main__":
#     asyncio.run(send_rows())
