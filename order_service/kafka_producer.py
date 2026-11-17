import json
import os
from aiokafka import AIOKafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "orders-events"

_producer: AIOKafkaProducer | None = None


async def start_producer():
    global _producer
    _producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await _producer.start()


async def stop_producer():
    if _producer:
        await _producer.stop()


async def publish_event(event_dict: dict):
    await _producer.send_and_wait(
        TOPIC,
        key=event_dict["order_id"],
        value=event_dict,
    )