import asyncio
import json
import os
from aiokafka import AIOKafkaConsumer
from projection import handle_event

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "orders-events"


async def consume_orders_events():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="read-model-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            handle_event(msg.value)
    finally:
        await consumer.stop()


def start_read_model_consumer():
    return asyncio.create_task(consume_orders_events())