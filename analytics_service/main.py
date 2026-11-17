import asyncio
import json
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "orders-events"

# Хранилище статистики
stats = {
    "total_orders": 0,
    "paid_sum": 0.0,
    "count_by_status": {"CREATED": 0, "PAID": 0, "CANCELLED": 0},
    "avg_order_amount": 0.0,
}

# Для идемпотентности и точного подсчета
_seen_event_ids = set()
_order_status = {}
_total_amount_sum = 0.0

def apply_event(ev: dict):
    if ev["event_id"] in _seen_event_ids:
        return
    _seen_event_ids.add(ev["event_id"])

    et = ev["event_type"]
    payload = ev.get("payload", {})
    oid = ev["order_id"]

    if et == "OrderCreated":
        _order_status[oid] = "CREATED"
        _total_amount_sum += payload.get("total_amount", 0.0)
    elif et == "OrderPaid":
        _order_status[oid] = "PAID"
        stats["paid_sum"] += payload.get("amount", 0.0)
    elif et == "OrderCancelled":
        _order_status[oid] = "CANCELLED"

    # Пересчитываем статистику на лету
    stats["total_orders"] = len(_order_status)
    stats["count_by_status"] = {"CREATED": 0, "PAID": 0, "CANCELLED": 0}
    for s in _order_status.values():
        stats["count_by_status"][s] += 1
    
    if _order_status:
        stats["avg_order_amount"] = round(_total_amount_sum / len(_order_status), 2)

async def consume():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="analytics-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            apply_event(msg.value)
    finally:
        await consumer.stop()

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume())
    yield
    task.cancel()

app = FastAPI(title="Analytics Service", lifespan=lifespan)

@app.get("/analytics/summary")
async def summary():
    return stats