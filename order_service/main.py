import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List

# Импорты из наших новых файлов (Event Sourcing + CQRS)
from event_store import event_store
from aggregate import rehydrate
from business_rules import assert_can_pay, assert_can_cancel, BusinessRuleError
from kafka_producer import start_producer, stop_producer, publish_event
from kafka_consumer import start_read_model_consumer
from read_model import read_model

# Импорты из ЛР5 (gRPC + Circuit Breaker)
import grpc
import pybreaker
import payment_pb2
import payment_pb2_grpc

# ==========================================
# БЛОК: CIRCUIT BREAKER (из ЛР5)
# ==========================================
payment_breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=10)

class PaymentListener(pybreaker.CircuitBreakerListener):
    def state_change(self, cb, old_state, new_state):
        print(f"\n[!!!CIRCUIT BREAKER!!!] Состояние изменилось: {old_state.name} -> {new_state.name}\n", flush=True)

payment_breaker.add_listener(PaymentListener())

@payment_breaker
def call_grpc_payment(order_id: str, amount: float, simulate_failure: bool):
    channel = grpc.insecure_channel('payment-service:50051')
    stub = payment_pb2_grpc.PaymentServiceStub(channel)
    request = payment_pb2.PaymentRequest(
        order_id=order_id, amount=amount, simulate_failure=simulate_failure
    )
    return stub.CheckPaymentStatus(request, timeout=2)

def payment_fallback(order_id: str, amount: float, simulate_failure: bool, exc):
    return {"status": "PROCESSING", "message": "Платежная система временно недоступна"}

# ==========================================
# МОДЕЛИ ДАННЫХ
# ==========================================
class OrderItem(BaseModel):
    name: str
    price: float
    quantity: int = 1

class OrderCreate(BaseModel):
    order_id: str | None = None
    items: List[OrderItem]
    total_amount: float | None = None

# ==========================================
# LIFESPAN (Старт/Стоп Kafka)
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    await start_producer()
    consumer_task = start_read_model_consumer() # Запуск фонового чтения Kafka для Read-модели
    yield
    consumer_task.cancel()
    await stop_producer()

app = FastAPI(title="Order Service (CQRS + ES)", lifespan=lifespan)

# ==========================================
# БЛОК 1: КОМАНДЫ (WRITE SIDE)
# ==========================================

@app.post("/orders", status_code=201)
async def create_order(req: OrderCreate):
    order_id = req.order_id or str(uuid.uuid4())
    
    # Проверяем, нет ли уже такого заказа
    if event_store.load(order_id):
        raise HTTPException(409, f"Order {order_id} already exists")

    total = req.total_amount or sum(i.price * i.quantity for i in req.items)
    
    # Формируем событие
    payload = {
        "items": [i.model_dump() for i in req.items],
        "total_amount": total,
    }
    
    # Сохраняем в Event Store (expected_version=0, так как событий еще нет)
    event = event_store.append(order_id, "OrderCreated", payload, expected_version=0)
    
    # Публикуем в Kafka
    await publish_event(event.to_dict())
    
    return {"order_id": order_id, "version": event.version}


@app.post("/orders/{order_id}/pay")
async def pay_order(order_id: str):
    events = event_store.load(order_id)
    if not events:
        raise HTTPException(404, "Order not found")

    # Восстанавливаем состояние (Rehydration)
    state = rehydrate(events)
    
    # Проверяем бизнес-правила
    try:
        assert_can_pay(state)
    except BusinessRuleError as e:
        raise HTTPException(409, str(e)) # Событие НЕ создается

    # Сохраняем событие оплаты
    event = event_store.append(
        order_id, "OrderPaid", {"amount": state.total_amount}, expected_version=state.version
    )
    await publish_event(event.to_dict())
    
    return {"order_id": order_id, "status": "PAID", "version": event.version}


@app.post("/orders/{order_id}/cancel")
async def cancel_order(order_id: str):
    events = event_store.load(order_id)
    if not events:
        raise HTTPException(404, "Order not found")

    state = rehydrate(events)
    try:
        assert_can_cancel(state)
    except BusinessRuleError as e:
        raise HTTPException(409, str(e))

    event = event_store.append(
        order_id, "OrderCancelled", {"reason": "user requested"}, expected_version=state.version
    )
    await publish_event(event.to_dict())
    
    return {"order_id": order_id, "status": "CANCELLED", "version": event.version}

# ==========================================
# БЛОК 2: ЗАПРОСЫ (READ SIDE)
# ==========================================

@app.get("/orders/{order_id}")
async def get_order(order_id: str):
    # Читаем ТОЛЬКО из read-модели!
    order = read_model.get(order_id)
    if not order:
        raise HTTPException(404, "Not yet projected (eventual consistency)")
    return order

@app.get("/orders")
async def list_orders():
    return read_model.list_all()

# Debug-эндпоинт для защиты: показать события
@app.get("/orders/{order_id}/events")
async def get_order_events(order_id: str):
    events = event_store.load(order_id)
    if not events:
        raise HTTPException(404, "Order not found")
    
    state = rehydrate(events)
    return {
        "events": [e.to_dict() for e in events],
        "rehydrated_state": {
            "order_id": state.order_id,
            "status": state.status,
            "total_amount": state.total_amount,
            "version": state.version
        },
    }