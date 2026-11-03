import json
import uuid
import pika
import threading
from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime
from typing import List

app = FastAPI(title="Order Service")
orders_db = {}

import time

def get_rabbitmq_channel():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
            channel = connection.channel()
            channel.exchange_declare(exchange='order_events', exchange_type='fanout')
            channel.exchange_declare(exchange='payment_events', exchange_type='fanout')
            print("[ORDER] Успешно подключился к RabbitMQ")
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            print("[ORDER] Ждем RabbitMQ...")
            time.sleep(3)

class OrderItem(BaseModel):
    productId: str
    quantity: int

class OrderCreate(BaseModel):
    userId: str
    items: List[OrderItem]
    totalAmount: float

def listen_to_payments():
    connection, channel = get_rabbitmq_channel()
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='payment_events', queue=queue_name)

    def callback(ch, method, properties, body):
        event = json.loads(body)
        order_id = event["orderId"]
        
        if order_id in orders_db and orders_db[order_id]["status"] == "PENDING":
            if event["eventType"] == "PaymentCompleted":
                orders_db[order_id]["status"] = "CONFIRMED"
                print(f"[ORDER] Заказ {order_id} УСПЕШНО ОПЛАЧЕН. Статус: CONFIRMED")
            elif event["eventType"] == "PaymentFailed":
                orders_db[order_id]["status"] = "CANCELLED"
                print(f"[ORDER] ОШИБКА ОПЛАТЫ заказа {order_id}. Статус: CANCELLED (КОМПЕНСАЦИЯ)")
                
                cancel_event = {"eventType": "OrderCancelled", "orderId": order_id}
                ch.basic_publish(exchange='order_events', routing_key='', body=json.dumps(cancel_event))

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=listen_to_payments, daemon=True)
    thread.start()

@app.post("/orders")
def create_order(order: OrderCreate):
    order_id = str(uuid.uuid4())
    orders_db[order_id] = {
        "orderId": order_id,
        "userId": order.userId,
        "items": [item.dict() for item in order.items],
        "totalAmount": order.totalAmount,
        "status": "PENDING",
        "createdAt": datetime.utcnow().isoformat() + "Z"
    }
    
    connection, channel = get_rabbitmq_channel()
    event = {"eventType": "OrderCreated", **orders_db[order_id]}
    channel.basic_publish(exchange='order_events', routing_key='', body=json.dumps(event))
    connection.close()
    
    print(f"[ORDER] Создан заказ {order_id}. Отправлено событие OrderCreated.")
    return {"orderId": order_id, "status": "PENDING"}

@app.get("/orders/{order_id}")
def get_order(order_id: str):
    return orders_db.get(order_id, {"error": "Not found"})

import grpc
import pybreaker
import payment_pb2
import payment_pb2_grpc
from fastapi import HTTPException

# 1. Настраиваем Circuit Breaker
payment_breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=10)

class PaymentListener(pybreaker.CircuitBreakerListener):
    def state_change(self, cb, old_state, new_state):
        print(f"\n[!!!CIRCUIT BREAKER!!!] Состояние изменилось: {old_state.name} -> {new_state.name}\n", flush=True)

payment_breaker.add_listener(PaymentListener())

# 2. Функция вызова gRPC
@payment_breaker
def call_grpc_payment(order_id: str, amount: float, simulate_failure: bool):
    channel = grpc.insecure_channel('payment-service:50051')
    stub = payment_pb2_grpc.PaymentServiceStub(channel)
    
    request = payment_pb2.PaymentRequest(
        order_id=order_id,
        amount=amount,
        simulate_failure=simulate_failure
    )
    response = stub.CheckPaymentStatus(request, timeout=2)
    return response

# 1. Переопределяем функцию fallback, чтобы она явно писала в консоль
def payment_fallback(order_id: str, amount: float, simulate_failure: bool, exc):
    print("\n" + "="*50, flush=True)
    print(f"[FALLBACK] СРАБОТАЛА ЗАГЛУШКА! Причина: {type(exc).__name__}", flush=True)
    print("="*50 + "\n", flush=True)
    return {
        "transaction_id": "fallback-tx",
        "status": "PROCESSING",
        "message": "Платежная система временно недоступна, попробуйте позже"
    }

# 2. Эндпоинт с логированием переходов
@app.post("/check-payment")
def check_payment(req: PaymentCheckRequest):
    # Выводим текущее состояние перед запросом
    print(f"[CB STATUS] Текущее состояние: {payment_breaker.current_state}", flush=True)
    
    try:
        response = call_grpc_payment(req.order_id, req.amount, req.simulate_failure)
        return {
            "transaction_id": response.transaction_id,
            "status": response.status,
            "message": response.message
        }
    except pybreaker.CircuitBreakerError as e:
        # Это исключение летит, когда цепь разомкнута (OPEN)
        print(f"[CB ERROR] Цепь разомкнута! Ошибка: {type(e).__name__}", flush=True)
        return payment_fallback(req.order_id, req.amount, req.simulate_failure, e)
    except Exception as e:
        # Это исключение летит, когда gRPC вернул ошибку UNAVAILABLE
        print(f"[CB ERROR] Ошибка gRPC: {type(e).__name__}", flush=True)
        return payment_fallback(req.order_id, req.amount, req.simulate_failure, e)