import json
import uuid
import pika
import threading
from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime
from typing import List
import time

# Инициализация FastAPI приложения и временной базы данных в памяти
app = FastAPI(title="Order Service")
orders_db = {}

# ==========================================
# БЛОК 1: ПОДКЛЮЧЕНИЕ К RABBITMQ (4-я лаба)
# ==========================================
def get_rabbitmq_channel():
    """Функция для подключения к брокеру сообщений RabbitMQ с повторными попытками."""
    while True:
        try:
            # Подключаемся к контейнеру rabbitmq
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
            channel = connection.channel()
            
            # Объявляем точки обмена (Exchanges) типа fanout (веерная рассылка)
            channel.exchange_declare(exchange='order_events', exchange_type='fanout')
            channel.exchange_declare(exchange='payment_events', exchange_type='fanout')
            print("[ORDER] Успешно подключился к RabbitMQ")
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            # Если RabbitMQ еще не проснулся, ждем 3 секунды и пробуем снова
            print("[ORDER] Ждем RabbitMQ...")
            time.sleep(3)

# Модели данных для валидации входящих запросов (Pydantic)
class OrderItem(BaseModel):
    productId: str
    quantity: int

class OrderCreate(BaseModel):
    userId: str
    items: List[OrderItem]
    totalAmount: float

# ==========================================
# БЛОК 2: АСИНХРОННЫЙ СЛУШАТЕЛЬ (ХОРЕОГРАФИЯ)
# ==========================================
def listen_to_payments():
    """Фоновая задача: слушает ответы от Payment Service и реагирует на них."""
    connection, channel = get_rabbitmq_channel()
    # Создаем временную очередь для этого сервиса
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    # Подписываем очередь на события от сервиса оплаты
    channel.queue_bind(exchange='payment_events', queue=queue_name)

    def callback(ch, method, properties, body):
        """Вызывается, когда в очереди появляется новое сообщение."""
        event = json.loads(body)
        order_id = event["orderId"]
        
        # Проверяем, что заказ существует и находится в статусе ожидания (идемпотентность)
        if order_id in orders_db and orders_db[order_id]["status"] == "PENDING":
            if event["eventType"] == "PaymentCompleted":
                # Оплата прошла успешно, подтверждаем заказ
                orders_db[order_id]["status"] = "CONFIRMED"
                print(f"[ORDER] Заказ {order_id} УСПЕШНО ОПЛАЧЕН. Статус: CONFIRMED")
                
            elif event["eventType"] == "PaymentFailed":
                # ❗ ЛОГИКА КОМПЕНСАЦИИ ❗
                # Оплата не прошла, отменяем заказ и отправляем событие для уведомления
                orders_db[order_id]["status"] = "CANCELLED"
                print(f"[ORDER] ОШИБКА ОПЛАТЫ заказа {order_id}. Статус: CANCELLED (КОМПЕНСАЦИЯ)")
                
                cancel_event = {"eventType": "OrderCancelled", "orderId": order_id}
                ch.basic_publish(exchange='order_events', routing_key='', body=json.dumps(cancel_event))

    # Запускаем бесконечный цикл прослушивания очереди
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

@app.on_event("startup")
def startup_event():
    """При старте приложения запускаем слушателя RabbitMQ в отдельном потоке."""
    thread = threading.Thread(target=listen_to_payments, daemon=True)
    thread.start()

# ==========================================
# БЛОК 3: СОЗДАНИЕ ЗАКАЗА (REST API)
# ==========================================
@app.post("/orders")
def create_order(order: OrderCreate):
    """Эндпоинт для создания нового заказа."""
    order_id = str(uuid.uuid4())
    
    # Сохраняем заказ в БД со статусом PENDING (в ожидании)
    orders_db[order_id] = {
        "orderId": order_id,
        "userId": order.userId,
        "items": [item.dict() for item in order.items],
        "totalAmount": order.totalAmount,
        "status": "PENDING",
        "createdAt": datetime.utcnow().isoformat() + "Z"
    }
    
    # Публикуем событие в брокер: "Я создал заказ!"
    connection, channel = get_rabbitmq_channel()
    event = {"eventType": "OrderCreated", **orders_db[order_id]}
    channel.basic_publish(exchange='order_events', routing_key='', body=json.dumps(event))
    connection.close()
    
    print(f"[ORDER] Создан заказ {order_id}. Отправлено событие OrderCreated.")
    return {"orderId": order_id, "status": "PENDING"}

@app.get("/orders/{order_id}")
def get_order(order_id: str):
    """Эндпоинт для проверки статуса заказа."""
    return orders_db.get(order_id, {"error": "Not found"})


# ==========================================
# БЛОК 4: gRPC КЛИЕНТ И CIRCUIT BREAKER (5-я лаба)
# ==========================================
import grpc
import pybreaker
import payment_pb2
import payment_pb2_grpc

# Настраиваем Circuit Breaker: 
# fail_max=3 (размыкаем после 3 ошибок), reset_timeout=10 (ждем 10 сек до полуоткрытия)
payment_breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=10)

class PaymentListener(pybreaker.CircuitBreakerListener):
    """Слушатель для вывода смены состояний CB в консоль."""
    def state_change(self, cb, old_state, new_state):
        print(f"\n[!!!CIRCUIT BREAKER!!!] Состояние изменилось: {old_state.name} -> {new_state.name}\n", flush=True)

payment_breaker.add_listener(PaymentListener())

@payment_breaker
def call_grpc_payment(order_id: str, amount: float, simulate_failure: bool):
    """Функция вызова gRPC-сервера, обернутая в Circuit Breaker."""
    channel = grpc.insecure_channel('payment-service:50051')
    stub = payment_pb2_grpc.PaymentServiceStub(channel)
    
    request = payment_pb2.PaymentRequest(
        order_id=order_id,
        amount=amount,
        simulate_failure=simulate_failure
    )
    # Ждем ответ не дольше 2 секунд (таймаут)
    response = stub.CheckPaymentStatus(request, timeout=2)
    return response

def payment_fallback(order_id: str, amount: float, simulate_failure: bool, exc):
    """Fallback-функция. Вызывается, если CB разомкнут или gRPC вернул ошибку."""
    print("\n" + "="*50, flush=True)
    print(f"[FALLBACK] СРАБОТАЛА ЗАГЛУШКА! Причина: {type(exc).__name__}", flush=True)
    print("="*50 + "\n", flush=True)
    return {
        "transaction_id": "fallback-tx",
        "status": "PROCESSING",
        "message": "Платежная система временно недоступна, попробуйте позже"
    }

# Модель для проверки оплаты
from pydantic import BaseModel
class PaymentCheckRequest(BaseModel):
    order_id: str
    amount: float
    simulate_failure: bool = False

@app.post("/check-payment")
def check_payment(req: PaymentCheckRequest):
    """Эндпоинт для ручной проверки оплаты через gRPC."""
    print(f"[CB STATUS] Текущее состояние: {payment_breaker.current_state}", flush=True)
    
    try:
        response = call_grpc_payment(req.order_id, req.amount, req.simulate_failure)
        return {
            "transaction_id": response.transaction_id,
            "status": response.status,
            "message": response.message
        }
    except pybreaker.CircuitBreakerError as e:
        # Цепь разомкнута (состояние OPEN)
        print(f"[CB ERROR] Цепь разомкнута! Ошибка: {type(e).__name__}", flush=True)
        return payment_fallback(req.order_id, req.amount, req.simulate_failure, e)
    except Exception as e:
        # gRPC вернул ошибку UNAVAILABLE (сервис упал)
        print(f"[CB ERROR] Ошибка gRPC: {type(e).__name__}", flush=True)
        return payment_fallback(req.order_id, req.amount, req.simulate_failure, e)