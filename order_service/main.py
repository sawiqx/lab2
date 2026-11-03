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
