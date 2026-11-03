import json
import pika
import threading
from fastapi import FastAPI

app = FastAPI(title="Notification Service")

import time

def get_rabbitmq_channel():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
            channel = connection.channel()
            channel.exchange_declare(exchange='payment_events', exchange_type='fanout')
            channel.exchange_declare(exchange='order_events', exchange_type='fanout')
            print("[NOTIFICATION] Успешно подключился к RabbitMQ")
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            print("[NOTIFICATION] Ждем RabbitMQ...")
            time.sleep(3)

def listen_to_events():
    connection, channel = get_rabbitmq_channel()
    
    result_pay = channel.queue_declare(queue='', exclusive=True)
    queue_pay = result_pay.method.queue
    channel.queue_bind(exchange='payment_events', queue=queue_pay)
    
    result_order = channel.queue_declare(queue='', exclusive=True)
    queue_order = result_order.method.queue
    channel.queue_bind(exchange='order_events', queue=queue_order)

    def callback_pay(ch, method, properties, body):
        event = json.loads(body)
        if event.get("eventType") == "PaymentCompleted":
            print(f"[NOTIFICATION] Отправлено уведомление: Заказ {event['orderId']} успешно оплачен!")

    def callback_order(ch, method, properties, body):
        event = json.loads(body)
        if event.get("eventType") == "OrderCancelled":
            print(f"[NOTIFICATION] Отправлено уведомление: Заказ {event['orderId']} отменен (ошибка оплаты).")

    channel.basic_consume(queue=queue_pay, on_message_callback=callback_pay, auto_ack=True)
    channel.basic_consume(queue=queue_order, on_message_callback=callback_order, auto_ack=True)
    
    channel.start_consuming()

@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=listen_to_events, daemon=True)
    thread.start()