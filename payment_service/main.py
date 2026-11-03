import json
import pika
import threading
import time
import random
from fastapi import FastAPI

app = FastAPI(title="Payment Service")

import time

def get_rabbitmq_channel():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
            channel = connection.channel()
            channel.exchange_declare(exchange='order_events', exchange_type='fanout')
            channel.exchange_declare(exchange='payment_events', exchange_type='fanout')
            print("[PAYMENT] Успешно подключился к RabbitMQ")
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            print("[PAYMENT] Ждем RabbitMQ...")
            time.sleep(3)

def listen_to_orders():
    connection, channel = get_rabbitmq_channel()
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='order_events', queue=queue_name)

    def callback(ch, method, properties, body):
        event = json.loads(body)
        
        if event.get("eventType") == "OrderCreated":
            order_id = event["orderId"]
            amount = event["totalAmount"]
            print(f"[PAYMENT] Получен заказ {order_id}. Сумма: {amount}")
            
            time.sleep(2) # Имитация запроса в банк
            
            if amount < 0 or random.random() < 0.3:
                print(f"[PAYMENT] ОШИБКА ОПЛАТЫ для {order_id}")
                response_event = {
                    "eventType": "PaymentFailed",
                    "orderId": order_id,
                    "reason": "Insufficient funds"
                }
            else:
                print(f"[PAYMENT] ОПЛАТА УСПЕШНА для {order_id}")
                response_event = {
                    "eventType": "PaymentCompleted",
                    "orderId": order_id,
                    "transactionId": f"txn_{order_id[:8]}"
                }
            
            ch.basic_publish(exchange='payment_events', routing_key='', body=json.dumps(response_event))

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=listen_to_orders, daemon=True)
    thread.start()