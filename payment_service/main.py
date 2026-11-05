import json
import pika
import threading
import time
import random
from fastapi import FastAPI

app = FastAPI(title="Payment Service")

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
            
            # Объявляем те же самые точки обмена (Exchanges), что и в Order Service
            channel.exchange_declare(exchange='order_events', exchange_type='fanout')
            channel.exchange_declare(exchange='payment_events', exchange_type='fanout')
            print("[PAYMENT] Успешно подключился к RabbitMQ")
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            # Если RabbitMQ еще не проснулся, ждем 3 секунды и пробуем снова
            print("[PAYMENT] Ждем RabbitMQ...")
            time.sleep(3)

# ==========================================
# БЛОК 2: АСИНХРОННЫЙ СЛУШАТЕЛЬ (ХОРЕОГРАФИЯ)
# ==========================================
def listen_to_orders():
    """Фоновая задача: слушает события от сервиса Заказов (Order)."""
    connection, channel = get_rabbitmq_channel()
    
    # Создаем временную очередь для этого сервиса
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    
    # Подписываем очередь на события от сервиса Заказов
    channel.queue_bind(exchange='order_events', queue=queue_name)

    def callback(ch, method, properties, body):
        """Вызывается, когда в очереди появляется новое сообщение (заказ)."""
        event = json.loads(body)
        
        # Реагируем только на событие создания заказа
        if event.get("eventType") == "OrderCreated":
            order_id = event["orderId"]
            amount = event["totalAmount"]
            print(f"[PAYMENT] Получен заказ {order_id}. Сумма: {amount}")
            
            # Имитация задержки (поход в банк за авторизацией платежа)
            time.sleep(2) 
            
            # Логика проверки оплаты: 
            # Если сумма отрицательная ИЛИ случайное число < 0.3 (30% шанс ошибки)
            if amount < 0 or random.random() < 0.3:
                print(f"[PAYMENT] ОШИБКА ОПЛАТЫ для {order_id}")
                # Формируем событие об ошибке
                response_event = {
                    "eventType": "PaymentFailed",
                    "orderId": order_id,
                    "reason": "Insufficient funds"
                }
            else:
                print(f"[PAYMENT] ОПЛАТА УСПЕШНА для {order_id}")
                # Формируем событие об успехе
                response_event = {
                    "eventType": "PaymentCompleted",
                    "orderId": order_id,
                    "transactionId": f"txn_{order_id[:8]}"
                }
            
            # Отправляем результат оплаты обратно в брокер (Order и Notification его прочитают)
            ch.basic_publish(exchange='payment_events', routing_key='', body=json.dumps(response_event))

    # Запускаем бесконечный цикл прослушивания очереди
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

@app.on_event("startup")
def startup_event():
    """При старте приложения запускаем слушателя RabbitMQ в отдельном потоке."""
    thread = threading.Thread(target=listen_to_orders, daemon=True)
    thread.start()