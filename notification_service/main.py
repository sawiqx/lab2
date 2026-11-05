import json
import pika
import threading
from fastapi import FastAPI

app = FastAPI(title="Notification Service")

# ==========================================
# БЛОК 1: ПОДКЛЮЧЕНИЕ К RABBITMQ (4-я лаба)
# ==========================================
import time

def get_rabbitmq_channel():
    """Функция для подключения к брокеру сообщений RabbitMQ с повторными попытками."""
    while True:
        try:
            # Подключаемся к контейнеру rabbitmq
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
            channel = connection.channel()
            
            # Объявляем те же точки обмена (Exchanges), что и в других сервисах
            channel.exchange_declare(exchange='payment_events', exchange_type='fanout')
            channel.exchange_declare(exchange='order_events', exchange_type='fanout')
            print("[NOTIFICATION] Успешно подключился к RabbitMQ")
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            # Если RabbitMQ еще не проснулся, ждем 3 секунды и пробуем снова
            print("[NOTIFICATION] Ждем RabbitMQ...")
            time.sleep(3)

# ==========================================
# БЛОК 2: АСИНХРОННЫЙ СЛУШАТЕЛЬ ДВУХ ОЧЕРЕДЕЙ
# ==========================================
def listen_to_events():
    """Фоновая задача: слушает события от сервиса Заказов и сервиса Оплаты."""
    connection, channel = get_rabbitmq_channel()
    
    # 1. Создаем временную очередь для событий ОПЛАТЫ
    result_pay = channel.queue_declare(queue='', exclusive=True)
    queue_pay = result_pay.method.queue
    channel.queue_bind(exchange='payment_events', queue=queue_pay)
    
    # 2. Создаем временную очередь для событий ЗАКАЗОВ
    result_order = channel.queue_declare(queue='', exclusive=True)
    queue_order = result_order.method.queue
    channel.queue_bind(exchange='order_events', queue=queue_order)

    def callback_pay(ch, method, properties, body):
        """Обработчик событий от сервиса Оплаты (Payment)."""
        event = json.loads(body)
        # Нас интересует только успешная оплата, чтобы отправить уведомление клиенту
        if event.get("eventType") == "PaymentCompleted":
            print(f"[NOTIFICATION] Отправлено уведомление: Заказ {event['orderId']} успешно оплачен!")

    def callback_order(ch, method, properties, body):
        """Обработчик событий от сервиса Заказов (Order)."""
        event = json.loads(body)
        # Нас интересует событие отмены заказа (компенсация), чтобы уведомить клиента
        if event.get("eventType") == "OrderCancelled":
            print(f"[NOTIFICATION] Отправлено уведомление: Заказ {event['orderId']} отменен (ошибка оплаты).")

    # Подписываем каждый обработчик на свою очередь
    channel.basic_consume(queue=queue_pay, on_message_callback=callback_pay, auto_ack=True)
    channel.basic_consume(queue=queue_order, on_message_callback=callback_order, auto_ack=True)
    
    # Запускаем прослушивание сразу всех очередей
    channel.start_consuming()

@app.on_event("startup")
def startup_event():
    """При старте приложения запускаем слушателя RabbitMQ в отдельном потоке."""
    thread = threading.Thread(target=listen_to_events, daemon=True)
    thread.start()