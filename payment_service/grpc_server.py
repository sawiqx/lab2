from concurrent import futures
import time
import grpc
import payment_pb2
import payment_pb2_grpc
import random

# Класс-реализация сервиса, унаследованный от сгенерированного интерфейса
class PaymentServiceServicer(payment_pb2_grpc.PaymentServiceServicer):
    
    # Метод, который вызывается gRPC-клиентом (Order Service)
    def CheckPaymentStatus(self, request, context):
        print(f"[GRPC PAYMENT] Получен запрос для заказа {request.order_id}, сумма: {request.amount}")

        # Если клиент просит имитировать сбой (флаг simulate_failure = true)
        if request.simulate_failure:
            print("[GRPC PAYMENT] Имитация сбоя! Возвращаем UNAVAILABLE")
            # Устанавливаем код ошибки gRPC (аналог HTTP 503)
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("Payment service is temporarily unavailable")
            return payment_pb2.PaymentResponse() # Возвращаем пустой ответ (ошибка передается через context)

        # Имитация задержки банка (чтобы сработал таймаут, если нужно)
        time.sleep(1)

        # Успешный ответ
        return payment_pb2.PaymentResponse(
            transaction_id=f"txn_{request.order_id[:8]}", # Генерируем ID транзакции из ID заказа
            status="SUCCESS",
            message="Payment processed successfully"
        )

def serve():
    # Создаем gRPC-сервер с пулом потоков (до 10 одновременных запросов)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Регистрируем наш класс PaymentServiceServicer на сервере
    payment_pb2_grpc.add_PaymentServiceServicer_to_server(PaymentServiceServicer(), server)
    
    # Открываем незащищенный порт 50051 для приема подключений
    server.add_insecure_port('[::]:50051') # gRPC работает на порту 50051
    print("[GRPC PAYMENT] Сервер запущен на порту 50051")
    
    # Запускаем сервер и блокируем выполнение (ждем завершения)
    server.start()
    server.wait_for_termination()

# Точка входа в скрипт
if __name__ == '__main__':
    serve()