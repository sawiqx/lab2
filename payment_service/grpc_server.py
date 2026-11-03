from concurrent import futures
import time
import grpc
import payment_pb2
import payment_pb2_grpc
import random

class PaymentServiceServicer(payment_pb2_grpc.PaymentServiceServicer):
    def CheckPaymentStatus(self, request, context):
        print(f"[GRPC PAYMENT] Получен запрос для заказа {request.order_id}, сумма: {request.amount}")

        # Если клиент просит имитировать сбой
        if request.simulate_failure:
            print("[GRPC PAYMENT] Имитация сбоя! Возвращаем UNAVAILABLE")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("Payment service is temporarily unavailable")
            return payment_pb2.PaymentResponse()

        # Имитация задержки банка (чтобы сработал таймаут, если нужно)
        time.sleep(1)

        # Успешный ответ
        return payment_pb2.PaymentResponse(
            transaction_id=f"txn_{request.order_id[:8]}",
            status="SUCCESS",
            message="Payment processed successfully"
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    payment_pb2_grpc.add_PaymentServiceServicer_to_server(PaymentServiceServicer(), server)
    server.add_insecure_port('[::]:50051') # gRPC работает на порту 50051
    print("[GRPC PAYMENT] Сервер запущен на порту 50051")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()