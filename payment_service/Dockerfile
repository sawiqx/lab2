FROM python:3.11-slim
WORKDIR /app
COPY payment_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY payment_service/ .
COPY proto/payment.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. payment.proto
CMD ["python", "grpc_server.py"]