FROM python:3.11-slim
WORKDIR /app
COPY order_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY order_service/ .
COPY proto/payment.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. payment.proto
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]