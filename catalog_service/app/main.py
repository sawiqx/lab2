import os
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager

app = FastAPI(title="Catalog Service")

INSTANCE_NAME = os.getenv("INSTANCE_NAME", "unknown_instance")
PORT = int(os.getenv("PORT", 8000))
CONSUL_URL = "http://consul:8500/v1/agent/service"

products_db = {
    1: {"id": 1, "name": "Laptop", "price": 1000},
    2: {"id": 2, "name": "Smartphone", "price": 500}
}
next_id = 3

class Product(BaseModel):
    name: str
    price: float

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Регистрация в Consul при старте
    service_data = {
        "ID": f"catalog-{INSTANCE_NAME}",
        "Name": "catalog-service",
        "Address": INSTANCE_NAME, # Имя сервиса в docker network
        "Port": PORT,
        "Check": {
            "HTTP": f"http://{INSTANCE_NAME}:{PORT}/products",
            "Interval": "10s"
        }
    }
    try:
        requests.put(f"{CONSUL_URL}/register", json=service_data)
        print(f"[{INSTANCE_NAME}] Зарегистрирован в Consul")
    except Exception as e:
        print(f"[{INSTANCE_NAME}] Ошибка регистрации в Consul: {e}")
    
    yield
    
    # Дерегистрация при остановке
    try:
        requests.put(f"{CONSUL_URL}/deregister/catalog-{INSTANCE_NAME}")
        print(f"[{INSTANCE_NAME}] Удален из Consul")
    except Exception as e:
        print(f"[{INSTANCE_NAME}] Ошибка дерегистрации: {e}")

app.router.lifespan_context = lifespan

@app.get("/products")
def get_products():
    return {"instance_id": INSTANCE_NAME, "products": list(products_db.values())}

@app.post("/products")
def create_product(product: Product):
    global next_id
    new_product = {"id": next_id, "name": product.name, "price": product.price}
    products_db[next_id] = new_product
    next_id += 1
    return {"instance_id": INSTANCE_NAME, "product": new_product}

@app.put("/products/{product_id}")
def update_product(product_id: int, product: Product):
    if product_id not in products_db:
        raise HTTPException(status_code=404, detail="Product not found")
    products_db[product_id]["name"] = product.name
    products_db[product_id]["price"] = product.price
    return {"instance_id": INSTANCE_NAME, "product": products_db[product_id]}

@app.delete("/products/{product_id}")
def delete_product(product_id: int):
    if product_id not in products_db:
        raise HTTPException(status_code=404, detail="Product not found")
    del products_db[product_id]
    return {"instance_id": INSTANCE_NAME, "message": "Product deleted"}
# ����� Catalog-�������: [���� ���]
