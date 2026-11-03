from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
import strawberry
from typing import List
import httpx

CATALOG_URL = "http://catalog-1:8000"

# --- Схема GraphQL ---
@strawberry.type
class Product:
    """Товар из Catalog Service"""
    id: int
    name: str
    price: float

@strawberry.type
class OrderItem:
    """Позиция заказа"""
    product: Product
    quantity: int

@strawberry.type
class Order:
    """Заказ пользователя"""
    id: int
    username: str
    items: List[OrderItem]

# --- Тестовые данные заказов ---
ORDERS_DB = {
    "alice": [
        {"id": 1, "username": "alice", "items": [{"product_id": 1, "quantity": 1}, {"product_id": 2, "quantity": 2}]},
    ],
    "bob": [
        {"id": 2, "username": "bob", "items": [{"product_id": 2, "quantity": 1}]},
    ],
}

_product_cache = {}

async def fetch_product(product_id: int) -> Product:
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{CATALOG_URL}/products")
        data = r.json()
        for p in data["products"]:
            if p["id"] == product_id:
                return Product(id=p["id"], name=p["name"], price=p["price"])
    raise ValueError("Product not found")

async def get_product_cached(product_id: int) -> Product:
    if product_id not in _product_cache:
        _product_cache[product_id] = await fetch_product(product_id)
    return _product_cache[product_id]

@strawberry.type
class Query:
    @strawberry.field(description="Заказы пользователя с товарами (агрегация Order + Catalog)")
    async def orders_by_user_id(self, username: str) -> List[Order]:
        result = []
        for o in ORDERS_DB.get(username, []):
            items = []
            for i in o["items"]:
                prod = await get_product_cached(i["product_id"])
                items.append(OrderItem(product=prod, quantity=i["quantity"]))
            result.append(Order(id=o["id"], username=o["username"], items=items))
        return result

schema = strawberry.Schema(query=Query)
app = FastAPI(
    title="GraphQL Gateway",
    description="Шлюз, агрегирующий данные Order и Catalog",
    version="1.0.0",
)
app.include_router(GraphQLRouter(schema), prefix="/graphql")