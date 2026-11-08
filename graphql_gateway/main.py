from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
import strawberry
from typing import List
import httpx

# Адрес REST API нашего сервиса Каталога (Catalog Service)
CATALOG_URL = "http://catalog-1:8000"

# ==========================================
# БЛОК 1: ОПИСАНИЕ СХЕМЫ GRAPHQL (3-я лаба)
# ==========================================

@strawberry.type
class Product:
    """Товар из Catalog Service"""
    id: int
    name: str
    price: float

@strawberry.type
class OrderItem:
    """Позиция заказа"""
    product: Product  # Вложенный объект: данные о товаре подтягиваются из REST Catalog
    quantity: int

@strawberry.type
class Order:
    """Заказ пользователя"""
    id: int
    username: str
    items: List[OrderItem]  # Список позиций в заказе

# ==========================================
# БЛОК 2: ТЕСТОВЫЕ ДАННЫЕ И КЭШИРОВАНИЕ (N+1)
# ==========================================

# Имитация базы данных заказов (в реальности здесь был бы вызов Order Service)
ORDERS_DB = {
    "alice": [
        {"id": 1, "username": "alice", "items": [{"product_id": 1, "quantity": 1}, {"product_id": 2, "quantity": 2}]},
    ],
    "bob": [
        {"id": 2, "username": "bob", "items": [{"product_id": 2, "quantity": 1}]},
    ],
}

# Словарь для кэширования товаров. Это решает проблему N+1 
# (чтобы не делать запрос в Catalog для одного и того же товара несколько раз).
_product_cache = {}

async def fetch_product(product_id: int) -> Product:
    """Делает HTTP-запрос к REST API сервиса Catalog, чтобы получить данные о товаре."""
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{CATALOG_URL}/products")
        data = r.json()
        # Ищем нужный товар в полученном списке
        for p in data["products"]:
            if p["id"] == product_id:
                return Product(id=p["id"], name=p["name"], price=p["price"])
    raise ValueError("Product not found")

async def get_product_cached(product_id: int) -> Product:
    """Проверяет, есть ли товар в кэше. Если нет — запрашивает и сохраняет."""
    if product_id not in _product_cache:
        _product_cache[product_id] = await fetch_product(product_id)
    return _product_cache[product_id]

# ==========================================
# БЛОК 3: РЕЗОЛВЕР (АГРЕГАЦИЯ ДАННЫХ)
# ==========================================

@strawberry.type
class Query:
    @strawberry.field(description="Заказы пользователя с товарами (агрегация Order + Catalog)")
    async def orders_by_user_id(self, username: str) -> List[Order]:
        """Главный резолвер: собирает заказы и подтягивает к ним данные о товарах."""
        result = []
        # Берем заказы пользователя из базы
        for o in ORDERS_DB.get(username, []):
            items = []
            # Для каждого товара в заказе идем в REST Catalog (через функцию с кэшем)
            for i in o["items"]:
                prod = await get_product_cached(i["product_id"])
                items.append(OrderItem(product=prod, quantity=i["quantity"]))
            # Собираем заказ с вложенными товарами
            result.append(Order(id=o["id"], username=o["username"], items=items))
        return result

# ==========================================
# БЛОК 4: ЗАПУСК СЕРВЕРА GRAPHQL
# ==========================================

schema = strawberry.Schema(query=Query)

app = FastAPI(
    title="GraphQL Gateway",
    description="Шлюз, агрегирующий данные Order и Catalog",
    version="1.0.0",
)
# Подключаем GraphQL по адресу /graphql (там будет доступен Playground)
app.include_router(GraphQLRouter(schema), prefix="/graphql")