import threading
from datetime import datetime, timezone


class ReadModel:
    def __init__(self):
        self._orders: dict[str, dict] = {}
        self._lock = threading.Lock()

    def upsert(self, order_id: str, data: dict):
        with self._lock:
            existing = self._orders.get(order_id, {"order_id": order_id})
            existing.update(data)
            existing["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._orders[order_id] = existing

    def get(self, order_id: str) -> dict | None:
        with self._lock:
            return self._orders.get(order_id)

    def list_all(self) -> list[dict]:
        with self._lock:
            return list(self._orders.values())


read_model = ReadModel()