import threading
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


@dataclass
class Event:
    event_id: str
    order_id: str
    event_type: str          # OrderCreated | OrderPaid | OrderCancelled
    payload: dict[str, Any]
    timestamp: str
    version: int

    def to_dict(self) -> dict:
        return asdict(self)


class EventStore:
    def __init__(self):
        self._data: dict[str, list[Event]] = {}
        self._lock = threading.Lock()

    def load(self, order_id: str) -> list[Event]:
        with self._lock:
            return list(self._data.get(order_id, []))

    def append(self, order_id: str, event_type: str, payload: dict,
               expected_version: int) -> Event:
        with self._lock:
            events = self._data.setdefault(order_id, [])
            current_version = events[-1].version if events else 0

            if current_version != expected_version:
                raise ValueError(
                    f"Concurrency conflict: expected {expected_version}, got {current_version}"
                )

            event = Event(
                event_id=str(uuid4()),
                order_id=order_id,
                event_type=event_type,
                payload=payload,
                timestamp=datetime.now(timezone.utc).isoformat(),
                version=current_version + 1,
            )
            events.append(event)
            return event


event_store = EventStore()