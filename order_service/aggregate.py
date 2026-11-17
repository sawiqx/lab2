from dataclasses import dataclass, field
from event_store import Event


@dataclass
class OrderState:
    order_id: str
    status: str = "NEW"          # NEW -> CREATED -> PAID / CANCELLED
    items: list[dict] = field(default_factory=list)
    total_amount: float = 0.0
    version: int = 0


def apply(state: OrderState, event: Event) -> OrderState:
    if event.event_type == "OrderCreated":
        state.status = "CREATED"
        state.items = event.payload.get("items", [])
        state.total_amount = event.payload.get("total_amount", 0.0)
    elif event.event_type == "OrderPaid":
        state.status = "PAID"
    elif event.event_type == "OrderCancelled":
        state.status = "CANCELLED"
    else:
        raise ValueError(f"Unknown event type: {event.event_type}")

    state.version = event.version
    return state


def rehydrate(events: list[Event]) -> OrderState:
    if not events:
        raise LookupError("Order not found (no events)")

    state = OrderState(order_id=events[0].order_id)
    for event in events:
        state = apply(state, event)
    return state