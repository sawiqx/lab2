from read_model import read_model


def handle_event(event_dict: dict):
    order_id = event_dict["order_id"]
    event_type = event_dict["event_type"]
    payload = event_dict.get("payload", {})

    if event_type == "OrderCreated":
        read_model.upsert(order_id, {
            "status": "CREATED",
            "items": payload.get("items", []),
            "total_amount": payload.get("total_amount", 0.0),
        })
    elif event_type == "OrderPaid":
        read_model.upsert(order_id, {"status": "PAID"})
    elif event_type == "OrderCancelled":
        read_model.upsert(order_id, {"status": "CANCELLED"})