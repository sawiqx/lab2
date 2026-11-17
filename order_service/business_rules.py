from aggregate import OrderState


class BusinessRuleError(Exception):
    pass


def assert_can_pay(state: OrderState):
    if state.status == "PAID":
        raise BusinessRuleError("Order is already paid")
    if state.status == "CANCELLED":
        raise BusinessRuleError("Cannot pay a cancelled order")


def assert_can_cancel(state: OrderState):
    if state.status == "CANCELLED":
        raise BusinessRuleError("Order is already cancelled")
    if state.status == "PAID":
        raise BusinessRuleError("Cannot cancel a paid order")
    