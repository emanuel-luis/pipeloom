from pipeloom import model, source


@model(
    inputs={"raw": source("raw", "raw_orders")},
    quality={"quarantine": True, "max_drop_ratio": 0.5},
    expectations=[
        {
            "name": "amount_not_negative",
            "check": {"range": {"column": "amount", "min": 0}},
            "on_violation": "drop",
        },
        {"name": "customer_set", "check": {"not_null": ["customer"]}, "on_violation": "warn"},
        {
            "name": "status_paid",
            "check": {"accepted_values": {"column": "status", "values": ["paid"]}},
            "on_violation": "warn",
        },
        {"name": "amount_small", "expr": "amount < 100", "on_violation": "warn"},
        {"name": "has_rows", "check": {"row_count": {"min": 1}}, "on_violation": "fail"},
        {"name": "id_unique", "check": {"unique": ["id"]}, "on_violation": "fail"},
    ],
)
def orders(ctx, raw):
    return raw


@model(
    inputs={"raw": source("raw", "raw_orders")},
    expectations=[
        {
            "name": "status_paid",
            "check": {"accepted_values": {"column": "status", "values": ["paid"]}},
            "on_violation": "fail",
        }
    ],
)
def orders_strict(ctx, raw):
    return raw
