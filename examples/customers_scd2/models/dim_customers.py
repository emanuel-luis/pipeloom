"""A customer dimension with history: one row per version of a customer."""

# --8<-- [start:model]
from pipeloom import model


@model(
    write={
        "strategy": "scd2",
        "unique_key": ["id"],
        "tracked_columns": ["name", "city", "tier"],
        "deletes": {"column": "status", "value": "closed", "mode": "hard"},
    },
)
def dim_customers(customers):
    return customers


# --8<-- [end:model]
