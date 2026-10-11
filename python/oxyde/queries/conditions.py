"""Condition class representing a single WHERE clause comparison.

Condition is the leaf node of the filter tree. It represents a single
comparison like "age >= 18" or "name LIKE '%alice%'".

Attributes:
    field: Python field name (e.g., "age")
    operator: SQL operator ("=", ">", "LIKE", "IN", "IS NULL", etc.)
    value: Comparison value (can be None for IS NULL)
    column: Database column name (may differ from field if db_column set)
    escape: Optional LIKE escape character for pattern filters
    db_type: The field's db_type override (array and JSON fields; PostgreSQL casts to it)

Operators:
    "="        → Equality
    ">", ">="  → Greater than
    "<", "<="  → Less than
    "LIKE"     → Pattern match (case-sensitive)
    "ILIKE"    → Pattern match (case-insensitive, PostgreSQL)
    "IN"       → Value in list
    "BETWEEN"  → Range (value is [low, high])
    "IS NULL"  → NULL check
    "IS NOT NULL" → NOT NULL check
    "ARRAY_CONTAINS"     → Array holds every given element
    "ARRAY_CONTAINED_BY" → Every stored element is among the given ones
    "ARRAY_OVERLAP"      → At least one element in common

Creation:
    Conditions are typically created via Field comparison operators:
        User.name == "Alice"  → Condition("name", "=", "Alice")
        User.age >= 18        → Condition("age", ">=", 18)

    Or via lookups module:
        _build_lookup_conditions(User, "name", "contains", "alice", meta, spec)
        → Condition("name", "LIKE", "%alice%")

IR Format:
    to_ir() returns dict for Rust codec. For a LIKE condition with an
    escape character:
    {
        "type": "condition",
        "field": "name",
        "op": "LIKE",
        "value": "A\\_%",
        "column": "name",
        "escape": "\\"
    }
"""

from __future__ import annotations

from typing import Any

from oxyde.core import ir
from oxyde.core.ir import FilterNode
from oxyde.core.types import serialize_value


class Condition:
    """Represents a filter condition."""

    def __init__(
        self,
        field: str,
        operator: str,
        value: Any,
        *,
        column: str | None = None,
        escape: str | None = None,
        db_type: str | None = None,
    ):
        self.field = field
        self.operator = operator
        self.value = value
        self.column = column or field
        self.escape = escape
        self.db_type = db_type

    def to_ir(self) -> FilterNode:
        """Convert to IR format expected by the Rust codec.

        Values are serialized via TYPE_REGISTRY so that date, UUID, Decimal
        etc. are converted to msgpack-safe representations before hitting
        the IR dict.
        """
        return ir.filter_condition(
            field=self.field,
            operator=self.operator,
            value=serialize_value(self.value),
            column=self.column,
            escape=self.escape,
            db_type=self.db_type,
        )


__all__ = ["Condition"]
