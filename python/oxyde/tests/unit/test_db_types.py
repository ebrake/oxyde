"""Test that col_types IR hints are correctly computed for all field configurations.

Validates _compute_col_types() output — the mapping sent to Rust for type-aware
binding and reading. One model per category, parametrized cases.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Annotated
from uuid import UUID

import pytest

from oxyde import Field, Model


# ── Model with all type variations ────────────────────────────────────


class DbTypesModel(Model):
    id: int | None = Field(default=None, db_pk=True)

    # Inferred from python_type (no db_type)
    infer_str: str = Field(default="")
    infer_int: int = Field(default=0)
    infer_float: float = Field(default=0.0)
    infer_bool: bool = Field(default=True)
    infer_bytes: bytes | None = Field(default=None, db_nullable=True)
    infer_datetime: datetime | None = Field(default=None, db_nullable=True)
    infer_date: date | None = Field(default=None, db_nullable=True)
    infer_time: time | None = Field(default=None, db_nullable=True)
    infer_timedelta: timedelta | None = Field(default=None, db_nullable=True)
    infer_uuid: UUID | None = Field(default=None, db_nullable=True)
    infer_decimal: Decimal | None = Field(default=None, db_nullable=True)
    infer_json: dict | None = Field(default=None, db_nullable=True)

    # Explicit db_type (scalar)
    db_uuid: str = Field(default="", db_type="UUID")
    db_jsonb: str = Field(default="", db_type="JSONB")
    db_json: str = Field(default="", db_type="JSON")
    db_varchar: str = Field(default="", db_type="VARCHAR(255)")
    db_char: str = Field(default="", db_type="CHAR(36)")
    db_text: str = Field(default="", db_type="TEXT")
    db_numeric: Decimal | None = Field(
        default=None, db_nullable=True, db_type="NUMERIC(10,2)"
    )
    db_decimal: Decimal | None = Field(
        default=None, db_nullable=True, db_type="DECIMAL(8,4)"
    )
    db_timestamp: datetime | None = Field(
        default=None, db_nullable=True, db_type="TIMESTAMP"
    )
    db_timestamptz: datetime | None = Field(
        default=None, db_nullable=True, db_type="TIMESTAMPTZ"
    )
    db_date: date | None = Field(default=None, db_nullable=True, db_type="DATE")
    db_time: time | None = Field(default=None, db_nullable=True, db_type="TIME")
    db_bigint: int = Field(default=0, db_type="BIGINT")
    db_integer: int = Field(default=0, db_type="INTEGER")
    db_smallint: int = Field(default=0, db_type="SMALLINT")
    db_serial: int | None = Field(default=None, db_pk=False, db_type="SERIAL")
    db_bigserial: int | None = Field(default=None, db_pk=False, db_type="BIGSERIAL")
    db_boolean: bool = Field(default=True, db_type="BOOLEAN")
    db_double: float = Field(default=0.0, db_type="DOUBLE PRECISION")
    db_real: float = Field(default=0.0, db_type="REAL")
    db_bytea: bytes | None = Field(default=None, db_nullable=True, db_type="BYTEA")
    db_blob: bytes | None = Field(default=None, db_nullable=True, db_type="BLOB")

    # Inferred array types (no db_type)
    infer_str_list: list[str] | None = Field(default=None, db_nullable=True)
    infer_int_list: list[int] | None = Field(default=None, db_nullable=True)
    infer_uuid_list: list[UUID] | None = Field(default=None, db_nullable=True)
    infer_decimal_list: list[Decimal] | None = Field(default=None, db_nullable=True)

    # Explicit db_type on arrays
    db_varchar_arr: list[str] | None = Field(
        default=None, db_nullable=True, db_type="VARCHAR(100)[]"
    )
    db_numeric_arr: list[Decimal] | None = Field(
        default=None, db_nullable=True, db_type="NUMERIC(10,2)[]"
    )
    db_uuid_arr: list[UUID] | None = Field(
        default=None, db_nullable=True, db_type="UUID[]"
    )
    db_int_arr: list[int] | None = Field(
        default=None, db_nullable=True, db_type="INTEGER[]"
    )
    db_text_arr: list[str] | None = Field(
        default=None, db_nullable=True, db_type="TEXT[]"
    )

    # Annotated inner constraints on arrays
    ann_str_list: list[Annotated[str, Field(max_length=100)]] | None = Field(
        default=None, db_nullable=True
    )
    ann_decimal_list: list[
        Annotated[Decimal, Field(max_digits=10, decimal_places=2)]
    ] | None = Field(default=None, db_nullable=True)

    class Meta:
        is_table = True
        table_name = "db_types_model"


# ── col_types tests ───────────────────────────────────────────────────

COL_TYPE_CASES = [
    # Inferred scalar
    ("infer_str", "str"),
    ("infer_int", "int"),
    ("infer_float", "float"),
    ("infer_bool", "bool"),
    ("infer_bytes", "bytes"),
    ("infer_datetime", "datetime"),
    ("infer_date", "date"),
    ("infer_time", "time"),
    ("infer_timedelta", "timedelta"),
    ("infer_uuid", "uuid"),
    ("infer_decimal", "decimal"),
    ("infer_json", "json"),
    # Explicit db_type scalar — passed as-is (uppercased)
    ("db_uuid", "UUID"),
    ("db_jsonb", "JSONB"),
    ("db_json", "JSON"),
    ("db_varchar", "VARCHAR(255)"),
    ("db_char", "CHAR(36)"),
    ("db_text", "TEXT"),
    ("db_numeric", "NUMERIC(10,2)"),
    ("db_decimal", "DECIMAL(8,4)"),
    ("db_timestamp", "TIMESTAMP"),
    ("db_timestamptz", "TIMESTAMPTZ"),
    ("db_date", "DATE"),
    ("db_time", "TIME"),
    ("db_bigint", "BIGINT"),
    ("db_integer", "INTEGER"),
    ("db_smallint", "SMALLINT"),
    ("db_serial", "SERIAL"),
    ("db_bigserial", "BIGSERIAL"),
    ("db_boolean", "BOOLEAN"),
    ("db_double", "DOUBLE PRECISION"),
    ("db_real", "REAL"),
    ("db_bytea", "BYTEA"),
    ("db_blob", "BLOB"),
    # Inferred arrays
    ("infer_str_list", "str[]"),
    ("infer_int_list", "int[]"),
    ("infer_uuid_list", "uuid[]"),
    ("infer_decimal_list", "decimal[]"),
    # Explicit db_type arrays — passed as-is (uppercased)
    ("db_varchar_arr", "VARCHAR(100)[]"),
    ("db_numeric_arr", "NUMERIC(10,2)[]"),
    ("db_uuid_arr", "UUID[]"),
    ("db_int_arr", "INTEGER[]"),
    ("db_text_arr", "TEXT[]"),
    # Annotated inner arrays (inferred from python_type)
    ("ann_str_list", "str[]"),
    ("ann_decimal_list", "decimal[]"),
]


class TestColTypes:
    @pytest.mark.parametrize("field,expected", COL_TYPE_CASES)
    def test_col_type(self, field, expected):
        col_types = DbTypesModel._db_meta.col_types
        assert col_types[field] == expected, (
            f"{field}: got {col_types.get(field)!r}, expected {expected!r}"
        )


# ── Annotated inner constraints extraction ────────────────────────────


class TestAnnotatedConstraints:
    def test_str_list_max_length(self):
        meta = DbTypesModel._db_meta.field_metadata["ann_str_list"]
        assert meta.max_length == 100

    def test_decimal_list_max_digits(self):
        meta = DbTypesModel._db_meta.field_metadata["ann_decimal_list"]
        assert meta.max_digits == 10
        assert meta.decimal_places == 2
