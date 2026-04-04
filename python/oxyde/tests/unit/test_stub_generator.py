"""Tests for .pyi stub generation utilities."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from uuid import UUID

import pytest

from oxyde import Field, Model
from oxyde.codegen.stub_generator import _get_python_type_name


class TestGetPythonTypeName:
    """Test _get_python_type_name returns valid type annotation strings."""

    @pytest.mark.parametrize(
        ("python_type", "expected"),
        [
            (str, "str"),
            (int, "int"),
            (float, "float"),
            (bool, "bool"),
            (bytes, "bytes"),
            (datetime, "datetime"),
            (date, "date"),
            (time, "time"),
            (Decimal, "Decimal"),
            (UUID, "UUID"),
        ],
    )
    def test_builtin_and_stdlib_types(self, python_type: type, expected: str):
        """Explicitly handled types return their name."""
        assert _get_python_type_name(python_type) == expected

    @pytest.mark.parametrize(
        ("python_type", "expected"),
        [
            (dict, "dict"),
            (list, "list"),
            (set, "set"),
            (tuple, "tuple"),
            (frozenset, "frozenset"),
        ],
    )
    def test_bare_container_types(self, python_type: type, expected: str):
        """Bare container types return their name, not repr (e.g. not <class 'dict'>)."""
        result = _get_python_type_name(python_type)
        assert result == expected
        assert "<class" not in result

    @pytest.mark.parametrize(
        ("python_type", "expected"),
        [
            (list[str], "list[str]"),
            (dict[str, int], "dict[str, int]"),
            (tuple[int, ...], "tuple[int, ...]"),
        ],
    )
    def test_parameterized_generic_types(self, python_type: type, expected: str):
        """Parameterized generics use their string representation."""
        assert _get_python_type_name(python_type) == expected

    def test_model_subclass(self):
        """Model subclasses return their class name."""

        class MyModel(Model):
            id: int = Field(db_pk=True)

            class Meta:
                is_table = True

        assert _get_python_type_name(MyModel) == "MyModel"
