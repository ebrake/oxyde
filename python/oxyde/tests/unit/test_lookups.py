"""Parameterized tests for all lookup types and field filtering."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

import pytest

from oxyde import Field, Model, Q
from oxyde.exceptions import FieldError, FieldLookupError, FieldLookupValueError
from oxyde.models.lookups import (
    _allowed_lookups_for_meta,
    _build_array_condition,
    _build_lookup_conditions,
    _lookup_category,
    _resolve_column_meta,
    _split_lookup_key,
)
from oxyde.models.registry import registered_tables


def get_filter_condition(ir: dict[str, Any]) -> dict[str, Any]:
    """Extract single filter condition from IR filter_tree."""
    tree = ir.get("filter_tree")
    if tree is None:
        raise KeyError("No filter_tree in IR")
    # Single condition
    if tree.get("type") == "condition":
        return tree
    # AND with conditions - return first
    raise ValueError(f"Expected single condition, got {tree.get('type')}")


class OxydeTestModel(Model):
    """Test model with various field types."""

    id: int | None = Field(default=None, db_pk=True)
    name: str
    email: str | None = None
    age: int
    price: float
    balance: Decimal = Decimal("0.00")
    is_active: bool = True
    created_at: datetime
    birth_date: date | None = None
    uuid_val: UUID | None = None

    class Meta:
        is_table = True


class TestSplitLookupKey:
    """Test _split_lookup_key function."""

    def test_simple_field(self):
        """Test field name without lookup."""
        field, lookup = _split_lookup_key("name")
        assert field == "name"
        assert lookup == "exact"

    def test_field_with_lookup(self):
        """Test field name with lookup."""
        field, lookup = _split_lookup_key("name__icontains")
        assert field == "name"
        assert lookup == "icontains"

    def test_field_with_double_underscore(self):
        """Test field with multiple underscores."""
        field, lookup = _split_lookup_key("created_at__year")
        assert field == "created_at"
        assert lookup == "year"

    def test_empty_field_raises(self):
        """Test that empty field name raises error."""
        with pytest.raises(FieldLookupError):
            _split_lookup_key("__exact")


class TestLookupCategory:
    """Test _lookup_category function."""

    def test_string_category(self):
        """Test string field category."""
        registered_tables()
        meta = _resolve_column_meta(OxydeTestModel, "name")
        assert _lookup_category(meta) == "string"

    def test_numeric_category_int(self):
        """Test integer field category."""
        registered_tables()
        meta = _resolve_column_meta(OxydeTestModel, "age")
        assert _lookup_category(meta) == "numeric"

    def test_numeric_category_float(self):
        """Test float field category."""
        registered_tables()
        meta = _resolve_column_meta(OxydeTestModel, "price")
        assert _lookup_category(meta) == "numeric"

    def test_numeric_category_decimal(self):
        """Test Decimal field category."""
        registered_tables()
        meta = _resolve_column_meta(OxydeTestModel, "balance")
        assert _lookup_category(meta) == "numeric"

    def test_datetime_category(self):
        """Test datetime field category."""
        registered_tables()
        meta = _resolve_column_meta(OxydeTestModel, "created_at")
        assert _lookup_category(meta) == "datetime"

    def test_date_category(self):
        """Test date field category."""
        registered_tables()
        meta = _resolve_column_meta(OxydeTestModel, "birth_date")
        assert _lookup_category(meta) == "datetime"

    def test_bool_category(self):
        """Test boolean field category."""
        registered_tables()
        meta = _resolve_column_meta(OxydeTestModel, "is_active")
        # Note: bool is treated as numeric in lookup system
        category = _lookup_category(meta)
        assert category in ("bool", "generic", "numeric")

    def test_uuid_category(self):
        meta = _resolve_column_meta(OxydeTestModel, "uuid_val")
        assert _lookup_category(meta) == "uuid"


class TestAllowedLookups:
    """Test _allowed_lookups_for_meta function."""

    def test_string_field_lookups(self):
        """Test allowed lookups for string fields."""
        registered_tables()
        meta = _resolve_column_meta(OxydeTestModel, "name")
        allowed = _allowed_lookups_for_meta(meta)

        assert "exact" in allowed
        assert "contains" in allowed
        assert "icontains" in allowed
        assert "startswith" in allowed
        assert "endswith" in allowed
        assert "iexact" in allowed
        assert "in" in allowed
        assert "isnull" in allowed

    def test_numeric_field_lookups(self):
        """Test allowed lookups for numeric fields."""
        registered_tables()
        meta = _resolve_column_meta(OxydeTestModel, "age")
        allowed = _allowed_lookups_for_meta(meta)

        assert "exact" in allowed
        assert "gt" in allowed
        assert "gte" in allowed
        assert "lt" in allowed
        assert "lte" in allowed
        assert "between" in allowed
        assert "in" in allowed
        assert "isnull" in allowed

    def test_datetime_field_lookups(self):
        """Test allowed lookups for datetime fields."""
        registered_tables()
        meta = _resolve_column_meta(OxydeTestModel, "created_at")
        allowed = _allowed_lookups_for_meta(meta)

        assert "exact" in allowed
        assert "gt" in allowed
        assert "gte" in allowed
        assert "lt" in allowed
        assert "lte" in allowed
        assert "between" in allowed
        assert "year" in allowed
        assert "month" in allowed
        assert "day" in allowed
        assert "in" in allowed
        assert "isnull" in allowed

    def test_bool_field_lookups(self):
        """Test allowed lookups for boolean fields."""
        registered_tables()
        meta = _resolve_column_meta(OxydeTestModel, "is_active")
        allowed = _allowed_lookups_for_meta(meta)

        assert "exact" in allowed
        assert "in" in allowed
        assert "isnull" in allowed

    def test_uuid_field_lookups(self):
        meta = _resolve_column_meta(OxydeTestModel, "uuid_val")
        allowed = _allowed_lookups_for_meta(meta)
        assert set(allowed) == {
            "exact",
            "gt",
            "gte",
            "lt",
            "lte",
            "between",
            "range",
            "in",
            "isnull",
        }


class TestUuidLookups:
    UUID_LOW = UUID("10000000-0000-4000-8000-000000000001")
    UUID_HIGH = UUID("90000000-0000-4000-8000-000000000003")

    @pytest.mark.parametrize(
        ("lookup", "operator"),
        [("gt", ">"), ("gte", ">="), ("lt", "<"), ("lte", "<=")],
    )
    def test_comparison(self, lookup, operator):
        ir = OxydeTestModel.objects.filter(
            **{f"uuid_val__{lookup}": self.UUID_LOW}
        ).to_ir()
        condition = get_filter_condition(ir)
        assert condition["operator"] == operator
        assert condition["value"] == str(self.UUID_LOW)

    @pytest.mark.parametrize("lookup", ["between", "range"])
    def test_inclusive_range(self, lookup):
        ir = OxydeTestModel.objects.filter(
            **{f"uuid_val__{lookup}": (self.UUID_LOW, self.UUID_HIGH)}
        ).to_ir()
        condition = get_filter_condition(ir)
        assert condition["operator"] == "BETWEEN"
        assert condition["value"] == [str(self.UUID_LOW), str(self.UUID_HIGH)]

    def test_existing_validation(self):
        with pytest.raises(FieldLookupValueError):
            OxydeTestModel.objects.filter(uuid_val__gt=None)
        with pytest.raises(FieldLookupValueError):
            OxydeTestModel.objects.filter(uuid_val__between=(self.UUID_LOW,))
        with pytest.raises(FieldLookupError):
            OxydeTestModel.objects.filter(uuid_val__contains="1000")
        with pytest.raises(FieldLookupError):
            OxydeTestModel.objects.filter(uuid_val__year=2026)


class ArrayLookupModel(Model):
    """Fields that do (and do not) support array lookups."""

    id: int | None = Field(default=None, db_pk=True)
    tags: list[str] | None = None
    scores: list[int] = Field(default_factory=list, db_type="INTEGER[]")
    amounts: list[Annotated[Decimal, Field(max_digits=10, decimal_places=2)]] = Field(
        default_factory=list
    )
    ids: list[UUID] = Field(default_factory=list)
    labels: list[str] = Field(default_factory=list, db_column="label_list")
    payload: dict | list | None = None
    raw: list[Any] | dict | None = Field(default=None, db_type="JSON")
    note: str = Field(default="", db_type="JSON")
    title: str = ""
    grid: list[list[int]] | None = None
    untyped: list | None = None

    class Meta:
        is_table = True


@pytest.fixture
def spec_child() -> type[Model]:
    """A child that shares the `status` column name with its parent, typed
    differently. Defined per test: the unit conftest clears the registry."""

    class SpecParent(Model):
        id: int | None = Field(default=None, db_pk=True)
        status: str = Field(default="", max_length=20)

        class Meta:
            is_table = True

    class SpecChild(Model):
        id: int | None = Field(default=None, db_pk=True)
        status: int = 0
        parent: SpecParent | None = Field(default=None, db_nullable=True)

        class Meta:
            is_table = True

    return SpecChild


class TestQueryColumnTypes:
    """Joined fields are typed by the `alias.column` entry of the query's
    column_types, not by the base model's same-named column."""

    def test_own_field(self, spec_child):
        ir = spec_child.objects.filter(status=1).to_ir()
        assert ir["column_types"]["status"] == {"kind": "big_integer"}
        assert "column_type" not in get_filter_condition(ir)

    @pytest.mark.parametrize(
        ("lookup", "value"),
        [("exact", "x"), ("in", ["a", "b"]), ("startswith", "x"), ("isnull", True)],
    )
    def test_fk_traversal_uses_the_related_models_spec(self, spec_child, lookup, value):
        ir = spec_child.objects.filter(**{f"parent__status__{lookup}": value}).to_ir()
        condition = get_filter_condition(ir)
        assert condition["column"] == "parent.status"
        assert "column_type" not in condition
        assert ir["column_types"]["parent.status"] == {"kind": "string", "length": 20}
        assert ir["column_types"]["status"] == {"kind": "big_integer"}

    def test_relation_field_uses_the_fk_columns_spec(self, spec_child):
        ir = spec_child.objects.filter(parent=1).to_ir()
        assert get_filter_condition(ir)["column"] == "parent_id"
        assert ir["column_types"]["parent_id"] == {"kind": "big_integer"}


@pytest.fixture
def window_child() -> type[Model]:
    """A child whose related model has fields named like lookups."""

    class Window(Model):
        id: int | None = Field(default=None, db_pk=True)
        overlap: bool = False
        contained_by: str = ""
        gt: int = 0
        range: int = 0

        class Meta:
            is_table = True

    class WindowChild(Model):
        id: int | None = Field(default=None, db_pk=True)
        window: Window | None = Field(default=None, db_nullable=True)
        tags: list[str] = Field(default_factory=list)

        class Meta:
            is_table = True

    return WindowChild


class TestFieldsWinOverLookupNames:
    def test_related_field_named_like_a_lookup(self, window_child):
        condition = get_filter_condition(
            window_child.objects.filter(window__overlap=True).to_ir()
        )
        assert condition["column"] == "window.overlap"
        assert condition["operator"] == "="
        assert condition["value"] is True

    def test_lookup_on_a_related_field_named_like_a_lookup(self, window_child):
        condition = get_filter_condition(
            window_child.objects.filter(window__contained_by__startswith="a").to_ir()
        )
        assert condition["column"] == "window.contained_by"
        assert condition["operator"] == "LIKE"

    def test_array_lookup_on_own_field_is_still_a_lookup(self, window_child):
        condition = get_filter_condition(
            window_child.objects.filter(tags__overlap=["a"]).to_ir()
        )
        assert condition["operator"] == "ARRAY_OVERLAP"

    def test_lookup_on_the_relation_itself(self, window_child):
        condition = get_filter_condition(
            window_child.objects.filter(window__in=[1, 2]).to_ir()
        )
        assert condition["operator"] == "IN"
        assert condition["column"] == "window_id"

    @pytest.mark.parametrize(
        ("key", "value", "operator"),
        [("window__gt", 5, ">"), ("window__range", (1, 10), "BETWEEN")],
    )
    def test_lookup_on_the_relation_wins_over_a_same_named_field(
        self, window_child, key, value, operator
    ):
        # Window has `gt` and `range` fields, but on the FK itself these are
        # valid lookups and keep the meaning they have always had.
        ir = window_child.objects.filter(**{key: value}).to_ir()
        condition = get_filter_condition(ir)
        assert condition["column"] == "window_id"
        assert condition["operator"] == operator
        assert not ir.get("joins")

    def test_explicit_exact_reaches_the_same_named_field(self, window_child):
        condition = get_filter_condition(
            window_child.objects.filter(window__gt__exact=5).to_ir()
        )
        assert condition["column"] == "window.gt"
        assert condition["operator"] == "="

    def test_unresolvable_paths_keep_their_errors(self, window_child):
        with pytest.raises(FieldError):
            window_child.objects.filter(nope__in=[1])
        with pytest.raises(FieldLookupError, match="not a foreign key"):
            window_child.objects.filter(tags__nope__in=[1])
        with pytest.raises(
            FieldLookupError, match="Lookup 'gt' is not supported for field 'tags'"
        ):
            window_child.objects.filter(tags__gt=["a"])
        # A target the registry cannot resolve is not a field path either.
        meta = window_child._db_meta.field_metadata["window"]
        original = meta.foreign_key.target
        meta.foreign_key.target = "NoSuchModel"
        try:
            with pytest.raises(
                FieldLookupError,
                match="Lookup 'overlap' is not supported for field 'window'",
            ):
                window_child.objects.filter(window__overlap=True)
        finally:
            meta.foreign_key.target = original


class TestArrayLookups:
    ARRAY_FIELDS = ["tags", "scores", "amounts", "ids", "labels", "payload", "raw"]

    @pytest.mark.parametrize("field", ARRAY_FIELDS)
    def test_array_and_json_fields_allow_array_lookups(self, field):
        allowed = _allowed_lookups_for_meta(
            _resolve_column_meta(ArrayLookupModel, field)
        )
        assert {"contains", "contained_by", "overlap"} <= set(allowed)
        assert not {"gt", "icontains", "year"} & set(allowed)

    @pytest.mark.parametrize("field", ["title", "grid", "untyped"])
    def test_other_fields_do_not(self, field):
        allowed = _allowed_lookups_for_meta(
            _resolve_column_meta(ArrayLookupModel, field)
        )
        assert not {"contained_by", "overlap"} & set(allowed)

    def test_string_field_stored_as_json_keeps_string_lookups(self):
        allowed = _allowed_lookups_for_meta(
            _resolve_column_meta(ArrayLookupModel, "note")
        )
        assert {"icontains", "startswith", "contained_by", "overlap"} <= set(allowed)

    @pytest.mark.parametrize(
        ("lookup", "operator"),
        [
            ("contains", "ARRAY_CONTAINS"),
            ("contained_by", "ARRAY_CONTAINED_BY"),
            ("overlap", "ARRAY_OVERLAP"),
        ],
    )
    def test_condition_carries_the_operator(self, lookup, operator):
        ir = ArrayLookupModel.objects.filter(**{f"tags__{lookup}": ["a", None]}).to_ir()
        condition = get_filter_condition(ir)
        assert condition["operator"] == operator
        assert condition["value"] == ["a", None]
        assert "db_type" not in condition
        assert ir["column_types"]["tags"] == {"kind": "array", "item": {"kind": "string"}}

    def test_db_type_and_renamed_column_are_forwarded(self):
        condition = get_filter_condition(
            ArrayLookupModel.objects.filter(scores__overlap=[1]).to_ir()
        )
        assert condition["db_type"] == "INTEGER[]"

        condition = get_filter_condition(
            ArrayLookupModel.objects.filter(labels__contains=["x"]).to_ir()
        )
        assert condition["column"] == "label_list"

    @pytest.mark.parametrize(
        ("filters", "db_type"),
        [
            # Every operator on an overridden array column, for the
            # PostgreSQL cast (integer[] has no = with bigint[]).
            ({"scores": [1, 2]}, "INTEGER[]"),
            ({"scores__in": [[1, 2]]}, "INTEGER[]"),
            ({"raw__contains": [1]}, "JSON"),
            ({"tags": ["a"]}, None),
            ({"title": "x"}, None),
        ],
    )
    def test_db_type_is_sent_for_array_and_json_fields(self, filters, db_type):
        condition = get_filter_condition(
            ArrayLookupModel.objects.filter(**filters).to_ir()
        )
        assert condition.get("db_type") == db_type

    @pytest.mark.parametrize(
        ("field", "lookup", "value", "db_type"),
        [
            ("scores", "overlap", [1], "INTEGER[]"),
            ("scores", "exact", [1], "INTEGER[]"),
            ("scores", "in", [[1]], "INTEGER[]"),
            ("raw", "contains", [1], "JSON"),
            ("tags", "overlap", ["a"], None),
            ("title", "exact", "x", None),
        ],
    )
    def test_db_type_is_set_without_q(self, field, lookup, value, db_type):
        # Conditions built directly, not through Q, still carry the cast.
        meta = _resolve_column_meta(ArrayLookupModel, field)
        (condition,) = _build_lookup_conditions(
            ArrayLookupModel, field, lookup, value, meta
        )
        assert condition.db_type == db_type

    def test_operands_are_serialized_and_tuples_become_lists(self):
        uid = UUID("10000000-0000-4000-8000-000000000001")
        condition = get_filter_condition(
            ArrayLookupModel.objects.filter(
                payload__contains=({"a": (1, uid)}, (Decimal("1.5"), None))
            ).to_ir()
        )
        assert condition["value"] == [{"a": [1, str(uid)]}, ["1.5", None]]

        condition = get_filter_condition(
            ArrayLookupModel.objects.filter(ids__contains=(uid,)).to_ir()
        )
        assert condition["value"] == [str(uid)]

    def test_string_contains_on_json_str_field_stays_like(self):
        condition = get_filter_condition(
            ArrayLookupModel.objects.filter(note__contains="x").to_ir()
        )
        assert condition["operator"] == "LIKE"
        assert condition["value"] == "%x%"

        condition = get_filter_condition(
            ArrayLookupModel.objects.filter(note__contains=["x"]).to_ir()
        )
        assert condition["operator"] == "ARRAY_CONTAINS"

    def test_plain_string_contains_is_unchanged(self):
        condition = get_filter_condition(
            ArrayLookupModel.objects.filter(title__contains="x").to_ir()
        )
        assert condition == {
            "type": "condition",
            "field": "title",
            "operator": "LIKE",
            "value": "%x%",
        }

    @pytest.mark.parametrize("value", ["rust", b"rust", {"a": 1}, {"rust"}, 5, None])
    def test_non_sequence_operands_are_rejected(self, value):
        with pytest.raises(FieldLookupValueError, match="list or tuple"):
            ArrayLookupModel.objects.filter(tags__contains=value)

    def test_array_lookups_on_other_fields_are_rejected(self):
        with pytest.raises(FieldLookupError):
            ArrayLookupModel.objects.filter(title__overlap=["a"])
        with pytest.raises(FieldLookupError):
            ArrayLookupModel.objects.filter(grid__contains=[[1]])
        with pytest.raises(FieldLookupError):
            _build_array_condition(
                "title",
                "overlap",
                ["a"],
                _resolve_column_meta(ArrayLookupModel, "title"),
                None,
            )

    def test_q_and_exclude_wrap_array_conditions(self):
        ir = ArrayLookupModel.objects.exclude(
            Q(tags__contains=["a"]) | Q(ids__overlap=[])
        ).to_ir()
        tree = ir["filter_tree"]
        assert tree["type"] == "not"
        operators = [c["operator"] for c in tree["condition"]["conditions"]]
        assert operators == ["ARRAY_CONTAINS", "ARRAY_OVERLAP"]


class TestStringLookups:
    """Test string-specific lookups."""

    @pytest.mark.parametrize(
        "lookup,operator,pattern_fn",
        [
            ("contains", "LIKE", lambda v: f"%{v}%"),
            ("icontains", "ILIKE", lambda v: f"%{v}%"),
            ("startswith", "LIKE", lambda v: f"{v}%"),
            ("istartswith", "ILIKE", lambda v: f"{v}%"),
            ("endswith", "LIKE", lambda v: f"%{v}"),
            ("iendswith", "ILIKE", lambda v: f"%{v}"),
        ],
    )
    def test_string_pattern_lookups(self, lookup, operator, pattern_fn):
        """Test string pattern lookups generate correct conditions."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(**{f"name__{lookup}": "test"}).to_ir()
        cond = get_filter_condition(ir)

        assert cond["operator"] == operator
        assert cond["value"] == pattern_fn("test")
        assert "escape" not in cond

    def test_iexact_lookup(self):
        """Test iexact lookup."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(name__iexact="Test").to_ir()
        cond = get_filter_condition(ir)

        assert cond["operator"] == "ILIKE"
        assert cond["value"] == "Test"
        assert "escape" not in cond

    def test_iexact_escapes_underscore(self):
        """Test that iexact escapes underscore."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(name__iexact="test_user").to_ir()
        cond = get_filter_condition(ir)

        assert cond["operator"] == "ILIKE"
        assert "\\_" in cond["value"]
        assert cond["escape"] == "\\"

    def test_iexact_escapes_percent(self):
        """Test that iexact escapes percent."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(name__iexact="100%done").to_ir()
        cond = get_filter_condition(ir)

        assert cond["operator"] == "ILIKE"
        assert "\\%" in cond["value"]
        assert cond["escape"] == "\\"

    def test_contains_escapes_wildcards(self):
        """Test that contains escapes SQL wildcards."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(name__contains="test%value").to_ir()
        cond = get_filter_condition(ir)

        assert cond["operator"] == "LIKE"
        assert "\\%" in cond["value"]
        assert cond["escape"] == "\\"

    def test_contains_escapes_underscore(self):
        """Test that contains escapes underscore."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(name__contains="test_value").to_ir()
        cond = get_filter_condition(ir)

        assert cond["operator"] == "LIKE"
        assert "\\_" in cond["value"]
        assert cond["escape"] == "\\"

    def test_contains_escapes_backslash(self):
        """Test that contains escapes backslash."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(name__contains=r"test\value").to_ir()
        cond = get_filter_condition(ir)

        assert cond["operator"] == "LIKE"
        assert "\\\\" in cond["value"]
        assert cond["escape"] == "\\"

    def test_string_lookup_requires_string_value(self):
        """Test that string lookups require string values."""
        registered_tables()

        with pytest.raises(FieldLookupValueError):
            OxydeTestModel.objects.filter(name__contains=123)

        with pytest.raises(FieldLookupValueError):
            OxydeTestModel.objects.filter(name__iexact=123)


class TestNumericLookups:
    """Test numeric-specific lookups."""

    @pytest.mark.parametrize(
        "lookup,operator",
        [
            ("gt", ">"),
            ("gte", ">="),
            ("lt", "<"),
            ("lte", "<="),
        ],
    )
    def test_comparison_lookups(self, lookup, operator):
        """Test comparison lookups generate correct conditions."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(**{f"age__{lookup}": 18}).to_ir()
        cond = get_filter_condition(ir)

        assert cond["operator"] == operator
        assert cond["value"] == 18

    def test_between_lookup(self):
        """Test between lookup."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(age__between=(18, 65)).to_ir()
        cond = get_filter_condition(ir)

        assert cond["operator"] == "BETWEEN"
        assert cond["value"] == [18, 65]

    def test_between_requires_tuple(self):
        """Test that between requires a tuple/list of 2 values."""
        registered_tables()

        with pytest.raises(FieldLookupValueError):
            OxydeTestModel.objects.filter(age__between=18)

        with pytest.raises(FieldLookupValueError):
            OxydeTestModel.objects.filter(age__between=(18,))

        with pytest.raises(FieldLookupValueError):
            OxydeTestModel.objects.filter(age__between=(18, 25, 30))

    def test_comparison_requires_non_null(self):
        """Test that comparison lookups require non-null values."""
        registered_tables()

        with pytest.raises(FieldLookupValueError):
            OxydeTestModel.objects.filter(age__gt=None)


class TestCommonLookups:
    """Test common lookups available for all field types."""

    def test_exact_lookup(self):
        """Test exact lookup."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(name="Alice").to_ir()
        cond = get_filter_condition(ir)

        assert cond["operator"] == "="
        assert cond["value"] == "Alice"

    def test_exact_with_none(self):
        """Test exact lookup with None value."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(email=None).to_ir()
        cond = get_filter_condition(ir)

        assert cond["operator"] == "IS NULL"

    def test_in_lookup(self):
        """Test in lookup."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(age__in=[18, 21, 25]).to_ir()
        cond = get_filter_condition(ir)

        assert cond["operator"] == "IN"
        assert cond["value"] == [18, 21, 25]

    def test_in_requires_iterable(self):
        """Test that in lookup requires an iterable."""
        registered_tables()

        with pytest.raises(FieldLookupValueError):
            OxydeTestModel.objects.filter(age__in=18)

        with pytest.raises(FieldLookupValueError):
            OxydeTestModel.objects.filter(age__in=None)

    def test_in_rejects_string(self):
        """Test that in lookup rejects string (which is iterable but wrong)."""
        registered_tables()

        with pytest.raises(FieldLookupValueError):
            OxydeTestModel.objects.filter(name__in="test")

    def test_isnull_true(self):
        """Test isnull=True lookup."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(email__isnull=True).to_ir()
        cond = get_filter_condition(ir)

        assert cond["operator"] == "IS NULL"

    def test_isnull_false(self):
        """Test isnull=False lookup."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(email__isnull=False).to_ir()
        cond = get_filter_condition(ir)

        assert cond["operator"] == "IS NOT NULL"


def get_and_conditions(ir: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract conditions from AND filter_tree."""
    tree = ir.get("filter_tree")
    if tree is None:
        raise KeyError("No filter_tree in IR")
    if tree.get("type") == "and":
        return tree.get("conditions", [])
    raise ValueError(f"Expected AND, got {tree.get('type')}")


class TestDatePartLookups:
    """Test date/datetime part lookups."""

    def test_year_lookup(self):
        """Test year lookup."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(created_at__year=2024).to_ir()
        conditions = get_and_conditions(ir)

        # Year lookup generates two conditions: >= start AND < end
        assert len(conditions) == 2
        assert conditions[0]["operator"] == ">="
        assert conditions[1]["operator"] == "<"
        assert "2024-01-01" in conditions[0]["value"]
        assert "2025-01-01" in conditions[1]["value"]

    def test_month_lookup(self):
        """Test month lookup."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(created_at__month=(2024, 3)).to_ir()
        conditions = get_and_conditions(ir)

        assert len(conditions) == 2
        assert "2024-03-01" in conditions[0]["value"]
        assert "2024-04-01" in conditions[1]["value"]

    def test_month_lookup_december_wraparound(self):
        """Test month lookup for December (wraps to next year)."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(created_at__month=(2024, 12)).to_ir()
        conditions = get_and_conditions(ir)

        assert "2024-12-01" in conditions[0]["value"]
        assert "2025-01-01" in conditions[1]["value"]

    def test_day_lookup(self):
        """Test day lookup."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(created_at__day=(2024, 3, 15)).to_ir()
        conditions = get_and_conditions(ir)

        assert len(conditions) == 2
        assert "2024-03-15" in conditions[0]["value"]
        assert "2024-03-16" in conditions[1]["value"]

    def test_month_requires_tuple(self):
        """Test that month lookup requires (year, month) tuple."""
        registered_tables()

        with pytest.raises(FieldLookupValueError):
            OxydeTestModel.objects.filter(created_at__month=3)

    def test_day_requires_tuple(self):
        """Test that day lookup requires (year, month, day) tuple."""
        registered_tables()

        with pytest.raises(FieldLookupValueError):
            OxydeTestModel.objects.filter(created_at__day=(2024, 3))

    def test_month_validates_range(self):
        """Test that month value is validated."""
        registered_tables()

        with pytest.raises(FieldLookupValueError):
            OxydeTestModel.objects.filter(created_at__month=(2024, 13))

        with pytest.raises(FieldLookupValueError):
            OxydeTestModel.objects.filter(created_at__month=(2024, 0))

    def test_day_validates_date(self):
        """Test that day lookup validates the date."""
        registered_tables()

        # Feb 30 doesn't exist
        with pytest.raises(FieldLookupValueError):
            OxydeTestModel.objects.filter(created_at__day=(2024, 2, 30))

    def test_date_part_lookups_on_date_field(self):
        """Test date part lookups on date field (not datetime)."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(birth_date__year=1990).to_ir()
        conditions = get_and_conditions(ir)

        assert len(conditions) == 2


class TestInvalidLookups:
    """Test error handling for invalid lookups."""

    def test_unknown_lookup_raises(self):
        """Test that unknown lookup raises FieldLookupError."""
        registered_tables()

        with pytest.raises(FieldLookupError):
            OxydeTestModel.objects.filter(name__unknown="test")

    def test_string_lookup_on_numeric_field_raises(self):
        """Test that string lookup on numeric field raises error."""
        registered_tables()

        with pytest.raises(FieldLookupError):
            OxydeTestModel.objects.filter(age__contains="18")

    def test_date_part_lookup_on_string_field_raises(self):
        """Test that date part lookup on string field raises error."""
        registered_tables()

        with pytest.raises(FieldLookupError):
            OxydeTestModel.objects.filter(name__year=2024)

    def test_nonexistent_field_raises(self):
        """Test that nonexistent field raises FieldError."""
        registered_tables()

        with pytest.raises(FieldError):
            OxydeTestModel.objects.filter(nonexistent="value")


class TestMultipleFilters:
    """Test combining multiple filters."""

    def test_multiple_filters_same_field(self):
        """Test multiple filters on the same field."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(age__gte=18, age__lte=65).to_ir()
        conditions = get_and_conditions(ir)

        assert len(conditions) == 2

    def test_multiple_filters_different_fields(self):
        """Test multiple filters on different fields."""
        registered_tables()
        ir = OxydeTestModel.objects.filter(name__icontains="test", age__gte=18).to_ir()
        conditions = get_and_conditions(ir)

        assert len(conditions) == 2

    def test_chained_filter_calls(self):
        """Test chaining filter() calls."""
        registered_tables()
        # Manager.filter() returns Query, chain with filter()
        query = OxydeTestModel.objects.filter(name__icontains="test").filter(
            age__gte=18
        )
        ir = query.to_ir()
        conditions = get_and_conditions(ir)

        assert len(conditions) == 2


class TestResolveColumnMeta:
    """Test _resolve_column_meta function."""

    def test_resolves_existing_field(self):
        """Test resolving metadata for existing field."""
        registered_tables()
        meta = _resolve_column_meta(OxydeTestModel, "name")

        assert meta.name == "name"
        assert meta.python_type is str

    def test_resolves_pk_field(self):
        """Test resolving metadata for primary key field."""
        registered_tables()
        meta = _resolve_column_meta(OxydeTestModel, "id")

        assert meta.primary_key is True

    def test_raises_for_nonexistent_field(self):
        """Test that nonexistent field raises FieldError."""
        registered_tables()

        with pytest.raises(FieldError):
            _resolve_column_meta(OxydeTestModel, "nonexistent")
