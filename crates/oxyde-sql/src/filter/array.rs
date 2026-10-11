//! Array lookups: `ARRAY_CONTAINS`, `ARRAY_CONTAINED_BY` and `ARRAY_OVERLAP`.
//!
//! Semantics shared by every dialect: the outer array is a set (order and
//! duplicates are ignored), elements compare as complete values (objects
//! ignore key order, nested arrays keep theirs), a `None` operand element
//! matches a null element, an SQL-NULL column yields NULL and a non-array
//! JSON value yields false.
//!
//! - PostgreSQL typed arrays use native `@>` / `<@` / `&&` against an operand
//!   cast to the column's array type. Native operators never match NULL
//!   elements, so operands containing NULL switch to an `unnest` form with
//!   `IS NOT DISTINCT FROM`.
//! - PostgreSQL JSON columns enumerate `jsonb_array_elements` and compare
//!   whole jsonb values.
//! - MySQL walks both arrays with `JSON_TABLE` and compares elements as JSON
//!   values. Its `JSON PATH '$'` columns turn JSON null elements into SQL
//!   NULL, so comparisons are null-safe.
//! - SQLite calls `oxyde_array_match`, a Rust function the driver registers on
//!   every pooled connection. A JSONB column value is passed as its JSON text
//!   (`json_valid(.., 8)` / `json()`, SQLite 3.45+; the bundled SQLite is
//!   newer).
//!
//! MySQL and SQLite store typed arrays as JSON. The operand is bound as a
//! `Value::Array` so the driver serializes it with the same encoder it uses
//! for stored values; every element is validated here first, because that
//! encoder (and PostgreSQL array binding) turns mismatched elements into NULL.

use oxyde_codec::sqlite_array_match as sqlite_fn;
use oxyde_codec::ColumnTypeSpec;
use rust_decimal::Decimal;
use sea_query::{Expr, SimpleExpr, Value};
use uuid::Uuid;

use crate::error::{QueryError, Result};
use crate::spec_sql::canonical_type;
use crate::utils::bind::{
    bind_value, coerce_element, element_array_type, parse_decimal_exact, quote_pg_type_path,
    string_value,
};
use crate::utils::value::rmpv_to_json;
use crate::Dialect;

/// Name of the SQLite function the driver registers for array lookups.
pub const SQLITE_ARRAY_MATCH_FN: &str = sqlite_fn::FUNCTION;

/// Largest integer magnitude an `f64` represents exactly (2^53).
const MAX_EXACT_DOUBLE_INT: u64 = 1 << 53;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArrayOp {
    Contains,
    ContainedBy,
    Overlap,
}

impl ArrayOp {
    pub(crate) fn from_operator(operator: &str) -> Option<Self> {
        match operator {
            "ARRAY_CONTAINS" => Some(Self::Contains),
            "ARRAY_CONTAINED_BY" => Some(Self::ContainedBy),
            "ARRAY_OVERLAP" => Some(Self::Overlap),
            _ => None,
        }
    }

    /// Lookup name; also the operator argument of `oxyde_array_match`.
    fn name(self) -> &'static str {
        match self {
            Self::Contains => sqlite_fn::CONTAINS,
            Self::ContainedBy => sqlite_fn::CONTAINED_BY,
            Self::Overlap => sqlite_fn::OVERLAP,
        }
    }

    fn pg_operator(self) -> &'static str {
        match self {
            Self::Contains => "@>",
            Self::ContainedBy => "<@",
            Self::Overlap => "&&",
        }
    }
}

/// What the filtered column stores.
enum Target<'a> {
    /// Typed array column (`list[T]`): PostgreSQL `T[]`, JSON elsewhere.
    Typed(&'a ColumnTypeSpec),
    /// JSON/JSONB column holding a top-level array.
    Json,
}

fn classify(spec: &ColumnTypeSpec) -> Result<Target<'_>> {
    match spec {
        ColumnTypeSpec::Array { item } => match item.as_ref() {
            ColumnTypeSpec::Array { .. } => Err(unsupported(
                "array lookups do not support nested typed arrays; store them in a JSON column",
            )),
            ColumnTypeSpec::Unknown => Err(unsupported(
                "array lookups do not support this array's element type",
            )),
            item => Ok(Target::Typed(item)),
        },
        ColumnTypeSpec::Json | ColumnTypeSpec::JsonBinary => Ok(Target::Json),
        _ => Err(unsupported("array lookups require an array or JSON column")),
    }
}

/// Build the predicate for an array lookup on `col`.
pub(crate) fn build_array_filter(
    col: &SimpleExpr,
    op: ArrayOp,
    value: &rmpv::Value,
    spec: &ColumnTypeSpec,
    db_type: Option<&str>,
    dialect: Dialect,
) -> Result<SimpleExpr> {
    let rmpv::Value::Array(items) = value else {
        return Err(invalid(format!(
            "Array lookup '{}' requires a list or tuple value",
            op.name()
        )));
    };

    match classify(spec)? {
        Target::Typed(item) => {
            if matches!(item, ColumnTypeSpec::Blob) && dialect != Dialect::Postgres {
                // The JSON encoder stores bytes elements as null on these
                // backends, so there is nothing to compare against.
                return Err(unsupported(
                    "array lookups on bytes arrays are only supported on PostgreSQL",
                ));
            }
            let arr_type = element_array_type(item)
                .ok_or_else(|| unsupported("array lookups do not support this element type"))?;
            let elements = items
                .iter()
                .map(|v| typed_element(v, item))
                .collect::<Result<Vec<_>>>()?;
            let operand = Value::Array(arr_type, Some(Box::new(elements)));
            let decimal = matches!(item, ColumnTypeSpec::Decimal { .. });

            Ok(match dialect {
                Dialect::Postgres => {
                    let array_type = pg_array_type(item, db_type)?;
                    check_pg_operand_range(&operand, &array_type)?;
                    let has_null = items.iter().any(rmpv::Value::is_nil);
                    postgres_typed(col, op, &operand, &array_type, has_null)
                }
                Dialect::Mysql => mysql(col, op, &operand, decimal),
                Dialect::Sqlite => sqlite(col, op, &operand, decimal),
            })
        }
        Target::Json => {
            let elements = items
                .iter()
                .map(|v| {
                    rmpv_to_json(v).ok_or_else(|| {
                        invalid(format!(
                            "Array lookup element {v} cannot be represented as JSON"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let operand = Value::Json(Some(Box::new(serde_json::Value::Array(elements))));

            Ok(match dialect {
                Dialect::Postgres => postgres_json(col, op, &operand, pg_json_needs_cast(db_type)),
                Dialect::Mysql => mysql(col, op, &operand, false),
                Dialect::Sqlite => sqlite(col, op, &operand, false),
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Operand validation
// ---------------------------------------------------------------------------

/// Strictly convert one operand element to the column's element type.
///
/// Unlike `bind_value`, nothing falls back to a string: an element that does
/// not convert is an error rather than a silent non-match (or NULL).
fn typed_element(value: &rmpv::Value, item: &ColumnTypeSpec) -> Result<Value> {
    use rmpv::Value as V;
    use ColumnTypeSpec as S;

    if value.is_nil() {
        return Ok(bind_value(value, item));
    }

    let converted = match (item, value) {
        (S::BigInteger | S::Timedelta, V::Integer(n)) => n.as_i64().map(|i| Value::BigInt(Some(i))),
        // Only integers a double holds exactly: beyond 2^53 the conversion
        // would round into a different value.
        #[allow(clippy::cast_precision_loss)]
        (S::Double, V::Integer(n)) => n
            .as_i64()
            .filter(|i| i.unsigned_abs() <= MAX_EXACT_DOUBLE_INT)
            .map(|i| Value::Double(Some(i as f64))),
        (S::Double, V::F32(_) | V::F64(_)) => match bind_value(value, item) {
            Value::Double(Some(f)) if f.is_finite() => Some(Value::Double(Some(f))),
            _ => None,
        },
        (S::Boolean, V::Boolean(b)) => Some(Value::Bool(Some(*b))),
        (S::Text | S::String { .. }, V::String(s)) => s.as_str().map(string_value),
        (S::Enum { values, .. }, V::String(s)) => s
            .as_str()
            .filter(|s| values.iter().any(|allowed| allowed == s))
            .map(string_value),
        (S::Blob, V::Binary(b)) => Some(Value::Bytes(Some(Box::new(b.clone())))),
        (S::Uuid, V::String(s)) => s
            .as_str()
            .and_then(|s| Uuid::parse_str(s).ok())
            .map(|u| Value::Uuid(Some(Box::new(u)))),
        (S::Decimal { .. }, V::String(s)) => s
            .as_str()
            .and_then(parse_decimal_exact)
            .map(|d| Value::Decimal(Some(Box::new(d)))),
        (S::Decimal { .. }, V::Integer(n)) => n
            .as_i64()
            .map(Decimal::from)
            .or_else(|| n.as_u64().map(Decimal::from))
            .map(|d| Value::Decimal(Some(Box::new(d)))),
        (S::DateTime | S::DateTimeUtc | S::Date | S::Time, V::String(_)) => {
            temporal_element(bind_value(value, item), item)
        }
        // Strings are stored as JSON text when they parse ('"a"' is stored
        // as "a") and as plain JSON strings otherwise, so operands are too.
        (S::Json | S::JsonBinary, V::String(_)) => {
            match coerce_element(bind_value(value, item), item) {
                json @ Value::Json(Some(_)) => Some(json),
                _ => None,
            }
        }
        (S::Json | S::JsonBinary, _) => rmpv_to_json(value).map(|j| Value::Json(Some(Box::new(j)))),
        _ => None,
    };

    converted.ok_or_else(|| {
        invalid(format!(
            "Array lookup element {value} is not a valid {} value",
            kind_name(item)
        ))
    })
}

/// Keep only parses that produced the element's own temporal type; aware and
/// naive datetimes are normalized to the column's flavour exactly as stored
/// elements are (`coerce_element`).
fn temporal_element(bound: Value, item: &ColumnTypeSpec) -> Option<Value> {
    use ColumnTypeSpec as S;
    match (item, coerce_element(bound, item)) {
        (S::DateTime, v @ Value::ChronoDateTime(Some(_)))
        | (S::DateTimeUtc, v @ Value::ChronoDateTimeUtc(Some(_)))
        | (S::Date, v @ Value::ChronoDate(Some(_)))
        | (S::Time, v @ Value::ChronoTime(Some(_))) => Some(v),
        _ => None,
    }
}

fn kind_name(item: &ColumnTypeSpec) -> &'static str {
    use ColumnTypeSpec as S;
    match item {
        S::BigInteger => "integer",
        S::Timedelta => "timedelta (microseconds)",
        S::Double => "float",
        S::Boolean => "boolean",
        S::Text | S::String { .. } => "string",
        S::Blob => "bytes",
        S::DateTime | S::DateTimeUtc => "datetime",
        S::Date => "date",
        S::Time => "time",
        S::Uuid => "UUID",
        S::Decimal { .. } => "decimal",
        S::Json | S::JsonBinary => "JSON",
        S::Enum { .. } => "enum",
        S::Array { .. } => "array",
        S::Unknown => "untyped",
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL
// ---------------------------------------------------------------------------

/// PostgreSQL array type the operand is cast to.
///
/// The column's DDL type: a `db_type` override (`INTEGER[]` where the spec
/// says `BigInteger`) or the canonical type DDL renders for the spec. Size
/// and precision modifiers are dropped so the cast can neither truncate nor
/// round the operand.
///
/// The override is spliced into the SQL text. It comes from the model
/// definition, never from query input, and `pg_array_base` only passes
/// plain type names; an array override it cannot read is an error rather
/// than a silent cast to the canonical type, which would fail later as a
/// confusing "operator does not exist".
pub(super) fn pg_array_type(item: &ColumnTypeSpec, db_type: Option<&str>) -> Result<String> {
    if let ColumnTypeSpec::Enum { name, .. } = item {
        return Ok(format!("{}[]", quote_pg_type_path(name)));
    }
    let ddl_type = match db_type.map(str::trim) {
        Some(db_type) if db_type.ends_with("[]") => db_type.to_string(),
        _ => format!("{}[]", canonical_type(item, Dialect::Postgres, false)),
    };
    let base = pg_array_base(&ddl_type).ok_or_else(|| {
        QueryError::UnsupportedOperation(format!(
            "array lookups cannot use db_type '{ddl_type}': expected a \
                 one-dimensional array of a plain type, such as INTEGER[] or VARCHAR(50)[]"
        ))
    })?;
    if base.eq_ignore_ascii_case("JSON") {
        // json has no equality operator, so neither native operators nor
        // element comparison can work.
        return Err(unsupported(
            "array lookups are not supported on json[] columns; use jsonb[]",
        ));
    }
    Ok(format!("{base}[]"))
}

/// Element type of an array DDL type (`VARCHAR(50)[]`) without modifiers.
/// `None` for non-array types and anything that is not a plain type name.
fn pg_array_base(db_type: &str) -> Option<String> {
    let inner = db_type.trim().strip_suffix("[]")?;

    // FLOAT's precision picks the type: FLOAT(1..24) is real, FLOAT(25..53)
    // double precision, and bare FLOAT double precision.
    if let Some(precision) = pg_float_precision(inner) {
        return match precision.parse::<u8>() {
            Ok(1..=24) => Some("REAL".to_string()),
            Ok(25..=53) => Some("DOUBLE PRECISION".to_string()),
            _ => None,
        };
    }

    // Drop "(n)" / "(p,s)" wherever it sits ("TIMESTAMP(3) WITH TIME ZONE").
    let mut base = String::with_capacity(inner.len());
    let mut depth = 0u32;
    for ch in inner.chars() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.checked_sub(1)?,
            _ if depth == 0 => base.push(ch),
            _ => {}
        }
    }
    if depth != 0 {
        return None;
    }

    let base = base.split_whitespace().collect::<Vec<_>>().join(" ");
    let plain = base
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | ' ' | '.' | '"'));
    if base.is_empty() || !plain {
        return None;
    }
    // A bare CHAR means CHAR(1); BPCHAR keeps the stored values' length.
    if matches!(base.to_ascii_uppercase().as_str(), "CHAR" | "CHARACTER") {
        return Some("BPCHAR".to_string());
    }
    Some(base)
}

/// The text inside the parentheses of `FLOAT ( p )`, any case and spacing.
fn pg_float_precision(element: &str) -> Option<&str> {
    let element = element.trim();
    let (name, rest) = element.split_at_checked(5)?;
    if !name.eq_ignore_ascii_case("FLOAT") {
        return None;
    }
    let inside = rest.trim_start().strip_prefix('(')?.strip_suffix(')')?;
    Some(inside.trim())
}

/// Value range of a narrower integer array type (`db_type="SMALLINT[]"`).
/// `None` when the type holds every `i64`.
fn pg_int_range(base: &str) -> Option<(i64, i64)> {
    match base.to_ascii_uppercase().as_str() {
        "SMALLINT" | "INT2" => Some((i16::MIN.into(), i16::MAX.into())),
        "INTEGER" | "INT" | "INT4" => Some((i32::MIN.into(), i32::MAX.into())),
        _ => None,
    }
}

/// Reject elements the column's narrower numeric type cannot hold: the
/// operand cast would otherwise fail the query ("smallint out of range",
/// "value out of range: overflow") instead of the element simply not
/// fitting, like any other mistyped element.
pub(super) fn check_pg_operand_range(operand: &Value, array_type: &str) -> Result<()> {
    let Some(base) = array_type.strip_suffix("[]") else {
        return Ok(());
    };
    let Value::Array(_, Some(elements)) = operand else {
        return Ok(());
    };
    let int_range = pg_int_range(base);
    let real = matches!(base.to_ascii_uppercase().as_str(), "REAL" | "FLOAT4");
    for element in elements.iter() {
        let out_of_range = match (element, int_range) {
            (Value::BigInt(Some(n)), Some((min, max))) if !(min..=max).contains(n) => {
                Some(n.to_string())
            }
            // PostgreSQL's float8 -> float4 cast raises on overflow, and on
            // underflow (a non-zero value that rounds to zero).
            #[allow(clippy::cast_possible_truncation)]
            (Value::Double(Some(x)), _)
                if real && (!(*x as f32).is_finite() || (*x as f32 == 0.0 && *x != 0.0)) =>
            {
                Some(x.to_string())
            }
            _ => None,
        };
        if let Some(shown) = out_of_range {
            return Err(invalid(format!(
                "Array element {shown} is out of range for {array_type}"
            )));
        }
    }
    Ok(())
}

/// Canonical DDL renders both JSON kinds as JSONB; only an explicit
/// `db_type="JSON"` column is `json`, which has no equality operator.
fn pg_json_needs_cast(db_type: Option<&str>) -> bool {
    db_type.is_some_and(|d| d.trim().eq_ignore_ascii_case("JSON"))
}

const PG_LEFT: &str = r#""__oxyde_l"("__oxyde_v")"#;
const PG_RIGHT: &str = r#""__oxyde_r"("__oxyde_v")"#;
/// The stored value inside `pg_hold`.
const PG_HELD: &str = r#""__oxyde_h"."__oxyde_c""#;

/// Run `body` over the stored value `doc`, which `body` reads as `PG_HELD`.
///
/// `{col}` is unqualified outside joins, so read inside `body`'s subqueries
/// it could resolve to one of their aliases instead (a column named
/// `__oxyde_v`). The holder reads it once, as the only FROM item of a
/// subquery, where nothing but the outer query is in scope.
fn pg_hold(doc: &str, body: &str) -> String {
    format!(r#"(SELECT {body} FROM (SELECT {doc} AS "__oxyde_c") AS "__oxyde_h")"#)
}

fn postgres_typed(
    col: &SimpleExpr,
    op: ArrayOp,
    operand: &Value,
    array_type: &str,
    has_null: bool,
) -> SimpleExpr {
    let arg = format!("{{arg}}::{array_type}");
    if !has_null {
        let sql = format!("({{col}} {} {arg})", op.pg_operator());
        return render(&sql, col, operand, Dialect::Postgres);
    }

    let left = format!("unnest({PG_HELD}) AS {PG_LEFT}");
    let right = format!("unnest({arg}) AS {PG_RIGHT}");
    let eq = r#""__oxyde_l"."__oxyde_v" IS NOT DISTINCT FROM "__oxyde_r"."__oxyde_v""#;
    let body = pg_hold("{col}", &set_predicate(op, &left, &right, eq));
    let sql = format!("(CASE WHEN {{col}} IS NULL THEN NULL ELSE {body} END)");
    render(&sql, col, operand, Dialect::Postgres)
}

fn postgres_json(col: &SimpleExpr, op: ArrayOp, operand: &Value, cast_column: bool) -> SimpleExpr {
    let doc = if cast_column {
        "({col})::jsonb"
    } else {
        "{col}"
    };
    let arg = "{arg}::jsonb";
    let left = format!("jsonb_array_elements({PG_HELD}) AS {PG_LEFT}");
    let right = format!("jsonb_array_elements({arg}) AS {PG_RIGHT}");
    let eq = r#""__oxyde_l"."__oxyde_v" = "__oxyde_r"."__oxyde_v""#;
    let body = pg_hold(doc, &set_predicate(op, &left, &right, eq));
    // CASE, not AND: PostgreSQL does not promise to evaluate the type check
    // first, and jsonb_array_elements raises on scalars and objects.
    let guarded = format!(
        "CASE WHEN {doc} IS NULL THEN NULL \
         WHEN jsonb_typeof({doc}) = 'array' THEN {body} ELSE FALSE END"
    );
    let sql = if op == ArrayOp::Contains {
        // Exact containment implies jsonb `@>`, which is false for every
        // non-array, so it can lead the predicate and use a GIN index. That
        // binds the operand a second time; binding it once would move it
        // into a subquery, where the index cannot be used.
        format!("({doc} @> {arg} AND {guarded})")
    } else {
        format!("({guarded})")
    };
    render(&sql, col, operand, Dialect::Postgres)
}

// ---------------------------------------------------------------------------
// MySQL
// ---------------------------------------------------------------------------

/// Decimal lexemes the MySQL decimal comparison casts: at most 35 integer
/// and 30 fractional digits, which DECIMAL(65,30) holds exactly, and a
/// two-digit exponent, which keeps the `+ 0e0` range check finite.
const MYSQL_DECIMAL_LEXEME: &str =
    "^[-+]?([0-9]{1,35}([.][0-9]{0,30})?|[.][0-9]{1,30})([eE][-+]?[0-9]{1,2})?$";

fn mysql(col: &SimpleExpr, op: ArrayOp, operand: &Value, decimal: bool) -> SimpleExpr {
    // `?` placeholders bind one value each, so the operand is bound once in
    // a derived table and every other use reads its column. JSON_TABLE can
    // refer to tables before it in the same FROM clause.
    //
    // `{col}` is unqualified outside joins, so read inside these subqueries
    // it could resolve to one of their columns instead (a column named
    // `oxyde_v`). It is read once, by a holder that is the only FROM item of
    // a subquery, where nothing but the outer query is in scope; a
    // JSON_TABLE, since derived tables cannot refer to the outer query
    // before MySQL 8.0.14.
    let left = "JSON_TABLE(oxyde_h.oxyde_c, '$[*]' COLUMNS (oxyde_v JSON PATH '$')) AS oxyde_l";
    let right = "(SELECT CAST({arg} AS JSON) AS oxyde_j) AS oxyde_a \
         CROSS JOIN JSON_TABLE(oxyde_a.oxyde_j, '$[*]' COLUMNS (oxyde_v JSON PATH '$')) AS oxyde_r";
    // Elements, extracted once per row of each JSON_TABLE. The path always
    // exists, so SQL NULL here is a JSON null element.
    let l = "oxyde_l.oxyde_v";
    let r = "oxyde_r.oxyde_v";

    let eq = if decimal {
        // Typed decimals are stored as JSON strings that keep their scale
        // ("1.50"); compare them as DECIMAL, never as floating point.
        // Operands are exact rust_decimal values (at most 28 fractional
        // digits, magnitude below ~7.9e28; anything that would round is
        // rejected), which DECIMAL(65,30) holds exactly.
        //
        // The stored side can hold anything, and CAST turns text that is not
        // a number into 0 (a false match), or raises under strict mode in
        // UPDATE/DELETE, as it does on overflow. So, in CASE order (MySQL
        // evaluates CASE lazily): JSON nulls never reach the cast; anything
        // that is not a decimal lexeme DECIMAL(65,30) can hold is a
        // non-match; and an exponent lexeme must be 0, or within the
        // operand range, checked as a double (`+ 0e0`, which cannot fail on
        // a validated lexeme), since it could overflow the cast or underflow
        // into 0, and have at most 30 fractional digits once expanded
        // ("1.001e-28" has 31 and would round to 1e-28). Without an
        // exponent, the lexeme pattern already bounds both.
        let lt = format!("JSON_UNQUOTE({l})");
        // Fractional digits of the expanded value: digits up to the last
        // non-zero one, minus integer-part digits, minus the exponent.
        let mantissa = format!("REGEXP_REPLACE({lt}, '^[-+]|[eE].*$', '')");
        let expanded = format!(
            "(LENGTH(TRIM(TRAILING '0' FROM REPLACE({mantissa}, '.', ''))) \
             - LENGTH(SUBSTRING_INDEX({mantissa}, '.', 1)) \
             - CAST(REGEXP_SUBSTR({lt}, '[-+]?[0-9]+$') AS SIGNED))"
        );
        format!(
            "CASE WHEN {l} IS NULL OR {r} IS NULL THEN {l} IS NULL AND {r} IS NULL \
             WHEN JSON_TYPE({l}) NOT IN ('STRING', 'INTEGER', 'UNSIGNED INTEGER', 'DECIMAL', 'DOUBLE') \
             OR {lt} NOT REGEXP '{MYSQL_DECIMAL_LEXEME}' THEN FALSE \
             WHEN {lt} REGEXP '[eE]' \
             AND NOT ({lt} + 0e0 = 0 OR (ABS({lt} + 0e0) BETWEEN 1e-28 AND 1e29 \
             AND {expanded} <= 30)) THEN FALSE \
             ELSE CAST({lt} AS DECIMAL(65,30)) = CAST(JSON_UNQUOTE({r}) AS DECIMAL(65,30)) END"
        )
    } else {
        format!("{l} <=> {r}")
    };

    let body = format!(
        "(SELECT {} FROM JSON_TABLE({{col}}, '$' COLUMNS (oxyde_c JSON PATH '$')) AS oxyde_h)",
        set_predicate(op, left, right, &eq)
    );
    // JSON_TABLE yields no rows for a scalar, which would make the NOT EXISTS
    // forms true; non-arrays are answered before it runs.
    let guarded = format!(
        "CASE WHEN {{col}} IS NULL THEN NULL \
         WHEN JSON_TYPE({{col}}) = 'ARRAY' THEN {body} ELSE FALSE END"
    );
    // Exact containment implies JSON_CONTAINS, which is linear, so it rejects
    // most rows before the pairwise subqueries run; it binds the operand a
    // second time, outside the derived table. (JSON_OVERLAPS would do
    // the same for overlap but needs MySQL 8.0.17.) Decimals compare as
    // DECIMAL, which JSON containment does not, so they keep the bare form.
    let prefilter = match op {
        _ if decimal => None,
        ArrayOp::Contains => Some("JSON_CONTAINS({col}, CAST({arg} AS JSON))"),
        ArrayOp::ContainedBy => Some("JSON_CONTAINS(CAST({arg} AS JSON), {col})"),
        ArrayOp::Overlap => None,
    };
    let sql = match prefilter {
        Some(prefilter) => format!("({prefilter} AND {guarded})"),
        None => format!("({guarded})"),
    };
    render(&sql, col, operand, Dialect::Mysql)
}

// ---------------------------------------------------------------------------
// SQLite
// ---------------------------------------------------------------------------

fn sqlite(col: &SimpleExpr, op: ArrayOp, operand: &Value, decimal: bool) -> SimpleExpr {
    let mode = if decimal {
        sqlite_fn::ITEMS_DECIMAL
    } else {
        sqlite_fn::ITEMS_JSON
    };
    // A JSONB blob (SQLite 3.45+; flag 8 = strictly valid JSONB, false for
    // any text) is passed as its JSON text; every other value as is.
    let stored = "CASE WHEN json_valid({col}, 8) THEN json({col}) ELSE {col} END";
    let sql = format!(
        "{SQLITE_ARRAY_MATCH_FN}({stored}, {{arg}}, '{}', '{mode}')",
        op.name()
    );
    render(&sql, col, operand, Dialect::Sqlite)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Set predicate over two FROM items enumerating the stored (`left`) and
/// operand (`right`) elements; `eq` matches one pair.
fn set_predicate(op: ArrayOp, left: &str, right: &str, eq: &str) -> String {
    match op {
        ArrayOp::Contains => format!(
            "NOT EXISTS (SELECT 1 FROM {right} WHERE NOT EXISTS (SELECT 1 FROM {left} WHERE {eq}))"
        ),
        ArrayOp::ContainedBy => format!(
            "NOT EXISTS (SELECT 1 FROM {left} WHERE NOT EXISTS (SELECT 1 FROM {right} WHERE {eq}))"
        ),
        ArrayOp::Overlap => {
            format!("EXISTS (SELECT 1 FROM {left} WHERE EXISTS (SELECT 1 FROM {right} WHERE {eq}))")
        }
    }
}

/// Render a template whose `{col}` / `{arg}` markers stand for the filtered
/// column and the bound operand.
///
/// PostgreSQL references them by number, so repeats render the same
/// expression; `?` dialects consume one value per placeholder, so values are
/// pushed in marker order.
fn render(template: &str, col: &SimpleExpr, operand: &Value, dialect: Dialect) -> SimpleExpr {
    render_exprs(
        template,
        col,
        Some(SimpleExpr::Value(operand.clone())),
        dialect,
    )
}

/// `render` for a template that only references `{col}`.
pub(super) fn render_col(template: &str, col: &SimpleExpr, dialect: Dialect) -> SimpleExpr {
    render_exprs(template, col, None, dialect)
}

fn render_exprs(
    template: &str,
    col: &SimpleExpr,
    arg: Option<SimpleExpr>,
    dialect: Dialect,
) -> SimpleExpr {
    if dialect == Dialect::Postgres {
        let sql = template.replace("{col}", "$1").replace("{arg}", "$2");
        return Expr::cust_with_exprs(sql, std::iter::once(col.clone()).chain(arg));
    }

    let mut sql = String::with_capacity(template.len());
    let mut values = Vec::new();
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        sql.push_str(&rest[..start]);
        let tail = &rest[start..];
        if let Some(after) = tail.strip_prefix("{col}") {
            values.push(col.clone());
            sql.push('?');
            rest = after;
        } else if let (Some(after), Some(arg)) = (tail.strip_prefix("{arg}"), &arg) {
            values.push(arg.clone());
            sql.push('?');
            rest = after;
        } else {
            sql.push('{');
            rest = &tail[1..];
        }
    }
    sql.push_str(rest);
    Expr::cust_with_exprs(sql, values)
}

fn invalid(message: String) -> QueryError {
    QueryError::InvalidQuery(message)
}

fn unsupported(message: &str) -> QueryError {
    QueryError::UnsupportedOperation(message.to_string())
}

#[cfg(test)]
mod tests {
    use sea_query::value::ArrayType;

    use super::*;

    #[test]
    fn array_base_drops_modifiers() {
        assert_eq!(pg_array_base("VARCHAR(100)[]").as_deref(), Some("VARCHAR"));
        assert_eq!(
            pg_array_base("numeric(10, 2)[]").as_deref(),
            Some("numeric")
        );
        assert_eq!(pg_array_base("INTEGER[]").as_deref(), Some("INTEGER"));
        assert_eq!(
            pg_array_base("TIMESTAMP(3) WITH TIME ZONE[]").as_deref(),
            Some("TIMESTAMP WITH TIME ZONE")
        );
        assert_eq!(pg_array_base("CHAR(3)[]").as_deref(), Some("BPCHAR"));
        assert_eq!(pg_array_base("FLOAT(24)[]").as_deref(), Some("REAL"));
        assert_eq!(pg_array_base("float ( 1 )[]").as_deref(), Some("REAL"));
        for precise in ["FLOAT(25)[]", "FLOAT(53)[]"] {
            assert_eq!(
                pg_array_base(precise).as_deref(),
                Some("DOUBLE PRECISION"),
                "{precise}"
            );
        }
        assert_eq!(pg_array_base("FLOAT[]").as_deref(), Some("FLOAT"));
        assert_eq!(pg_array_base("FLOAT8[]").as_deref(), Some("FLOAT8"));
        for invalid in ["FLOAT(0)[]", "FLOAT(54)[]", "FLOAT(x)[]"] {
            assert_eq!(pg_array_base(invalid), None, "{invalid}");
        }
        assert_eq!(
            pg_array_base(r#""app"."status"[]"#).as_deref(),
            Some(r#""app"."status""#)
        );
    }

    #[test]
    fn array_base_rejects_non_arrays_and_odd_text() {
        assert_eq!(pg_array_base("JSONB"), None);
        assert_eq!(pg_array_base("INT); DROP TABLE x; --[]"), None);
        assert_eq!(pg_array_base("VARCHAR(10[]"), None);
        assert_eq!(pg_array_base("[]"), None);
    }

    #[test]
    fn unreadable_array_override_is_an_error() {
        let item = ColumnTypeSpec::BigInteger;
        assert_eq!(
            pg_array_type(&item, Some("INTEGER(4)[]")).unwrap(),
            "INTEGER[]"
        );
        // Not an array override: the canonical type applies.
        assert_eq!(pg_array_type(&item, Some("JSONB")).unwrap(), "BIGINT[]");
        assert_eq!(pg_array_type(&item, None).unwrap(), "BIGINT[]");
        for db_type in ["INT[][]", "VARCHAR(10[]", "INT; DROP TABLE x[]", "[]"] {
            let err = pg_array_type(&item, Some(db_type)).unwrap_err().to_string();
            assert!(err.contains(db_type), "{db_type}: {err}");
        }
    }

    #[test]
    fn canonical_array_types_drop_modifiers() {
        use ColumnTypeSpec as S;
        let cases = [
            (S::BigInteger, "BIGINT[]"),
            (S::Timedelta, "BIGINT[]"),
            (S::Double, "DOUBLE PRECISION[]"),
            (S::Boolean, "BOOLEAN[]"),
            (S::String { length: Some(20) }, "VARCHAR[]"),
            (S::Text, "TEXT[]"),
            (S::Blob, "BYTEA[]"),
            (S::DateTime, "TIMESTAMP[]"),
            (S::DateTimeUtc, "TIMESTAMPTZ[]"),
            (S::Date, "DATE[]"),
            (S::Time, "TIME[]"),
            (S::Uuid, "UUID[]"),
            (
                S::Decimal {
                    precision: Some(10),
                    scale: Some(2),
                },
                "NUMERIC[]",
            ),
            (S::Json, "JSONB[]"),
            (S::JsonBinary, "JSONB[]"),
            (
                S::Enum {
                    name: "app.status".into(),
                    values: vec![],
                },
                r#""app"."status"[]"#,
            ),
        ];
        for (item, expected) in cases {
            assert_eq!(pg_array_type(&item, None).unwrap(), expected, "{item:?}");
        }
    }

    #[test]
    fn nested_and_unknown_element_types_have_their_own_errors() {
        let nested = ColumnTypeSpec::Array {
            item: Box::new(ColumnTypeSpec::Array {
                item: Box::new(ColumnTypeSpec::BigInteger),
            }),
        };
        let err = classify(&nested).err().unwrap().to_string();
        assert!(err.contains("nested typed arrays"), "{err}");

        let unknown = ColumnTypeSpec::Array {
            item: Box::new(ColumnTypeSpec::Unknown),
        };
        let err = classify(&unknown).err().unwrap().to_string();
        assert!(err.contains("element type"), "{err}");
    }

    #[test]
    fn decimal_elements_beyond_rust_decimal_are_rejected() {
        let item = ColumnTypeSpec::Decimal {
            precision: None,
            scale: None,
        };
        assert!(typed_element(&rmpv::Value::String("2E+40".into()), &item).is_err());
    }

    #[test]
    fn decimal_elements_that_would_round_are_rejected() {
        let item = ColumnTypeSpec::Decimal {
            precision: None,
            scale: None,
        };
        for s in [
            "1.00000000000000000000000000001",
            "1.00000000000000000000000000001E+1",
        ] {
            assert!(
                typed_element(&rmpv::Value::String(s.into()), &item).is_err(),
                "{s}"
            );
        }
    }

    #[test]
    fn decimal_elements_with_trailing_zeros_are_exact() {
        let item = ColumnTypeSpec::Decimal {
            precision: None,
            scale: None,
        };
        let cases = [
            ("1.0000000000000000000000000000", "1"),
            ("1.000000000000000000000000000000", "1"),
            ("1.50000000000000000000000000000", "1.5"),
            ("1.5E-3", "0.0015"),
            ("1.500E-27", "0.0000000000000000000000000015"),
            ("-0", "0"),
            // Zero is exact beyond rust_decimal's ±28 exponent range.
            ("0E-30", "0"),
            ("0E+40", "0"),
        ];
        for (s, expected) in cases {
            let value = typed_element(&rmpv::Value::String(s.into()), &item).unwrap();
            let Value::Decimal(Some(d)) = value else {
                panic!("{s}: {value:?}");
            };
            assert_eq!(d.normalize(), expected.parse::<Decimal>().unwrap(), "{s}");
        }
    }

    #[test]
    fn decimal_elements_accept_exponents() {
        let item = ColumnTypeSpec::Decimal {
            precision: None,
            scale: None,
        };
        let value = typed_element(&rmpv::Value::String("1E+2".into()), &item).unwrap();
        assert_eq!(value, Value::Decimal(Some(Box::new(Decimal::from(100)))));
    }

    #[test]
    fn decimal_elements_accept_unsigned_integers() {
        let item = ColumnTypeSpec::Decimal {
            precision: None,
            scale: None,
        };
        for n in [1_u64 << 63, u64::MAX] {
            let value = typed_element(&rmpv::Value::Integer(n.into()), &item).unwrap();
            assert_eq!(
                value,
                Value::Decimal(Some(Box::new(Decimal::from(n)))),
                "{n}"
            );
        }
    }

    #[test]
    fn json_string_elements_match_how_they_were_inserted() {
        for item in [ColumnTypeSpec::Json, ColumnTypeSpec::JsonBinary] {
            let array = ColumnTypeSpec::Array {
                item: Box::new(item.clone()),
            };
            let cases = [
                (r#""hello""#, serde_json::json!("hello")),
                (r#"{"a": 1}"#, serde_json::json!({"a": 1})),
                // Text that is not JSON is a plain string
                ("hello", serde_json::json!("hello")),
                ("{oops", serde_json::json!("{oops")),
            ];
            for (text, expected) in cases {
                let expected = Value::Json(Some(Box::new(expected)));
                let operand = typed_element(&rmpv::Value::String(text.into()), &item).unwrap();
                assert_eq!(operand, expected, "{text}");
                let stored = bind_value(&rmpv::Value::Array(vec![text.into()]), &array);
                assert_eq!(
                    stored,
                    Value::Array(ArrayType::Json, Some(Box::new(vec![expected])))
                );
            }
            // Non-string elements convert as before.
            let map = rmpv::Value::Map(vec![("a".into(), 1.into())]);
            assert_eq!(
                typed_element(&map, &item).unwrap(),
                Value::Json(Some(Box::new(serde_json::json!({"a": 1}))))
            );
        }
    }

    #[test]
    fn mismatched_elements_are_rejected() {
        let cases = [
            (ColumnTypeSpec::BigInteger, rmpv::Value::String("1".into())),
            (ColumnTypeSpec::BigInteger, rmpv::Value::Boolean(true)),
            (ColumnTypeSpec::Boolean, rmpv::Value::Integer(1.into())),
            (
                ColumnTypeSpec::Uuid,
                rmpv::Value::String("not-a-uuid".into()),
            ),
            (
                ColumnTypeSpec::Date,
                rmpv::Value::String("2024-01-01T10:00:00Z".into()),
            ),
            (ColumnTypeSpec::Double, rmpv::Value::F64(f64::NAN)),
            // 2^53 + 1 would round to 2^53 as a double.
            (
                ColumnTypeSpec::Double,
                rmpv::Value::Integer(((1_i64 << 53) + 1).into()),
            ),
            (
                ColumnTypeSpec::Double,
                rmpv::Value::Integer((-(1_i64 << 53) - 1).into()),
            ),
            (
                ColumnTypeSpec::Enum {
                    name: "e".into(),
                    values: vec!["a".into()],
                },
                rmpv::Value::String("b".into()),
            ),
        ];
        for (item, value) in cases {
            assert!(
                typed_element(&value, &item).is_err(),
                "{value} should not convert to {item:?}"
            );
        }
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn float_elements_accept_exactly_representable_integers() {
        for n in [0_i64, 3, 1 << 53, -(1 << 53)] {
            let value = typed_element(&rmpv::Value::Integer(n.into()), &ColumnTypeSpec::Double);
            assert_eq!(value.unwrap(), Value::Double(Some(n as f64)), "{n}");
        }
    }

    #[test]
    fn narrow_integer_overrides_reject_out_of_range_elements() {
        let ints = |ns: &[Option<i64>]| {
            let elements = ns.iter().map(|n| Value::BigInt(*n)).collect();
            Value::Array(ArrayType::BigInt, Some(Box::new(elements)))
        };
        assert!(check_pg_operand_range(&ints(&[Some(1), Some(-32768)]), "SMALLINT[]").is_ok());
        let err = check_pg_operand_range(&ints(&[Some(1), Some(100_000)]), "smallint[]")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("100000") && err.contains("smallint[]"),
            "{err}"
        );

        let over_i32 = ints(&[Some(i64::from(i32::MAX) + 1)]);
        assert!(check_pg_operand_range(&over_i32, "INTEGER[]").is_err());
        assert!(check_pg_operand_range(&over_i32, "INT4[]").is_err());
        assert!(check_pg_operand_range(&over_i32, "BIGINT[]").is_ok());
        // NULL elements are never out of range.
        assert!(check_pg_operand_range(&ints(&[None]), "SMALLINT[]").is_ok());
    }

    #[test]
    fn real_overrides_reject_out_of_range_elements() {
        let floats = |xs: &[f64]| {
            let elements = xs.iter().map(|x| Value::Double(Some(*x))).collect();
            Value::Array(ArrayType::Double, Some(Box::new(elements)))
        };
        let ok = floats(&[3.0e38, -3.0e38, 0.0, -0.0, 1.5, 1e-40]);
        assert!(check_pg_operand_range(&ok, "REAL[]").is_ok());
        assert!(check_pg_operand_range(&ok, "float4[]").is_ok());
        for x in [1e300, -1e300, 1e-300] {
            let err = check_pg_operand_range(&floats(&[1.0, x]), "REAL[]")
                .unwrap_err()
                .to_string();
            assert!(err.contains("REAL[]"), "{err}");
            assert!(check_pg_operand_range(&floats(&[x]), "DOUBLE PRECISION[]").is_ok());
        }

        // FLOAT(24) is real, so it gets the same check.
        let real = pg_array_type(&ColumnTypeSpec::Double, Some("FLOAT(24)[]")).unwrap();
        assert_eq!(real, "REAL[]");
        assert!(check_pg_operand_range(&floats(&[1e300]), &real).is_err());
    }

    #[test]
    fn null_elements_bind_as_typed_nulls() {
        let value = typed_element(&rmpv::Value::Nil, &ColumnTypeSpec::Uuid).unwrap();
        assert_eq!(value, Value::Uuid(None));
    }
}
