//! PostgreSQL array types of columns with an array `db_type` override: the
//! type a filter value is cast to (`INTEGER[]` where the spec alone would say
//! `BIGINT[]`), and which values it can hold.

use oxyde_codec::ColumnTypeSpec;
use sea_query::Value;

use crate::error::{QueryError, Result};
use crate::utils::bind::quote_pg_type_path;

/// PostgreSQL array type of a column whose `db_type` is an array override,
/// for casting values to it. Size and precision modifiers are dropped
/// (`VARCHAR(5)[]` casts to `VARCHAR[]`) so the cast can neither truncate
/// nor round a value into a match.
///
/// The override is spliced into the SQL text. It comes from the model
/// definition, never from query input, and `pg_array_base` only passes
/// plain type names; an override it cannot read is an error rather than an
/// uncast value, which would fail later as a confusing "operator does not
/// exist".
pub(super) fn pg_array_type(item: &ColumnTypeSpec, db_type: &str) -> Result<String> {
    if let ColumnTypeSpec::Enum { name, .. } = item {
        return Ok(format!("{}[]", quote_pg_type_path(name)));
    }
    let db_type = db_type.trim();
    let base = pg_array_base(db_type).ok_or_else(|| {
        QueryError::UnsupportedOperation(format!(
            "db_type '{db_type}' is not a one-dimensional array of a plain type, \
             such as INTEGER[] or VARCHAR(50)[]"
        ))
    })?;
    if base.eq_ignore_ascii_case("JSON") {
        // json has no equality operator, so json[] values cannot compare.
        return Err(QueryError::UnsupportedOperation(
            "json[] columns have no equality operator; use jsonb[]".into(),
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

/// The first element of an array value that the column's array type cannot
/// hold, shown as text: an integer outside a `SMALLINT[]` / `INTEGER[]`
/// range, or a finite float that overflows or underflows `REAL[]`. Casting
/// such a value would fail the query ("smallint out of range", "value out
/// of range: overflow"). NaN and ±Infinity fit `REAL`, as they do in
/// PostgreSQL.
pub(super) fn out_of_range_element(value: &Value, array_type: &str) -> Option<String> {
    let base = array_type.strip_suffix("[]")?;
    let Value::Array(_, Some(elements)) = value else {
        return None;
    };
    let int_range = pg_int_range(base);
    let real = matches!(base.to_ascii_uppercase().as_str(), "REAL" | "FLOAT4");
    elements
        .iter()
        .find_map(|element| match (element, int_range) {
            (Value::BigInt(Some(n)), Some((min, max))) if !(min..=max).contains(n) => {
                Some(n.to_string())
            }
            #[allow(clippy::cast_possible_truncation)]
            (Value::Double(Some(x)), _)
                if real
                    && x.is_finite()
                    && (!(*x as f32).is_finite() || (*x as f32 == 0.0 && *x != 0.0)) =>
            {
                Some(x.to_string())
            }
            _ => None,
        })
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
        assert_eq!(pg_array_type(&item, "INTEGER(4)[]").unwrap(), "INTEGER[]");
        assert_eq!(pg_array_type(&item, " smallint[] ").unwrap(), "smallint[]");
        for db_type in ["INT[][]", "VARCHAR(10[]", "INT; DROP TABLE x[]", "[]"] {
            let err = pg_array_type(&item, db_type).unwrap_err().to_string();
            assert!(err.contains(db_type), "{db_type}: {err}");
        }
        let err = pg_array_type(&ColumnTypeSpec::Json, "JSON[]")
            .unwrap_err()
            .to_string();
        assert!(err.contains("use jsonb[]"), "{err}");
        // Enum arrays use the enum's own type.
        let status = ColumnTypeSpec::Enum {
            name: "app.status".into(),
            values: vec![],
        };
        assert_eq!(
            pg_array_type(&status, "status[]").unwrap(),
            r#""app"."status"[]"#
        );
    }

    fn ints(ns: &[Option<i64>]) -> Value {
        let elements = ns.iter().map(|n| Value::BigInt(*n)).collect();
        Value::Array(ArrayType::BigInt, Some(Box::new(elements)))
    }

    fn floats(xs: &[f64]) -> Value {
        let elements = xs.iter().map(|x| Value::Double(Some(*x))).collect();
        Value::Array(ArrayType::Double, Some(Box::new(elements)))
    }

    #[test]
    fn narrow_integer_types_hold_only_their_range() {
        assert_eq!(
            out_of_range_element(&ints(&[Some(1), Some(-32768)]), "SMALLINT[]"),
            None
        );
        assert_eq!(
            out_of_range_element(&ints(&[Some(1), Some(100_000)]), "smallint[]").as_deref(),
            Some("100000")
        );
        let over_i32 = ints(&[Some(i64::from(i32::MAX) + 1)]);
        assert!(out_of_range_element(&over_i32, "INTEGER[]").is_some());
        assert!(out_of_range_element(&over_i32, "INT4[]").is_some());
        assert_eq!(out_of_range_element(&over_i32, "BIGINT[]"), None);
        // NULL elements are never out of range.
        assert_eq!(out_of_range_element(&ints(&[None]), "SMALLINT[]"), None);
    }

    #[test]
    fn real_holds_its_range_and_non_finite_values() {
        let fits = floats(&[3.0e38, -3.0e38, 0.0, -0.0, 1.5, 1e-40]);
        assert_eq!(out_of_range_element(&fits, "REAL[]"), None);
        assert_eq!(out_of_range_element(&fits, "float4[]"), None);
        // PostgreSQL's real holds NaN and the infinities.
        let non_finite = floats(&[f64::NAN, f64::INFINITY, f64::NEG_INFINITY]);
        assert_eq!(out_of_range_element(&non_finite, "REAL[]"), None);
        for x in [1e300, -1e300, 1e-300] {
            assert!(out_of_range_element(&floats(&[1.0, x]), "REAL[]").is_some());
            assert_eq!(
                out_of_range_element(&floats(&[x]), "DOUBLE PRECISION[]"),
                None
            );
        }

        // FLOAT(24) is real, so it gets the same check.
        let real = pg_array_type(&ColumnTypeSpec::Double, "FLOAT(24)[]").unwrap();
        assert_eq!(real, "REAL[]");
        assert!(out_of_range_element(&floats(&[1e300]), &real).is_some());
    }
}
