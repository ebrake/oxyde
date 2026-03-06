//! rmpv::Value to sea_query Value conversion utilities

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use sea_query::{Expr, SimpleExpr, Value};

use crate::error::{QueryError, Result};
use crate::utils::identifier::ColumnIdent;

/// Convert rmpv value to sea_query Value without type hint (legacy behavior)
pub fn rmpv_to_value(value: &rmpv::Value) -> Value {
    rmpv_to_value_typed(value, None)
}

/// Convert rmpv value to sea_query Value with optional type hint from col_types.
///
/// When `col_type` is provided, parses string values into appropriate types:
/// - "datetime" -> Value::ChronoDateTime (parses ISO format string)
/// - "date" -> Value::ChronoDate
/// - "time" -> Value::ChronoTime
/// - "uuid" -> Value::String (UUID stored as string, DB handles it)
/// - "decimal" -> Value::String (Decimal stored as string, DB handles it)
pub fn rmpv_to_value_typed(value: &rmpv::Value, col_type: Option<&str>) -> Value {
    match value {
        rmpv::Value::Nil => Value::String(None),
        rmpv::Value::Boolean(b) => Value::Bool(Some(*b)),
        rmpv::Value::Integer(n) => {
            if let Some(i) = n.as_i64() {
                Value::BigInt(Some(i))
            } else if let Some(u) = n.as_u64() {
                // u64 that doesn't fit i64 — store as string
                Value::String(Some(Box::new(u.to_string())))
            } else {
                Value::String(Some(Box::new(n.to_string())))
            }
        }
        rmpv::Value::F32(f) => Value::Double(Some(f64::from(*f))),
        rmpv::Value::F64(f) => Value::Double(Some(*f)),
        rmpv::Value::String(s) => {
            let s = s.as_str().unwrap_or_default();
            // With type hint: try naive datetime/date/time formats
            if let Some(typ) = col_type {
                match typ.to_uppercase().as_str() {
                    "DATETIME" | "TIMESTAMP" | "TIMESTAMPTZ" => {
                        if let Ok(dt) = parse_datetime(s) {
                            return Value::ChronoDateTime(Some(Box::new(dt)));
                        }
                    }
                    "DATE" => {
                        if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                            return Value::ChronoDate(Some(Box::new(d)));
                        }
                    }
                    "TIME" => {
                        if let Ok(t) = parse_time(s) {
                            return Value::ChronoTime(Some(Box::new(t)));
                        }
                    }
                    _ => {}
                }
            }
            // RFC3339 is strict enough to try without type hint
            if let Some(dt) = parse_datetime_utc(s) {
                return Value::ChronoDateTimeUtc(Some(Box::new(dt)));
            }
            Value::String(Some(Box::new(s.to_string())))
        }
        rmpv::Value::Binary(b) => {
            // Native binary — pass through as bytes
            Value::Bytes(Some(Box::new(b.clone())))
        }
        rmpv::Value::Array(_) | rmpv::Value::Map(_) | rmpv::Value::Ext(_, _) => {
            // Fallback: serialize to JSON string
            Value::String(Some(Box::new(format!("{value}"))))
        }
    }
}

/// Parse tz-aware datetime string and normalize to UTC
fn parse_datetime_utc(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

/// Parse datetime string in various ISO-like formats
fn parse_datetime(s: &str) -> std::result::Result<NaiveDateTime, chrono::ParseError> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
}

/// Parse time string in various formats
fn parse_time(s: &str) -> std::result::Result<NaiveTime, chrono::ParseError> {
    NaiveTime::parse_from_str(s, "%H:%M:%S")
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"))
}

/// Convert rmpv value to SimpleExpr if it contains an expression
pub fn rmpv_to_simple_expr(value: &rmpv::Value) -> Result<Option<SimpleExpr>> {
    if let Some(expr) = map_get(value, "__expr__") {
        return Ok(Some(parse_expression(expr)?));
    }
    Ok(None)
}

/// Parse expression node from rmpv map
pub fn parse_expression(node: &rmpv::Value) -> Result<SimpleExpr> {
    let expr_type = map_get_str(node, "type")
        .ok_or_else(|| QueryError::InvalidQuery("Expression node missing type".into()))?;

    match expr_type {
        "value" => {
            let val = map_get(node, "value")
                .ok_or_else(|| QueryError::InvalidQuery("Value node missing 'value'".into()))?;
            Ok(Expr::val(rmpv_to_value(val)).into())
        }
        "column" => {
            let name = map_get_str(node, "name")
                .ok_or_else(|| QueryError::InvalidQuery("Column node missing 'name'".into()))?;
            Ok(Expr::col(ColumnIdent(name.to_string())).into())
        }
        "op" => {
            let op = map_get_str(node, "op")
                .ok_or_else(|| QueryError::InvalidQuery("Operator node missing 'op'".into()))?;
            let lhs =
                parse_expression(map_get(node, "lhs").ok_or_else(|| {
                    QueryError::InvalidQuery("Operator node missing 'lhs'".into())
                })?)?;
            let rhs =
                parse_expression(map_get(node, "rhs").ok_or_else(|| {
                    QueryError::InvalidQuery("Operator node missing 'rhs'".into())
                })?)?;
            let expr = match op {
                "add" => Expr::expr(lhs).add(rhs),
                "sub" => Expr::expr(lhs).sub(rhs),
                "mul" => Expr::expr(lhs).mul(rhs),
                "div" => Expr::expr(lhs).div(rhs),
                other => {
                    return Err(QueryError::InvalidQuery(format!(
                        "Unsupported arithmetic operator '{other}'"
                    )))
                }
            };
            Ok(expr)
        }
        "neg" => {
            let inner =
                parse_expression(map_get(node, "expr").ok_or_else(|| {
                    QueryError::InvalidQuery("Negation node missing 'expr'".into())
                })?)?;
            Ok(Expr::val(Value::BigInt(Some(0))).sub(inner))
        }
        other => Err(QueryError::InvalidQuery(format!(
            "Unsupported expression node type '{other}'"
        ))),
    }
}

// ── rmpv map helpers ──────────────────────────────────────────────────

/// Get a value from an rmpv Map by string key.
fn map_get<'a>(map: &'a rmpv::Value, key: &str) -> Option<&'a rmpv::Value> {
    match map {
        rmpv::Value::Map(pairs) => pairs.iter().find_map(|(k, v)| {
            if k.as_str() == Some(key) {
                Some(v)
            } else {
                None
            }
        }),
        _ => None,
    }
}

/// Get a string value from an rmpv Map by key.
fn map_get_str<'a>(map: &'a rmpv::Value, key: &str) -> Option<&'a str> {
    map_get(map, key).and_then(|v| v.as_str())
}
