//! Filter expression building for WHERE clauses

use std::collections::HashMap;

use oxyde_codec::{Aggregate, ColumnTypeSpec, Filter, FilterNode};
use sea_query::{Expr, Func, LikeExpr, SimpleExpr, Value};

use super::array::{
    build_array_filter, check_pg_operand_range, pg_array_type, render_col, ArrayOp,
};
use crate::aggregate::build_aggregate;
use crate::error::{QueryError, Result};
use crate::utils::{bind_value, typed_value_expr, ColumnIdent, TableIdent};
use crate::Dialect;

/// Create column expression, handling "table.column" format for joins
/// If default_table is provided and column is not already qualified, prepend it
fn make_col_expr(col_name: &str, default_table: Option<&str>) -> Expr {
    if let Some((table, column)) = col_name.split_once('.') {
        // "user.age" -> ("user", "age") -> "user"."age"
        Expr::col((
            TableIdent(table.to_string()),
            ColumnIdent(column.to_string()),
        ))
    } else if let Some(table) = default_table {
        // Qualify with default table (for JOIN queries)
        Expr::col((
            TableIdent(table.to_string()),
            ColumnIdent(col_name.to_string()),
        ))
    } else {
        // Simple column name (no JOIN)
        Expr::col(ColumnIdent(col_name.to_string()))
    }
}

/// Resolve column type hint from col_types map.
///
/// A qualified name ("user.age") has its own entry when the joined column
/// has a spec. It never falls back to the bare column name, which is the
/// base table's column of that name and may have a different type.
fn resolve_col_spec<'a>(
    col_name: &str,
    col_types: Option<&'a HashMap<String, ColumnTypeSpec>>,
) -> &'a ColumnTypeSpec {
    col_types
        .and_then(|ct| ct.get(col_name))
        .unwrap_or(&ColumnTypeSpec::Unknown)
}

/// PostgreSQL array type that a filter's array values are cast to: the
/// column's `db_type` override, since an `INTEGER[]` column has no `=` with
/// the `bigint[]` the value binds as. Overrides that array lookups cannot
/// read either (`INT[][]`) keep the plain value, as before.
fn pg_array_cast(spec: &ColumnTypeSpec, db_type: Option<&str>, dialect: Dialect) -> Option<String> {
    match (dialect, spec, db_type) {
        (Dialect::Postgres, ColumnTypeSpec::Array { item }, Some(db_type))
            if db_type.trim().ends_with("[]") =>
        {
            pg_array_type(item, Some(db_type)).ok()
        }
        _ => None,
    }
}

/// `typed_value_expr`, plus the `pg_array_cast` of an array value.
fn filter_value_expr(
    value: Value,
    spec: &ColumnTypeSpec,
    array_cast: Option<&str>,
    dialect: Dialect,
) -> Result<SimpleExpr> {
    if let (Some(array_type), Value::Array(..)) = (array_cast, &value) {
        check_pg_operand_range(&value, array_type)?;
        return Ok(Expr::cust_with_values(
            format!("$1::{array_type}"),
            vec![value],
        ));
    }
    Ok(typed_value_expr(value, spec, dialect))
}

/// Left side of LIKE / ILIKE. A `str` field stored as JSON matches the
/// string it reads back as, not its quoted JSON text; other JSON values
/// match their JSON text.
fn like_target(col: Expr, spec: &ColumnTypeSpec, dialect: Dialect) -> Expr {
    if !matches!(spec, ColumnTypeSpec::Json | ColumnTypeSpec::JsonBinary) {
        return col;
    }
    let template = match dialect {
        Dialect::Postgres => "{col} #>> '{}'",
        Dialect::Mysql => "JSON_UNQUOTE({col})",
        // SQLite does not validate JSON columns, and json_type() raises on
        // text that is not JSON, so it is checked first.
        Dialect::Sqlite => {
            "CASE WHEN json_valid({col}) THEN CASE json_type({col}) \
             WHEN 'text' THEN json_extract({col}, '$') ELSE {col} END ELSE {col} END"
        }
    };
    Expr::expr(render_col(template, &col.into(), dialect))
}

fn like_expr(filter: &Filter, pattern: String) -> Result<LikeExpr> {
    let expr = LikeExpr::new(pattern);
    let Some(escape) = filter.escape.as_deref() else {
        return Ok(expr);
    };

    let mut chars = escape.chars();
    match (chars.next(), chars.next()) {
        (Some(ch), None) => Ok(expr.escape(ch)),
        _ => Err(QueryError::InvalidQuery(
            "LIKE escape requires a single character".into(),
        )),
    }
}

/// Build filter clause from FilterNode tree.
///
/// default_table: if provided, unqualified columns will be prefixed with this table name
/// col_types: type hints for typed value conversion in filter conditions
/// aggregates: if provided (HAVING), alias references are substituted with aggregate expressions
pub fn build_filter_node(
    node: &FilterNode,
    default_table: Option<&str>,
    col_types: Option<&HashMap<String, ColumnTypeSpec>>,
    aggregates: Option<&[Aggregate]>,
    dialect: Dialect,
) -> Result<SimpleExpr> {
    match node {
        FilterNode::Condition(filter) => {
            apply_filter(filter, default_table, col_types, aggregates, dialect)
        }
        FilterNode::And { conditions } => {
            if conditions.is_empty() {
                return Err(QueryError::InvalidQuery(
                    "AND node must have at least one condition".into(),
                ));
            }
            let first = build_filter_node(
                &conditions[0],
                default_table,
                col_types,
                aggregates,
                dialect,
            )?;
            let mut result = first;
            for cond in &conditions[1..] {
                let next = build_filter_node(cond, default_table, col_types, aggregates, dialect)?;
                result = result.and(next);
            }
            Ok(result)
        }
        FilterNode::Or { conditions } => {
            if conditions.is_empty() {
                return Err(QueryError::InvalidQuery(
                    "OR node must have at least one condition".into(),
                ));
            }
            let first = build_filter_node(
                &conditions[0],
                default_table,
                col_types,
                aggregates,
                dialect,
            )?;
            let mut result = first;
            for cond in &conditions[1..] {
                let next = build_filter_node(cond, default_table, col_types, aggregates, dialect)?;
                result = result.or(next);
            }
            Ok(result)
        }
        FilterNode::Not { condition } => {
            let inner =
                build_filter_node(condition, default_table, col_types, aggregates, dialect)?;
            Ok(inner.not())
        }
    }
}

/// Apply filter to expression.
/// If `aggregates` is provided and the field matches an aggregate alias,
/// the aggregate expression (e.g. SUM("views")) is used instead of a column ref.
fn apply_filter(
    filter: &Filter,
    default_table: Option<&str>,
    col_types: Option<&HashMap<String, ColumnTypeSpec>>,
    aggregates: Option<&[Aggregate]>,
    dialect: Dialect,
) -> Result<SimpleExpr> {
    let col_name = filter.column.as_ref().unwrap_or(&filter.field);

    // HAVING: substitute aggregate alias with aggregate expression
    let agg_match =
        aggregates.and_then(|aggs| aggs.iter().find(|a| a.alias.as_deref() == Some(col_name)));

    let spec = resolve_col_spec(col_name, col_types);

    if let Some(op) = ArrayOp::from_operator(&filter.operator) {
        if agg_match.is_some() {
            return Err(QueryError::InvalidQuery(
                "Array lookups cannot filter on aggregate aliases".into(),
            ));
        }
        if matches!(spec, ColumnTypeSpec::Unknown) {
            return Err(QueryError::InvalidQuery(format!(
                "Array lookup on '{col_name}' needs its column type, which this query does not provide"
            )));
        }
        return build_array_filter(
            &make_col_expr(col_name, default_table).into(),
            op,
            &filter.value,
            spec,
            filter.db_type.as_deref(),
            dialect,
        );
    }

    let col = if let Some(agg) = agg_match {
        Expr::expr(build_aggregate(agg)?)
    } else {
        make_col_expr(col_name, default_table)
    };

    // Resolved once: an IN list would otherwise reparse db_type per value.
    let array_cast = pg_array_cast(spec, filter.db_type.as_deref(), dialect);
    let typed = |value: Value| filter_value_expr(value, spec, array_cast.as_deref(), dialect);
    let val = bind_value(&filter.value, spec);

    let expr = match filter.operator.as_str() {
        "=" => col.eq(typed(val)?),
        "!=" => col.ne(typed(val)?),
        ">" => col.gt(typed(val)?),
        ">=" => col.gte(typed(val)?),
        "<" => col.lt(typed(val)?),
        "<=" => col.lte(typed(val)?),
        "LIKE" => {
            let text = filter.value.as_str().ok_or_else(|| {
                QueryError::InvalidQuery("LIKE operator requires string value".into())
            })?;
            let col = if agg_match.is_some() {
                col
            } else {
                like_target(col, spec, dialect)
            };
            col.like(like_expr(filter, text.to_string())?)
        }
        "ILIKE" => {
            let text = filter.value.as_str().ok_or_else(|| {
                QueryError::InvalidQuery("ILIKE operator requires string value".into())
            })?;
            let lowered = text.to_lowercase();
            let lower_col = Func::lower(like_target(
                make_col_expr(col_name, default_table),
                spec,
                dialect,
            ));
            Expr::expr(lower_col).like(like_expr(filter, lowered)?)
        }
        "IN" => {
            if let rmpv::Value::Array(arr) = &filter.value {
                let values = arr
                    .iter()
                    .map(|v| typed(bind_value(v, spec)))
                    .collect::<Result<Vec<_>>>()?;
                col.is_in(values)
            } else {
                return Err(QueryError::InvalidQuery(
                    "IN operator requires array value".to_string(),
                ));
            }
        }
        "BETWEEN" => {
            if let rmpv::Value::Array(arr) = &filter.value {
                if arr.len() != 2 {
                    return Err(QueryError::InvalidQuery(
                        "BETWEEN operator requires exactly two values".to_string(),
                    ));
                }
                let start = typed(bind_value(&arr[0], spec))?;
                let end = typed(bind_value(&arr[1], spec))?;
                col.between(start, end)
            } else {
                return Err(QueryError::InvalidQuery(
                    "BETWEEN operator requires array value".to_string(),
                ));
            }
        }
        "IS NULL" => col.is_null(),
        "IS NOT NULL" => col.is_not_null(),
        op => {
            return Err(QueryError::UnsupportedOperation(format!(
                "Unsupported operator: {op}",
            )))
        }
    };

    Ok(expr)
}
