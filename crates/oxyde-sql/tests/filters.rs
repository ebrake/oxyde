//! SQL generation for filters on array and JSON columns across dialects.

use std::collections::HashMap;

use oxyde_codec::{Aggregate, AggregateOp, ColumnTypeSpec, Filter, FilterNode, QueryIR};
use oxyde_sql::{build_sql, Dialect};
use sea_query::Value;

fn array_of(item: ColumnTypeSpec) -> ColumnTypeSpec {
    ColumnTypeSpec::Array {
        item: Box::new(item),
    }
}

fn s(v: &str) -> rmpv::Value {
    rmpv::Value::String(v.into())
}

fn arr(items: Vec<rmpv::Value>) -> rmpv::Value {
    rmpv::Value::Array(items)
}

fn filter(operator: &str, value: rmpv::Value) -> FilterNode {
    FilterNode::Condition(Filter {
        field: "tags".into(),
        operator: operator.into(),
        value,
        column: None,
        escape: None,
    })
}

/// One condition on `posts.tags`, whose spec is `spec`.
struct Condition {
    operator: &'static str,
    value: rmpv::Value,
    spec: ColumnTypeSpec,
    db_type: Option<&'static str>,
}

impl Condition {
    fn new(operator: &'static str, value: rmpv::Value, spec: ColumnTypeSpec) -> Self {
        Self {
            operator,
            value,
            spec,
            db_type: None,
        }
    }

    fn ir(&self) -> QueryIR {
        QueryIR {
            table: "posts".into(),
            cols: Some(vec!["id".into()]),
            column_types: Some(HashMap::from([("tags".into(), self.spec.clone())])),
            column_db_types: self
                .db_type
                .map(|db_type| HashMap::from([("tags".into(), db_type.into())])),
            filter_tree: Some(filter(self.operator, self.value.clone())),
            ..Default::default()
        }
    }

    fn build(&self, dialect: Dialect) -> (String, Vec<Value>) {
        build_sql(&self.ir(), dialect).unwrap()
    }

    fn error(&self, dialect: Dialect) -> String {
        build_sql(&self.ir(), dialect).unwrap_err().to_string()
    }
}

fn array_override(
    operator: &'static str,
    value: rmpv::Value,
    item: ColumnTypeSpec,
    db_type: &'static str,
) -> Condition {
    let mut condition = Condition::new(operator, value, array_of(item));
    condition.db_type = Some(db_type);
    condition
}

fn int_array_override(operator: &'static str, value: rmpv::Value) -> Condition {
    array_override(operator, value, ColumnTypeSpec::BigInteger, "INTEGER[]")
}

#[test]
fn postgres_casts_array_values_to_the_override_for_every_operator() {
    let ints = || arr(vec![1.into(), 2.into()]);
    for (operator, sql_op) in [("=", "="), ("!=", "<>"), ("<", "<"), (">=", ">=")] {
        let (sql, params) = int_array_override(operator, ints()).build(Dialect::Postgres);
        assert!(
            sql.ends_with(&format!(r#""tags" {sql_op} ($1::INTEGER[])"#)),
            "{sql}"
        );
        assert_eq!(params.len(), 1);
    }
    let (sql, params) =
        int_array_override("IN", arr(vec![ints(), arr(vec![3.into()])])).build(Dialect::Postgres);
    assert!(
        sql.ends_with(r#""tags" IN ($1::INTEGER[], $2::INTEGER[])"#),
        "{sql}"
    );
    assert_eq!(params.len(), 2);

    // Other dialects store arrays as JSON: no cast.
    for dialect in [Dialect::Mysql, Dialect::Sqlite] {
        let (sql, _) = int_array_override("=", ints()).build(dialect);
        assert!(!sql.contains("INTEGER[]"), "{dialect:?}: {sql}");
    }
}

#[test]
fn values_the_array_type_cannot_hold_equal_nothing() {
    let too_big = || arr(vec![1.into(), (1_i64 << 31).into()]);

    // `=` is false and `!=` true, NULL for a NULL column either way.
    let (sql, params) = int_array_override("=", too_big()).build(Dialect::Postgres);
    assert!(
        sql.ends_with(r#"(CASE WHEN "tags" IS NULL THEN NULL ELSE FALSE END)"#),
        "{sql}"
    );
    assert!(params.is_empty());
    let (sql, _) = int_array_override("!=", too_big()).build(Dialect::Postgres);
    assert!(
        sql.ends_with(r#"(CASE WHEN "tags" IS NULL THEN NULL ELSE TRUE END)"#),
        "{sql}"
    );

    // IN drops them, and matches nothing when none are left.
    let (sql, params) = int_array_override("IN", arr(vec![arr(vec![1.into()]), too_big()]))
        .build(Dialect::Postgres);
    assert!(sql.ends_with(r#""tags" IN ($1::INTEGER[])"#), "{sql}");
    assert_eq!(params.len(), 1);
    let (sql, _) = int_array_override("IN", arr(vec![too_big()])).build(Dialect::Postgres);
    assert!(sql.ends_with("ELSE FALSE END)"), "{sql}");

    // An ordering against one has no clear answer.
    for operator in ["<", ">="] {
        let err = int_array_override(operator, too_big()).error(Dialect::Postgres);
        assert!(
            err.contains("2147483648 is out of range for INTEGER[]"),
            "{err}"
        );
    }

    // NaN and the infinities fit REAL; 1e300 does not.
    let real = |x: f64| {
        array_override(
            "=",
            arr(vec![rmpv::Value::F64(x)]),
            ColumnTypeSpec::Double,
            "REAL[]",
        )
    };
    for x in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let (sql, _) = real(x).build(Dialect::Postgres);
        assert!(sql.ends_with(r#""tags" = ($1::REAL[])"#), "{x}: {sql}");
    }
    let (sql, _) = real(1e300).build(Dialect::Postgres);
    assert!(sql.ends_with("ELSE FALSE END)"), "{sql}");
}

#[test]
fn unreadable_array_overrides_are_an_error() {
    let ints = || arr(vec![1.into()]);
    for db_type in ["INTEGER[][]", "VARCHAR(10[]"] {
        let mut condition = int_array_override("=", ints());
        condition.db_type = Some(db_type);
        let err = condition.error(Dialect::Postgres);
        assert!(err.contains(db_type), "{err}");
    }
    let json = array_override("=", arr(vec![s("a")]), ColumnTypeSpec::Json, "JSON[]");
    assert!(json.error(Dialect::Postgres).contains("use jsonb[]"));

    // Only filters that cast a value need the override.
    let mut isnull = int_array_override("IS NULL", rmpv::Value::Nil);
    isnull.db_type = Some("VARCHAR(10[]");
    let (sql, _) = isnull.build(Dialect::Postgres);
    assert!(sql.ends_with(r#""tags" IS NULL"#), "{sql}");
}

#[test]
fn array_values_without_an_array_override_are_not_cast() {
    let ints = || arr(vec![1.into()]);
    let (sql, _) =
        Condition::new("=", ints(), array_of(ColumnTypeSpec::BigInteger)).build(Dialect::Postgres);
    assert!(sql.ends_with(r#""tags" = $1"#), "{sql}");
    let mut condition = int_array_override("=", ints());
    condition.db_type = Some("JSONB");
    let (sql, _) = condition.build(Dialect::Postgres);
    assert!(sql.ends_with(r#""tags" = $1"#), "{sql}");
}

#[test]
fn joined_columns_use_their_own_db_type() {
    // The base `tags` has no override; the joined `a.tags` is SMALLINT[].
    let ir = QueryIR {
        table: "posts".into(),
        cols: Some(vec!["id".into()]),
        column_types: Some(HashMap::from([
            ("tags".into(), array_of(ColumnTypeSpec::BigInteger)),
            ("a.tags".into(), array_of(ColumnTypeSpec::BigInteger)),
        ])),
        column_db_types: Some(HashMap::from([("a.tags".into(), "SMALLINT[]".into())])),
        filter_tree: Some(FilterNode::And {
            conditions: vec![
                filter("=", arr(vec![1.into()])),
                FilterNode::Condition(Filter {
                    field: "tags".into(),
                    operator: "=".into(),
                    value: arr(vec![1.into()]),
                    column: Some("a.tags".into()),
                    escape: None,
                }),
            ],
        }),
        ..Default::default()
    };
    let (sql, _) = build_sql(&ir, Dialect::Postgres).unwrap();
    assert!(
        sql.contains(r#""tags" = $1 AND "a"."tags" = ($2::SMALLINT[])"#),
        "{sql}"
    );
}

#[test]
fn like_on_json_columns_matches_the_unquoted_string() {
    // A `str` field stored as JSON matches the string it reads back as, not
    // its quoted JSON text.
    for spec in [ColumnTypeSpec::Json, ColumnTypeSpec::JsonBinary] {
        let case_sensitive = Condition::new("LIKE", s("%x%"), spec.clone());
        let case_insensitive = Condition::new("ILIKE", s("%x%"), spec);

        let (sql, _) = case_sensitive.build(Dialect::Postgres);
        assert!(sql.contains(r#"("tags" #>> '{}') LIKE $1"#), "{sql}");
        let (sql, _) = case_insensitive.build(Dialect::Postgres);
        assert!(sql.contains(r#"LOWER("tags" #>> '{}') LIKE $1"#), "{sql}");

        let (sql, _) = case_sensitive.build(Dialect::Mysql);
        assert!(sql.contains("(JSON_UNQUOTE(`tags`)) LIKE ?"), "{sql}");

        let (sql, _) = case_sensitive.build(Dialect::Sqlite);
        assert!(
            sql.contains(
                r#"(CASE WHEN json_valid("tags", 9) THEN json_extract("tags", '$') ELSE "tags" END) LIKE ?"#
            ),
            "{sql}"
        );
    }

    // Text columns are untouched.
    let (sql, _) = Condition::new("LIKE", s("%x%"), ColumnTypeSpec::Text).build(Dialect::Postgres);
    assert!(sql.contains(r#""tags" LIKE $1"#), "{sql}");
}

#[test]
fn having_uses_the_aggregate_not_a_same_named_column() {
    // `tags` is both a JSON array column with an override and the alias of
    // MAX("name"): the HAVING condition is on the aggregate.
    for (operator, expected) in [
        ("LIKE", r#"MAX("name") LIKE $1"#),
        ("ILIKE", r#"LOWER(MAX("name")) LIKE $1"#),
        ("=", r#"MAX("name") = $1"#),
    ] {
        let ir = QueryIR {
            table: "posts".into(),
            cols: Some(vec!["status".into()]),
            column_types: Some(HashMap::from([(
                "tags".into(),
                array_of(ColumnTypeSpec::Json),
            )])),
            column_db_types: Some(HashMap::from([("tags".into(), "JSONB[]".into())])),
            aggregates: Some(vec![Aggregate {
                op: AggregateOp::Max,
                field: Some("name".into()),
                alias: Some("tags".into()),
                distinct: None,
            }]),
            group_by: Some(vec!["status".into()]),
            having: Some(filter(operator, s("x"))),
            ..Default::default()
        };
        let (sql, _) = build_sql(&ir, Dialect::Postgres).unwrap();
        assert!(sql.contains(&format!("HAVING {expected}")), "{sql}");
    }
}
