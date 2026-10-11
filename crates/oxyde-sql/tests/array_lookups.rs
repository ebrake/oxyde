//! SQL generation for array lookups (ARRAY_CONTAINS / ARRAY_CONTAINED_BY /
//! ARRAY_OVERLAP) across dialects.

use std::collections::HashMap;

use oxyde_codec::{ColumnTypeSpec, Filter, FilterNode, JoinColumn, JoinSpec, QueryIR};
use oxyde_sql::{build_sql, Dialect, QueryError};
use sea_query::value::ArrayType;
use sea_query::Value;

const OPS: [&str; 3] = ["ARRAY_CONTAINS", "ARRAY_CONTAINED_BY", "ARRAY_OVERLAP"];

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

struct Lookup {
    column: Option<&'static str>,
    operator: &'static str,
    value: rmpv::Value,
    /// Spec of the filtered column, keyed in `column_types` by its name.
    spec: ColumnTypeSpec,
    db_type: Option<&'static str>,
    /// Other `column_types` entries.
    table_types: HashMap<String, ColumnTypeSpec>,
}

impl Lookup {
    fn new(operator: &'static str, value: rmpv::Value, spec: ColumnTypeSpec) -> Self {
        Self {
            column: None,
            operator,
            value,
            spec,
            db_type: None,
            table_types: HashMap::new(),
        }
    }

    fn ir(&self) -> QueryIR {
        let mut column_types = self.table_types.clone();
        column_types.insert(self.column.unwrap_or("tags").into(), self.spec.clone());
        QueryIR {
            table: "posts".into(),
            cols: Some(vec!["id".into()]),
            column_types: Some(column_types),
            filter_tree: Some(FilterNode::Condition(Filter {
                field: "tags".into(),
                operator: self.operator.into(),
                value: self.value.clone(),
                column: self.column.map(Into::into),
                escape: None,
                db_type: self.db_type.map(Into::into),
            })),
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

#[test]
fn postgres_typed_arrays_use_native_operators_with_cast_operands() {
    for (op, sql_op) in OPS.iter().zip(["@>", "<@", "&&"]) {
        let (sql, params) = Lookup::new(op, arr(vec![s("rust")]), array_of(ColumnTypeSpec::Text))
            .build(Dialect::Postgres);
        assert!(
            sql.ends_with(&format!(r#"WHERE ("tags" {sql_op} $1::TEXT[])"#)),
            "{sql}"
        );
        assert_eq!(
            params,
            [Value::Array(
                ArrayType::String,
                Some(Box::new(vec![Value::String(Some(Box::new("rust".into())))]))
            )]
        );
    }
}

#[test]
fn postgres_casts_follow_the_real_column_type_without_modifiers() {
    let cases = [
        (array_of(ColumnTypeSpec::BigInteger), None, "BIGINT[]"),
        (
            array_of(ColumnTypeSpec::BigInteger),
            Some("INTEGER[]"),
            "INTEGER[]",
        ),
        (
            array_of(ColumnTypeSpec::String { length: Some(20) }),
            None,
            "VARCHAR[]",
        ),
        (
            array_of(ColumnTypeSpec::String { length: Some(20) }),
            Some("VARCHAR(20)[]"),
            "VARCHAR[]",
        ),
        (
            array_of(ColumnTypeSpec::String { length: Some(3) }),
            Some("CHAR(3)[]"),
            "BPCHAR[]",
        ),
        (
            array_of(ColumnTypeSpec::Double),
            Some("FLOAT(24)[]"),
            "REAL[]",
        ),
        (
            array_of(ColumnTypeSpec::Double),
            Some("FLOAT(53)[]"),
            "DOUBLE PRECISION[]",
        ),
        (
            array_of(ColumnTypeSpec::Decimal {
                precision: Some(10),
                scale: Some(2),
            }),
            Some("NUMERIC(10,2)[]"),
            "NUMERIC[]",
        ),
        (array_of(ColumnTypeSpec::DateTimeUtc), None, "TIMESTAMPTZ[]"),
        (array_of(ColumnTypeSpec::Uuid), None, "UUID[]"),
        (array_of(ColumnTypeSpec::JsonBinary), None, "JSONB[]"),
        (
            array_of(ColumnTypeSpec::Enum {
                name: "app.post_status".into(),
                values: vec!["draft".into()],
            }),
            None,
            r#""app"."post_status"[]"#,
        ),
    ];
    for (spec, db_type, cast) in cases {
        let mut lookup = Lookup::new("ARRAY_CONTAINS", arr(vec![]), spec);
        lookup.db_type = db_type;
        let (sql, _) = lookup.build(Dialect::Postgres);
        assert!(
            sql.ends_with(&format!(r#"("tags" @> $1::{cast})"#)),
            "{sql}"
        );
    }
}

#[test]
fn postgres_null_operand_elements_switch_to_null_aware_unnest() {
    let (sql, params) = Lookup::new(
        "ARRAY_CONTAINS",
        arr(vec![s("a"), rmpv::Value::Nil]),
        array_of(ColumnTypeSpec::Text),
    )
    .build(Dialect::Postgres);
    assert!(
        sql.contains(r#"CASE WHEN "tags" IS NULL THEN NULL"#),
        "{sql}"
    );
    assert!(sql.contains("unnest($1::TEXT[])"), "{sql}");
    // The column is read once, by a holder outside the unnest subqueries,
    // so a column named like their aliases cannot be shadowed.
    assert!(
        sql.contains(r#"FROM (SELECT "tags" AS "__oxyde_c") AS "__oxyde_h")"#),
        "{sql}"
    );
    assert!(sql.contains(r#"unnest("__oxyde_h"."__oxyde_c")"#), "{sql}");
    assert_eq!(sql.matches(r#""tags""#).count(), 2, "{sql}");
    assert!(sql.contains("IS NOT DISTINCT FROM"), "{sql}");
    assert!(!sql.contains("@>"), "{sql}");
    assert_eq!(params.len(), 1);
}

#[test]
fn postgres_json_columns_compare_whole_elements_behind_a_type_guard() {
    let spec = ColumnTypeSpec::JsonBinary;
    let (sql, params) =
        Lookup::new("ARRAY_CONTAINS", arr(vec![1.into()]), spec.clone()).build(Dialect::Postgres);
    // GIN-usable containment prefilter, then the exact check.
    assert!(sql.contains(r#"("tags" @> $1::jsonb AND CASE"#), "{sql}");
    assert!(
        sql.contains(r#"WHEN jsonb_typeof("tags") = 'array' THEN"#),
        "{sql}"
    );
    assert!(sql.contains("jsonb_array_elements($2::jsonb)"), "{sql}");
    assert!(
        sql.contains(r#"jsonb_array_elements("__oxyde_h"."__oxyde_c")"#),
        "{sql}"
    );
    assert!(
        sql.contains(r#"FROM (SELECT "tags" AS "__oxyde_c") AS "__oxyde_h")"#),
        "{sql}"
    );
    assert!(sql.contains("ELSE FALSE END"), "{sql}");
    assert_eq!(params.len(), 2);
    assert!(matches!(&params[0], Value::Json(Some(j)) if j.to_string() == "[1]"));

    for op in ["ARRAY_CONTAINED_BY", "ARRAY_OVERLAP"] {
        let (sql, _) = Lookup::new(op, arr(vec![1.into()]), spec.clone()).build(Dialect::Postgres);
        assert!(!sql.contains("@>"), "{sql}");
        assert!(sql.contains("jsonb_array_elements"), "{sql}");
    }
}

#[test]
fn postgres_json_db_type_casts_the_column_to_jsonb() {
    let mut lookup = Lookup::new("ARRAY_OVERLAP", arr(vec![1.into()]), ColumnTypeSpec::Json);
    lookup.db_type = Some("JSON");
    let (sql, _) = lookup.build(Dialect::Postgres);
    assert!(sql.contains(r#"jsonb_typeof(("tags")::jsonb)"#), "{sql}");
    assert!(
        sql.contains(r#"(SELECT ("tags")::jsonb AS "__oxyde_c") AS "__oxyde_h""#),
        "{sql}"
    );
}

#[test]
fn mysql_walks_both_arrays_with_json_table() {
    for op in OPS {
        let (sql, params) = Lookup::new(op, arr(vec![s("rust")]), array_of(ColumnTypeSpec::Text))
            .build(Dialect::Mysql);
        assert!(
            sql.contains("WHEN JSON_TYPE(`tags`) = 'ARRAY' THEN"),
            "{sql}"
        );
        // The column is read once, by a holder outside the JSON_TABLE
        // subqueries, so a column named like their columns (`oxyde_v`)
        // cannot be shadowed.
        assert!(
            sql.contains(
                "FROM JSON_TABLE(`tags`, '$' COLUMNS (oxyde_c JSON PATH '$')) AS oxyde_h)"
            ),
            "{sql}"
        );
        assert!(
            sql.contains("JSON_TABLE(oxyde_h.oxyde_c, '$[*]' COLUMNS (oxyde_v JSON PATH '$'))"),
            "{sql}"
        );
        assert!(
            sql.contains("(SELECT CAST(? AS JSON) AS oxyde_j) AS oxyde_a CROSS JOIN JSON_TABLE(oxyde_a.oxyde_j, '$[*]'"),
            "{sql}"
        );
        // Elements are JSON_TABLE columns, never re-extracted per pair, and
        // compare null-safe: JSON null elements read as SQL NULL.
        assert!(
            sql.contains("WHERE oxyde_l.oxyde_v <=> oxyde_r.oxyde_v"),
            "{sql}"
        );
        assert!(!sql.contains("JSON_EXTRACT"), "{sql}");
        assert!(!sql.contains("CAST(JSON_UNQUOTE"), "{sql}");
        // Containment leads with a JSON_CONTAINS prefilter, which binds the
        // operand a second time; overlap binds it once.
        let prefilter = match op {
            "ARRAY_CONTAINS" => Some("WHERE (JSON_CONTAINS(`tags`, CAST(? AS JSON)) AND CASE"),
            "ARRAY_CONTAINED_BY" => Some("WHERE (JSON_CONTAINS(CAST(? AS JSON), `tags`) AND CASE"),
            _ => None,
        };
        let binds = if let Some(prefilter) = prefilter {
            assert!(sql.contains(prefilter), "{sql}");
            2
        } else {
            assert!(!sql.contains("JSON_CONTAINS"), "{sql}");
            1
        };
        assert_eq!(sql.matches('?').count(), binds, "{sql}");
        // NULL check, type check, holder, and the prefilter if any.
        assert_eq!(sql.matches("`tags`").count(), 2 + binds, "{sql}");
        assert_eq!(params.len(), binds);
    }
}

#[test]
fn mysql_compares_typed_decimals_as_decimal() {
    let (sql, params) = Lookup::new(
        "ARRAY_CONTAINS",
        arr(vec![s("1.5")]),
        array_of(ColumnTypeSpec::Decimal {
            precision: None,
            scale: None,
        }),
    )
    .build(Dialect::Mysql);
    assert!(sql.contains("AS DECIMAL(65,30))"), "{sql}");
    assert!(!sql.contains("AS DOUBLE"), "{sql}");
    // No JSON_CONTAINS prefilter: it would compare "1.50" and "1.5" as text.
    assert!(!sql.contains("JSON_CONTAINS"), "{sql}");
    // The operand is bound once (the guard's regex has `?` of its own).
    assert_eq!(sql.matches("CAST(? AS JSON)").count(), 1, "{sql}");
    assert_eq!(params.len(), 1);
}

#[test]
fn mysql_decimal_cast_is_guarded_against_values_it_cannot_take() {
    let (sql, _) = Lookup::new(
        "ARRAY_CONTAINS",
        arr(vec![s("1.5")]),
        array_of(ColumnTypeSpec::Decimal {
            precision: None,
            scale: None,
        }),
    )
    .build(Dialect::Mysql);
    let l = "JSON_UNQUOTE(oxyde_l.oxyde_v)";
    // Each guard is a CASE branch before the cast, in this order.
    let guards = [
        // JSON null elements read as SQL NULL and match only each other.
        "CASE WHEN oxyde_l.oxyde_v IS NULL OR oxyde_r.oxyde_v IS NULL \
         THEN oxyde_l.oxyde_v IS NULL AND oxyde_r.oxyde_v IS NULL"
            .to_string(),
        "NOT IN ('STRING', 'INTEGER', 'UNSIGNED INTEGER', 'DECIMAL', 'DOUBLE')".to_string(),
        format!("{l} NOT REGEXP '^[-+]?([0-9]{{1,35}}([.][0-9]{{0,30}})?|[.][0-9]{{1,30}})([eE][-+]?[0-9]{{1,2}})?$' THEN FALSE"),
        format!("ABS({l} + 0e0) BETWEEN 1e-28 AND 1e29 AND "),
        // Expanded fractional digits: "1.001e-28" would round into 1e-28.
        format!("REGEXP_REPLACE({l}, '^[-+]|[eE].*$', '')"),
        format!("REGEXP_SUBSTR({l}, '[-+]?[0-9]+$') AS SIGNED)) <= 30)) THEN FALSE"),
        format!("ELSE CAST({l} AS DECIMAL(65,30))"),
    ];
    let positions: Vec<_> = guards
        .iter()
        .map(|g| {
            sql.find(g.as_str())
                .unwrap_or_else(|| panic!("{g} not in {sql}"))
        })
        .collect();
    assert!(positions.windows(2).all(|w| w[0] < w[1]), "{sql}");
}

#[test]
fn sqlite_calls_the_registered_function() {
    for (op, name) in OPS.iter().zip(["contains", "contained_by", "overlap"]) {
        let (sql, params) = Lookup::new(op, arr(vec![s("rust")]), array_of(ColumnTypeSpec::Text))
            .build(Dialect::Sqlite);
        assert!(
            sql.ends_with(&format!(
                r#"WHERE oxyde_array_match(CASE WHEN json_valid("tags", 8) THEN json("tags") ELSE "tags" END, ?, '{name}', 'json')"#
            )),
            "{sql}"
        );
        assert_eq!(params.len(), 1);
    }
    let (sql, _) = Lookup::new(
        "ARRAY_OVERLAP",
        arr(vec![s("1")]),
        array_of(ColumnTypeSpec::Decimal {
            precision: None,
            scale: None,
        }),
    )
    .build(Dialect::Sqlite);
    assert!(sql.contains("'overlap', 'decimal')"), "{sql}");
}

#[test]
fn identifiers_are_quoted_and_qualified() {
    let mut lookup = Lookup::new(
        "ARRAY_CONTAINS",
        arr(vec![s("x")]),
        array_of(ColumnTypeSpec::Text),
    );
    lookup.column = Some(r#"au"thor.ta"gs"#);
    let (sql, _) = lookup.build(Dialect::Postgres);
    assert!(
        sql.contains(r#"("au""thor"."ta""gs" @> $1::TEXT[])"#),
        "{sql}"
    );
    let (sql, _) = lookup.build(Dialect::Mysql);
    assert!(sql.contains("JSON_TYPE(`au\"thor`.`ta\"gs`)"), "{sql}");
    let (sql, _) = lookup.build(Dialect::Sqlite);
    assert!(
        sql.contains(r#"oxyde_array_match(CASE WHEN json_valid("au""thor"."ta""gs", 8)"#),
        "{sql}"
    );
}

#[test]
fn qualified_column_type_wins_over_same_named_base_column() {
    // A joined model's `tags` is an array; the base model's `tags` is text.
    let mut lookup = Lookup::new(
        "ARRAY_CONTAINS",
        arr(vec![s("x")]),
        array_of(ColumnTypeSpec::Text),
    );
    lookup.column = Some("author.tags");
    lookup.table_types = HashMap::from([("tags".into(), ColumnTypeSpec::Text)]);
    let (sql, _) = lookup.build(Dialect::Postgres);
    assert!(sql.contains(r#"("author"."tags" @> $1::TEXT[])"#), "{sql}");
}

#[test]
fn qualified_column_never_takes_the_base_columns_type() {
    // The query has no spec for `sched.tags` (an UPDATE carries no joins);
    // the base model's same-named `tags` array must not stand in for it.
    let mut ir = Lookup::new(
        "ARRAY_CONTAINS",
        arr(vec![s("x")]),
        array_of(ColumnTypeSpec::Text),
    )
    .ir();
    if let Some(FilterNode::Condition(filter)) = ir.filter_tree.as_mut() {
        filter.column = Some("sched.tags".into());
    }
    for dialect in [Dialect::Postgres, Dialect::Mysql, Dialect::Sqlite] {
        let err = build_sql(&ir, dialect).unwrap_err().to_string();
        assert!(
            err.contains("'sched.tags' needs its column type"),
            "{dialect:?}: {err}"
        );
    }
}

fn int_array_override(operator: &'static str, value: rmpv::Value) -> Lookup {
    let mut lookup = Lookup::new(operator, value, array_of(ColumnTypeSpec::BigInteger));
    lookup.db_type = Some("INTEGER[]");
    lookup
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

    // Elements the column type cannot hold are rejected, as for lookups.
    let err = int_array_override("=", arr(vec![(1_i64 << 31).into()])).error(Dialect::Postgres);
    assert!(err.contains("out of range for INTEGER[]"), "{err}");

    // Other dialects store arrays as JSON: no cast.
    for dialect in [Dialect::Mysql, Dialect::Sqlite] {
        let (sql, _) = int_array_override("=", ints()).build(dialect);
        assert!(!sql.contains("INTEGER[]"), "{dialect:?}: {sql}");
    }
}

#[test]
fn array_values_without_a_readable_override_are_not_cast() {
    let ints = || arr(vec![1.into()]);
    // No override: the SQL is unchanged.
    let (sql, _) =
        Lookup::new("=", ints(), array_of(ColumnTypeSpec::BigInteger)).build(Dialect::Postgres);
    assert!(sql.ends_with(r#""tags" = $1"#), "{sql}");
    // An override array lookups cannot read either keeps the plain value.
    let mut lookup = int_array_override("=", ints());
    lookup.db_type = Some("INTEGER[][]");
    let (sql, _) = lookup.build(Dialect::Postgres);
    assert!(sql.ends_with(r#""tags" = $1"#), "{sql}");
}

#[test]
fn joined_select_qualifies_the_array_column() {
    let mut lookup = Lookup::new(
        "ARRAY_CONTAINS",
        arr(vec![s("x")]),
        array_of(ColumnTypeSpec::Text),
    );
    lookup.column = Some("tags");
    let mut ir = lookup.ir();
    ir.joins = Some(vec![JoinSpec {
        path: "author".into(),
        alias: "author".into(),
        parent: None,
        table: "authors".into(),
        source_column: "author_id".into(),
        target_column: "id".into(),
        result_prefix: "author".into(),
        columns: vec![JoinColumn {
            field: "name".into(),
            column: "name".into(),
        }],
    }]);
    let (sql, _) = build_sql(&ir, Dialect::Postgres).unwrap();
    assert!(sql.contains(r#"("posts"."tags" @> $1::TEXT[])"#), "{sql}");
}

#[test]
fn invalid_operands_are_rejected() {
    let text_array = array_of(ColumnTypeSpec::Text);
    let int_array = array_of(ColumnTypeSpec::BigInteger);
    let cases = [
        Lookup::new("ARRAY_CONTAINS", s("rust"), text_array.clone()),
        Lookup::new("ARRAY_CONTAINS", rmpv::Value::Nil, text_array.clone()),
        Lookup::new("ARRAY_CONTAINS", arr(vec![1.into()]), text_array),
        Lookup::new("ARRAY_OVERLAP", arr(vec![s("1")]), int_array.clone()),
        Lookup::new(
            "ARRAY_OVERLAP",
            arr(vec![rmpv::Value::Boolean(true)]),
            int_array,
        ),
        Lookup::new(
            "ARRAY_CONTAINS",
            arr(vec![rmpv::Value::Ext(1, vec![0])]),
            ColumnTypeSpec::Json,
        ),
    ];
    for lookup in cases {
        for dialect in [Dialect::Postgres, Dialect::Mysql, Dialect::Sqlite] {
            let err = build_sql(&lookup.ir(), dialect).unwrap_err();
            assert!(matches!(err, QueryError::InvalidQuery(_)), "{err}");
        }
    }
}

#[test]
fn unsupported_columns_are_rejected() {
    let nested = array_of(array_of(ColumnTypeSpec::BigInteger));
    let cases = [
        (
            Lookup::new("ARRAY_CONTAINS", arr(vec![]), ColumnTypeSpec::Text),
            "array or JSON column",
        ),
        (
            Lookup::new("ARRAY_CONTAINS", arr(vec![]), ColumnTypeSpec::Unknown),
            "'tags' needs its column type",
        ),
        (
            Lookup::new("ARRAY_CONTAINS", arr(vec![]), nested),
            "nested typed arrays",
        ),
    ];
    for (lookup, message) in cases {
        for dialect in [Dialect::Postgres, Dialect::Mysql, Dialect::Sqlite] {
            assert!(
                lookup.error(dialect).contains(message),
                "{}",
                lookup.error(dialect)
            );
        }
    }

    let bytes = Lookup::new(
        "ARRAY_CONTAINS",
        arr(vec![rmpv::Value::Binary(vec![1])]),
        array_of(ColumnTypeSpec::Blob),
    );
    lookup_ok(&bytes, Dialect::Postgres);
    assert!(bytes
        .error(Dialect::Mysql)
        .contains("only supported on PostgreSQL"));
    assert!(bytes
        .error(Dialect::Sqlite)
        .contains("only supported on PostgreSQL"));

    let mut json_array = Lookup::new(
        "ARRAY_CONTAINS",
        arr(vec![]),
        array_of(ColumnTypeSpec::Json),
    );
    json_array.db_type = Some("JSON[]");
    assert!(json_array.error(Dialect::Postgres).contains("json[]"));
}

fn lookup_ok(lookup: &Lookup, dialect: Dialect) {
    build_sql(&lookup.ir(), dialect).unwrap();
}

#[test]
fn array_lookups_cannot_target_aggregates() {
    let mut ir = Lookup::new(
        "ARRAY_CONTAINS",
        arr(vec![]),
        array_of(ColumnTypeSpec::Text),
    )
    .ir();
    ir.having = ir.filter_tree.take();
    ir.group_by = Some(vec!["id".into()]);
    ir.aggregates = Some(vec![oxyde_codec::Aggregate {
        op: oxyde_codec::AggregateOp::Count,
        field: Some("id".into()),
        alias: Some("tags".into()),
        distinct: None,
    }]);
    let err = build_sql(&ir, Dialect::Postgres).unwrap_err().to_string();
    assert!(err.contains("aggregate"), "{err}");
}

#[test]
fn negation_wraps_the_whole_predicate() {
    let mut ir = Lookup::new(
        "ARRAY_CONTAINS",
        arr(vec![s("x")]),
        array_of(ColumnTypeSpec::Text),
    )
    .ir();
    ir.filter_tree = Some(FilterNode::Not {
        condition: Box::new(ir.filter_tree.take().unwrap()),
    });
    let (sql, _) = build_sql(&ir, Dialect::Postgres).unwrap();
    assert!(sql.contains(r#"NOT (("tags" @> $1::TEXT[]))"#), "{sql}");
}

#[test]
fn existing_payloads_without_array_metadata_still_deserialize() {
    let legacy = r#"{"type": "condition", "field": "name", "operator": "LIKE", "value": "%a%"}"#;
    let node: FilterNode = serde_json::from_str(legacy).unwrap();
    let FilterNode::Condition(filter) = node else {
        panic!("expected condition");
    };
    assert!(filter.db_type.is_none());

    let with_metadata = r#"{"type": "condition", "field": "tags", "operator": "ARRAY_OVERLAP",
        "value": ["a"], "db_type": "TEXT[]"}"#;
    let node: FilterNode = serde_json::from_str(with_metadata).unwrap();
    let FilterNode::Condition(filter) = node else {
        panic!("expected condition");
    };
    assert_eq!(filter.db_type.as_deref(), Some("TEXT[]"));
}

#[test]
fn like_on_json_columns_matches_the_unquoted_string() {
    // A `str` field stored as JSON keeps LIKE for a string `contains`, and
    // matches the string it reads back as, not its quoted JSON text.
    for spec in [ColumnTypeSpec::Json, ColumnTypeSpec::JsonBinary] {
        let case_sensitive = Lookup::new("LIKE", s("%x%"), spec.clone());
        let case_insensitive = Lookup::new("ILIKE", s("%x%"), spec);

        let (sql, _) = case_sensitive.build(Dialect::Postgres);
        assert!(sql.contains(r#"("tags" #>> '{}') LIKE $1"#), "{sql}");
        let (sql, _) = case_insensitive.build(Dialect::Postgres);
        assert!(sql.contains(r#"LOWER("tags" #>> '{}') LIKE $1"#), "{sql}");

        let (sql, _) = case_sensitive.build(Dialect::Mysql);
        assert!(sql.contains("(JSON_UNQUOTE(`tags`)) LIKE ?"), "{sql}");

        let (sql, _) = case_sensitive.build(Dialect::Sqlite);
        assert!(
            sql.contains(
                r#"(CASE WHEN json_valid("tags") THEN CASE json_type("tags") WHEN 'text' THEN json_extract("tags", '$') ELSE "tags" END ELSE "tags" END) LIKE ?"#
            ),
            "{sql}"
        );
    }

    // Text columns are untouched.
    let (sql, _) = Lookup::new("LIKE", s("%x%"), ColumnTypeSpec::Text).build(Dialect::Postgres);
    assert!(sql.contains(r#""tags" LIKE $1"#), "{sql}");
}
