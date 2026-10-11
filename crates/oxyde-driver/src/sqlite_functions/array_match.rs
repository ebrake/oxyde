//! Safe core of the `oxyde_array_match` SQLite function.
//!
//! Both arguments are JSON texts. Elements are reduced to a canonical form so
//! that equality follows the array-lookup contract shared with PostgreSQL and
//! MySQL: objects compare as complete values regardless of key order, nested
//! arrays keep their order, strings are case-sensitive, booleans never equal
//! numbers, and numbers compare by exact decimal value (`1`, `1.0` and `1e0`
//! are equal, and large numbers are never rounded through `f64`).
//!
//! Numbers are read through `RawValue` so their lexemes reach this module
//! untouched; serde_json would otherwise parse anything outside `i64`/`u64`
//! as `f64`.

use std::collections::BTreeMap;

use oxyde_codec::sqlite_array_match as sqlite_fn;
use serde_json::value::RawValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MatchOp {
    Contains,
    ContainedBy,
    Overlap,
}

impl MatchOp {
    pub(crate) fn parse(name: &str) -> Result<Self, String> {
        match name {
            sqlite_fn::CONTAINS => Ok(Self::Contains),
            sqlite_fn::CONTAINED_BY => Ok(Self::ContainedBy),
            sqlite_fn::OVERLAP => Ok(Self::Overlap),
            other => Err(format!("unknown operator '{other}'")),
        }
    }
}

/// How top-level elements are read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ItemMode {
    /// Plain JSON values.
    Json,
    /// Typed decimal arrays, stored as JSON strings that keep their scale
    /// ("1.50"): numeric strings compare by value.
    Decimal,
}

impl ItemMode {
    pub(crate) fn parse(name: &str) -> Result<Self, String> {
        match name {
            sqlite_fn::ITEMS_JSON => Ok(Self::Json),
            sqlite_fn::ITEMS_DECIMAL => Ok(Self::Decimal),
            other => Err(format!("unknown item spec '{other}'")),
        }
    }
}

/// A parsed operand: constant for a statement, so the SQLite callback
/// parses it once and reuses it for every row.
#[derive(Debug)]
pub(crate) struct Operand {
    mode: ItemMode,
    set: Vec<Canon>,
}

impl Operand {
    pub(crate) fn parse(text: &str, mode: ItemMode) -> Result<Self, String> {
        let set = canonical_set(text, mode)
            .map_err(|e| format!("operand: {e}"))?
            .ok_or_else(|| "operand must be a JSON array".to_string())?;
        Ok(Self { mode, set })
    }

    /// Evaluate the lookup against a stored value. A stored value that is
    /// not a JSON array does not match, including text that is not JSON at
    /// all: SQLite does not validate JSON columns, and Oxyde itself stores a
    /// `str` that is not JSON as is.
    pub(crate) fn matches(&self, stored: &str, op: MatchOp) -> bool {
        let Ok(Some(stored)) = canonical_set(stored, self.mode) else {
            return false;
        };
        let operand = &self.set;

        match op {
            MatchOp::Contains => is_subset(operand, &stored),
            MatchOp::ContainedBy => is_subset(&stored, operand),
            MatchOp::Overlap => operand.iter().any(|e| stored.binary_search(e).is_ok()),
        }
    }
}

/// Evaluate an array lookup in one go (parse the operand, then match).
#[cfg(test)]
fn array_match(stored: &str, operand: &str, op: MatchOp, mode: ItemMode) -> Result<bool, String> {
    Ok(Operand::parse(operand, mode)?.matches(stored, op))
}

/// Every element of `needles` appears in `haystack` (both sorted).
fn is_subset(needles: &[Canon], haystack: &[Canon]) -> bool {
    needles.iter().all(|e| haystack.binary_search(e).is_ok())
}

/// Sorted, de-duplicated canonical elements; `None` for a non-array.
///
/// Each value is parsed once here; nested containers are re-read once per
/// level by `canonical`, since keeping number lexemes in a single pass would
/// need serde_json's `arbitrary_precision`, which changes numbers crate-wide.
fn canonical_set(text: &str, mode: ItemMode) -> Result<Option<Vec<Canon>>, String> {
    let invalid = |e: serde_json::Error| format!("invalid JSON: {e}");
    if !text.trim_start().starts_with('[') {
        // Still validated: text that is not JSON at all is an error.
        // (`Operand::matches` treats it as a non-match for stored values.)
        serde_json::from_str::<&RawValue>(text).map_err(invalid)?;
        return Ok(None);
    }
    let items: Vec<&RawValue> = serde_json::from_str(text).map_err(invalid)?;
    let mut set = items
        .into_iter()
        .map(|item| canonical(item, mode))
        .collect::<Result<Vec<_>, _>>()?;
    set.sort();
    set.dedup();
    Ok(Some(set))
}

/// Canonical JSON value. The derived ordering is arbitrary but total, which
/// is all sorting and de-duplication need.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Canon {
    Null,
    Bool(bool),
    Number(Number),
    /// A number whose exponent does not fit `Number`; equal only to the
    /// same lexeme.
    RawNumber(String),
    String(String),
    Array(Vec<Canon>),
    Object(BTreeMap<String, Canon>),
}

fn canonical(raw: &RawValue, mode: ItemMode) -> Result<Canon, String> {
    let text = raw.get();
    let parse_err = |e: serde_json::Error| e.to_string();
    Ok(match text.as_bytes().first() {
        Some(b'{') => {
            let members: BTreeMap<String, &RawValue> =
                serde_json::from_str(text).map_err(parse_err)?;
            Canon::Object(
                members
                    .into_iter()
                    .map(|(key, value)| Ok((key, canonical(value, ItemMode::Json)?)))
                    .collect::<Result<_, String>>()?,
            )
        }
        Some(b'[') => {
            let items: Vec<&RawValue> = serde_json::from_str(text).map_err(parse_err)?;
            Canon::Array(
                items
                    .into_iter()
                    .map(|item| canonical(item, ItemMode::Json))
                    .collect::<Result<_, _>>()?,
            )
        }
        Some(b'"') => {
            let s: String = serde_json::from_str(text).map_err(parse_err)?;
            match mode {
                ItemMode::Decimal => Number::parse(&s).map_or(Canon::String(s), Canon::Number),
                ItemMode::Json => Canon::String(s),
            }
        }
        Some(b't') => Canon::Bool(true),
        Some(b'f') => Canon::Bool(false),
        Some(b'n') => Canon::Null,
        _ => Number::parse(text).map_or_else(|| Canon::RawNumber(text.to_string()), Canon::Number),
    })
}

/// Exact decimal value `±digits × 10^exponent` with no leading or trailing
/// zeros in `digits`; zero is the empty digit string.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Number {
    negative: bool,
    digits: String,
    exponent: i64,
}

impl Number {
    /// Parse a decimal lexeme (`-12.50`, `1E+2`, `.5`). `None` for anything
    /// else, including exponents beyond `i64`.
    fn parse(text: &str) -> Option<Self> {
        let (negative, unsigned) = match text.as_bytes().first()? {
            b'-' => (true, &text[1..]),
            b'+' => (false, &text[1..]),
            _ => (false, text),
        };
        let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
            Some(i) => (&unsigned[..i], unsigned[i + 1..].parse::<i64>().ok()?),
            None => (unsigned, 0),
        };
        let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        let all_digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
        if (int_part.is_empty() && frac_part.is_empty())
            || !all_digits(int_part)
            || !all_digits(frac_part)
        {
            return None;
        }

        let combined = format!("{int_part}{frac_part}");
        let significant = combined.trim_start_matches('0');
        let digits = significant.trim_end_matches('0');
        if digits.is_empty() {
            return Some(Self {
                negative: false,
                digits: String::new(),
                exponent: 0,
            });
        }
        let trailing_zeros = i64::try_from(significant.len() - digits.len()).ok()?;
        let frac_len = i64::try_from(frac_part.len()).ok()?;
        Some(Self {
            negative,
            digits: digits.to_string(),
            exponent: exponent
                .checked_sub(frac_len)?
                .checked_add(trailing_zeros)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(stored: &str, operand: &str, op: MatchOp) -> bool {
        array_match(stored, operand, op, ItemMode::Json).unwrap()
    }

    #[test]
    fn set_semantics_ignore_order_and_duplicates() {
        assert!(check("[1, 2, 2, 3]", "[3, 1, 1]", MatchOp::Contains));
        assert!(!check("[1, 2]", "[1, 4]", MatchOp::Contains));
        assert!(check("[2, 1, 1]", "[1, 2, 3]", MatchOp::ContainedBy));
        assert!(!check("[1, 5]", "[1, 2, 3]", MatchOp::ContainedBy));
        assert!(check("[1, 5]", "[5, 9]", MatchOp::Overlap));
        assert!(!check("[1, 5]", "[6, 9]", MatchOp::Overlap));
    }

    #[test]
    fn empty_operands() {
        assert!(check("[1]", "[]", MatchOp::Contains));
        assert!(!check("[1]", "[]", MatchOp::Overlap));
        assert!(!check("[1]", "[]", MatchOp::ContainedBy));
        assert!(check("[]", "[]", MatchOp::ContainedBy));
        assert!(check("[]", "[]", MatchOp::Contains));
    }

    #[test]
    fn objects_compare_whole_ignoring_key_order() {
        let stored = r#"[{"a": 1, "b": {"x": [1, 2]}}, {"c": 3}]"#;
        assert!(check(
            stored,
            r#"[{"b": {"x": [1, 2]}, "a": 1}]"#,
            MatchOp::Contains
        ));
        // Partial objects are not elements of the array.
        assert!(!check(stored, r#"[{"a": 1}]"#, MatchOp::Contains));
        // Nested arrays keep their order.
        assert!(!check(
            stored,
            r#"[{"a": 1, "b": {"x": [2, 1]}}]"#,
            MatchOp::Contains
        ));
    }

    #[test]
    fn nested_arrays_are_ordered_values() {
        assert!(check("[[1, 2], [3]]", "[[3], [1, 2]]", MatchOp::Contains));
        assert!(!check("[[1, 2]]", "[[2, 1]]", MatchOp::Contains));
        assert!(!check("[[1, 2]]", "[1]", MatchOp::Contains));
    }

    #[test]
    fn scalar_types_stay_distinct() {
        assert!(!check("[true]", "[1]", MatchOp::Contains));
        assert!(!check("[\"1\"]", "[1]", MatchOp::Contains));
        assert!(!check("[\"A\"]", "[\"a\"]", MatchOp::Contains));
        assert!(!check("[0]", "[false]", MatchOp::Overlap));
    }

    #[test]
    fn null_elements_match_null() {
        assert!(check("[1, null]", "[null]", MatchOp::Contains));
        assert!(!check("[1]", "[null]", MatchOp::Contains));
        assert!(check("[null]", "[1, null]", MatchOp::ContainedBy));
    }

    #[test]
    fn numbers_compare_by_exact_value() {
        assert!(check("[1.0]", "[1]", MatchOp::Contains));
        assert!(check("[100]", "[1e2]", MatchOp::Contains));
        assert!(check("[0.5]", "[5E-1]", MatchOp::Contains));
        assert!(check("[-0.0]", "[0]", MatchOp::Contains));
        // Beyond f64 precision: these are distinct values.
        assert!(!check(
            "[123456789012345678901234567890]",
            "[123456789012345678901234567891]",
            MatchOp::Contains
        ));
        assert!(check(
            "[123456789012345678901234567890]",
            "[1.2345678901234567890123456789e29]",
            MatchOp::Contains
        ));
        assert!(!check(
            "[9007199254740993.5]",
            "[9007199254740993.0]",
            MatchOp::Contains
        ));
    }

    #[test]
    fn decimal_mode_compares_numeric_strings_by_value() {
        let decimal = |stored, operand| {
            array_match(stored, operand, MatchOp::Contains, ItemMode::Decimal).unwrap()
        };
        assert!(decimal(r#"["1.50", "2"]"#, r#"["1.5"]"#));
        assert!(decimal(r#"["100"]"#, r#"["1E+2"]"#));
        assert!(!decimal(r#"["1.51"]"#, r#"["1.5"]"#));
        assert!(decimal(r#"["1.50", null]"#, "[null]"));
        // JSON mode keeps them as strings.
        assert!(!check(r#"["1.50"]"#, r#"["1.5"]"#, MatchOp::Contains));
    }

    #[test]
    fn non_arrays_do_not_match() {
        assert!(!check("5", "[]", MatchOp::Contains));
        assert!(!check(r#"{"a": 1}"#, "[]", MatchOp::ContainedBy));
        assert!(!check("null", "[]", MatchOp::Contains));
        assert!(!check(r#""[1]""#, "[1]", MatchOp::Contains));
    }

    #[test]
    fn invalid_stored_json_does_not_match() {
        for op in [MatchOp::Contains, MatchOp::ContainedBy, MatchOp::Overlap] {
            assert!(!check("[1,", "[1]", op));
            assert!(!check("hello", "[]", op));
        }
    }

    #[test]
    fn numbers_beyond_range_compare_by_lexeme() {
        let huge = "[1, 1e99999999999999999999]";
        assert!(check(huge, "[1]", MatchOp::Overlap));
        assert!(check(huge, "[1e99999999999999999999]", MatchOp::Contains));
        assert!(!check(huge, "[2]", MatchOp::Overlap));
    }

    #[test]
    fn invalid_operand_is_an_error() {
        assert!(array_match("[1]", "{", MatchOp::Contains, ItemMode::Json).is_err());
        assert!(array_match("[1]", "1", MatchOp::Contains, ItemMode::Json).is_err());
    }

    #[test]
    fn number_parsing() {
        let n = |s| Number::parse(s);
        assert_eq!(n("1.50"), n("1.5"));
        assert_eq!(n("-0"), n("0"));
        assert_eq!(n("0.000"), n("0"));
        assert_eq!(n("12e-1"), n("1.2"));
        assert_eq!(n(".5"), n("0.5"));
        assert_ne!(n("-1"), n("1"));
        assert_eq!(n("abc"), None);
        assert_eq!(n("1e99999999999999999999"), None);
        assert_eq!(n(""), None);
        assert_eq!(n("-"), None);
    }
}
