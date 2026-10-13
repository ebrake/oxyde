//! Rust-implemented SQL functions registered on every SQLite connection.
//!
//! `oxyde_array_match(stored, operand, operator, item_spec)` backs the array
//! lookups (`contains` / `contained_by` / `overlap`) that `oxyde-sql`
//! generates for SQLite. SQL that calls it only runs on connections opened
//! by an oxyde pool; other SQLite clients do not have the function. A stored
//! value that cannot hold a JSON array (a BLOB, text that is not UTF-8 or
//! not JSON) does not match; the generated SQL passes JSONB as JSON text.
//!
//! All FFI lives in this module. SQLx 0.8 has no function-registration API,
//! so registration goes through the raw handle (`lock_handle`), which SQLx
//! documents as tied to its `libsqlite3-sys` version. Ours is the same crate:
//! `libsqlite3-sys` declares `links = "sqlite3"`, so Cargo cannot resolve a
//! graph with two versions of it. The callback never unwinds into
//! SQLite: matcher errors and panics are reported as SQL errors.

#![allow(unsafe_code)]

mod array_match;

use std::ffi::{c_int, c_void};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;

use libsqlite3_sys as ffi;
use oxyde_codec::sqlite_array_match as sqlite_fn;
use sqlx::sqlite::SqliteConnection;

use array_match::{ItemMode, MatchOp, Operand};

/// Argument index of the operand, whose parsed form is cached as aux data.
const OPERAND_ARG: c_int = 1;

/// Register oxyde's SQL functions on a freshly opened connection.
pub(crate) async fn register(conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
    let mut handle = conn.lock_handle().await?;
    let db = handle.as_raw_handle().as_ptr();

    // SAFETY: `db` is a live connection handle, exclusively locked for the
    // duration of the call. The name is a static C string. No user data
    // pointer is registered, so no destructor is needed.
    let rc = unsafe {
        ffi::sqlite3_create_function_v2(
            db,
            sqlite_fn::FUNCTION_C.as_ptr(),
            4,
            ffi::SQLITE_UTF8 | ffi::SQLITE_DETERMINISTIC | ffi::SQLITE_INNOCUOUS,
            ptr::null_mut(),
            Some(array_match_callback),
            None,
            None,
            None,
        )
    };

    if rc == ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(sqlx::Error::Configuration(
            format!(
                "failed to register SQLite function {} (code {rc})",
                sqlite_fn::FUNCTION
            )
            .into(),
        ))
    }
}

unsafe extern "C" fn array_match_callback(
    ctx: *mut ffi::sqlite3_context,
    arg_count: c_int,
    arg_values: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite passes `arg_count` valid value pointers in `arg_values`
    // that stay valid for the duration of this call.
    let outcome = catch_unwind(AssertUnwindSafe(|| unsafe {
        evaluate(ctx, arg_count, arg_values)
    }));
    // SAFETY: `ctx` is the context SQLite handed to this call.
    unsafe {
        match outcome {
            Ok(Ok(Some(matched))) => ffi::sqlite3_result_int(ctx, c_int::from(matched)),
            Ok(Ok(None)) => ffi::sqlite3_result_null(ctx),
            Ok(Err(message)) => result_error(ctx, &format!("oxyde_array_match: {message}")),
            Err(_) => result_error(ctx, "oxyde_array_match: internal error"),
        }
    }
}

/// `Ok(None)` is SQL NULL: a NULL column (or operand) keeps NULL semantics.
unsafe fn evaluate(
    ctx: *mut ffi::sqlite3_context,
    arg_count: c_int,
    arg_values: *mut *mut ffi::sqlite3_value,
) -> Result<Option<bool>, String> {
    let count = usize::try_from(arg_count).map_err(|_| "invalid argument count".to_string())?;
    // SAFETY: see `array_match_callback`.
    let values = unsafe { std::slice::from_raw_parts(arg_values, count) };
    let &[stored, operand, op, mode] = values else {
        return Err(format!("expected 4 arguments, got {count}"));
    };

    // SAFETY: each pointer is a valid argument value for this call.
    let (stored, operand, op, mode) =
        unsafe { (stored_text(stored)?, text(operand)?, text(op)?, text(mode)?) };
    // Every argument Oxyde generates is validated before the stored value is
    // looked at, so a malformed call fails whatever the table holds.
    let op = MatchOp::parse(op.ok_or("operator must not be NULL")?)?;
    let mode = ItemMode::parse(mode.ok_or("item spec must not be NULL")?)?;
    let Some(operand) = operand else {
        return Ok(None);
    };

    // SAFETY: `ctx` is the context of the current call.
    if let Some(cached) = unsafe { cached_operand(ctx, operand, mode) } {
        return Ok(answer(cached, &stored, op));
    }
    let parsed = CachedOperand {
        text: operand.to_owned(),
        mode,
        operand: Operand::parse(operand, mode)?,
    };
    let matched = answer(&parsed.operand, &stored, op);
    // SAFETY: `ctx` is the context of the current call; `parsed` is not used
    // after this, as SQLite may free it before the call returns.
    unsafe { cache_operand(ctx, parsed) };
    Ok(matched)
}

/// The lookup's result for one stored value.
fn answer(operand: &Operand, stored: &Stored<'_>, op: MatchOp) -> Option<bool> {
    match stored {
        Stored::Null => None,
        Stored::Text(stored) => Some(operand.matches(stored, op)),
        // Not a JSON array, like any other non-array value.
        Stored::Unreadable => Some(false),
    }
}

/// Parsed operand kept as SQLite aux data on the operand argument, so a
/// statement parses its constant operand once rather than once per row.
struct CachedOperand {
    /// The text it was parsed from. SQLite discards aux data when the
    /// argument changes; comparing the text as well costs a `memcmp` per row
    /// and does not depend on that.
    text: String,
    mode: ItemMode,
    operand: Operand,
}

/// The cached operand, if SQLite kept one for this argument and it was
/// parsed from the same text and mode.
///
/// SAFETY: `ctx` must be the context of the current call. The reference must
/// not outlive the call.
unsafe fn cached_operand<'a>(
    ctx: *mut ffi::sqlite3_context,
    text: &str,
    mode: ItemMode,
) -> Option<&'a Operand> {
    // SAFETY: the only aux data set on this argument is a `CachedOperand`
    // (see `cache_operand`), which SQLite keeps alive during the call.
    let cached = unsafe { ffi::sqlite3_get_auxdata(ctx, OPERAND_ARG) }.cast::<CachedOperand>();
    let cached = unsafe { cached.as_ref() }?;
    (cached.text == text && cached.mode == mode).then_some(&cached.operand)
}

/// Hand `cached` to SQLite, which frees it with `drop_cached_operand`.
///
/// SAFETY: `ctx` must be the context of the current call.
unsafe fn cache_operand(ctx: *mut ffi::sqlite3_context, cached: CachedOperand) {
    let ptr = Box::into_raw(Box::new(cached));
    unsafe {
        ffi::sqlite3_set_auxdata(ctx, OPERAND_ARG, ptr.cast(), Some(drop_cached_operand));
    }
}

unsafe extern "C" fn drop_cached_operand(ptr: *mut c_void) {
    // SAFETY: SQLite passes back the pointer from `cache_operand`, once.
    drop(unsafe { Box::from_raw(ptr.cast::<CachedOperand>()) });
}

/// Read an argument as UTF-8 text; `None` for SQL NULL. Other storage
/// classes use SQLite's own text conversion.
///
/// SAFETY: `value` must be a valid argument of the current call; the returned
/// slice must not outlive it.
unsafe fn text<'a>(value: *mut ffi::sqlite3_value) -> Result<Option<&'a str>, String> {
    // SAFETY: as above.
    let Some(bytes) = (unsafe { text_bytes(value) })? else {
        return Ok(None);
    };
    std::str::from_utf8(bytes)
        .map(Some)
        .map_err(|_| "argument is not valid UTF-8".into())
}

/// The stored-value argument. Its content comes from the table, not from
/// Oxyde, so what cannot hold a JSON array is not an error.
enum Stored<'a> {
    Null,
    Text(&'a str),
    /// A BLOB (the generated SQL already turns JSONB into JSON text), or
    /// text that is not valid UTF-8.
    Unreadable,
}

/// Read the stored-value argument; numbers use SQLite's text conversion.
///
/// SAFETY: as for `text`.
unsafe fn stored_text<'a>(value: *mut ffi::sqlite3_value) -> Result<Stored<'a>, String> {
    // SAFETY: `value` is a valid argument of the current call.
    if unsafe { ffi::sqlite3_value_type(value) } == ffi::SQLITE_BLOB {
        return Ok(Stored::Unreadable);
    }
    // SAFETY: as above.
    Ok(match unsafe { text_bytes(value) }? {
        None => Stored::Null,
        Some(bytes) => std::str::from_utf8(bytes).map_or(Stored::Unreadable, Stored::Text),
    })
}

/// Raw text bytes of an argument; `None` for SQL NULL.
///
/// SAFETY: as for `text`.
unsafe fn text_bytes<'a>(value: *mut ffi::sqlite3_value) -> Result<Option<&'a [u8]>, String> {
    unsafe {
        if ffi::sqlite3_value_type(value) == ffi::SQLITE_NULL {
            return Ok(None);
        }
        // Per the SQLite docs: call _text() first, then _bytes() for its length.
        let data = ffi::sqlite3_value_text(value);
        if data.is_null() {
            return Err("out of memory reading argument".into());
        }
        let len = usize::try_from(ffi::sqlite3_value_bytes(value))
            .map_err(|_| "invalid argument length".to_string())?;
        Ok(Some(std::slice::from_raw_parts(data, len)))
    }
}

/// SAFETY: `ctx` must be the context of the current call. SQLite copies the
/// message before returning.
unsafe fn result_error(ctx: *mut ffi::sqlite3_context, message: &str) {
    let len = c_int::try_from(message.len()).unwrap_or(c_int::MAX);
    unsafe { ffi::sqlite3_result_error(ctx, message.as_ptr().cast(), len) };
}

#[cfg(test)]
mod tests {
    use sqlx::{Connection, Row};

    use super::*;

    async fn query(conn: &mut SqliteConnection, sql: &str) -> Result<Option<i64>, sqlx::Error> {
        sqlx::query(sql).fetch_one(conn).await.map(|row| row.get(0))
    }

    #[tokio::test]
    async fn registered_function_matches_and_propagates_null() {
        let mut conn = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        register(&mut conn).await.unwrap();

        let sql = r#"SELECT oxyde_array_match('[{"a": 1, "b": 2}]', '[{"b": 2, "a": 1}]', 'contains', 'json')"#;
        assert_eq!(query(&mut conn, sql).await.unwrap(), Some(1));

        let sql = "SELECT oxyde_array_match('[1]', '[2]', 'overlap', 'json')";
        assert_eq!(query(&mut conn, sql).await.unwrap(), Some(0));

        let sql = "SELECT oxyde_array_match(NULL, '[2]', 'contains', 'json')";
        assert_eq!(query(&mut conn, sql).await.unwrap(), None);

        let sql = "SELECT NOT oxyde_array_match('5', '[]', 'contained_by', 'json')";
        assert_eq!(query(&mut conn, sql).await.unwrap(), Some(1));
    }

    #[tokio::test]
    async fn cached_operand_follows_the_bound_value() {
        let mut conn = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        register(&mut conn).await.unwrap();
        sqlx::query("CREATE TABLE docs (id INTEGER PRIMARY KEY, tags TEXT)")
            .execute(&mut conn)
            .await
            .unwrap();
        for tags in [r#"["a", "b"]"#, r#"["b"]"#, r#"["c"]"#, r#"["a"]"#] {
            sqlx::query("INSERT INTO docs (tags) VALUES (?)")
                .bind(tags)
                .execute(&mut conn)
                .await
                .unwrap();
        }

        // One prepared statement (SQLx caches it), several rows per run, and
        // a different operand per run: each run must see its own operand.
        let sql = "SELECT id FROM docs \
                   WHERE oxyde_array_match(tags, ?, 'contains', 'json') ORDER BY id";
        for (operand, expected) in [
            (r#"["a"]"#, vec![1, 4]),
            (r#"["b"]"#, vec![1, 2]),
            (r#"["a", "b"]"#, vec![1]),
            ("[]", vec![1, 2, 3, 4]),
            (r#"["a"]"#, vec![1, 4]),
        ] {
            let ids: Vec<i64> = sqlx::query_scalar(sql)
                .bind(operand)
                .fetch_all(&mut conn)
                .await
                .unwrap();
            assert_eq!(ids, expected, "operand {operand}");
        }

        // Operands that differ per row (a column) are never served stale.
        let sql = "SELECT a.id, b.id FROM docs a JOIN docs b \
                   ON oxyde_array_match(a.tags, b.tags, 'contains', 'json') \
                   WHERE a.id != b.id ORDER BY a.id, b.id";
        let pairs: Vec<(i64, i64)> = sqlx::query_as(sql).fetch_all(&mut conn).await.unwrap();
        assert_eq!(pairs, vec![(1, 2), (1, 4)]);
    }

    #[tokio::test]
    async fn failures_are_sql_errors() {
        let mut conn = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        register(&mut conn).await.unwrap();

        // Whatever the stored value: a malformed call fails on every row, not
        // only on rows that hold readable text.
        for stored in ["'[1]'", "X'FF'", "CAST(X'FF' AS TEXT)", "NULL"] {
            for args in [
                "'[1,', 'contains', 'json'",
                "'[1]', 'nope', 'json'",
                "'[1]', 'contains', 'nope'",
                "'[1]', NULL, 'json'",
                "NULL, 'nope', 'json'",
            ] {
                let sql = format!("SELECT oxyde_array_match({stored}, {args})");
                let err = query(&mut conn, &sql).await.unwrap_err().to_string();
                assert!(err.contains("oxyde_array_match"), "{sql}: {err}");
            }
        }
        // The connection stays usable after an error.
        assert_eq!(query(&mut conn, "SELECT 1").await.unwrap(), Some(1));
        // A stored value that is not JSON does not match rather than failing.
        let sql = "SELECT oxyde_array_match('[1,', '[1]', 'contains', 'json')";
        assert_eq!(query(&mut conn, sql).await.unwrap(), Some(0));
    }

    #[tokio::test]
    async fn blobs_and_invalid_utf8_do_not_match() {
        let mut conn = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        register(&mut conn).await.unwrap();

        for stored in ["X'FF'", "CAST(X'FF' AS TEXT)", "jsonb('[1]')"] {
            for op in ["contains", "contained_by", "overlap"] {
                let sql = format!("SELECT oxyde_array_match({stored}, '[1]', '{op}', 'json')");
                assert_eq!(query(&mut conn, &sql).await.unwrap(), Some(0), "{sql}");
            }
        }
        // The operand is Oxyde's own: unreadable input there is still an error.
        let sql = "SELECT oxyde_array_match('[1]', X'FF', 'contains', 'json')";
        let err = query(&mut conn, sql).await.unwrap_err().to_string();
        assert!(err.contains("UTF-8"), "{err}");
        assert_eq!(query(&mut conn, "SELECT 1").await.unwrap(), Some(1));
    }

    #[tokio::test]
    async fn unregistered_connections_do_not_have_the_function() {
        let mut conn = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        let sql = "SELECT oxyde_array_match('[1]', '[1]', 'contains', 'json')";
        let err = query(&mut conn, sql).await.unwrap_err().to_string();
        assert!(err.contains("no such function"), "{err}");
    }
}
