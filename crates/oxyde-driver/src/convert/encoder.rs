//! CellEncoder trait and generic columnar encoding.
//!
//! Encodes database rows directly to msgpack bytes without intermediate
//! serde_json::Value. Each backend implements CellEncoder with its
//! own type-specific decoding; the generic functions handle the
//! columnar structure.

use std::collections::HashMap;

use sqlx::{Column, Database, Row};

/// Column metadata for encoding.
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name: String,
    pub db_type: String,
    pub ir_type: Option<String>,
}

/// Backend-specific cell encoder.
///
/// Implementations decode a single cell from an sqlx Row and write
/// it directly to a msgpack buffer via `rmp::encode::write_*`.
pub trait CellEncoder {
    type Row: sqlx::Row;

    /// Extract column metadata from the first row.
    fn extract_columns(
        row: &Self::Row,
        col_types: Option<&HashMap<String, String>>,
    ) -> Vec<ColumnMeta>
    where
        <<Self::Row as Row>::Database as Database>::Column: Column,
    {
        row.columns()
            .iter()
            .map(|c| {
                let name = Column::name(c).to_string();
                let ir_type = col_types.and_then(|ct| ct.get(&name).cloned());
                ColumnMeta {
                    db_type: Column::type_info(c).to_string().to_uppercase(),
                    ir_type,
                    name,
                }
            })
            .collect()
    }

    /// Try to encode a cell using the IR type hint.
    /// Returns `true` if the type was recognized and encoded.
    fn try_encode_by_ir_type(buf: &mut Vec<u8>, row: &Self::Row, idx: usize, ir_type: &str)
        -> bool;

    /// Encode a cell using the database column type (fallback).
    fn encode_by_db_type(buf: &mut Vec<u8>, row: &Self::Row, idx: usize, db_type: &str);

    /// Encode a single cell, trying IR type first, then DB type.
    fn encode_cell(buf: &mut Vec<u8>, row: &Self::Row, idx: usize, col: &ColumnMeta) {
        if let Some(ir_type) = &col.ir_type {
            if Self::try_encode_by_ir_type(buf, row, idx, ir_type) {
                return;
            }
        }
        Self::encode_by_db_type(buf, row, idx, &col.db_type);
    }
}

// ── msgpack write helpers ──────────────────────────────────────────────
// Vec<u8> Write impl never fails, so unwrap is safe.

#[inline]
pub fn write_nil(buf: &mut Vec<u8>) {
    rmp::encode::write_nil(buf).unwrap();
}

#[inline]
pub fn write_bool(buf: &mut Vec<u8>, v: bool) {
    rmp::encode::write_bool(buf, v).unwrap();
}

#[inline]
pub fn write_i64(buf: &mut Vec<u8>, v: i64) {
    rmp::encode::write_sint(buf, v).unwrap();
}

#[inline]
pub fn write_i32_as_i64(buf: &mut Vec<u8>, v: i32) {
    rmp::encode::write_sint(buf, i64::from(v)).unwrap();
}

#[inline]
pub fn write_u64(buf: &mut Vec<u8>, v: u64) {
    rmp::encode::write_uint(buf, v).unwrap();
}

#[inline]
pub fn write_f64(buf: &mut Vec<u8>, v: f64) {
    rmp::encode::write_f64(buf, v).unwrap();
}

#[inline]
pub fn write_str(buf: &mut Vec<u8>, v: &str) {
    rmp::encode::write_str(buf, v).unwrap();
}

#[inline]
pub fn write_bin(buf: &mut Vec<u8>, v: &[u8]) {
    rmp::encode::write_bin(buf, v).unwrap();
}

#[inline]
fn write_array_len(buf: &mut Vec<u8>, len: u32) {
    rmp::encode::write_array_len(buf, len).unwrap();
}

#[inline]
fn write_map_len(buf: &mut Vec<u8>, len: u32) {
    rmp::encode::write_map_len(buf, len).unwrap();
}

/// Write a serde_json::Value as msgpack (for JSON columns).
pub fn write_json_value(buf: &mut Vec<u8>, val: &serde_json::Value) {
    match val {
        serde_json::Value::Null => write_nil(buf),
        serde_json::Value::Bool(b) => write_bool(buf, *b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                write_i64(buf, i);
            } else if let Some(u) = n.as_u64() {
                write_u64(buf, u);
            } else if let Some(f) = n.as_f64() {
                write_f64(buf, f);
            } else {
                write_nil(buf);
            }
        }
        serde_json::Value::String(s) => write_str(buf, s),
        serde_json::Value::Array(arr) => {
            write_array_len(buf, arr.len() as u32);
            for v in arr {
                write_json_value(buf, v);
            }
        }
        serde_json::Value::Object(map) => {
            write_map_len(buf, map.len() as u32);
            for (k, v) in map {
                write_str(buf, k);
                write_json_value(buf, v);
            }
        }
    }
}

// ── Generic encode functions ───────────────────────────────────────────

/// Write column names as a msgpack array.
fn encode_column_names(buf: &mut Vec<u8>, columns: &[ColumnMeta]) {
    write_array_len(buf, columns.len() as u32);
    for col in columns {
        write_str(buf, &col.name);
    }
}

/// Write row data as a msgpack array of arrays.
fn encode_row_data<E: CellEncoder>(buf: &mut Vec<u8>, rows: &[E::Row], columns: &[ColumnMeta]) {
    write_array_len(buf, rows.len() as u32);
    for row in rows {
        write_array_len(buf, columns.len() as u32);
        for (i, col) in columns.iter().enumerate() {
            E::encode_cell(buf, row, i, col);
        }
    }
}

/// Encode rows in columnar format: `[column_names, [[row_values], ...]]`
///
/// Returns `(msgpack_bytes, row_count)`.
pub fn encode_rows_columnar<E: CellEncoder>(
    rows: &[E::Row],
    col_types: Option<&HashMap<String, String>>,
) -> (Vec<u8>, usize)
where
    <<E::Row as Row>::Database as Database>::Column: Column,
{
    if rows.is_empty() {
        let mut buf = Vec::with_capacity(4);
        write_array_len(&mut buf, 2);
        write_array_len(&mut buf, 0);
        write_array_len(&mut buf, 0);
        return (buf, 0);
    }

    let columns = E::extract_columns(&rows[0], col_types);
    let num_rows = rows.len();

    let mut buf = Vec::with_capacity(num_rows * columns.len() * 16);
    write_array_len(&mut buf, 2);
    encode_column_names(&mut buf, &columns);
    encode_row_data::<E>(&mut buf, rows, &columns);

    (buf, num_rows)
}

/// Info about a JOIN relation for dedup encoding.
pub struct RelationInfo {
    /// Column prefix in result set, e.g. "author"
    pub prefix: String,
    /// PK column name in result set, e.g. "author__id"
    pub pk_col: String,
}

/// Msgpack nil byte (0xc0) — used to detect NULL PKs from LEFT JOINs.
const MSGPACK_NIL: u8 = 0xc0;

/// Encode a single cell into a small buffer (for PK extraction).
fn encode_pk_cell<E: CellEncoder>(row: &E::Row, col_idx: usize, col_meta: &ColumnMeta) -> Vec<u8> {
    let mut pk_buf = Vec::with_capacity(16);
    E::encode_cell(&mut pk_buf, row, col_idx, col_meta);
    pk_buf
}

/// Encode rows with JOIN dedup: `[main_columns, main_rows, relations_map]`
///
/// - `main_columns`/`main_rows`: only columns without any relation prefix
/// - `relations_map`: `{prefix: {"columns": [...], "data": {pk: [values]}, "refs": [pk...]}}`
///
/// Returns `(msgpack_bytes, main_row_count)`.
pub fn encode_rows_dedup<E: CellEncoder>(
    rows: &[E::Row],
    col_types: Option<&HashMap<String, String>>,
    relations: &[RelationInfo],
) -> (Vec<u8>, usize)
where
    <<E::Row as Row>::Database as Database>::Column: Column,
{
    if rows.is_empty() {
        let mut buf = Vec::with_capacity(8);
        write_array_len(&mut buf, 3);
        write_array_len(&mut buf, 0);
        write_array_len(&mut buf, 0);
        write_map_len(&mut buf, 0);
        return (buf, 0);
    }

    let all_columns = E::extract_columns(&rows[0], col_types);

    // ── Partition columns into main vs relation groups ────────────────

    let mut main_indices: Vec<usize> = Vec::new();
    let mut main_columns: Vec<&ColumnMeta> = Vec::new();

    struct RelGroup<'a> {
        prefix: &'a str,
        pk_col_idx: usize, // index in all_columns
        col_indices: Vec<usize>,
        col_metas: Vec<&'a ColumnMeta>,
        stripped_names: Vec<String>,
    }

    let prefixes_sep: Vec<String> = relations
        .iter()
        .map(|r| format!("{}__", r.prefix))
        .collect();

    // Temporary: collect pk_col_idx per relation (filled below)
    let mut rel_groups: Vec<RelGroup<'_>> = relations
        .iter()
        .map(|ri| RelGroup {
            prefix: &ri.prefix,
            pk_col_idx: usize::MAX,
            col_indices: Vec::new(),
            col_metas: Vec::new(),
            stripped_names: Vec::new(),
        })
        .collect();

    for (idx, col) in all_columns.iter().enumerate() {
        let mut matched = false;
        for (gi, psep) in prefixes_sep.iter().enumerate() {
            if col.name.starts_with(psep) {
                if col.name == relations[gi].pk_col {
                    rel_groups[gi].pk_col_idx = idx;
                }
                rel_groups[gi].col_indices.push(idx);
                rel_groups[gi].col_metas.push(col);
                rel_groups[gi]
                    .stripped_names
                    .push(col.name[psep.len()..].to_string());
                matched = true;
                break;
            }
        }
        if !matched {
            main_indices.push(idx);
            main_columns.push(col);
        }
    }

    // ── Pass 1: scan rows, collect unique PKs and refs ───────────────

    struct RelScan {
        seen: HashMap<Vec<u8>, usize>, // encoded PK bytes → index in unique_pks
        unique_pk_bytes: Vec<Vec<u8>>, // ordered unique PK msgpack bytes
        first_row_idx: Vec<usize>,     // index of first row for each unique PK
        refs: Vec<Option<usize>>,      // per main row: index into unique_pk_bytes
    }

    let mut rel_scans: Vec<RelScan> = rel_groups
        .iter()
        .map(|_| RelScan {
            seen: HashMap::new(),
            unique_pk_bytes: Vec::new(),
            first_row_idx: Vec::new(),
            refs: Vec::with_capacity(rows.len()),
        })
        .collect();

    for (row_idx, row) in rows.iter().enumerate() {
        for (gi, group) in rel_groups.iter().enumerate() {
            let pk_bytes =
                encode_pk_cell::<E>(row, group.pk_col_idx, &all_columns[group.pk_col_idx]);

            if pk_bytes.len() == 1 && pk_bytes[0] == MSGPACK_NIL {
                // NULL PK (LEFT JOIN produced no match)
                rel_scans[gi].refs.push(None);
            } else {
                let scan = &mut rel_scans[gi];
                let unique_idx = if let Some(&idx) = scan.seen.get(&pk_bytes) {
                    idx
                } else {
                    let idx = scan.unique_pk_bytes.len();
                    scan.seen.insert(pk_bytes.clone(), idx);
                    scan.unique_pk_bytes.push(pk_bytes);
                    scan.first_row_idx.push(row_idx);
                    idx
                };
                scan.refs.push(Some(unique_idx));
            }
        }
    }

    // ── Pass 2: encode ───────────────────────────────────────────────

    let num_rows = rows.len();
    let mut buf = Vec::with_capacity(num_rows * main_columns.len() * 16 + 256);

    // [main_columns, main_rows, relations]
    write_array_len(&mut buf, 3);

    // main_columns
    write_array_len(&mut buf, main_columns.len() as u32);
    for col in &main_columns {
        write_str(&mut buf, &col.name);
    }

    // main_rows
    write_array_len(&mut buf, num_rows as u32);
    for row in rows {
        write_array_len(&mut buf, main_indices.len() as u32);
        for (pos, &idx) in main_indices.iter().enumerate() {
            E::encode_cell(&mut buf, row, idx, main_columns[pos]);
        }
    }

    // relations map
    write_map_len(&mut buf, rel_groups.len() as u32);
    for (gi, group) in rel_groups.iter().enumerate() {
        let scan = &rel_scans[gi];

        write_str(&mut buf, group.prefix);
        write_map_len(&mut buf, 3);

        // "columns"
        write_str(&mut buf, "columns");
        write_array_len(&mut buf, group.stripped_names.len() as u32);
        for name in &group.stripped_names {
            write_str(&mut buf, name);
        }

        // "data": {pk_bytes: [row_values]}
        write_str(&mut buf, "data");
        write_map_len(&mut buf, scan.unique_pk_bytes.len() as u32);
        for (ui, pk_bytes) in scan.unique_pk_bytes.iter().enumerate() {
            // Key: PK value (already encoded msgpack bytes)
            buf.extend_from_slice(pk_bytes);
            // Value: relation row from first occurrence
            let first_row = &rows[scan.first_row_idx[ui]];
            write_array_len(&mut buf, group.col_indices.len() as u32);
            for (pos, &idx) in group.col_indices.iter().enumerate() {
                E::encode_cell(&mut buf, first_row, idx, group.col_metas[pos]);
            }
        }

        // "refs": [pk_bytes, pk_bytes, nil, ...]
        write_str(&mut buf, "refs");
        write_array_len(&mut buf, scan.refs.len() as u32);
        for r in &scan.refs {
            match r {
                Some(ui) => buf.extend_from_slice(&scan.unique_pk_bytes[*ui]),
                None => write_nil(&mut buf),
            }
        }
    }

    (buf, num_rows)
}

/// Encode rows as a mutation-with-returning result:
/// `{"affected": N, "columns": [...], "rows": [[...], ...]}`
pub fn encode_mutation_returning<E: CellEncoder>(
    rows: &[E::Row],
    col_types: Option<&HashMap<String, String>>,
) -> Vec<u8>
where
    <<E::Row as Row>::Database as Database>::Column: Column,
{
    if rows.is_empty() {
        let mut buf = Vec::with_capacity(32);
        write_map_len(&mut buf, 3);
        write_str(&mut buf, "affected");
        write_u64(&mut buf, 0);
        write_str(&mut buf, "columns");
        write_array_len(&mut buf, 0);
        write_str(&mut buf, "rows");
        write_array_len(&mut buf, 0);
        return buf;
    }

    let columns = E::extract_columns(&rows[0], col_types);

    let mut buf = Vec::with_capacity(rows.len() * columns.len() * 16 + 64);
    write_map_len(&mut buf, 3);
    write_str(&mut buf, "affected");
    write_u64(&mut buf, rows.len() as u64);
    write_str(&mut buf, "columns");
    encode_column_names(&mut buf, &columns);
    write_str(&mut buf, "rows");
    encode_row_data::<E>(&mut buf, rows, &columns);

    buf
}
