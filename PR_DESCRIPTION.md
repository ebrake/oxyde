# Title

Add array and JSON lookups across PostgreSQL, MySQL, and SQLite

# Description

Depends on [mr-fatalyst/oxyde#50](https://github.com/mr-fatalyst/oxyde/pull/50). Stacked on [`ebrake/oxyde:join-db-column`](https://github.com/ebrake/oxyde/tree/join-db-column).

Add `contains`, `contained_by`, and `overlap` filters for typed arrays and JSON fields holding arrays. For example, `tags__contains=["rust"]` selects arrays containing `"rust"`, while `tags__overlap=("rust", "python")` selects arrays sharing either element. Operands accept lists or tuples, ignore outer array order and duplicates, and compare objects as complete values.

## Changes

- Generate PostgreSQL native array predicates with column-type casts and null-aware element comparison, MySQL `JSON_TABLE` predicates, and SQLite predicates using `oxyde_array_match()`, registered on every pooled connection. SQLite also handles JSONB and malformed values written by other clients.
- Validate typed operands and preserve UUID, JSON string, datetime, and decimal values consistently during storage and filtering. Normalize scientific decimal strings before checking representability, so exact values such as `"1000000000000000000000000000000E-2"` succeed; reject operands that would require rounding or overflow, and handle extreme exponents without panicking.
- Support related fields whose names overlap with lookup names and HAVING annotation aliases that end in lookup names.
- Fix PostgreSQL array `db_type` overrides, mixed timezone elements, and string lookups on JSON-backed string fields.
- Extend generated filter stubs, positive and negative type-check fixtures, backend integration tests, and filtering documentation.
- Make backend-specific test coverage explicit, enable TLS in the PostgreSQL fixture, and fail when full-suite prerequisites are missing. Replace the five ignored Rust documentation snippets with executable examples using the current APIs.

## Backend considerations

PostgreSQL can use native array indexes for ordinary typed operands. MySQL requires 8.0.4+ and compares non-integer JSON numbers with floating-point precision. SQLite array predicates require an Oxyde connection; the database file remains readable and writable by other SQLite clients. Nested typed arrays remain unsupported. Existing rows with previously mishandled timezone elements retain their stored values until saved again.

## Validation

- Python: **1,818 passed, zero skipped** with a source-built Rust extension, SQLite, disposable PostgreSQL 16/MySQL 8 containers, PostgreSQL TLS enabled, and mypy 1.19.1, Pyright 1.1.411, and ty 0.0.59. All 24 previously skipped type-check tests and the PostgreSQL TLS test execute. Backend-only cases are parametrized for supported databases, preserving all applicable cases without runtime skips.
- Rust: 240 tests passed, including all five formerly ignored documentation examples, with `cargo test --workspace --no-default-features --locked` on macOS. Zero ignored tests.
- All repository pre-commit checks passed, including Ruff, mypy, Cargo formatting, checking, and Clippy with warnings denied.
