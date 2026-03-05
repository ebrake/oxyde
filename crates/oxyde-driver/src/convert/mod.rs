//! Type conversion: database rows → msgpack bytes.
//!
//! `encoder` defines the `CellEncoder` trait and generic columnar encoding.
//! `postgres`, `mysql`, `sqlite` implement it per backend.
//! All encoding writes directly to `Vec<u8>` msgpack buffers.

pub mod encoder;
pub mod mysql;
pub mod postgres;
pub mod sqlite;
