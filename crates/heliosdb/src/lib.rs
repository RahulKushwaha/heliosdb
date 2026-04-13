//! heliosDB — Ressi-inspired storage engine.
//!
//! # Quick start
//! ```no_run
//! use heliosdb::{DB, Options, SkipListMemTable};
//!
//! let db = DB::<SkipListMemTable>::open("/tmp/mydb", Options::default()).unwrap();
//! db.put(b"hello", b"world").unwrap();
//! assert_eq!(db.get(b"hello").unwrap().as_deref(), Some(b"world".as_ref()));
//! db.delete(b"hello").unwrap();
//! ```

mod db;

pub use db::{Options, DB};
pub use heliosdb_engine::memtable::{MemTable, SkipListMemTable};
pub use heliosdb_types::{HeliosError, Result};
