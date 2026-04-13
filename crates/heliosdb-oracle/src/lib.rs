//! Correctness oracle for heliosDB.
//!
//! Runs every operation against both heliosDB and RocksDB, asserting that
//! results match. RocksDB is the source of truth.

use std::path::Path;

use heliosdb::{Options, SkipListMemTable, DB};

/// Dual-engine wrapper. Every mutating operation is applied to both engines.
/// Every read is compared; a mismatch panics with a descriptive message.
pub struct Oracle {
    helios: DB<SkipListMemTable>,
    rocks: rocksdb::DB,
}

/// Errors from the heliosDB side (RocksDB errors are treated as unexpected
/// and will panic).
pub type Result<T> = std::result::Result<T, heliosdb::HeliosError>;

impl Oracle {
    /// Open both engines in subdirectories of `dir`.
    pub fn open(dir: &Path, opts: Options) -> Self {
        let helios_dir = dir.join("helios");
        let rocks_dir = dir.join("rocks");

        let helios = DB::<SkipListMemTable>::open(&helios_dir, opts)
            .expect("heliosDB open failed");

        let mut rocks_opts = rocksdb::Options::default();
        rocks_opts.create_if_missing(true);
        let rocks = rocksdb::DB::open(&rocks_opts, &rocks_dir)
            .expect("RocksDB open failed");

        Self { helios, rocks }
    }

    /// Open with default heliosDB options.
    pub fn open_default(dir: &Path) -> Self {
        Self::open(dir, Options::default())
    }

    /// Put a key-value pair into both engines.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.helios.put(key, value)?;
        self.rocks.put(key, value).expect("RocksDB put failed");
        Ok(())
    }

    /// Delete a key from both engines.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.helios.delete(key)?;
        self.rocks.delete(key).expect("RocksDB delete failed");
        Ok(())
    }

    /// Get a key from both engines and assert the results match.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let helios_val = self.helios.get(key)?;
        let rocks_val = self.rocks.get(key).expect("RocksDB get failed");

        let helios_bytes = helios_val.as_ref().map(|b| b.as_ref());
        let rocks_bytes = rocks_val.as_deref();

        assert_eq!(
            helios_bytes, rocks_bytes,
            "get mismatch for key {:?}:\n  helios = {:?}\n  rocks  = {:?}",
            String::from_utf8_lossy(key),
            helios_bytes,
            rocks_bytes,
        );

        Ok(rocks_val)
    }

    /// Flush both engines.
    pub fn flush(&self) -> Result<()> {
        self.helios.flush()?;
        self.rocks.flush().expect("RocksDB flush failed");
        Ok(())
    }

    /// Scan a key range in both engines and assert the results match.
    ///
    /// Range is `[start, end)`.
    pub fn verify_scan(&self, start: &[u8], end: &[u8]) -> Result<()> {
        // heliosDB scan
        let helios_results = self.helios.scan(start, end)?;
        let helios_pairs: Vec<(&[u8], &[u8])> = helios_results
            .iter()
            .map(|(k, v)| (k.as_ref(), v.as_ref()))
            .collect();

        // RocksDB scan
        let mut rocks_pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut iter = self.rocks.raw_iterator();
        iter.seek(start);
        while iter.valid() {
            let key = iter.key().unwrap();
            if key >= end {
                break;
            }
            let value = iter.value().unwrap();
            rocks_pairs.push((key.to_vec(), value.to_vec()));
            iter.next();
        }

        let rocks_refs: Vec<(&[u8], &[u8])> = rocks_pairs
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        assert_eq!(
            helios_pairs.len(),
            rocks_refs.len(),
            "scan [{:?}, {:?}) count mismatch: helios={}, rocks={}",
            String::from_utf8_lossy(start),
            String::from_utf8_lossy(end),
            helios_pairs.len(),
            rocks_refs.len(),
        );

        for (i, (h, r)) in helios_pairs.iter().zip(rocks_refs.iter()).enumerate() {
            assert_eq!(
                h, r,
                "scan mismatch at index {i}:\n  helios = ({:?}, {:?})\n  rocks  = ({:?}, {:?})",
                String::from_utf8_lossy(h.0),
                String::from_utf8_lossy(h.1),
                String::from_utf8_lossy(r.0),
                String::from_utf8_lossy(r.1),
            );
        }

        Ok(())
    }
}
