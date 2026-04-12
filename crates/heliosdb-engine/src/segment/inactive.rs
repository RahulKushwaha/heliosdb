//! InactiveSegment — a sealed, immutable SST file containing historical /
//! overflow data.
//!
//! The bloom filter on an inactive segment acts as a **negative existence
//! filter**: if `definitely_not_here(key)` returns `true`, the active segment
//! is the authoritative source and this file can be skipped entirely.

use std::path::{Path, PathBuf};

use bytes::Bytes;
use heliosdb_types::{Result, Value};

pub struct InactiveSegment {
    path:   PathBuf,
    reader: heliosdb_sst::SstReader,
    level:  u32,
}

impl InactiveSegment {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_at_level(path, 1)
    }

    pub fn open_at_level(path: impl AsRef<Path>, level: u32) -> Result<Self> {
        let reader = heliosdb_sst::SstReader::open(path.as_ref())?;
        Ok(Self { path: path.as_ref().to_path_buf(), reader, level })
    }

    /// Returns `true` if the key is **definitely not** in this segment.
    /// When `true`, the caller can skip this segment entirely for lookups.
    #[inline]
    pub fn definitely_not_here(&self, key: &[u8]) -> bool {
        self.reader.definitely_not_here(key)
    }

    /// Perform a point lookup. Returns `None` if bloom says absent.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Value>> {
        self.reader.get(key)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn level(&self) -> u32 {
        self.level
    }

    pub fn file_size(&self) -> u64 {
        self.reader.file_size()
    }

    /// Iterate all entries (used during compaction).
    pub fn iter(&mut self) -> Result<heliosdb_sst::reader::SstIter> {
        self.reader.iter()
    }
}
