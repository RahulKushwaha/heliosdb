//! ActiveSegment — the single on-disk file that always holds the latest version
//! of every live key in the database.
//!
//! It is built by the flush pipeline and can only be *read* or *sealed*.
//! Writes go through `segment::flush`, not directly here.

use std::path::{Path, PathBuf};

use bytes::Bytes;
use heliosdb_types::{Result, SeqNum, Value};

use crate::segment::inactive::InactiveSegment;

/// An ActiveSegment wraps a sealed SST file that is designated as the
/// authoritative current-values file.
///
/// Invariant: every live key's latest version is in this file.
/// (This invariant is maintained by the flush pipeline.)
pub struct ActiveSegment {
    path:   PathBuf,
    reader: heliosdb_sst::SstReader,
}

impl ActiveSegment {
    /// Open an existing SST file as the active segment.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let reader = heliosdb_sst::SstReader::open(path.as_ref())?;
        Ok(Self { path: path.as_ref().to_path_buf(), reader })
    }

    /// Look up the latest value for `key` in the active segment.
    /// Returns `None` if the key is absent.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Value>> {
        self.reader.get(key)
    }

    /// Seal this active segment: it becomes an InactiveSegment at `inactive_path`.
    /// The file is simply renamed (zero-copy).
    pub fn seal(self, inactive_path: impl AsRef<Path>) -> Result<InactiveSegment> {
        std::fs::rename(&self.path, inactive_path.as_ref())?;
        InactiveSegment::open(inactive_path.as_ref())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn file_size(&self) -> u64 {
        self.reader.file_size()
    }

    /// Iterate all (encoded_key, value) pairs in the active segment.
    /// Used by the flush pipeline to merge with the MemTable.
    pub fn iter(&mut self) -> Result<heliosdb_sst::reader::SstIter> {
        self.reader.iter()
    }
}
