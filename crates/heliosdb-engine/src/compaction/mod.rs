//! Leveled compaction of InactiveSegments.
//!
//! The ActiveSegment is never touched by compaction.
//! Compaction operates only on InactiveSegments, merging files within a level
//! and pushing the result to the next level.
//!
//! Level size limits:
//!   L1:  10 MB
//!   L2: 100 MB
//!   L3:   1 GB
//!   LN:   L(N-1) * 10

use std::{
    io::Cursor,
    path::{Path, PathBuf},
};

use heliosdb_sst::{builder::SstBuilder, CompressionType};
use heliosdb_types::{InternalKey, OpType, Result};

use crate::{
    iterator::MergeIterator,
    manifest::{Edit, Manifest, VersionSet},
    segment::InactiveSegment,
};

const LEVEL_SIZE_LIMITS: &[u64] = &[
    0,                   // L0 (unused — active segment replaces L0)
    10 * 1024 * 1024,    // L1: 10 MB
    100 * 1024 * 1024,   // L2: 100 MB
    1024 * 1024 * 1024,  // L3: 1 GB
];

/// Returns the size limit for level `l` (L4+ multiply by 10 each time).
fn level_limit(l: u32) -> u64 {
    if l == 0 {
        return u64::MAX;
    }
    let l = l as usize;
    if l < LEVEL_SIZE_LIMITS.len() {
        LEVEL_SIZE_LIMITS[l]
    } else {
        LEVEL_SIZE_LIMITS.last().unwrap() * 10u64.pow((l - LEVEL_SIZE_LIMITS.len() + 1) as u32)
    }
}

pub struct Compactor<'a> {
    db_dir:   &'a Path,
    manifest: &'a mut Manifest,
    version:  &'a mut VersionSet,
}

impl<'a> Compactor<'a> {
    pub fn new(db_dir: &'a Path, manifest: &'a mut Manifest, version: &'a mut VersionSet) -> Self {
        Self { db_dir, manifest, version }
    }

    /// Run one round of compaction: find the highest-priority level that needs
    /// compaction and compact it into the next level.
    pub fn maybe_compact(&mut self) -> Result<bool> {
        // Find the first level that exceeds its size limit.
        for level in 1u32..=6 {
            let total: u64 = self
                .version
                .inactive_at_level(level)
                .iter()
                .filter_map(|p| std::fs::metadata(p).ok())
                .map(|m| m.len())
                .sum();

            if total > level_limit(level) {
                self.compact_level(level)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn compact_level(&mut self, level: u32) -> Result<()> {
        let input_paths: Vec<PathBuf> =
            self.version.inactive_at_level(level).to_vec();
        if input_paths.is_empty() {
            return Ok(());
        }

        // Open all input segments and collect iterators.
        let mut iters: Vec<Box<dyn Iterator<Item = (InternalKey, bytes::Bytes)> + Send>> = Vec::new();
        for path in &input_paths {
            let mut seg = InactiveSegment::open_at_level(path, level)?;
            let entries: Vec<_> = seg
                .iter()?
                .filter_map(|r| r.ok())
                .filter_map(|(raw_key, val)| {
                    let ikey = InternalKey::decode(&raw_key).ok()?;
                    Some((ikey, val))
                })
                .collect();
            iters.push(Box::new(entries.into_iter()));
        }

        let merged = MergeIterator::new(iters);

        // Write compacted output to next level.
        let out_path = self.db_dir.join(format!(
            "l{}_compacted_{}.sst",
            level + 1,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros()
        ));

        let mut buf = Vec::new();
        let mut builder = SstBuilder::new(Cursor::new(&mut buf), 0, CompressionType::None);

        for (ikey, value) in merged {
            // Drop tombstones when compacting to the deepest level (they've
            // already propagated past all inactive segments above).
            if ikey.op_type == OpType::Delete && level >= 6 {
                continue;
            }
            builder.add(&ikey.encode(), &value)?;
        }
        builder.finish()?;
        std::fs::write(&out_path, &buf)?;

        // Update manifest: add compacted file, remove inputs.
        self.manifest.append(&Edit::AddInactive { level: level + 1, path: out_path.clone() })?;
        for path in &input_paths {
            self.manifest.append(&Edit::RemoveInactive { level, path: path.clone() })?;
        }

        // Update version
        self.version.apply(&Edit::AddInactive { level: level + 1, path: out_path });
        for path in &input_paths {
            self.version.apply(&Edit::RemoveInactive { level, path: path.clone() });
            let _ = std::fs::remove_file(path); // best-effort cleanup
        }

        Ok(())
    }
}
