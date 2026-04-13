//! Top-level DB struct — coordinates MemTableSet, WAL, ActiveSegment,
//! InactiveSegments, Manifest, and Compaction.

use std::{
    io::Cursor,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use parking_lot::RwLock;

use heliosdb_engine::{
    compaction::Compactor,
    iterator::MergeIterator,
    manifest::{Edit, Manifest, VersionSet},
    memtable::{GetResult, MemTable, MemTableSet, SkipListMemTable},
    segment::{ActiveSegment, InactiveSegment},
    Wal,
};
use heliosdb_sst::{builder::SstBuilder, CompressionType};
use heliosdb_types::{InternalKey, Result};

/// Default write-buffer size: seal the active memtable at 64 MiB.
const DEFAULT_WRITE_BUFFER_SIZE: usize = 64 * 1024 * 1024;

/// Default immutable queue depth.  Writes stall when this many sealed
/// memtables are waiting to be flushed.
const DEFAULT_MAX_IMMUTABLE: usize = 2;

#[derive(Debug, Clone)]
pub struct Options {
    /// Seal the active memtable when it exceeds this many bytes.
    pub write_buffer_size: usize,
    /// Maximum number of immutable memtables before writes stall.
    pub max_immutable_count: usize,
    /// Compression codec for SST data blocks.
    pub compression: CompressionType,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            write_buffer_size:  DEFAULT_WRITE_BUFFER_SIZE,
            max_immutable_count: DEFAULT_MAX_IMMUTABLE,
            compression:        CompressionType::None,
        }
    }
}

/// Internal mutable state, protected by a single RwLock.
struct DbState<M> {
    memtable_set: MemTableSet<M>,
    active:       Option<ActiveSegment>,
    inactive:     Vec<InactiveSegment>,
    manifest:     Manifest,
    version:      VersionSet,
    wal:          Wal,
}

pub struct DB<M = SkipListMemTable> {
    dir:      PathBuf,
    opts:     Options,
    next_seq: AtomicU64,
    state:    RwLock<DbState<M>>,
}

impl<M: MemTable> DB<M> {
    /// Open (or create) a heliosDB at `dir`.
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let manifest_path = dir.join("MANIFEST");
        let wal_path      = dir.join("WAL");

        // Recover version set from manifest.
        let version  = Manifest::recover(&manifest_path)?;
        let next_seq = AtomicU64::new(version.next_seq().max(1));

        // Open or create manifest writer.
        let manifest = Manifest::open(&manifest_path)?;

        // Replay WAL into a temporary memtable, then hand it to MemTableSet.
        // All replayed entries land in the active slot; the immutable queue
        // starts empty regardless of what was in flight before the crash.
        let recovered: M = M::default();
        Wal::replay(&wal_path, |key, value| {
            let seq = key.seq_num;
            match key.op_type {
                heliosdb_types::OpType::Put    => recovered.put(key.user_key, seq, value),
                heliosdb_types::OpType::Delete => recovered.delete(key.user_key, seq),
            }
            let _ = next_seq.fetch_max(seq + 1, Ordering::Relaxed);
        })?;
        let memtable_set = MemTableSet::with_active(
            recovered,
            opts.write_buffer_size,
            opts.max_immutable_count,
        );

        // Open active segment if one exists.
        let active = if let Some(p) = version.active_path() {
            if p.exists() { Some(ActiveSegment::open(p)?) } else { None }
        } else {
            None
        };

        // Open inactive segments.
        let mut inactive = Vec::new();
        for (level, path) in version.all_inactive() {
            if path.exists() {
                inactive.push(InactiveSegment::open_at_level(path, level)?);
            }
        }

        let wal = Wal::open(&wal_path)?;

        Ok(Self {
            dir,
            opts,
            next_seq,
            state: RwLock::new(DbState {
                memtable_set,
                active,
                inactive,
                manifest,
                version,
                wal,
            }),
        })
    }

    // -----------------------------------------------------------------------
    // Write path
    // -----------------------------------------------------------------------

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let seq   = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let ikey  = InternalKey::new_put(Bytes::copy_from_slice(key), seq);
        let value = Bytes::copy_from_slice(value);

        let mut state = self.state.write();
        state.wal.append(&ikey, &value)?;
        state.memtable_set.put(ikey.user_key, seq, value);

        self.maybe_rotate(&mut state)?;
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let seq  = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let ikey = InternalKey::new_delete(Bytes::copy_from_slice(key), seq);

        let mut state = self.state.write();
        state.wal.append(&ikey, &Bytes::new())?;
        state.memtable_set.delete(ikey.user_key, seq);

        self.maybe_rotate(&mut state)?;
        Ok(())
    }

    /// Check after every write: if the active memtable has hit the threshold,
    /// flush the oldest immutable (if queue is full) then rotate.
    fn maybe_rotate(&self, state: &mut DbState<M>) -> Result<()> {
        if !state.memtable_set.should_rotate() {
            return Ok(());
        }
        if state.memtable_set.is_at_capacity() {
            self.flush_oldest_immutable_locked(state)?;
        }
        state.memtable_set.rotate();
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Read path
    // -----------------------------------------------------------------------

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let read_seq = self.next_seq.load(Ordering::Relaxed);
        let mut state = self.state.write();

        // 1. MemTableSet: active → immutables newest → oldest
        match state.memtable_set.get(key, read_seq) {
            Some(GetResult::Value(v))  => return Ok(Some(v)),
            Some(GetResult::Tombstone) => return Ok(None),
            None => {}
        }

        // 2. Active segment
        if let Some(ref mut active) = state.active {
            if let Some(value) = active_get(active, key)? {
                return Ok(Some(value));
            }
        }

        // 3. Inactive segments (bloom-filtered)
        for inactive in &mut state.inactive {
            if inactive.definitely_not_here(key) { continue; }
            if let Some(value) = inactive_get(inactive, key)? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        let read_seq = self.next_seq.load(Ordering::Relaxed);
        let mut results: std::collections::BTreeMap<Bytes, Bytes> =
            std::collections::BTreeMap::new();

        let mut state = self.state.write();

        // 1. Inactive segments (lowest priority)
        for inactive in &mut state.inactive {
            for item in inactive.iter()? {
                let (raw_key, val) = item?;
                if let Ok(ikey) = InternalKey::decode(&raw_key) {
                    if ikey.seq_num > read_seq { continue; }
                    let uk = ikey.user_key.clone();
                    if uk.as_ref() >= start && uk.as_ref() < end {
                        if ikey.op_type == heliosdb_types::OpType::Put {
                            results.entry(uk).or_insert(val);
                        }
                    }
                }
            }
        }

        // 2. Active segment
        if let Some(ref mut active) = state.active {
            for item in active.iter()? {
                let (raw_key, val) = item?;
                if let Ok(ikey) = InternalKey::decode(&raw_key) {
                    if ikey.seq_num > read_seq { continue; }
                    let uk = ikey.user_key.clone();
                    if uk.as_ref() >= start && uk.as_ref() < end {
                        if ikey.op_type == heliosdb_types::OpType::Put {
                            results.insert(uk, val);
                        } else {
                            results.remove(&uk);
                        }
                    }
                }
            }
        }

        // 3. MemTableSet sources: oldest immutable → newest → active
        //    Each overwrites the previous, so active ends up highest priority.
        for entries in state.memtable_set.iter_all_by_priority() {
            for (ikey, val) in entries {
                if ikey.seq_num > read_seq { continue; }
                let uk = ikey.user_key.clone();
                if uk.as_ref() >= start && uk.as_ref() < end {
                    if ikey.op_type == heliosdb_types::OpType::Put {
                        results.insert(uk, val);
                    } else {
                        results.remove(&uk);
                    }
                }
            }
        }

        Ok(results.into_iter().collect())
    }

    // -----------------------------------------------------------------------
    // Flush
    // -----------------------------------------------------------------------

    /// Flush one immutable memtable into the active SST.
    /// Does NOT rotate the WAL — the WAL still covers the remaining
    /// immutables and the current active memtable.
    fn flush_oldest_immutable_locked(&self, state: &mut DbState<M>) -> Result<()> {
        let oldest = match state.memtable_set.pop_oldest_immutable() {
            Some(m) => m,
            None    => return Ok(()),
        };

        let new_active_path = self.new_active_path();

        // Collect old active SST entries (lower priority than the immutable).
        let (old_active_path, active_entries) = self.drain_active_sst(state)?;

        // Merge: immutable (priority 0) beats old active SST (priority 1).
        let mem_iter: Box<dyn Iterator<Item = (InternalKey, Bytes)> + Send> =
            Box::new(oldest.iter().into_iter());
        let sst_iter: Box<dyn Iterator<Item = (InternalKey, Bytes)> + Send> =
            Box::new(active_entries.into_iter());
        let merged = MergeIterator::new(vec![mem_iter, sst_iter]);

        let expected = (oldest.size_bytes() / 64).max(64);
        self.write_sst(&new_active_path, merged, expected)?;

        if let Some(ref p) = old_active_path { let _ = std::fs::remove_file(p); }
        self.promote_active(state, new_active_path)?;

        Ok(())
    }

    /// Full flush: seal the active memtable, drain the entire immutable queue,
    /// then rotate the WAL (now safe — all data is persisted in SST).
    fn flush_locked(&self, state: &mut DbState<M>) -> Result<()> {
        // Seal the active memtable if it has data.
        if !state.memtable_set.active_is_empty() {
            if state.memtable_set.is_at_capacity() {
                self.flush_oldest_immutable_locked(state)?;
            }
            state.memtable_set.rotate();
        }

        // Flush every immutable in order (oldest first).
        while state.memtable_set.immutable_count() > 0 {
            self.flush_oldest_immutable_locked(state)?;
        }

        // All data is now in SST — safe to truncate the WAL.
        state.wal = Wal::create(&self.dir.join("WAL"))?;
        Ok(())
    }

    /// Force a full flush even if the memtable hasn't hit the size limit.
    pub fn flush(&self) -> Result<()> {
        let mut state = self.state.write();
        self.flush_locked(&mut state)
    }

    // -----------------------------------------------------------------------
    // Flush helpers
    // -----------------------------------------------------------------------

    fn new_active_path(&self) -> PathBuf {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        self.dir.join(format!("active_{ts}.sst"))
    }

    /// Take the current active SST out of state, returning its path and entries.
    fn drain_active_sst(
        &self,
        state: &mut DbState<M>,
    ) -> Result<(Option<PathBuf>, Vec<(InternalKey, Bytes)>)> {
        if let Some(mut old) = state.active.take() {
            let path = Some(old.path().to_path_buf());
            let entries = old.iter()?
                .filter_map(|r| r.ok())
                .filter_map(|(raw, val)| InternalKey::decode(&raw).ok().map(|ik| (ik, val)))
                .collect();
            Ok((path, entries))
        } else {
            Ok((None, Vec::new()))
        }
    }

    fn write_sst(
        &self,
        path: &Path,
        merged: impl Iterator<Item = (InternalKey, Bytes)>,
        expected_keys: usize,
    ) -> Result<()> {
        let mut buf = Vec::new();
        let mut builder = SstBuilder::new(
            Cursor::new(&mut buf),
            expected_keys,
            self.opts.compression,
        );
        for (ikey, value) in merged {
            builder.add(&ikey.encode(), &value)?;
        }
        builder.finish()?;
        std::fs::write(path, &buf)?;
        Ok(())
    }

    fn promote_active(&self, state: &mut DbState<M>, path: PathBuf) -> Result<()> {
        let new_active = ActiveSegment::open(&path)?;
        state.active = Some(new_active);
        state.manifest.append(&Edit::SetActive { path: path.clone() })?;
        state.manifest.append(&Edit::SetNextSeq {
            seq: self.next_seq.load(Ordering::Relaxed),
        })?;
        state.version.set_active(path);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers: point-lookup inside segment files
// ---------------------------------------------------------------------------

fn active_get(active: &mut ActiveSegment, user_key: &[u8]) -> Result<Option<Bytes>> {
    for item in active.iter()? {
        let (raw_key, val) = item?;
        if let Ok(ikey) = InternalKey::decode(&raw_key) {
            if ikey.user_key.as_ref() == user_key {
                return match ikey.op_type {
                    heliosdb_types::OpType::Put    => Ok(Some(val)),
                    heliosdb_types::OpType::Delete => Ok(None),
                };
            }
            if ikey.user_key.as_ref() > user_key { break; }
        }
    }
    Ok(None)
}

fn inactive_get(inactive: &mut InactiveSegment, user_key: &[u8]) -> Result<Option<Bytes>> {
    for item in inactive.iter()? {
        let (raw_key, val) = item?;
        if let Ok(ikey) = InternalKey::decode(&raw_key) {
            if ikey.user_key.as_ref() == user_key {
                return match ikey.op_type {
                    heliosdb_types::OpType::Put    => Ok(Some(val)),
                    heliosdb_types::OpType::Delete => Ok(None),
                };
            }
            if ikey.user_key.as_ref() > user_key { break; }
        }
    }
    Ok(None)
}
