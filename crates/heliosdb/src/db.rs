//! Top-level DB struct — coordinates MemTable, WAL, ActiveSegment,
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
    memtable::{GetResult, MemTable},
    segment::{ActiveSegment, InactiveSegment},
    Wal,
};
use heliosdb_sst::{builder::SstBuilder, CompressionType};
use heliosdb_types::{InternalKey, Result};

/// Default MemTable size threshold before a flush is triggered (64 MiB).
const DEFAULT_MEMTABLE_SIZE_LIMIT: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct Options {
    /// Flush MemTable to disk when it exceeds this many bytes.
    pub memtable_size_limit: usize,
    /// Compression codec for SST data blocks.
    pub compression: CompressionType,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            memtable_size_limit: DEFAULT_MEMTABLE_SIZE_LIMIT,
            compression: CompressionType::None,
        }
    }
}

/// Internal mutable state, protected by a single RwLock.
struct DbState {
    memtable:         MemTable,
    active:           Option<ActiveSegment>,
    inactive:         Vec<InactiveSegment>,
    manifest:         Manifest,
    version:          VersionSet,
    wal:              Wal,
}

pub struct DB {
    dir:     PathBuf,
    opts:    Options,
    next_seq: AtomicU64,
    state:   RwLock<DbState>,
}

impl DB {
    /// Open (or create) a heliosDB at `dir`.
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let manifest_path = dir.join("MANIFEST");
        let wal_path      = dir.join("WAL");

        // Recover version set from manifest.
        let version = Manifest::recover(&manifest_path)?;
        let next_seq = AtomicU64::new(version.next_seq().max(1));

        // Open or create manifest writer.
        let manifest = Manifest::open(&manifest_path)?;

        // Replay WAL → MemTable.
        let memtable = MemTable::new();
        Wal::replay(&wal_path, |key, value| {
            let seq = key.seq_num;
            match key.op_type {
                heliosdb_types::OpType::Put => {
                    memtable.put(key.user_key, seq, value);
                }
                heliosdb_types::OpType::Delete => {
                    memtable.delete(key.user_key, seq);
                }
            }
            // Advance next_seq past any replayed sequence numbers.
            let _ = next_seq.fetch_max(seq + 1, Ordering::Relaxed);
        })?;

        // Open active segment if one exists.
        let active = if let Some(p) = version.active_path() {
            if p.exists() {
                Some(ActiveSegment::open(p)?)
            } else {
                None
            }
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
                memtable,
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
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let ikey  = heliosdb_types::InternalKey::new_put(Bytes::copy_from_slice(key), seq);
        let value = Bytes::copy_from_slice(value);

        let mut state = self.state.write();
        state.wal.append(&ikey, &value)?;
        state.memtable.put(ikey.user_key, seq, value);

        if state.memtable.size_bytes() >= self.opts.memtable_size_limit {
            self.flush_locked(&mut state)?;
        }
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let ikey  = heliosdb_types::InternalKey::new_delete(Bytes::copy_from_slice(key), seq);
        let empty = Bytes::new();

        let mut state = self.state.write();
        state.wal.append(&ikey, &empty)?;
        state.memtable.delete(ikey.user_key, seq);

        if state.memtable.size_bytes() >= self.opts.memtable_size_limit {
            self.flush_locked(&mut state)?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Read path (Ressi active/inactive separation)
    // -----------------------------------------------------------------------

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let read_seq = self.next_seq.load(Ordering::Relaxed);
        let mut state = self.state.write(); // need &mut for segment readers

        // 1. MemTable (most recent)
        match state.memtable.get(key, read_seq) {
            Some(GetResult::Value(v)) => return Ok(Some(v)),
            Some(GetResult::Tombstone) => return Ok(None),
            None => {}
        }

        // 2. Active segment (authoritative for all live keys)
        if let Some(ref mut active) = state.active {
            // The active segment stores encoded InternalKeys as SST keys.
            // We need to find the entry whose user_key == key.
            // Use a bloom-safe seek: look for the raw encoded key prefix.
            // Simplified: scan active for matching user_key prefix.
            if let Some(value) = active_get(active, key)? {
                return Ok(Some(value));
            }
        }

        // 3. Inactive segments (fill in for historical/overflow data)
        //    Negative bloom filter skips most of these.
        for inactive in &mut state.inactive {
            if inactive.definitely_not_here(key) {
                continue;
            }
            if let Some(value) = inactive_get(inactive, key)? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    /// Scan all keys in [start, end). Returns entries in sorted order.
    /// Primary path: iterate active segment sequentially.
    /// Gap-fill: consult inactive segments for keys not in active.
    pub fn scan(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let read_seq = self.next_seq.load(Ordering::Relaxed);
        let mut results: std::collections::BTreeMap<Bytes, Bytes> = std::collections::BTreeMap::new();

        let mut state = self.state.write();

        // Collect from inactive (lowest priority — will be overwritten)
        for inactive in &mut state.inactive {
            let iter = inactive.iter()?;
            for item in iter {
                let (raw_key, val) = item?;
                if let Ok(ikey) = heliosdb_types::InternalKey::decode(&raw_key) {
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

        // Collect from active segment (overwrites inactive)
        if let Some(ref mut active) = state.active {
            let iter = active.iter()?;
            for item in iter {
                let (raw_key, val) = item?;
                if let Ok(ikey) = heliosdb_types::InternalKey::decode(&raw_key) {
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

        // MemTable has highest priority
        for (ikey, val) in state.memtable.iter().into_iter() {
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

        Ok(results.into_iter().collect())
    }

    // -----------------------------------------------------------------------
    // Flush
    // -----------------------------------------------------------------------

    /// Flush the MemTable into a new ActiveSegment by merging it with the
    /// existing active segment (if any).  The old active file is deleted.
    ///
    /// **Invariant maintained**: the new active segment always contains the
    /// latest version of every live key — MemTable entries win over old active
    /// entries for the same user_key.
    fn flush_locked(&self, state: &mut DbState) -> Result<()> {
        if state.memtable.is_empty() {
            return Ok(());
        }

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        let new_active_path = self.dir.join(format!("active_{ts}.sst"));

        // --- collect old active entries (lower priority) ---
        let old_active_path: Option<PathBuf>;
        let active_entries: Vec<(InternalKey, Bytes)> = if let Some(mut old) = state.active.take() {
            old_active_path = Some(old.path().to_path_buf());
            old.iter()?
                .filter_map(|r| r.ok())
                .filter_map(|(raw_key, val)| {
                    InternalKey::decode(&raw_key).ok().map(|ik| (ik, val))
                })
                .collect()
        } else {
            old_active_path = None;
            Vec::new()
        };

        // --- merge: MemTable (priority 0) beats old active (priority 1) ---
        let mem_iter: Box<dyn Iterator<Item = (InternalKey, Bytes)> + Send> =
            Box::new(state.memtable.iter().into_iter());
        let active_iter: Box<dyn Iterator<Item = (InternalKey, Bytes)> + Send> =
            Box::new(active_entries.into_iter());
        let merged = MergeIterator::new(vec![mem_iter, active_iter]);

        // --- write merged output to new active SST ---
        let mut buf: Vec<u8> = Vec::new();
        let expected = (state.memtable.size_bytes() / 64).max(64);
        let mut builder = SstBuilder::new(Cursor::new(&mut buf), expected, self.opts.compression);
        for (ikey, value) in merged {
            builder.add(&ikey.encode(), &value)?;
        }
        builder.finish()?;
        std::fs::write(&new_active_path, &buf)?;

        // --- atomically swap: delete old active, promote new active ---
        if let Some(ref old_path) = old_active_path {
            let _ = std::fs::remove_file(old_path);
        }

        let new_active = ActiveSegment::open(&new_active_path)?;
        state.active = Some(new_active);

        // --- update manifest ---
        state.manifest.append(&Edit::SetActive { path: new_active_path.clone() })?;
        state.manifest.append(&Edit::SetNextSeq {
            seq: self.next_seq.load(Ordering::Relaxed),
        })?;
        state.version.set_active(new_active_path);

        // --- reset MemTable and rotate WAL ---
        state.memtable = MemTable::new();
        state.wal = Wal::create(&self.dir.join("WAL"))?;

        Ok(())
    }

    /// Force a flush even if the MemTable hasn't hit the size limit.
    pub fn flush(&self) -> Result<()> {
        let mut state = self.state.write();
        self.flush_locked(&mut state)
    }
}

// ---------------------------------------------------------------------------
// Helpers: look up a user_key in segment files (which store encoded InternalKeys)
// ---------------------------------------------------------------------------

fn active_get(active: &mut ActiveSegment, user_key: &[u8]) -> Result<Option<Bytes>> {
    let iter = active.iter()?;
    for item in iter {
        let (raw_key, val) = item?;
        if let Ok(ikey) = heliosdb_types::InternalKey::decode(&raw_key) {
            if ikey.user_key.as_ref() == user_key {
                return match ikey.op_type {
                    heliosdb_types::OpType::Put    => Ok(Some(val)),
                    heliosdb_types::OpType::Delete => Ok(None),
                };
            }
            if ikey.user_key.as_ref() > user_key {
                break; // sorted order — key not present
            }
        }
    }
    Ok(None)
}

fn inactive_get(inactive: &mut InactiveSegment, user_key: &[u8]) -> Result<Option<Bytes>> {
    let iter = inactive.iter()?;
    for item in iter {
        let (raw_key, val) = item?;
        if let Ok(ikey) = heliosdb_types::InternalKey::decode(&raw_key) {
            if ikey.user_key.as_ref() == user_key {
                return match ikey.op_type {
                    heliosdb_types::OpType::Put    => Ok(Some(val)),
                    heliosdb_types::OpType::Delete => Ok(None),
                };
            }
            if ikey.user_key.as_ref() > user_key {
                break;
            }
        }
    }
    Ok(None)
}
