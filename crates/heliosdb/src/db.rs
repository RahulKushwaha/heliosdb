//! Top-level DB struct — coordinates MemTableSet, WAL, ActiveSegment,
//! InactiveSegments, Manifest, and Compaction.
//!
//! A background **flusher thread** drains the immutable memtable queue.
//! A bounded `crossbeam_channel` acts as the queue between the write path
//! and the flusher — backpressure is natural (send blocks when full).

use std::{
    io::Cursor,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread::JoinHandle,
};

use bytes::Bytes;
use crossbeam_channel::{bounded, Receiver, Sender};
use parking_lot::RwLock;

use heliosdb_engine::{
    manifest::{Edit, Manifest, VersionSet},
    memtable::{GetResult, MemTable, MemTableSet, SkipListMemTable},
    segment::{ActiveSegment, InactiveSegment},
    Wal,
};
use heliosdb_sst::{builder::SstBuilder, CompressionType};
use heliosdb_types::{InternalKey, Result};

/// Default write-buffer size: seal the active memtable at 64 MiB.
const DEFAULT_WRITE_BUFFER_SIZE: usize = 64 * 1024 * 1024;

/// Default immutable queue depth.
const DEFAULT_MAX_IMMUTABLE: usize = 2;

#[derive(Debug, Clone)]
pub struct Options {
    pub write_buffer_size: usize,
    pub max_immutable_count: usize,
    pub compression: CompressionType,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            write_buffer_size:   DEFAULT_WRITE_BUFFER_SIZE,
            max_immutable_count: DEFAULT_MAX_IMMUTABLE,
            compression:         CompressionType::None,
        }
    }
}

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

struct DbState<M> {
    memtable_set: MemTableSet<M>,
    active:       Option<ActiveSegment>,
    inactive:     Vec<InactiveSegment>,
    manifest:     Manifest,
    version:      VersionSet,
    wal:          Wal,
}

struct DbInner<M> {
    dir:      PathBuf,
    opts:     Options,
    next_seq: AtomicU64,
    state:    RwLock<DbState<M>>,
}

pub struct DB<M = SkipListMemTable> {
    inner:    Arc<DbInner<M>>,
    flush_tx: Sender<Arc<M>>,
    flusher:  Option<JoinHandle<()>>,
}

// ---------------------------------------------------------------------------
// DB implementation
// ---------------------------------------------------------------------------

impl<M: MemTable + 'static> DB<M> {
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let manifest_path = dir.join("MANIFEST");
        let wal_path      = dir.join("WAL");

        let version  = Manifest::recover(&manifest_path)?;
        let next_seq = AtomicU64::new(version.next_seq().max(1));
        let manifest = Manifest::open(&manifest_path)?;

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

        let active = if let Some(p) = version.active_path() {
            if p.exists() { Some(ActiveSegment::open(p)?) } else { None }
        } else {
            None
        };

        let mut inactive = Vec::new();
        for (level, path) in version.all_inactive() {
            if path.exists() {
                inactive.push(InactiveSegment::open_at_level(path, level)?);
            }
        }

        let wal = Wal::open(&wal_path)?;

        let inner = Arc::new(DbInner {
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
        });

        // Bounded channel = the queue.  Capacity = max_immutable_count.
        let (flush_tx, flush_rx) = bounded(inner.opts.max_immutable_count);

        let flusher_inner = Arc::clone(&inner);
        let flusher = std::thread::Builder::new()
            .name("helios-flusher".into())
            .spawn(move || flusher_loop(flusher_inner, flush_rx))
            .map_err(|e| heliosdb_types::HeliosError::InvalidArgument(
                format!("failed to spawn flusher: {e}"),
            ))?;

        Ok(Self { inner, flush_tx, flusher: Some(flusher) })
    }

    // -----------------------------------------------------------------------
    // Write path
    // -----------------------------------------------------------------------

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let seq   = self.inner.next_seq.fetch_add(1, Ordering::Relaxed);
        let ikey  = InternalKey::new_put(Bytes::copy_from_slice(key), seq);
        let value = Bytes::copy_from_slice(value);

        let sealed = {
            let mut state = self.inner.state.write();
            state.wal.append(&ikey, &value)?;
            state.memtable_set.put(ikey.user_key, seq, value);
            if state.memtable_set.should_rotate() {
                Some(state.memtable_set.rotate_arc())
            } else {
                None
            }
        };

        if let Some(arc) = sealed {
            self.flush_tx.send(arc).map_err(|_| {
                heliosdb_types::HeliosError::InvalidArgument("flusher thread died".into())
            })?;
        }
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let seq  = self.inner.next_seq.fetch_add(1, Ordering::Relaxed);
        let ikey = InternalKey::new_delete(Bytes::copy_from_slice(key), seq);

        let sealed = {
            let mut state = self.inner.state.write();
            state.wal.append(&ikey, &Bytes::new())?;
            state.memtable_set.delete(ikey.user_key, seq);
            if state.memtable_set.should_rotate() {
                Some(state.memtable_set.rotate_arc())
            } else {
                None
            }
        };

        if let Some(arc) = sealed {
            self.flush_tx.send(arc).map_err(|_| {
                heliosdb_types::HeliosError::InvalidArgument("flusher thread died".into())
            })?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Read path
    // -----------------------------------------------------------------------

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let read_seq = self.inner.next_seq.load(Ordering::Relaxed);
        let mut state = self.inner.state.write();

        // 1. MemTableSet (highest priority)
        match state.memtable_set.get(key, read_seq) {
            Some(GetResult::Value(v))  => return Ok(Some(v)),
            Some(GetResult::Tombstone) => return Ok(None),
            None => {}
        }

        // 2. Active segment (from recovery — legacy)
        if let Some(ref mut seg) = state.active {
            match segment_get(seg.iter()?, key, read_seq)? {
                Some(GetResult::Value(v))  => return Ok(Some(v)),
                Some(GetResult::Tombstone) => return Ok(None),
                None => {}
            }
        }

        // 3. Inactive segments — newest first (last pushed = newest)
        for seg in state.inactive.iter_mut().rev() {
            match segment_get(seg.iter()?, key, read_seq)? {
                Some(GetResult::Value(v))  => return Ok(Some(v)),
                Some(GetResult::Tombstone) => return Ok(None),
                None => {}
            }
        }

        Ok(None)
    }

    pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        let read_seq = self.inner.next_seq.load(Ordering::Relaxed);
        let mut results: std::collections::BTreeMap<Bytes, Bytes> =
            std::collections::BTreeMap::new();

        let mut state = self.inner.state.write();

        // Lowest → highest priority.  Each layer overwrites the previous.

        // 1. Inactive segments (oldest first = lowest priority)
        for seg in state.inactive.iter_mut() {
            scan_segment(seg.iter()?, start, end, read_seq, &mut results)?;
        }

        // 2. Active segment (legacy, from recovery)
        if let Some(ref mut seg) = state.active {
            scan_segment(seg.iter()?, start, end, read_seq, &mut results)?;
        }

        // 3. MemTableSet: oldest immutable → newest → active
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
    // Flush (explicit, synchronous)
    // -----------------------------------------------------------------------

    pub fn flush(&self) -> Result<()> {
        let mut state = self.inner.state.write();

        if !state.memtable_set.active_is_empty() {
            if state.memtable_set.is_at_capacity() {
                flush_oldest(&self.inner, &mut state)?;
            }
            state.memtable_set.rotate();
        }

        while state.memtable_set.immutable_count() > 0 {
            flush_oldest(&self.inner, &mut state)?;
        }

        state.wal = Wal::create(&self.inner.dir.join("WAL"))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Flush logic (shared by flusher thread and explicit flush)
// ---------------------------------------------------------------------------

fn flush_oldest<M: MemTable>(inner: &DbInner<M>, state: &mut DbState<M>) -> Result<()> {
    let oldest = match state.memtable_set.pop_oldest_immutable() {
        Some(m) => m,
        None    => return Ok(()),
    };

    // Write the sealed memtable to a new SST file — no merging.
    let path = inner.new_sst_path();
    let expected = (oldest.size_bytes() / 64).max(64);
    inner.write_sst(&path, oldest.iter().into_iter(), expected)?;

    // Register as level-0 inactive segment.
    let segment = InactiveSegment::open_at_level(&path, 0)?;
    state.inactive.push(segment);

    state.manifest.append(&Edit::AddInactive { level: 0, path })?;
    state.manifest.append(&Edit::SetNextSeq {
        seq: inner.next_seq.load(Ordering::Relaxed),
    })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Background flusher
// ---------------------------------------------------------------------------

fn flusher_loop<M: MemTable>(inner: Arc<DbInner<M>>, rx: Receiver<Arc<M>>) {
    while let Ok(_sealed) = rx.recv() {
        let mut state = inner.state.write();
        if state.memtable_set.immutable_count() == 0 {
            continue;
        }
        if let Err(e) = flush_oldest(&inner, &mut state) {
            tracing::error!("background flush failed: {e}");
        }
    }
}

// ---------------------------------------------------------------------------
// DbInner helpers
// ---------------------------------------------------------------------------

impl<M: MemTable> DbInner<M> {
    fn new_sst_path(&self) -> PathBuf {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        self.dir.join(format!("l0_{ts}.sst"))
    }

    fn write_sst(
        &self,
        path: &Path,
        entries: impl Iterator<Item = (InternalKey, Bytes)>,
        expected_keys: usize,
    ) -> Result<()> {
        let mut buf = Vec::new();
        let mut builder = SstBuilder::new(
            Cursor::new(&mut buf),
            expected_keys,
            self.opts.compression,
        );
        for (ikey, value) in entries {
            builder.add(&ikey.encode(), &value)?;
        }
        builder.finish()?;
        std::fs::write(path, &buf)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Clean shutdown
// ---------------------------------------------------------------------------

impl<M> Drop for DB<M> {
    fn drop(&mut self) {
        let (_dummy, _) = bounded(0);
        let _ = std::mem::replace(&mut self.flush_tx, _dummy);
        if let Some(h) = self.flusher.take() {
            let _ = h.join();
        }
    }
}

// ---------------------------------------------------------------------------
// Segment helpers
// ---------------------------------------------------------------------------

/// Point-lookup inside an SST iterator. Returns `Some(Value(...))` or
/// `Some(Tombstone)` on match, `None` if the key isn't in this segment.
fn segment_get(
    iter: heliosdb_sst::reader::SstIter,
    user_key: &[u8],
    read_seq: u64,
) -> Result<Option<GetResult>> {
    for item in iter {
        let (raw_key, val) = item?;
        if let Ok(ikey) = InternalKey::decode(&raw_key) {
            if ikey.seq_num > read_seq { continue; }
            if ikey.user_key.as_ref() == user_key {
                return match ikey.op_type {
                    heliosdb_types::OpType::Put    => Ok(Some(GetResult::Value(val))),
                    heliosdb_types::OpType::Delete => Ok(Some(GetResult::Tombstone)),
                };
            }
            if ikey.user_key.as_ref() > user_key { break; }
        }
    }
    Ok(None)
}

/// Scan an SST and merge entries into `results`.  Puts overwrite, deletes
/// remove — so calling this from lowest to highest priority gives the
/// correct final state.
fn scan_segment(
    iter: heliosdb_sst::reader::SstIter,
    start: &[u8],
    end: &[u8],
    read_seq: u64,
    results: &mut std::collections::BTreeMap<Bytes, Bytes>,
) -> Result<()> {
    // SST entries are sorted (user_key asc, seq_num desc).  For each key
    // we only want the first visible version (highest seq ≤ read_seq).
    let mut last_key: Option<Bytes> = None;
    for item in iter {
        let (raw_key, val) = item?;
        if let Ok(ikey) = InternalKey::decode(&raw_key) {
            if ikey.seq_num > read_seq { continue; }
            if last_key.as_deref() == Some(ikey.user_key.as_ref()) { continue; }
            last_key = Some(ikey.user_key.clone());

            let uk = ikey.user_key;
            if uk.as_ref() >= start && uk.as_ref() < end {
                if ikey.op_type == heliosdb_types::OpType::Put {
                    results.insert(uk, val);
                } else {
                    results.remove(&uk);
                }
            }
        }
    }
    Ok(())
}
