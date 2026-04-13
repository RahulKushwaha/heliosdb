//! Top-level DB struct — coordinates MemTableSet, WAL, ActiveSegment,
//! InactiveSegments, Manifest, and Compaction.
//!
//! A background **flusher thread** drains the immutable memtable queue
//! asynchronously. A bounded `crossbeam_channel` acts as the ring-buffer
//! between the write path and the flusher, providing natural backpressure
//! (send blocks when full) without holding any lock.

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
            write_buffer_size:   DEFAULT_WRITE_BUFFER_SIZE,
            max_immutable_count: DEFAULT_MAX_IMMUTABLE,
            compression:         CompressionType::None,
        }
    }
}

// ---------------------------------------------------------------------------
// Flush pipeline message
// ---------------------------------------------------------------------------

/// Message sent from the write path to the background flusher.
enum FlushMsg<M> {
    /// A sealed memtable to flush to SST.
    Sealed(Arc<M>),
    /// Synchronisation barrier: the flusher sends `()` on the ack channel
    /// after it has processed every `Sealed` message that preceded this one.
    FlushAll(Sender<()>),
}

// ---------------------------------------------------------------------------
// Internal mutable state
// ---------------------------------------------------------------------------

/// State protected by a single RwLock.
struct DbState<M> {
    memtable_set: MemTableSet<M>,
    active:       Option<ActiveSegment>,
    inactive:     Vec<InactiveSegment>,
    manifest:     Manifest,
    version:      VersionSet,
    wal:          Wal,
}

/// Shared inner state, accessible from both `DB` and the flusher thread.
struct DbInner<M> {
    dir:      PathBuf,
    opts:     Options,
    next_seq: AtomicU64,
    state:    RwLock<DbState<M>>,
}

pub struct DB<M = SkipListMemTable> {
    inner:    Arc<DbInner<M>>,
    /// Sole sender for the flush ring-buffer.  Dropping this closes the
    /// channel, which causes the flusher thread to exit.
    flush_tx: Sender<FlushMsg<M>>,
    /// Background flusher thread handle.
    flusher:  Option<JoinHandle<()>>,
}

impl<M: MemTable + 'static> DB<M> {
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

        // Bounded channel: capacity = max_immutable_count.
        // send() blocks when full → backpressure without holding any lock.
        let (flush_tx, flush_rx) = bounded(inner.opts.max_immutable_count);

        let flusher_inner = Arc::clone(&inner);
        let flusher = std::thread::Builder::new()
            .name("helios-flusher".into())
            .spawn(move || flusher_loop(flusher_inner, flush_rx))
            .map_err(|e| heliosdb_types::HeliosError::InvalidArgument(
                format!("failed to spawn flusher thread: {e}"),
            ))?;

        Ok(Self {
            inner,
            flush_tx,
            flusher: Some(flusher),
        })
    }

    // -----------------------------------------------------------------------
    // Write path
    // -----------------------------------------------------------------------

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let seq   = self.inner.next_seq.fetch_add(1, Ordering::Relaxed);
        let ikey  = InternalKey::new_put(Bytes::copy_from_slice(key), seq);
        let value = Bytes::copy_from_slice(value);

        // Hold write lock only for WAL + memtable + rotation decision.
        let sealed = {
            let mut state = self.inner.state.write();
            state.wal.append(&ikey, &value)?;
            state.memtable_set.put(ikey.user_key, seq, value);
            if state.memtable_set.should_rotate() {
                Some(state.memtable_set.rotate_arc())
            } else {
                None
            }
        }; // ← write lock released

        // Channel send (may block if ring-buffer full — backpressure).
        if let Some(arc) = sealed {
            self.flush_tx
                .send(FlushMsg::Sealed(arc))
                .map_err(|_| heliosdb_types::HeliosError::InvalidArgument(
                    "flusher thread died".into(),
                ))?;
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
            self.flush_tx
                .send(FlushMsg::Sealed(arc))
                .map_err(|_| heliosdb_types::HeliosError::InvalidArgument(
                    "flusher thread died".into(),
                ))?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Read path
    // -----------------------------------------------------------------------

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let read_seq = self.inner.next_seq.load(Ordering::Relaxed);
        let mut state = self.inner.state.write();

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
        let read_seq = self.inner.next_seq.load(Ordering::Relaxed);
        let mut results: std::collections::BTreeMap<Bytes, Bytes> =
            std::collections::BTreeMap::new();

        let mut state = self.inner.state.write();

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
    // Flush (explicit / public)
    // -----------------------------------------------------------------------

    /// Force a full flush: seal the active memtable, wait for the background
    /// flusher to drain every pending immutable, then truncate the WAL.
    pub fn flush(&self) -> Result<()> {
        // Rotate active into the flush pipeline if it has data.
        let sealed = {
            let mut state = self.inner.state.write();
            if !state.memtable_set.active_is_empty() {
                Some(state.memtable_set.rotate_arc())
            } else {
                None
            }
        };
        if let Some(arc) = sealed {
            self.flush_tx
                .send(FlushMsg::Sealed(arc))
                .map_err(|_| heliosdb_types::HeliosError::InvalidArgument(
                    "flusher thread died".into(),
                ))?;
        }

        // Send a barrier and wait for the flusher to process everything
        // that precedes it (channel is FIFO).
        let (ack_tx, ack_rx) = bounded(1);
        self.flush_tx
            .send(FlushMsg::FlushAll(ack_tx))
            .map_err(|_| heliosdb_types::HeliosError::InvalidArgument(
                "flusher thread died".into(),
            ))?;
        ack_rx
            .recv()
            .map_err(|_| heliosdb_types::HeliosError::InvalidArgument(
                "flusher thread died".into(),
            ))?;

        // All data is now in SST — safe to truncate the WAL.
        let mut state = self.inner.state.write();
        state.wal = Wal::create(&self.inner.dir.join("WAL"))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DbInner helpers (used by both DB methods and the flusher thread)
// ---------------------------------------------------------------------------

impl<M: MemTable> DbInner<M> {
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
// Background flusher
// ---------------------------------------------------------------------------

/// Main loop for the background flusher thread.
///
/// Receives sealed memtables over a bounded channel and flushes them to SST.
/// The channel acts as a ring-buffer: capacity = `max_immutable_count`.
/// Exits when the channel is closed (all senders dropped → `DB::drop`).
fn flusher_loop<M: MemTable>(inner: Arc<DbInner<M>>, rx: Receiver<FlushMsg<M>>) {
    while let Ok(msg) = rx.recv() {
        match msg {
            FlushMsg::Sealed(sealed) => {
                if let Err(e) = flush_one(&inner, &sealed) {
                    tracing::error!("background flush failed: {e}");
                }
            }
            FlushMsg::FlushAll(ack) => {
                // Channel is FIFO — all prior Sealed messages have already
                // been processed by the time we see this barrier.
                let _ = ack.send(());
            }
        }
    }
    tracing::debug!("flusher thread exiting");
}

/// Flush a single sealed memtable into the active SST.
///
/// Holds the write lock for the entire operation so that reads never see a
/// window where `state.active` has been drained but the new SST hasn't been
/// promoted yet.  The background thread's value is that **writers don't do
/// the flush inline** — `put()` returns immediately after the channel send,
/// and the flusher does the work in a separate thread.
fn flush_one<M: MemTable>(inner: &DbInner<M>, sealed: &Arc<M>) -> Result<()> {
    let mut state = inner.state.write();

    let new_path = inner.new_active_path();
    let (old_active_path, active_entries) = inner.drain_active_sst(&mut state)?;

    // Merge: immutable (priority 0) beats old active SST (priority 1).
    let mem_iter: Box<dyn Iterator<Item = (InternalKey, Bytes)> + Send> =
        Box::new(sealed.iter().into_iter());
    let sst_iter: Box<dyn Iterator<Item = (InternalKey, Bytes)> + Send> =
        Box::new(active_entries.into_iter());
    let merged = MergeIterator::new(vec![mem_iter, sst_iter]);

    let expected = (sealed.size_bytes() / 64).max(64);
    inner.write_sst(&new_path, merged, expected)?;

    state.memtable_set.pop_oldest_immutable();
    if let Some(ref p) = old_active_path {
        let _ = std::fs::remove_file(p);
    }
    inner.promote_active(&mut state, new_path)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Clean shutdown
// ---------------------------------------------------------------------------

impl<M> Drop for DB<M> {
    fn drop(&mut self) {
        // Replace flush_tx with a dummy sender, dropping the real one.
        // This closes the channel → flusher's recv() returns Err → loop exits.
        let (_dummy_tx, _dummy_rx) = bounded(0);
        let _ = std::mem::replace(&mut self.flush_tx, _dummy_tx);

        // Wait for the flusher to finish processing any in-flight messages.
        if let Some(handle) = self.flusher.take() {
            let _ = handle.join();
        }
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
