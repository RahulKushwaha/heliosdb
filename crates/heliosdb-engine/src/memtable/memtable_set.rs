use std::collections::VecDeque;
use std::sync::Arc;

use heliosdb_types::{InternalKey, SeqNum, UserKey, Value};

use super::traits::{GetResult, MemTable};

/// Manages one mutable (active) MemTable and a queue of immutable ones.
///
/// ## Lifecycle
///
/// ```text
/// writes → active
///              │ size >= write_buffer_size
///              ▼
///         immutable[n]  ← newest
///         immutable[n-1]
///         ...
///         immutable[0]  ← oldest, flushed first
/// ```
///
/// Immutable memtables are stored as `Arc<M>` so they can be shared with the
/// background flusher thread without holding the main write lock.
///
/// ## WAL safety
///
/// Rotating the active into the immutable queue does NOT rotate the WAL.
/// The WAL continues to cover all writes for both the active and every
/// immutable memtable. Only after all immutables have been flushed to SST
/// and the active is empty is it safe to truncate the WAL — the caller
/// (`DB::flush`) is responsible for that step.
pub struct MemTableSet<M> {
    /// Receives all new writes.
    active: M,
    /// Sealed memtables awaiting flush.  Front = oldest, back = newest.
    immutable: VecDeque<Arc<M>>,
    /// Seal the active when its size exceeds this threshold.
    write_buffer_size: usize,
    /// Maximum length of the immutable queue before writes must stall.
    max_immutable: usize,
}

impl<M: MemTable> MemTableSet<M> {
    pub fn new(write_buffer_size: usize, max_immutable: usize) -> Self {
        Self {
            active: M::default(),
            immutable: VecDeque::new(),
            write_buffer_size,
            max_immutable,
        }
    }

    /// Build from a pre-populated memtable (e.g., after WAL replay).
    pub fn with_active(active: M, write_buffer_size: usize, max_immutable: usize) -> Self {
        Self {
            active,
            immutable: VecDeque::new(),
            write_buffer_size,
            max_immutable,
        }
    }

    // -----------------------------------------------------------------------
    // Write path
    // -----------------------------------------------------------------------

    pub fn put(&self, user_key: UserKey, seq_num: SeqNum, value: Value) {
        self.active.put(user_key, seq_num, value);
    }

    pub fn delete(&self, user_key: UserKey, seq_num: SeqNum) {
        self.active.delete(user_key, seq_num);
    }

    // -----------------------------------------------------------------------
    // Rotation
    // -----------------------------------------------------------------------

    /// True when the active memtable has reached the write-buffer threshold.
    pub fn should_rotate(&self) -> bool {
        self.active.size_bytes() >= self.write_buffer_size
    }

    /// True when the immutable queue is full.
    pub fn is_at_capacity(&self) -> bool {
        self.immutable.len() >= self.max_immutable
    }

    /// Seal the active memtable and replace it with a fresh one.
    ///
    /// Panics if called while `is_at_capacity()` — flush first.
    pub fn rotate(&mut self) {
        assert!(
            !self.is_at_capacity(),
            "MemTableSet::rotate called while at capacity ({} immutables); flush first",
            self.immutable.len()
        );
        let _ = self.rotate_arc();
    }

    /// Seal the active memtable, store it in the immutable queue for reads,
    /// and return an `Arc` so the caller can send it to the flush pipeline.
    ///
    /// Does NOT check capacity — backpressure is the caller's responsibility
    /// (e.g., a bounded channel).
    pub fn rotate_arc(&mut self) -> Arc<M> {
        let sealed = std::mem::replace(&mut self.active, M::default());
        let arc = Arc::new(sealed);
        self.immutable.push_back(Arc::clone(&arc));
        arc
    }

    // -----------------------------------------------------------------------
    // Flush support
    // -----------------------------------------------------------------------

    /// Remove and return the oldest immutable memtable for flushing to SST.
    pub fn pop_oldest_immutable(&mut self) -> Option<Arc<M>> {
        self.immutable.pop_front()
    }

    pub fn immutable_count(&self) -> usize {
        self.immutable.len()
    }

    // -----------------------------------------------------------------------
    // Read path
    // -----------------------------------------------------------------------

    /// Check active first, then immutables newest → oldest.
    /// Returns on the first match (most recent version wins).
    pub fn get(&self, user_key: &[u8], read_seq: SeqNum) -> Option<GetResult> {
        if let Some(r) = self.active.get(user_key, read_seq) {
            return Some(r);
        }
        for imm in self.immutable.iter().rev() {
            if let Some(r) = imm.get(user_key, read_seq) {
                return Some(r);
            }
        }
        None
    }

    /// Iterate all memtable sources from **lowest to highest priority**:
    /// `[oldest immutable, ..., newest immutable, active]`.
    ///
    /// Used by the scan path, which applies each source in order so that
    /// higher-priority sources overwrite lower-priority ones.
    pub fn iter_all_by_priority(&self) -> impl Iterator<Item = Vec<(InternalKey, Value)>> + '_ {
        self.immutable
            .iter()
            .map(|m| m.iter())
            .chain(std::iter::once(self.active.iter()))
    }

    // -----------------------------------------------------------------------
    // Misc
    // -----------------------------------------------------------------------

    pub fn active_is_empty(&self) -> bool {
        self.active.is_empty()
    }

    pub fn active_size_bytes(&self) -> usize {
        self.active.size_bytes()
    }

    pub fn active(&self) -> &M {
        &self.active
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::SkipListMemTable;
    use bytes::Bytes;

    fn key(s: &str) -> Bytes { Bytes::from(s.to_owned()) }
    fn val(s: &str) -> Bytes { Bytes::from(s.to_owned()) }

    #[test]
    fn get_from_active() {
        let mut set: MemTableSet<SkipListMemTable> = MemTableSet::new(4096, 2);
        set.put(key("k"), 1, val("v1"));
        assert!(matches!(set.get(b"k", 1), Some(GetResult::Value(v)) if v == val("v1")));
    }

    #[test]
    fn get_from_immutable_after_rotate() {
        let mut set: MemTableSet<SkipListMemTable> = MemTableSet::new(4096, 2);
        set.put(key("k"), 1, val("v1"));
        set.rotate();
        // key is now in the immutable, active is empty
        assert!(matches!(set.get(b"k", 1), Some(GetResult::Value(v)) if v == val("v1")));
    }

    #[test]
    fn active_shadows_immutable() {
        let mut set: MemTableSet<SkipListMemTable> = MemTableSet::new(4096, 2);
        set.put(key("k"), 1, val("old"));
        set.rotate();
        set.put(key("k"), 2, val("new"));
        // active (seq=2) must win over immutable (seq=1)
        assert!(matches!(set.get(b"k", 2), Some(GetResult::Value(v)) if v == val("new")));
    }

    #[test]
    fn tombstone_in_active_hides_immutable_value() {
        let mut set: MemTableSet<SkipListMemTable> = MemTableSet::new(4096, 2);
        set.put(key("k"), 1, val("v"));
        set.rotate();
        set.delete(key("k"), 2);
        assert!(matches!(set.get(b"k", 2), Some(GetResult::Tombstone)));
    }

    #[test]
    fn pop_oldest_removes_front() {
        let mut set: MemTableSet<SkipListMemTable> = MemTableSet::new(4096, 3);
        set.put(key("a"), 1, val("v1")); set.rotate();
        set.put(key("b"), 2, val("v2")); set.rotate();
        assert_eq!(set.immutable_count(), 2);
        let oldest = set.pop_oldest_immutable().unwrap();
        // oldest had key "a"
        assert!(matches!(oldest.get(b"a", 1), Some(GetResult::Value(_))));
        assert_eq!(set.immutable_count(), 1);
    }

    #[test]
    #[should_panic(expected = "at capacity")]
    fn rotate_at_capacity_panics() {
        let mut set: MemTableSet<SkipListMemTable> = MemTableSet::new(4096, 1);
        set.put(key("k"), 1, val("v")); set.rotate(); // fills the 1 slot
        set.put(key("k"), 2, val("v")); set.rotate(); // should panic
    }

    #[test]
    fn rotate_arc_returns_shared_arc() {
        let mut set: MemTableSet<SkipListMemTable> = MemTableSet::new(4096, 4);
        set.put(key("k"), 1, val("v1"));
        let arc = set.rotate_arc();
        // Arc is readable
        assert!(matches!(arc.get(b"k", 1), Some(GetResult::Value(v)) if v == val("v1")));
        // Also readable through the VecDeque (for DB reads during flush)
        assert!(matches!(set.get(b"k", 1), Some(GetResult::Value(v)) if v == val("v1")));
        assert_eq!(set.immutable_count(), 1);
    }
}
