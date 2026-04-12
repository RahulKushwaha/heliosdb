//! Merge iterator: merges N sorted iterators into one sorted stream.
//!
//! Used during compaction and the flush pipeline.
//! When two iterators yield the same user_key, the one with the higher
//! priority (lower index = more recent) wins.

use std::collections::BinaryHeap;

use bytes::Bytes;
use heliosdb_types::{InternalKey, Value};

// ---------------------------------------------------------------------------
// MergeItem — wrapper that defines heap ordering
// ---------------------------------------------------------------------------

struct MergeItem {
    key:      InternalKey,
    value:    Value,
    priority: usize, // lower = higher priority (more recent source)
}

impl PartialEq for MergeItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.priority == other.priority
    }
}
impl Eq for MergeItem {}

impl PartialOrd for MergeItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap is a max-heap; we want the *smallest* key first,
        // so we reverse the key comparison.
        // For equal keys, lower priority number (more recent) wins → comes first.
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.priority.cmp(&self.priority))
    }
}

// ---------------------------------------------------------------------------
// MergeIterator
// ---------------------------------------------------------------------------

pub struct MergeIterator {
    iters:    Vec<Box<dyn Iterator<Item = (InternalKey, Value)> + Send>>,
    heap:     BinaryHeap<MergeItem>,
    last_user_key: Option<Bytes>,
}

impl MergeIterator {
    /// Create a merge iterator from `iters`.
    ///
    /// Index 0 = highest priority (most recent, e.g., MemTable).
    /// When two sources yield the same user_key, index 0 wins.
    pub fn new(mut iters: Vec<Box<dyn Iterator<Item = (InternalKey, Value)> + Send>>) -> Self {
        let mut heap = BinaryHeap::new();
        for (priority, it) in iters.iter_mut().enumerate() {
            if let Some((key, value)) = it.next() {
                heap.push(MergeItem { key, value, priority });
            }
        }
        Self { iters, heap, last_user_key: None }
    }
}

impl Iterator for MergeIterator {
    type Item = (InternalKey, Value);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.heap.pop()?;

            // Advance that iterator
            if let Some((k, v)) = self.iters[item.priority].next() {
                self.heap.push(MergeItem { key: k, value: v, priority: item.priority });
            }

            let user_key = item.key.user_key.clone();

            // Skip older versions of the same user_key (deduplication).
            if self.last_user_key.as_deref() == Some(user_key.as_ref()) {
                continue;
            }
            self.last_user_key = Some(user_key);
            return Some((item.key, item.value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use heliosdb_types::OpType;

    fn mk_iter(
        entries: Vec<(&'static str, u64, &'static str)>,
    ) -> Box<dyn Iterator<Item = (InternalKey, Value)> + Send> {
        let v: Vec<_> = entries
            .into_iter()
            .map(|(k, seq, val)| {
                (
                    InternalKey::new_put(Bytes::from(k), seq),
                    Bytes::from(val),
                )
            })
            .collect();
        Box::new(v.into_iter())
    }

    #[test]
    fn simple_merge() {
        // Priority 0 (MemTable) has higher seq_nums (more recent writes).
        // Priority 1 (old active) has lower seq_nums (older writes).
        // For key "c" in both: priority-0's version has seq=5 (wins over seq=3).
        let a = mk_iter(vec![("a", 5, "a5"), ("c", 5, "c_new")]);
        let b = mk_iter(vec![("b", 3, "b3"), ("c", 3, "c_old")]);
        let mut it = MergeIterator::new(vec![a, b]);
        let results: Vec<_> = it.map(|(k, v)| (k.user_key.clone(), v)).collect();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0.as_ref(), b"a");
        assert_eq!(results[1].0.as_ref(), b"b");
        assert_eq!(results[2].0.as_ref(), b"c");
        // "c" from priority-0 (seq=5) beats priority-1 (seq=3)
        assert_eq!(results[2].1.as_ref(), b"c_new");
    }

    #[test]
    fn single_source() {
        let a = mk_iter(vec![("x", 1, "v1"), ("y", 2, "v2")]);
        let results: Vec<_> = MergeIterator::new(vec![a]).collect();
        assert_eq!(results.len(), 2);
    }
}
