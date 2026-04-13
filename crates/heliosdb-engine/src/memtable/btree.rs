use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use parking_lot::RwLock;
use heliosdb_types::{InternalKey, OpType, SeqNum, UserKey, Value};

use super::traits::{GetResult, MemTable};

/// MemTable backed by a `parking_lot::RwLock<BTreeMap>`.
///
/// Reads take a shared lock; writes take an exclusive lock.
/// Unlike the skip-list implementation, concurrent reads do not run in
/// parallel with concurrent writes, so this trades some write throughput
/// for simpler allocation behaviour (no epoch-based GC).
pub struct BTreeMemTable {
    map:        RwLock<BTreeMap<InternalKey, Value>>,
    size_bytes: AtomicUsize,
}

impl BTreeMemTable {
    pub fn new() -> Self {
        Self {
            map:        RwLock::new(BTreeMap::new()),
            size_bytes: AtomicUsize::new(0),
        }
    }
}

impl Default for BTreeMemTable {
    fn default() -> Self {
        Self::new()
    }
}

impl MemTable for BTreeMemTable {
    fn put(&self, user_key: UserKey, seq_num: SeqNum, value: Value) {
        let ikey = InternalKey::new_put(user_key, seq_num);
        let delta = ikey.user_key.len() + 8 + value.len();
        self.map.write().insert(ikey, value);
        self.size_bytes.fetch_add(delta, Ordering::Relaxed);
    }

    fn delete(&self, user_key: UserKey, seq_num: SeqNum) {
        let ikey = InternalKey::new_delete(user_key, seq_num);
        let delta = ikey.user_key.len() + 8;
        self.map.write().insert(ikey, Bytes::new());
        self.size_bytes.fetch_add(delta, Ordering::Relaxed);
    }

    fn get(&self, user_key: &[u8], read_seq: SeqNum) -> Option<GetResult> {
        // Same probe trick as the skip-list: within a user_key, seq_nums are
        // sorted descending, so (user_key, read_seq) is the boundary between
        // "too new" and "visible" entries.  range(probe..).next() gives the
        // first entry with seq_num <= read_seq for this user_key.
        let probe = InternalKey::new_put(Bytes::copy_from_slice(user_key), read_seq);
        let map = self.map.read();
        let (key, value) = map.range(probe..).next()?;
        if key.user_key.as_ref() != user_key || key.seq_num > read_seq {
            return None;
        }
        match key.op_type {
            OpType::Put    => Some(GetResult::Value(value.clone())),
            OpType::Delete => Some(GetResult::Tombstone),
        }
    }

    fn size_bytes(&self) -> usize {
        self.size_bytes.load(Ordering::Relaxed)
    }

    fn is_empty(&self) -> bool {
        self.map.read().is_empty()
    }

    fn iter(&self) -> Vec<(InternalKey, Value)> {
        self.map.read().iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn put_and_get() {
        let mt = BTreeMemTable::new();
        mt.put(Bytes::from("hello"), 1, Bytes::from("world"));
        let result = mt.get(b"hello", 1).unwrap();
        assert!(matches!(result, GetResult::Value(v) if v == Bytes::from("world")));
    }

    #[test]
    fn delete_returns_tombstone() {
        let mt = BTreeMemTable::new();
        mt.put(Bytes::from("k"), 1, Bytes::from("v"));
        mt.delete(Bytes::from("k"), 2);
        let result = mt.get(b"k", 2).unwrap();
        assert!(matches!(result, GetResult::Tombstone));
    }

    #[test]
    fn snapshot_isolation() {
        let mt = BTreeMemTable::new();
        mt.put(Bytes::from("k"), 1, Bytes::from("v1"));
        mt.put(Bytes::from("k"), 3, Bytes::from("v3"));
        let result = mt.get(b"k", 2).unwrap();
        assert!(matches!(result, GetResult::Value(v) if v == Bytes::from("v1")));
    }

    #[test]
    fn missing_key() {
        let mt = BTreeMemTable::new();
        assert!(mt.get(b"nope", 100).is_none());
    }
}
