use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use heliosdb_types::{InternalKey, OpType, SeqNum, UserKey, Value};

use super::traits::{GetResult, MemTable};

pub struct SkipListMemTable {
    map:        SkipMap<InternalKey, Value>,
    size_bytes: std::sync::atomic::AtomicUsize,
}

impl SkipListMemTable {
    pub fn new() -> Self {
        Self {
            map:        SkipMap::new(),
            size_bytes: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl Default for SkipListMemTable {
    fn default() -> Self {
        Self::new()
    }
}

impl MemTable for SkipListMemTable {
    fn put(&self, user_key: UserKey, seq_num: SeqNum, value: Value) {
        let ikey = InternalKey::new_put(user_key, seq_num);
        let delta = ikey.user_key.len() + 8 + value.len();
        self.map.insert(ikey, value);
        self.size_bytes.fetch_add(delta, std::sync::atomic::Ordering::Relaxed);
    }

    fn delete(&self, user_key: UserKey, seq_num: SeqNum) {
        let ikey = InternalKey::new_delete(user_key, seq_num);
        let delta = ikey.user_key.len() + 8;
        self.map.insert(ikey, Bytes::new());
        self.size_bytes.fetch_add(delta, std::sync::atomic::Ordering::Relaxed);
    }

    fn get(&self, user_key: &[u8], read_seq: SeqNum) -> Option<GetResult> {
        // Probe key: user_key with the highest possible seq_num so we land
        // just before the first real entry for this user_key.
        let probe = InternalKey::new_put(Bytes::copy_from_slice(user_key), read_seq);

        // The skip list is sorted: equal user_key, descending seq_num.
        // `lower_bound` gives us the first key >= probe.
        // Since probe has max seq for this user_key, the first entry >= probe
        // is the latest version of user_key with seq <= read_seq.
        let entry = self.map.lower_bound(std::ops::Bound::Included(&probe))?;
        if entry.key().user_key.as_ref() != user_key {
            return None;
        }
        if entry.key().seq_num > read_seq {
            return None;
        }
        match entry.key().op_type {
            OpType::Put    => Some(GetResult::Value(entry.value().clone())),
            OpType::Delete => Some(GetResult::Tombstone),
        }
    }

    fn size_bytes(&self) -> usize {
        self.size_bytes.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn iter(&self) -> Vec<(InternalKey, Value)> {
        self.map.iter().map(|e| (e.key().clone(), e.value().clone())).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn put_and_get() {
        let mt = SkipListMemTable::new();
        mt.put(Bytes::from("hello"), 1, Bytes::from("world"));
        let result = mt.get(b"hello", 1).unwrap();
        assert!(matches!(result, GetResult::Value(v) if v == Bytes::from("world")));
    }

    #[test]
    fn delete_returns_tombstone() {
        let mt = SkipListMemTable::new();
        mt.put(Bytes::from("k"), 1, Bytes::from("v"));
        mt.delete(Bytes::from("k"), 2);
        let result = mt.get(b"k", 2).unwrap();
        assert!(matches!(result, GetResult::Tombstone));
    }

    #[test]
    fn snapshot_isolation() {
        let mt = SkipListMemTable::new();
        mt.put(Bytes::from("k"), 1, Bytes::from("v1"));
        mt.put(Bytes::from("k"), 3, Bytes::from("v3"));
        // Read at seq=2 should see v1, not v3
        let result = mt.get(b"k", 2).unwrap();
        assert!(matches!(result, GetResult::Value(v) if v == Bytes::from("v1")));
    }

    #[test]
    fn missing_key() {
        let mt = SkipListMemTable::new();
        assert!(mt.get(b"nope", 100).is_none());
    }
}
