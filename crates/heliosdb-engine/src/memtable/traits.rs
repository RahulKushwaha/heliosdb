use heliosdb_types::{InternalKey, SeqNum, UserKey, Value};

pub enum GetResult {
    Value(Value),
    Tombstone,
}

/// The interface every MemTable implementation must satisfy.
///
/// Implementations must be concurrency-safe (`Send + Sync`) because the DB
/// holds one behind a `RwLock` and may call `put`/`delete` from multiple
/// threads. `Default` is required so the flush pipeline can replace the
/// current MemTable with a fresh empty one.
pub trait MemTable: Default + Send + Sync {
    fn put(&self, user_key: UserKey, seq_num: SeqNum, value: Value);
    fn delete(&self, user_key: UserKey, seq_num: SeqNum);

    /// Look up the latest version of `user_key` visible at `read_seq`.
    fn get(&self, user_key: &[u8], read_seq: SeqNum) -> Option<GetResult>;

    /// Approximate memory usage in bytes.
    fn size_bytes(&self) -> usize;

    fn is_empty(&self) -> bool;

    /// All entries in InternalKey order, used by the flush pipeline.
    fn iter(&self) -> Vec<(InternalKey, Value)>;
}
