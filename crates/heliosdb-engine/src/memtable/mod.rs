//! MemTable abstraction.
//!
//! Keys are stored as encoded InternalKeys (user_key + seq_num + op_type).
//! The skip list is ordered by InternalKey ordering:
//!   - ascending user_key
//!   - descending seq_num (latest version sorts first)

mod btree;
mod memtable_set;
mod skiplist;
mod traits;

pub use btree::BTreeMemTable;
pub use memtable_set::MemTableSet;
pub use skiplist::SkipListMemTable;
pub use traits::{GetResult, MemTable};
