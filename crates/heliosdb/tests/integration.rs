use bytes::Bytes;
use heliosdb::{Options, SkipListMemTable, DB};
use std::collections::HashMap;
use tempfile::tempdir;

type TestDB = DB<SkipListMemTable>;

fn small_opts() -> Options {
    Options {
        write_buffer_size: 4 * 1024, // 4 KiB — triggers flush quickly
        ..Options::default()
    }
}

// ---------------------------------------------------------------------------
// Basic CRUD
// ---------------------------------------------------------------------------

#[test]
fn put_get_roundtrip() {
    let dir = tempdir().unwrap();
    let db = TestDB::open(dir.path(), Options::default()).unwrap();
    db.put(b"hello", b"world").unwrap();
    assert_eq!(db.get(b"hello").unwrap().as_deref(), Some(b"world".as_ref()));
}

#[test]
fn get_missing_returns_none() {
    let dir = tempdir().unwrap();
    let db = TestDB::open(dir.path(), Options::default()).unwrap();
    assert!(db.get(b"ghost").unwrap().is_none());
}

#[test]
fn delete_removes_key() {
    let dir = tempdir().unwrap();
    let db = TestDB::open(dir.path(), Options::default()).unwrap();
    db.put(b"k", b"v").unwrap();
    db.delete(b"k").unwrap();
    assert!(db.get(b"k").unwrap().is_none());
}

#[test]
fn overwrite_returns_latest_value() {
    let dir = tempdir().unwrap();
    let db = TestDB::open(dir.path(), Options::default()).unwrap();
    db.put(b"k", b"v1").unwrap();
    db.put(b"k", b"v2").unwrap();
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(b"v2".as_ref()));
}

// ---------------------------------------------------------------------------
// Flush + read from active segment
// ---------------------------------------------------------------------------

#[test]
fn read_after_explicit_flush() {
    let dir = tempdir().unwrap();
    let db = TestDB::open(dir.path(), Options::default()).unwrap();
    db.put(b"persisted", b"yes").unwrap();
    db.flush().unwrap();
    assert_eq!(db.get(b"persisted").unwrap().as_deref(), Some(b"yes".as_ref()));
}

#[test]
fn flush_triggered_by_memtable_size() {
    let dir = tempdir().unwrap();
    let db = TestDB::open(dir.path(), small_opts()).unwrap();
    // Write enough to trigger at least one automatic flush
    for i in 0u32..200 {
        db.put(
            format!("key{i:04}").as_bytes(),
            format!("value_{i:04}_padding_padding_padding").as_bytes(),
        )
        .unwrap();
    }
    // All keys should still be readable
    for i in 0u32..200 {
        let key = format!("key{i:04}");
        let expected = format!("value_{i:04}_padding_padding_padding");
        let got = db.get(key.as_bytes()).unwrap();
        assert_eq!(
            got.as_deref(),
            Some(expected.as_bytes()),
            "missing key {key}"
        );
    }
}

// ---------------------------------------------------------------------------
// WAL recovery
// ---------------------------------------------------------------------------

#[test]
fn wal_recovery_after_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = TestDB::open(dir.path(), Options::default()).unwrap();
        db.put(b"durable", b"yes").unwrap();
        // No explicit flush — data is only in WAL + MemTable
    }
    // Reopen: WAL should be replayed
    let db2 = TestDB::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db2.get(b"durable").unwrap().as_deref(), Some(b"yes".as_ref()));
}

// ---------------------------------------------------------------------------
// Active segment invariant: latest value always in active after flush
// ---------------------------------------------------------------------------

#[test]
fn active_segment_holds_latest_after_flush() {
    let dir = tempdir().unwrap();
    let db = TestDB::open(dir.path(), Options::default()).unwrap();

    db.put(b"fruit", b"apple").unwrap();
    db.flush().unwrap();

    db.put(b"fruit", b"banana").unwrap();
    db.flush().unwrap();

    // After two flushes, the latest value must be visible
    assert_eq!(db.get(b"fruit").unwrap().as_deref(), Some(b"banana".as_ref()));
}

// ---------------------------------------------------------------------------
// Scan correctness (oracle test: compare against HashMap)
// ---------------------------------------------------------------------------

#[test]
fn scan_matches_hashmap_oracle() {
    let dir = tempdir().unwrap();
    let db = TestDB::open(dir.path(), small_opts()).unwrap();

    let mut oracle: HashMap<String, String> = HashMap::new();

    for i in 0u32..500 {
        let k = format!("k{i:05}");
        let v = format!("v{i:05}");
        db.put(k.as_bytes(), v.as_bytes()).unwrap();
        oracle.insert(k, v);
    }
    // Delete some
    for i in (0u32..500).step_by(7) {
        let k = format!("k{i:05}");
        db.delete(k.as_bytes()).unwrap();
        oracle.remove(&k);
    }

    db.flush().unwrap();

    let results = db.scan(b"k", b"l").unwrap(); // all "k..." keys
    let result_map: HashMap<String, String> = results
        .into_iter()
        .map(|(k, v)| (
            String::from_utf8(k.to_vec()).unwrap(),
            String::from_utf8(v.to_vec()).unwrap(),
        ))
        .collect();

    assert_eq!(result_map, oracle, "scan result does not match oracle");
}

// ---------------------------------------------------------------------------
// Tombstone propagation
// ---------------------------------------------------------------------------

#[test]
fn tombstone_visible_after_flush() {
    let dir = tempdir().unwrap();
    let db = TestDB::open(dir.path(), Options::default()).unwrap();
    db.put(b"dead", b"value").unwrap();
    db.flush().unwrap();
    db.delete(b"dead").unwrap();
    db.flush().unwrap();
    assert!(db.get(b"dead").unwrap().is_none());
}
