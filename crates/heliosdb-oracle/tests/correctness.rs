use heliosdb::Options;
use heliosdb_oracle::Oracle;
use rand::prelude::*;
use tempfile::tempdir;

fn small_opts() -> Options {
    Options {
        write_buffer_size: 4 * 1024, // 4 KiB — forces frequent flushes
        ..Options::default()
    }
}

// ---------------------------------------------------------------------------
// Basic CRUD
// ---------------------------------------------------------------------------

#[test]
fn sequential_put_get() {
    let dir = tempdir().unwrap();
    let oracle = Oracle::open_default(dir.path());

    for i in 0u32..100 {
        let key = format!("key{i:04}");
        let val = format!("val{i:04}");
        oracle.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    for i in 0u32..100 {
        let key = format!("key{i:04}");
        let val = format!("val{i:04}");
        let got = oracle.get(key.as_bytes()).unwrap();
        assert_eq!(got.as_deref(), Some(val.as_bytes()));
    }
}

#[test]
fn get_missing_key() {
    let dir = tempdir().unwrap();
    let oracle = Oracle::open_default(dir.path());
    let got = oracle.get(b"nonexistent").unwrap();
    assert!(got.is_none());
}

// ---------------------------------------------------------------------------
// Overwrite
// ---------------------------------------------------------------------------

#[test]
fn overwrite_returns_latest() {
    let dir = tempdir().unwrap();
    let oracle = Oracle::open_default(dir.path());

    oracle.put(b"k", b"v1").unwrap();
    oracle.put(b"k", b"v2").unwrap();
    oracle.put(b"k", b"v3").unwrap();

    let got = oracle.get(b"k").unwrap();
    assert_eq!(got.as_deref(), Some(b"v3".as_ref()));
}

// ---------------------------------------------------------------------------
// Tombstones
// ---------------------------------------------------------------------------

#[test]
fn delete_removes_key() {
    let dir = tempdir().unwrap();
    let oracle = Oracle::open_default(dir.path());

    oracle.put(b"dead", b"value").unwrap();
    oracle.delete(b"dead").unwrap();

    let got = oracle.get(b"dead").unwrap();
    assert!(got.is_none());
}

#[test]
fn delete_nonexistent_key() {
    let dir = tempdir().unwrap();
    let oracle = Oracle::open_default(dir.path());

    oracle.delete(b"ghost").unwrap();
    let got = oracle.get(b"ghost").unwrap();
    assert!(got.is_none());
}

// ---------------------------------------------------------------------------
// Flush + read
// ---------------------------------------------------------------------------

#[test]
fn read_after_flush() {
    let dir = tempdir().unwrap();
    let oracle = Oracle::open_default(dir.path());

    oracle.put(b"persist", b"yes").unwrap();
    oracle.flush().unwrap();

    let got = oracle.get(b"persist").unwrap();
    assert_eq!(got.as_deref(), Some(b"yes".as_ref()));
}

#[test]
fn tombstone_survives_flush() {
    let dir = tempdir().unwrap();
    let oracle = Oracle::open_default(dir.path());

    oracle.put(b"k", b"v").unwrap();
    oracle.flush().unwrap();
    oracle.delete(b"k").unwrap();
    oracle.flush().unwrap();

    let got = oracle.get(b"k").unwrap();
    assert!(got.is_none());
}

// ---------------------------------------------------------------------------
// Large batch (forces multiple flushes)
// ---------------------------------------------------------------------------

#[test]
fn large_batch_with_small_buffer() {
    let dir = tempdir().unwrap();
    let oracle = Oracle::open(dir.path(), small_opts());

    for i in 0u32..500 {
        let key = format!("key{i:05}");
        let val = format!("value_{i:05}_padding_padding");
        oracle.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    // Verify every key
    for i in 0u32..500 {
        let key = format!("key{i:05}");
        let val = format!("value_{i:05}_padding_padding");
        let got = oracle.get(key.as_bytes()).unwrap();
        assert_eq!(
            got.as_deref(),
            Some(val.as_bytes()),
            "mismatch at key {key}"
        );
    }
}

// ---------------------------------------------------------------------------
// Interleaved writes and deletes
// ---------------------------------------------------------------------------

#[test]
fn interleaved_puts_and_deletes() {
    let dir = tempdir().unwrap();
    let oracle = Oracle::open(dir.path(), small_opts());

    // Insert 300 keys
    for i in 0u32..300 {
        let key = format!("k{i:05}");
        let val = format!("v{i:05}");
        oracle.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    // Delete every 3rd key
    for i in (0u32..300).step_by(3) {
        let key = format!("k{i:05}");
        oracle.delete(key.as_bytes()).unwrap();
    }

    // Overwrite every 5th key
    for i in (0u32..300).step_by(5) {
        let key = format!("k{i:05}");
        let val = format!("updated_{i:05}");
        oracle.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    oracle.flush().unwrap();

    // Verify all keys
    for i in 0u32..300 {
        let key = format!("k{i:05}");
        oracle.get(key.as_bytes()).unwrap();
    }
}

// ---------------------------------------------------------------------------
// Scan correctness
// ---------------------------------------------------------------------------

#[test]
fn scan_full_range() {
    let dir = tempdir().unwrap();
    let oracle = Oracle::open(dir.path(), small_opts());

    for i in 0u32..200 {
        let key = format!("s{i:04}");
        let val = format!("v{i:04}");
        oracle.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    // Delete some
    for i in (0u32..200).step_by(7) {
        let key = format!("s{i:04}");
        oracle.delete(key.as_bytes()).unwrap();
    }

    oracle.flush().unwrap();

    // Scan all "s..." keys
    oracle.verify_scan(b"s", b"t").unwrap();
}

#[test]
fn scan_partial_range() {
    let dir = tempdir().unwrap();
    let oracle = Oracle::open_default(dir.path());

    for i in 0u32..100 {
        let key = format!("r{i:04}");
        let val = format!("v{i:04}");
        oracle.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    oracle.flush().unwrap();

    // Only scan a sub-range
    oracle.verify_scan(b"r0020", b"r0050").unwrap();
}

#[test]
fn scan_empty_range() {
    let dir = tempdir().unwrap();
    let oracle = Oracle::open_default(dir.path());

    oracle.put(b"a", b"1").unwrap();
    oracle.put(b"z", b"2").unwrap();

    // Scan a range with no keys
    oracle.verify_scan(b"m", b"n").unwrap();
}

// ---------------------------------------------------------------------------
// Randomized stress test
// ---------------------------------------------------------------------------

#[test]
fn randomized_ops() {
    let dir = tempdir().unwrap();
    let oracle = Oracle::open(dir.path(), small_opts());
    let mut rng = StdRng::seed_from_u64(42);

    let num_ops = 1000;
    let key_space = 200; // keys 0..200

    for _ in 0..num_ops {
        let k = rng.gen_range(0..key_space);
        let key = format!("rnd{k:04}");

        match rng.gen_range(0u8..10) {
            0..=6 => {
                // 70% puts
                let val = format!("val_{}", rng.gen::<u32>());
                oracle.put(key.as_bytes(), val.as_bytes()).unwrap();
            }
            7..=8 => {
                // 20% deletes
                oracle.delete(key.as_bytes()).unwrap();
            }
            _ => {
                // 10% reads (verified automatically)
                oracle.get(key.as_bytes()).unwrap();
            }
        }
    }

    // Flush to ensure all data is on disk before verifying.
    oracle.flush().unwrap();

    // Final verification: read every key in the key space
    for k in 0..key_space {
        let key = format!("rnd{k:04}");
        oracle.get(key.as_bytes()).unwrap();
    }

    // Verify scan
    oracle.verify_scan(b"rnd", b"rne").unwrap();
}
