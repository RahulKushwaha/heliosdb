# Write-Ahead Log (WAL)

**Crate**: `heliosdb-engine`  **File**: `src/wal/mod.rs`

The WAL guarantees **crash durability**: every write that `put()` acknowledges
to the caller is recoverable even if the process dies before the MemTable is
flushed to the active segment.

## Record format

Each WAL record:

```
[record_type: u8]    0 = Full (entire entry in one record)
[data_len: u32]      length of the payload
[data: bytes]        encoded entry payload
[crc32: u32]         checksum of data bytes only
```

A full entry payload:

```
[key_len: u32]
[encoded_internal_key: bytes]   user_key + seq_num + op_type
[value_len: u32]
[value: bytes]
```

Fragmented records (First / Middle / Last) are supported by the format but
heliosDB currently produces only Full records. The type field is reserved for
future large-value fragmentation.

## Durability guarantee

After encoding the record, the WAL calls `fsync` (via `File::sync_data`) before
returning to the caller. This ensures the data has reached stable storage, not
just the OS page cache.

```rust
fn append(&mut self, key: &InternalKey, value: &Value) -> Result<()> {
    let payload = encode_entry(key, value);
    write_record(&mut self.writer, RECORD_FULL, &payload)?;
    self.writer.flush()?;        // flush BufWriter → OS
    self.writer.get_ref().sync_data()?;  // fsync → disk
    Ok(())
}
```

## Replay on open

`DB::open` calls `Wal::replay` before doing anything else:

```rust
Wal::replay(&wal_path, |key, value| {
    match key.op_type {
        OpType::Put    => memtable.put(key.user_key, key.seq_num, value),
        OpType::Delete => memtable.delete(key.user_key, key.seq_num),
    }
})?;
```

Replay stops at the first record with a CRC mismatch, treating it as
end-of-log (a partial write that occurred during a crash).

## WAL rotation

After a successful flush, the WAL is rotated: the old WAL file is replaced with
a new empty one. The flushed entries are now safe in the active segment and no
longer need WAL protection.

## File location

The WAL lives at `<db_dir>/WAL`. There is always at most one WAL file active.
