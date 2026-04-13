# Architecture Overview

heliosDB is organized as a four-crate Cargo workspace. Each crate has a strict
dependency direction — lower crates never depend on higher ones — which keeps the
layers independently testable and benchmarkable.

```
heliosdb          (public API, CLI)
    └── heliosdb-engine   (WAL, MemTable, Segments, Compaction, Manifest)
            └── heliosdb-sst      (Block, Bloom, Index, SST Builder/Reader)
                    └── heliosdb-types    (InternalKey, Error, SeqNum)
```

## Component map

```
┌──────────────────────────────────────────────────────────┐
│                     heliosdb (DB)                        │
│  put / get / delete / scan / flush                       │
│                                                          │
│  ┌──────────────┐   ┌─────────────────────────────────┐  │
│  │  MemTableSet │   │  Level-0 SST files              │  │
│  │  active +    │──►│  (one per flush, newest-first   │  │
│  │  immutables  │   │   reads, oldest-first scans)    │  │
│  └──────────────┘   └─────────────────────────────────┘  │
│         │ background flusher                             │
│         │ (bounded channel)                              │
│         ▼                                                │
│  ┌──────────────┐   ┌──────────────────────────────────┐ │
│  │  WAL         │   │  Manifest                        │ │
│  │  (crash      │   │  (version edits, file membership)│ │
│  │   recovery)  │   │                                  │ │
│  └──────────────┘   └──────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

## Flush model

Each sealed memtable is written to its own SST file (no merge with
existing SSTs). This keeps the flush path simple and fast — the
background flusher just serializes the memtable entries and registers the
new file in the manifest.

Multiple level-0 SSTs can have overlapping key ranges. The read path
handles this by checking them in recency order (newest first for point
lookups, oldest first for scans with overwrites).

Compaction merges overlapping level-0 SSTs into non-overlapping higher
levels, reducing read amplification over time.
