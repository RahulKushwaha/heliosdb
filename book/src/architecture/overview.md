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
┌────────────────────────────────────────────────┐
│                 heliosdb (DB)                  │
│  put / get / delete / scan / flush             │
│                                                │
│  ┌──────────────┐   ┌──────────────────────┐  │
│  │  MemTable    │   │  ActiveSegment        │  │
│  │  (skip list) │   │  (one SST, all live   │  │
│  │              │   │   keys, latest ver.)  │  │
│  └──────┬───────┘   └──────────┬───────────┘  │
│         │  flush                │ seal          │
│         └──────────────────────▼              │
│                         InactiveSegments       │
│                         (historical, bloom-    │
│                          filtered reads)       │
│                                                │
│  ┌──────────────┐   ┌──────────────────────┐  │
│  │  WAL         │   │  Manifest            │  │
│  │  (crash      │   │  (version edits,     │  │
│  │   recovery)  │   │   file membership)   │  │
│  └──────────────┘   └──────────────────────┘  │
└────────────────────────────────────────────────┘
```

## Key invariant

> The active segment always contains the latest version of every currently-live key.

This invariant is established by the flush pipeline and never broken:
- Every `put` lands in the MemTable.
- Every flush merges the MemTable **into** the active segment (not alongside it).
- The result is a new active segment with up-to-date values for all keys.

Inactive segments hold historical data and overflow from older compaction cycles.
They are never needed for current-snapshot reads unless a key is absent from the
active segment — which the negative bloom filter catches cheaply.
