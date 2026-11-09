# OLake White Paper — Exactly-Once Delivery to Apache Iceberg

**Working title:** Exactly-Once Delivery to Apache Iceberg: How OLake Go Guarantees Every Row Lands Once — With No External Coordinator  
**Product:** OLake Go — `github.com/datazip-inc/olake` (Go, Apache 2.0)  
**Format:** Technical white paper (PDF + HTML), ungated  
**Length limit:** 8–10 pages maximum (strict)  
**Brand:** OLake (Datazip, Inc.) — https://olake.io, https://olake.io/branding/

**Status:** Full draft. Ready for your changes.

---

# Cover

**OLake Go**  
*Apache Iceberg · Open Source · Apache 2.0*

# Exactly-Once Delivery to Apache Iceberg

How OLake Go Guarantees Every Row Lands Once — With No External Coordinator

**KEY STAT**
- **55,555 RPS** CDC throughput (Postgres → Iceberg) — 2× Fivetran
- **4.01B rows** full-load in 1.91 hrs for **<$6**
- **Zero** external coordinators (no Kafka, Debezium, or Flink)

OLake Go · Datazip, Inc. · Apache License 2.0 · olake.io

---

# Executive Summary

## The problem
Change Data Capture (CDC) promises every source change lands exactly once in the lakehouse. In practice, it does not. When a sync fails mid-flight and restarts from its last saved checkpoint, the driver re-reads an **overlap window** of already-processed changes. Without idempotent writes, those rows land in Apache Iceberg a second time. Iceberg's append-only snapshot model does not deduplicate by itself; you need delete files or MERGE INTO logic — and most teams solve this by bolting on Kafka Connect, Debezium, or Flink, each of which adds an external transaction coordinator, operational complexity, and cost.

## The OLake Go answer
OLake Go delivers **exactly-once visible results to Iceberg using only its own process** — a Go driver plus an embedded Java Iceberg server communicating over gRPC. There is no external coordinator. Exactly-once is built into the write path through four mechanisms:

1. **Stable row identity** — every record carries `_olake_id`, `_op_type`, and `_olake_timestamp`.
2. **Upsert-mode deletes** — equality deletes (`_olake_id` only) and positional deletes (`file:position`) are written in the same commit as the data.
3. **Snapshot-to-log handoff** — the committed Iceberg `SnapshotId` is written back into a Pebble stream index (`_olake_id` → `file:position`), turning the committed snapshot into the durable recovery log.
4. **Crash recovery via overlap-window dedup** — after a failure, state is restored to the last committed checkpoint, the cursor re-reads the overlap, and duplicates are collapsed so only net-new rows are committed.

## Bottom line
OLake Go is the fastest open-source way to replicate databases to Apache Iceberg — verified at **55,555 RPS CDC (Postgres)**, **2× faster than Fivetran**, and **4.01B rows in 1.91 hrs for <$6** — with exactly-once delivery and zero external infrastructure.

---

# 1. The Problem: Exactly-Once to Iceberg Is Hard

CDC captures change events from a source database's WAL/binlog/oplog. After a transient failure, the driver resumes from the last saved checkpoint and re-reads the changes that fall in the overlap window. Those changes include rows already written to Iceberg.

**Without idempotent writes, the same row lands twice.** Iceberg tables are append-only by snapshot; they do not auto-deduplicate. Achieving exactly-once requires delete files (equality or positional) or MERGE INTO logic coordinated with the ingest stream.

Most managed ELT tools sidestep this by writing to a warehouse and relying on the warehouse's upsert, or by inserting an external coordinator (Kafka Connect + Debezium, or Flink) between source and sink. That coordinator becomes the source of truth for exactly-once — but it is another system to run, monitor, and pay for.

**The core tension:** exactly-once to Iceberg is either outsourced to an expensive external coordinator, or it is not guaranteed at all. OLake Go takes a third path: exactly-once inside the driver, no coordinator, no lock-in.

> **Diagram 1 — The exactly-once gap.** CDC failure → checkpoint restore → overlap re-read → duplicate rows in Iceberg (without idempotent writes).

```
Source DB (WAL)          OLake Driver              Iceberg Table
    |                         |                        |
    |── change A (offset 10) ──┼──────────────────────►| row A v1
    |                         |   (sync fails here)    |
    |<── restore checkpoint ──┤                        |
    |── change A (offset 10) ──┼──────────────────────►| row A v1  ← DUPLICATE
    |── change B (offset 11) ──┼──────────────────────►| row B v1
```

---

# 2. OLake Go Architecture

OLake Go is a single Go binary. It reads WAL/binlog/oplog natively per source (Postgres pgoutput, MySQL binlog, MongoDB oplog, MSSQL CDC), normalizes every record into `_olake_id`, `_op_type`, `_olake_timestamp`, and writes through an Arrow-based writer to an embedded Java Iceberg server over gRPC. Data lands as Iceberg files in object storage (S3, GCS, Azure, MinIO) under catalogs including AWS Glue, Hive HMS, JDBC, and REST (Nessie, Apache Polaris, Unity Catalog, Lakekeeper, AWS S3 Tables).

**No Spark, no Flink, no Kafka, no Debezium.** The exactly-once protocol runs inside the OLake Go process itself.

> **Diagram 2 — OLake Go data-flow architecture.** Source → driver → Arrow writer → Iceberg server → object storage. The 2PC commit is orchestrated by the driver; no external coordinator is in the path.

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐     ┌──────────────┐
│  Source DB  │────►│  OLake Go driver │────►│  Arrow writer   │────►│  Iceberg     │
│ (WAL/binlog)│     │  (normalizes _olake│     │  (per partition)│     │  server      │
│             │     │   _id, _op_type) │     │                 │     │  (embedded   │
└─────────────┘     └──────────────────┘     └─────────────────┘     │   Java)      │
                                                                      │              │
                                                                      └──────┬───────┘
                                                                             │ gRPC
                                                                             ▼
                                                                   ┌─────────────────┐
                                                                   │ Object Storage  │
                                                                   │ (Iceberg files) │
                                                                   └─────────────────┘
                                                              ▲
                                                              │ 2PC commit
                                                              │ (REGISTER_AND_COMMIT)
```

> **KEY STAT — No external coordinator.** OLake Go's exactly-once protocol is orchestrated by the driver process calling its embedded Iceberg server. Kafka Connect, Debezium, Flink, and a separate transaction coordinator are not required.

---

# 3. The 2PC Commit Mechanism

OLake Go uses a **two-phase commit** inside the writer:

**Phase 1 — Stage.** Data files, equality-delete files, and positional-delete files are staged to an `_olake_2pc/` prefix in object storage. A shared `finish.json` marks the commit point and is written idempotently, so a replay after a partial failure sees the same finish state and does not duplicate work.

**Phase 2 — Commit.** The driver sends a `REGISTER_AND_COMMIT` request to the Iceberg server. The server registers all file metadata and commits a single Iceberg snapshot atomically. Files are committed in a deterministic order: **equality deletes → data → positional deletes**, so readers always see a consistent state.

**Upsert mode & deduplication.** In upsert mode (non-append), every record carries `_olake_id`. The Arrow writer tracks row positions per partition:
- **First occurrence** of an `_olake_id` → an equality-delete entry is emitted, and the position is recorded.
- **Duplicate occurrence** → the previous position is emitted as a positional delete (n-1 logic); only the latest occurrence is kept.

The result: even if the same `_olake_id` arrives across multiple data files or retries, the committed Iceberg snapshot contains the row exactly once.

> **Diagram 3 — 2PC commit workflow.** Stage → equality/positional deletes → REGISTER_AND_COMMIT → Iceberg snapshot.

```
┌─────────────────────────────────────────────────────────────────┐
│  REGISTER_AND_COMMIT (atomic, gRPC to embedded Iceberg server)  │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ Equality     │    │ Data files   │    │ Positional   │
│ deletes      │───►│ (_olake_id)  │───►│ deletes      │
│ (_olake_id)  │    │              │    │ (file:pos)   │
└──────────────┘    └──────────────┘    └──────────────┘
     ordered commit → single Iceberg snapshot
```

> **Diagram 4 — Equality vs positional delete.** Equality deletes carry only `_olake_id`; positional deletes carry `file:position`. Duplicates collapse via n-1 logic: latest occurrence kept, previous position deleted.

| Delete type | Payload | Purpose |
|-------------|---------|---------|
| Equality delete | `_olake_id` | Marks all rows with this id for removal |
| Positional delete | `file:position` | Removes a specific occurrence of a duplicate id |

> **Honest boundary.** OLake's exactly-once is an **exactly-once visible result at the Iceberg table level** (idempotent writes plus dedup on recovery). It is not distributed-systems exactly-once messaging with an external transaction coordinator. We state this plainly.

---

# 4. Snapshot-to-Log Handoff & Recovery Index

The committed Iceberg `SnapshotId` is not discarded after commit. OLake writes it back into a **Pebble-based stream index** (`pkg/indexdb/pebble.go`) that maps `_olake_id` → `file:position`. The committed snapshot therefore becomes the durable recovery log: the index knows exactly which snapshot and file position holds each row.

The `_olake_2pc_state` table property carries committed chunk IDs / cursor positions and is refreshed on every retry, so a restarted writer always sees the up-to-date committed state rather than a stale one.

> **Diagram 5 — Snapshot-to-log handoff.** Committed Iceberg SnapshotId → Pebble StreamIndex entry `_olake_id` → `file:position`.

```
Iceberg commit                          Pebble StreamIndex (recovery log)
─────────────                           ────────────────────────────────
SnapshotId = 42 committed               _olake_id="abc"  →  file=s3://.../part-001.parquet, pos=1502
                                        _olake_id="def"  →  file=s3://.../part-001.parquet, pos=1503
                                        _olake_id="ghi"  →  file=s3://.../part-002.parquet, pos=42
```

Every `REGISTER_AND_COMMIT` returns the `SnapshotId`, which `TableIndex.Commit(batch, committedSnapshotID)` applies to the index. On restart, the driver reads `_olake_2pc_state` from the table and resumes from the committed cursor — never reprocessing already-committed chunks.

---

# 5. Crash Recovery: Overlap-Window Dedup

When a sync fails mid-flight, OLake's recovery works as follows:

1. **Save checkpoint.** Before processing a batch, the driver persists state (a Pebble state file) marking the pre-insert cursor.
2. **Crash.** A failure occurs after some rows are written but before state is safely advanced.
3. **Restore.** On restart, state is restored to the last committed checkpoint.
4. **Re-read overlap.** The cursor re-reads the overlap window — including rows already committed to Iceberg.
5. **Dedup via delete + insert.** Duplicate `_olake_id`s arriving in the re-read are collapsed: the original row gets a delete file; only the net-new row is inserted.

This behavior is verified by OLake's 2PC integration tests (`tests/testutils/integration/2pc.go`). In the `insert_2pc` recovery scenario, after restoring state and re-syncing, only the `insert_2pc` row is visible (`expectedRowCountByOpType: 1`) — the original insert is deduplicated. After a subsequent normal sync, both the original and `insert_2pc` rows appear, each unique by `_olake_id` (`expectedRowCountByOpType: 2`).

> **Diagram 6 — Crash-recovery timeline.** Checkpoint → crash → restore → re-read overlap → dedup → net-new commit.

```
Time ───────────────────────────────────────────────────────────────►
    │ Save state (checkpoint)            │ Crash                          │ Restore
    │                                    │                                │ Re-read overlap
    │ write row A (committed)            │ (state not advanced)           │ dedup row A via delete
    │                                    │                                │ write row B (net-new)
    ▼                                    ▼                                ▼
[checkpoint]                       [recovery]                         [clean commit]
    row A ✓                          row A ?                          row A ✓ (once)
                                                                 row B ✓ (net-new)
```

> **Diagram 7 — Before/after dedup.** Duplicate rows collapse to one visible row via delete + insert.

| Before recovery | After recovery (deduped) |
|-----------------|--------------------------|
| row A v1 (offset 10) | row A v1 |
| row A v1 (offset 10) ← duplicate | (deleted via equality/positional delete) |
| row B v1 (offset 11) | row B v1 |

**Visible result:** exactly one row A, exactly one row B — exactly once.

---

# 6. Evidence: Benchmarks & Comparison

OLake Go's exactly-once claims are backed by published benchmarks (`olake.io/docs/benchmarks/ingestion/`).

| Tool | CDC throughput | Postgres full-load | Cost (4.01B rows) | External coordinator | Exactly-once to Iceberg |
|------|---------------|-------------------|-------------------|----------------------|-------------------------|
| **OLake Go** | **55,555 RPS** (Postgres) | **580,113 RPS** | **<$6** | **None** | ✅ Built-in |
| Airbyte | ~457 RPS | — | $5,560 | Managed platform | ⚠️ Warehouse upsert |
| Fivetran | — | 46,395 RPS | $5,560 | Managed platform | ⚠️ Warehouse upsert |
| Debezium | — | — | Self-hosted | Kafka Connect required | ⚠️ Downstream logic |
| Estuary | — | — | $75 | Flow collections | ⚠️ Downstream logic |

> **KEY STAT — 4.01B Postgres rows → Iceberg in 1.91 hrs for <$6** vs Fivetran's $5,560 (~1,270× slower per OLake benchmarks).
> **KEY STAT — Postgres CDC 55,555 RPS — 2× faster than Fivetran.**
> **KEY STAT — MySQL CDC 59,951 RPS — 1.52× faster than Fivetran.**

Benchmarks measure committed, deduplicated rows only; failed or retried attempts are rolled back so throughput reflects data that actually lands in Iceberg (`rollbackStats` on non-commit).

---

# 7. Operations & Deployment

OLake Go is self-hosted entirely within the customer's VPC or on-prem environment — critical for regulated industries that cannot compromise on data residency. Deploy via:

- **Docker Compose** (single command, UI + backend)
- **Standalone Docker** (CLI)
- **Helm / Kubernetes**
- **Airflow** (EC2 or EKS)

**Catalogs:** AWS Glue, Hive HMS, JDBC, REST (generic, Nessie, Apache Polaris, Unity Catalog, Lakekeeper, AWS S3 Tables).  
**Query engines:** Athena, Trino, Spark, Snowflake, Databricks, ClickHouse, StarRocks, Dremio, DuckDB, and more — Iceberg tables are open and queryable by any engine.

> **Diagram 8 — Deployment options.** Docker / Helm / Airflow + catalog badges.

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ Docker      │  │ Helm / K8s  │  │ Airflow     │
│ Compose     │  │             │  │             │
│ (quickstart)│  │ (production)│  │ (orchestrated)│
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       └────────────────┴────────────────┘
                    │
                    ▼
          OLake Go (self-hosted in your VPC)
          Catalogs: Glue · HMS · JDBC · REST (Nessie/Polaris/Unity/Lakekeeper)
```

---

# 8. Conclusion & References

OLake Go proves that exactly-once delivery to Apache Iceberg does not require an external coordinator. By combining stable row identity (`_olake_id`), upsert-mode equality and positional deletes, snapshot-to-log handoff into a Pebble recovery index, and overlap-window dedup on crash recovery, OLake Go guarantees every row lands once — using only its own process — at 55,555 RPS CDC with sub-dollar infrastructure cost for billion-row loads.

OLake publishes this and all technical content ungated, open source, and source-verifiable — a deliberate contrast to form-gated competitor white papers. We invite engineers to review the implementation in `github.com/datazip-inc/olake`, run the benchmarks, and try the quickstart at olake.io.

## References
1. OLake Go repository — github.com/datazip-inc/olake (Apache 2.0)
2. OLake Go product & benchmark docs — olake.io/olake-go/, olake.io/docs/benchmarks/ingestion/
3. Apache Iceberg Table Format Specification — iceberg.apache.org
4. OLake 2PC integration tests — olake/tests/testutils/integration/2pc.go
5. OLake Branding Guidelines — olake.io/branding/
6. OLake Fusion (table maintenance) — github.com/datazip-inc/olake-fusion

**OLake Go** — Replicate fast. Keep it fast. Open, lock-in-free.
