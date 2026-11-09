# OLake Company, Products & Technology Research

**Source:** olake.io, github.com/datazip-inc, LinkedIn, HN benchmark discussion  
**Purpose:** Foundation reference for OLake white paper authoring

---

## 1. The Company

| Attribute | Detail |
|-----------|--------|
| Legal company | Datazip, Inc. (does business as OLake) |
| Website | https://olake.io |
| Founded | 2022 |
| Headquarters | Lewes, Delaware, US |
| Engineering office | HSR Layout, Bangalore, India |
| Leaders | Shubham Satish Baldava (CTO), Rohan Khameshra (CEO) |
| License | Apache 2.0 (all core repos) |
| GitHub org | https://github.com/datazip-inc |
| Customers (named) | PhysicsWallah, AstroTalk, LendingKart, Cordial, Bitespeed, Xeno (+logo wall: Booking.com, Qualcomm, Razorpay, HDFC Securities, Nutanix, Lenskart, ShareCars, SumUp, AtoB, IXL Learning, Alma Media, etc.) |

**Mission / positioning:** "Fastest open-source tool for replicating Databases to Apache Iceberg or Data Lakehouse." OLake is an Iceberg-first, vendor-lock-in-free EL engine that makes database → Apache Iceberg simple, fast, and observable — replacing brittle scripts and legacy ETL with a single self-hosted binary.

**Enterprise model:** Core is open-source/self-hosted; Datazip offers custom enterprise support (GRC requirements, professional assistance, pilot programs).

---

## 2. OLake Go — Core Ingestion Engine

**Repo:** https://github.com/datazip-inc/olake (Go, Apache 2.0, ~1.6k stars)  
**Product page:** https://olake.io/olake-go/

### What it does
Replicates data from transactional databases, event streams, and object storage into **Apache Iceberg** (or plain Parquet), without requiring Kafka Connect, Flink, Spark, or Debezium in the path. Reads WAL/binlog/change streams directly and writes Iceberg natively.

### Supported sources
| Source | Full Load | CDC | Incremental |
|--------|-----------|-----|-------------|
| PostgreSQL | ✅ | ✅ (pgoutput) | ✅ |
| MySQL | ✅ | ✅ (binlog) | ✅ |
| MongoDB | ✅ | ✅ (oplog) | ✅ |
| Oracle | ✅ | WIP | ✅ |
| DB2 LUW | ✅ | – | ✅ |
| MSSQL | ✅ | ✅ | ✅ |
| Kafka | – | – | bounded incremental |
| S3 (MinIO/LocalStack) | ✅ | – | ✅ |

### Destinations / catalogs
Apache Iceberg via AWS Glue, Hive HMS, JDBC, REST (Nessie, Apache Polaris, Unity Catalog, Lakekeeper, AWS S3 Tables, generic REST); Parquet to S3/GCS/local.

### Key features
- Full, incremental, CDC syncs from one tool
- Smart sync: automatic schema discovery + schema/partition evolution
- Parallelized chunking for fast full snapshots
- Stateful, resumable syncs (Pebble-based checkpoint store)
- Exactly-once-style delivery; Arrow-based writes
- Iceberg-native writes (no proprietary staging layer)
- Self-serve web UI + CLI (`spec`, `check`, `discover`, `sync`, `clear-destination`)
- Deploy: Docker Compose, standalone Docker, Helm/Kubernetes, Airflow

### Architecture (from repo)
- `drivers/` — per-source drivers (abstract interface/backfill/CDC/incremental; db2, kafka, mongodb, mssql, mysql, oracle, postgres, s3)
- `pkg/` — core libs: `indexdb` (Pebble state/checkpoint), `jdbc`, `kafka`, `objstorage` (S3 reader), `waljs` (Postgres WAL/pgoutput)
- `protocol/` — CLI commands: spec, check, discover, sync, clear-destination

### Published benchmarks (https://olake.io/docs/benchmarks/ingestion/)
| Source | RPS | vs Fivetran |
|--------|-----|-------------|
| Postgres → Iceberg (full) | 580,113 | 12.5× faster |
| MySQL → Iceberg (full) | 139,773 | 1.91× faster |
| MongoDB → Iceberg (full) | 37,879 | – |
| Oracle → Iceberg (full) | 526,337 | – |
| MSSQL → Iceberg (full) | 345,866 | 4.32× faster |
| Kafka → Iceberg | 209,065 MPS | 1.23× slower than Flink |
| Postgres CDC | 55,555 RPS | 2× faster than Fivetran |
| MySQL CDC | 59,951 RPS | 1.52× faster than Fivetran |

**Flagship claim:** 4.01B Postgres rows → Iceberg in 1.91 hrs for <$6 (vs Fivetran $5,560, ~1270× slower).

---

## 3. OLake Fusion — Iceberg Table Maintenance

**Repo:** https://github.com/datazip-inc/olake-fusion (Java, ~33 stars)  
**Product page:** https://olake.io/olake-fusion/  
**Built on:** fork of Apache Amoro (https://github.com/datazip-inc/amoro-forked)

### What it does
Lakehouse table management system for Apache Iceberg that automates day-2 operations (compaction, snapshot expiry, orphan-file cleanup) so Iceberg tables stay performant and storage costs stay bounded.

### Architecture
- **Fusion (Management Service):** table lifecycle operations, self-optimization, data expiration, unified catalog interface
- **Spark Optimizer:** Spark-based optimization tasks improving file layout and read efficiency
- Multi-catalog: Glue, JDBC, REST-based catalogs
- Deployable on private/public/hybrid/multi-cloud

### Key features
- Cron-based table compaction via UI
- Multi-catalog support (Glue, JDBC, REST)
- Cloud/infrastructure agnostic
- Iceberg table maintenance (compact small files, expire snapshots, remove orphan files)

### Published benchmarks (https://olake.io/docs/benchmarks/compaction/)
- Up to 2.06× faster than vanilla Spark compaction (27m02s vs 55m47s on 1.8B rows)
- ~52% lower cost per compaction job ($1.065 vs $2.19)
- 110× simpler configuration (1 parameter vs 10+)

---

## 4. Product Portfolio & Tech Stack

| Repo | Language | Role |
|------|----------|------|
| datazip-inc/olake | Go | OLake Go ingestion engine |
| datazip-inc/olake-fusion | Java | OLake Fusion table maintenance |
| datazip-inc/olake-ui | TypeScript | Web UI + BFF |
| datazip-inc/olake-helm | Go | Helm chart (K8s) |
| datazip-inc/olake-docs | MDX/Docusaurus | Website, docs, blog |
| datazip-inc/amoro-forked | Java | Apache Amoro fork (Fusion base) |
| datazip-inc/workshops | Python | Iceberg hands-on workshops |

**Stack:** Go (ingestion), Java (Fusion/Amoro), TypeScript (UI). Table format: Apache Iceberg (primary), Parquet (alt). CDC: native WAL/binlog/oplog. State: Pebble. Deployment: single Go binary, Docker, Docker Compose, Helm/K8s, Airflow.

---

## 5. Ecosystem & Adjacent Projects

- **Lakekeeper** (Apache Iceberg REST catalog server, same Datazip family) — OLake supports Lakekeeper as an Iceberg catalog destination
- **Competitors:** Fivetran, Airbyte, Estuary, Debezium, dlt, Hevo, Stitch, Meltano, Matillion, Confluent, Striim, Qlik (ingestion); Spark, Tabular, Onehouse, Dremio, Hudi, Paimon (table maintenance)

---
