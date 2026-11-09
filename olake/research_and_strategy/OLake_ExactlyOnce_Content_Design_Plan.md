# OLake White Paper — Content & Design Plan

**Working title:** Exactly-Once Delivery to Apache Iceberg: How OLake Go Guarantees Every Row Lands Once — With No External Coordinator

**Product:** OLake Go (Go-based ingestion engine, `github.com/datazip-inc/olake`)
**Format:** Technical white paper (PDF + HTML), ungated
**Length limit:** 8–10 pages maximum (strict)
**Brand:** OLake (Datazip, Inc.) — https://olake.io, https://olake.io/branding/

**Status:** Plan only. Full draft NOT yet written. Ready for review before drafting.

---

## 0. Research foundation (already completed)

- OLake company, products, architecture researched and documented at:
  `OLake/Research/OLake_Company_Products_Research.md`
- Competitor landscape audited at:
  `Competitors/COMPETITOR_RESEARCH_NOTES.md`
- OLake white paper strategy at:
  `OLake/Strategy/OLake_White_Paper_Strategy.md`
- Competitor PDFs collected (14 total, on disk):
  - `Competitors/Fivetran/` — 5 PDFs (form-gated alternatives noted for Airbyte, Confluent, Striim, Qlik, Estuary, Matillion)
  - `Competitors/Apache_Spark/` — 9 academic PDFs (rigor model)
  - Form-gated/not downloadable: Airbyte, Confluent, Dremio, Onehouse, Tabular, Matillion, Striim, Qlik, Hevo, Stitch, Meltano, Estuary, Debezium, dlt, Hudi, Paimon

## 1. Objective, audience, argument, takeaways

### Objective
Establish OLake as the technical authority on **exactly-once CDC delivery to Apache Iceberg without an external coordinator**, and substantiate the `olake_2pc` two-phase commit mechanism with source-code-verifiable claims, benchmarks, and crash-recovery evidence.

### Central argument
OLake Go delivers exactly-once visible results to Iceberg using only its own process (Go driver + embedded Java Iceberg server) — no Kafka Connect, no Debezium, no Flink, no separate transaction coordinator. It achieves this through four mechanisms: (1) a stable row identity (`_olake_id`), (2) upsert-mode equality + positional deletes written as part of the same commit, (3) a Pebble-based stream index mapping `_olake_id` → Iceberg file:position, and (4) atomic `REGISTER_AND_COMMIT` where the committed Iceberg snapshot ID is handed back into the index (snapshot-to-log handoff). On crash, state is restored to the last committed checkpoint, the cursor re-reads the overlap window, and duplicates are collapsed via MERGE INTO-style delete/insert so only net-new rows survive.

### Target audience
- **Primary:** Data platform engineers, analytics architects, lakehouse teams evaluating Iceberg ingestion.
- **Secondary:** CTOs/VP Engineering at mid-market enterprises considering self-hosted open-source vs managed ELT (Fivetran/Airbyte).
- **Tone:** Technical, vendor-transparent, engineering-first; not a sales deck.

### Key takeaways (one per major section)
1. Exactly-once to Iceberg is hard because CDC re-reads overlap windows after failure; idempotency must be built into the write path, not bolted on.
2. OLake Go needs no external coordinator: the 2PC lives inside the driver process and the committed Iceberg snapshot doubles as the recovery log.
3. `_olake_id` + equality/positional deletes + Pebble index make duplicate rows visible-once, even under concurrent retries.
4. Crash recovery = restore Pebble checkpoint → re-read overlap → dedup via delete files → only net-new rows committed.
5. Verified benchmarks: Postgres CDC 55,555 RPS (2× Fivetran), MySQL CDC 59,951 RPS (1.52× Fivetran), 4.01B rows full-load in 1.91 hrs for <$6.

---

## 2. Technical verification summary (claims → source)

Every claim in the paper must be traceable to `github.com/datazip-inc/olake`.

| Claim | Source file | Line(s) | Mechanism |
|-------|-------------|---------|-----------|
| Row identity `_olake_id` | `constants/constants.go` | 19 | `OlakeID = "_olake_id"` |
| Op type (`i`/`u`/`d`/`c`/`r`) | `constants/constants.go` | 21 | `OpType = "_op_type"` |
| CDC timestamps | `constants/constants.go` | 20 | `OlakeTimestamp = "_olake_timestamp"` |
| 2PC staging dir `_olake_2pc` | `destination/parquet/parquet_2pc.go` | 21 | `parquet2PCDir = "_olake_2pc"` |
| Shared finish object `finish.json` (idempotent replay) | `destination/parquet/parquet_2pc.go` | 538–540 | `sharedFinishObjectKey()`, `currentStagingPrefix()` |
| Fresh `olake_2pc` state per retry (committed chunk IDs / cursor positions) | `destination/writers.go` | 218–219 | comment + `NewWriter` flow |
| `olake_2pc_state` read from Iceberg table properties | `destination/iceberg/iceberg.go` | 133–137 | `GetOlake_2PcState()` unmarshal → `MetadataState` |
| Arrow writer: `_olake_id` equality + positional deletes, n-1 dedup | `destination/iceberg/arrow-writer/writer.go` | 195–222 | `extract()`: first occurrence → equality delete + track pos; duplicate → previous pos to positional deletes, latest kept |
| Pebble stream index `_olake_id` → RowLocation (file:position) | `destination/iceberg/arrow-writer/writer.go` | 234–256 | `indexRecord()`: `indexThread.Lookup` / `Put` |
| Ordered commit: equality deletes → data → positional deletes | `destination/iceberg/arrow-writer/writer.go` | 404–410 | `Close()` builds `orderedFiles` |
| `REGISTER_AND_COMMIT` atomic commit | `destination/iceberg/arrow-writer/writer.go` | 412–441 | `ArrowPayload_REGISTER_AND_COMMIT`, returns `SnapshotId` |
| Snapshot-to-log handoff | `destination/iceberg/arrow-writer/writer.go` | 447–453 | `TableIndex.Commit(batch, committedSnapshotID)` |
| Deletion vectors (positional deletes server-side) | `destination/iceberg/arrow-writer/writer.go` | 632–672 | `sendPendingVectors()` / `DELETION_VECTORS` |
| Stats rollback on non-commit (committed-data-only accounting) | `destination/writers.go` | 323–327 | `rollbackStats()` |
| Writer abort on context cancel (nothing reaches destination) | `destination/writers.go` | 329–341 | `Close()`: `group.Block()` → `rollbackStats()` |
| Crash recovery: save state → insert → restore state → re-read overlap → dedup via MERGE INTO | `tests/testutils/integration/2pc.go` | 77–268 | `Iceberg2PCCDCRecovery`, `Iceberg2PCIncrementalRecovery` |
| Benchmarks (Postgres CDC 55,555 RPS, etc.) | `olake.io/docs/benchmarks/ingestion/` | website | cited numbers above |

**Honest boundary (do not overclaim):** OLake's exactly-once is **exactly-once visible result at the Iceberg table level** (idempotent writes + dedup on recovery), not distributed-systems exactly-once messaging with an external transaction coordinator. The paper states this plainly — this honesty is a differentiator vs vendor hype.

**No-external-coordinator claim:** Verified. The 2PC is orchestrated by the OLake Go process calling its embedded Java Iceberg server over gRPC (`internal.ServerClient`). No Kafka Connect, Debezium, Flink, or separate coordinator is in the path (README: "No Spark, no Flink, no Kafka, no Debezium").

---

## 3. Page-by-page outline (8–10 pages)

**Hard limit: 10 pages including cover and references.**

| Page | Section | Content | Approx. words | Visuals |
|------|---------|---------|---------------|---------|
| 1 | Cover | Title, subtitle, OLake logo (blue), OLake Blue band, author/date, "Open source · Apache 2.0" | ~30 | Cover logo, color block |
| 2 | Executive summary | Problem in one paragraph, OLake Go answer, 3 key stats (KEY STAT callouts), bottom-line recommendation | ~350 | 3 stat callout boxes |
| 3 | 1. The problem: exactly-once to Iceberg is hard | Why CDC + Iceberg ≠ exactly-once by default; overlap windows on retry; why warehouses hide this but lakehouses expose it; the coordinator tax (Kafka/Debezium/Flink) | ~450 | Diagram A: "The exactly-once gap" (CDC re-read → duplicate rows) |
| 4 | 2. OLake Go architecture (one page) | Source → driver → Arrow writer → Iceberg server → object storage; where 2PC sits; no external coordinator callout | ~350 | Diagram B: architecture / data-flow |
| 5 | 3. The 2PC commit mechanism | `_olake_id` row identity; upsert mode; equality + positional deletes; n-1 dedup; `REGISTER_AND_COMMIT`; ordered file list; base snapshot ID | ~450 | Diagram C: commit workflow; Diagram D: equality vs positional delete |
| 6 | 4. Snapshot-to-log handoff & recovery index | Committed Iceberg SnapshotId → Pebble StreamIndex (`_olake_id` → file:position); how the index becomes the recovery log; `_olake_2pc_state` table property | ~400 | Diagram E: snapshot-to-log handoff; Diagram F: index entry |
| 7 | 5. Crash recovery: overlap-window dedup | Failure scenario step-by-step; save checkpoint → crash → restore → re-read overlap → dedup via MERGE INTO → net-new only; `insert_2pc` test evidence | ~450 | Diagram G: crash-recovery timeline; Diagram H: before/after dedup |
| 8 | 6. Evidence: benchmarks & comparison | Table: OLake Go vs Fivetran/Airbyte/Debezium/Estuary (throughput, cost, coordinator required, exactly-once); 4.01B rows <$6 stat; CDC RPS | ~300 | Comparison table; benchmark stat callout |
| 9 | 7. Operations & deployment | Self-hosted in VPC; Docker / Helm / Airflow; catalogs (Glue, HMS, JDBC, REST incl. Nessie/Polaris/Unity/Lakekeeper); observability | ~300 | Diagram I: deployment options (small) |
| 10 | 8. Conclusion + references | Summary, thought-leadership positioning, call to action (try quickstart), references (OLake repo, docs, Iceberg spec, competitor papers) | ~300 | None (references list) |

**Page budget check:** 10 pages total. Cover(1) + exec(1) + 7 content pages(7) + conclusion/refs(1) = 10. Tight but within limit; prose must be tight, visuals carry weight.

---

## 4. Technical explanations, evidence, and references per section

### Section 1 — The problem
- **Explanation:** CDC captures change events; after a transient failure the driver resumes from the last saved checkpoint and re-reads an overlap window. Without idempotent writes, the same row lands in Iceberg twice. Iceberg's append-only snapshot model does not dedup by itself; you need delete files or MERGE INTO.
- **Evidence:** none needed beyond logic; reference integration test `tests/testutils/integration/2pc.go` "State Save Failure Sync".
- **Reference:** Apache Iceberg spec (snapshots, append-only, delete files); OLake 2PC integration test.

### Section 2 — Architecture
- **Explanation:** Driver reads WAL/binlog/oplog natively; normalizes to `_olake_id`, `_op_type`, `_olake_timestamp`; Arrow writer buffers per partition; commits to embedded Java Iceberg server; files land in object storage under the Iceberg table path; `_olake_2pc/` staging used for 2PC state.
- **Evidence:** `drivers/abstract/cdc.go` (`_olake_id` = `GetKeysHash`), `destination/iceberg/iceberg.go` `Setup()`.
- **Reference:** OLake README ("No Spark, no Flink, no Kafka, no Debezium").

### Section 3 — 2PC commit mechanism
- **Explanation:** Phase 1 = stage data/delete files to `_olake_2pc/` staging prefix; Phase 2 = `REGISTER_AND_COMMIT` registers file metadata and commits the Iceberg snapshot atomically. Upsert mode writes equality deletes (`_olake_id` only) and positional deletes (file:position). Duplicates handled with n-1 logic: latest occurrence kept, previous position deleted.
- **Evidence:** `arrow-writer/writer.go` `extract()` (lines 195–222), `Close()` (lines 394–456).
- **Reference:** `destination/parquet/parquet_2pc.go` (finish.json idempotent replay pattern).

### Section 4 — Snapshot-to-log handoff
- **Explanation:** The committed Iceberg `SnapshotId` is not discarded; it is written back into the Pebble `StreamIndex` via `TableIndex.Commit(batch, committedSnapshotID)`. The index therefore doubles as the durable recovery log: `_olake_id` → which snapshot/file:position holds the row. `olake_2pc_state` table property carries committed chunk IDs / cursor positions and is refreshed per retry.
- **Evidence:** `arrow-writer/writer.go` lines 447–453; `iceberg.go` lines 133–137; `writers.go` lines 218–219.
- **Reference:** Pebble KV (`pkg/indexdb/pebble.go`).

### Section 5 — Crash recovery
- **Explanation:** On failure mid-sync, restore the Pebble state file to the pre-failure checkpoint. The cursor re-reads the overlap window. Duplicate `_olake_id`s arriving in the re-read are collapsed: original row gets a delete file, only the net-new row is inserted. Verified by `insert_2pc` integration tests where only `insert_2pc` is visible after recovery (`expectedRowCountByOpType: 1`).
- **Evidence:** `tests/testutils/integration/2pc.go` `Iceberg2PCCDCRecovery` and `Iceberg2PCIncrementalRecovery`.
- **Reference:** Iceberg MERGE INTO semantics (delete + insert = upsert).

### Section 6 — Evidence
- **Table columns:** Tool | Throughput (CDC) | Full-load Postgres | Cost | External coordinator | Exactly-once to Iceberg | Iceberg-native
- **Numbers:** OLake Go Postgres CDC 55,555 RPS / MySQL CDC 59,951 RPS / Postgres full 580,113 RPS / 4.01B rows in 1.91 hrs for <$6 vs Fivetran $5,560 (~1270× slower). Sources: `olake.io/docs/benchmarks/ingestion/`.
- **Reference:** OLake benchmarks page; Fivetran white papers (collected, `Competitors/Fivetran/`) used only as comparative context, not reproduced.

### Section 7 — Operations
- Self-hosted in customer VPC (data residency); deploy via Docker Compose / standalone Docker / Helm (K8s) / Airflow; catalogs: Glue, Hive HMS, JDBC, REST (Nessie, Polaris, Unity, Lakekeeper, S3 Tables); queryable by Athena, Trino, Spark, Snowflake, Databricks, ClickHouse, etc.

---

## 5. Visual plan

| # | Diagram | Purpose | Placement | Complexity |
|---|---------|---------|-----------|------------|
| 1 | The exactly-once gap | CDC re-read → duplicate rows without idempotency | p3 (Sec 1) | Low |
| 2 | OLake Go data-flow architecture | Source → driver → Arrow writer → Iceberg server → object storage; "no external coordinator" badge | p4 (Sec 2) | Medium |
| 3 | 2PC commit workflow | Stage → equality/positional deletes → REGISTER_AND_COMMIT → Iceberg snapshot | p5 (Sec 3) | Medium |
| 4 | Equality vs positional delete | Equality delete = `_olake_id` list; positional = file:position; n-1 dedup example | p5 (Sec 3) | Low |
| 5 | Snapshot-to-log handoff | Committed SnapshotId → Pebble index entry `_olake_id` → file:position | p6 (Sec 4) | Medium |
| 6 | Crash-recovery timeline | Checkpoint → crash → restore → re-read overlap → dedup → net-new commit | p7 (Sec 5) | Medium |
| 7 | Before/after dedup | Duplicate rows collapsed to one via delete + insert | p7 (Sec 5) | Low |
| 8 | Benchmark comparison table | OLake Go vs Fivetran/Airbyte/Debezium/Estuary | p8 (Sec 6) | Low (table) |
| 9 | Deployment options | Docker / Helm / Airflow + catalog badges | p9 (Sec 7) | Low |

**Style:** Minimal, flat, OLake Blue (#193AE6) for primary elements, Ink (#202020) lines, Surface Alt (#FAFAFA) backgrounds, JetBrains Mono for any labels/code. No 3D, no gradients, no clip art.

---

## 6. Design specification (OLake branding)

### Color palette (per olake.io/branding/)
| Role | Hex | Use |
|------|-----|-----|
| OLake Blue | #193AE6 | Primary buttons, links, diagram accents, cover band |
| Blue Hover | #132DB3 | Active/pressed states |
| On Blue | #E7E7E0 | Text/icons on blue |
| Ink | #202020 | Headings |
| Body | #393939 | Body text |
| Secondary | #5D5D5D | Section/card copy |
| Muted | #767676 | Captions, footnotes |
| Border | #ECECEC | Card/table borders |
| Rule | #E7E7E7 | Section rules |
| White | #FFFFFF | Page background |
| Surface Alt | #FAFAFA | Code blocks, table headers, chips |
| Surface Muted | #F4F4F4 | Muted background areas |
| Dark | #171717 | Dark sections on light pages |

### Typography (self-hosted fonts, per branding)
- **Headings + body:** Geist (Regular 400, Medium 500, Semibold 600)
- **Code snippets:** JetBrains Mono (Regular 400, Medium 500, Bold 700)
- **Type scale:** Display 46px, H2 38px, H3 22px, Lead 20px, Body 15px, Body small 14px, Caption 12px

### Layout rules
- White (#FFFFFF) pages; subtle grey surfaces (#FAFAFA / #F4F4F4) for cards and code blocks.
- Cover: OLake Blue (#193AE6) band or full-bleed cover with OLake logo in On Blue (#E7E7E0).
- Section hierarchy: numbered H2 sections; H3 subsections; consistent spacing.
- Code blocks: JetBrains Mono on Surface Alt (#FAFAFA), bordered #ECECEC.
- KEY STAT callouts: OLake Blue background with On Blue text, or Ink text on Surface Alt with blue accent rule.
- Tables: white rows, Surface Alt header, Border (#ECECEC) rules.
- Figures: captioned below in Caption (12px, Muted #767676), Geist.
- Consistent margins, gutter, and figure/section spacing across all pages.

### Cover page
- OLake logo (blue with text, or mark) top-left or centered.
- Title in Geist Display/Semibold, Ink.
- Subtitle in Geist Lead, Body grey.
- Key stat strip (3 stats) optionally on cover or exec summary.
- "OLake Go · Apache 2.0 · Open source" + date.
- Footer: olake.io, GitHub link.

---

## 7. Review checklist (gate before drafting)

Before any prose is written, confirm:

- [ ] **Technical accuracy:** Every claim mapped to a source file/line (Section 2 table above). No unsupported "exactly-once" absolute claims; honest boundary stated.
- [ ] **No-external-coordinator:** Verified against source (Go process + embedded Java server, gRPC). No Kafka/Debezium/Flink dependency claimed.
- [ ] **Originality:** Positions OLake as first open-source, ungated, Iceberg-native exactly-once CDC paper; competitor papers are form-gated marketing (Fivetran) or academic (Spark). OLake's angle = verifiable source code + benchmarks.
- [ ] **Readability:** 8–10 pages, visuals carry weight, body ≤15px Geist, code JetBrains Mono, KEY STAT callouts break up text.
- [ ] **Page limit:** 10 pages max including cover and references; tight prose budget per section above.
- [ ] **Brand compliance:** OLake Blue #193AE6, Geist headings/body, JetBrains Mono code, white/grey surfaces, minimal style.
- [ ] **Sourcing:** References list includes OLake repo, OLake docs/benchmarks, Apache Iceberg spec, and collected competitor papers (for context only).

---

## 8. Next step

**This document is the plan only.** Upon review approval (technical accuracy, originality, readability, page limits), proceed to draft the full white paper (8–10 pages) following this outline and design spec.

**Plan location:** `/Users/gauri/Code/White Papers/OLake/Strategy/OLake_ExactlyOnce_Content_Design_Plan.md`
