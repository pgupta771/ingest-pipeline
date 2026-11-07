# Resumable Data Ingestion Pipeline (`ingest-pipeline`)

![CI](../../actions/workflows/ci.yml/badge.svg)

> **Disclaimer:** This repository is a portfolio demonstration system built from scratch using public open-data schemas (Dutch RDW Socrata SODA API and European Central Bank FX Reference Rates XML) and deterministic local test fixtures. It contains zero proprietary employer code or confidential data.

A Java 21 / Spring Boot 3.3 data ingestion framework (`SourceConnector` SPI) that extracts from paginated HTTP APIs (Dutch RDW Socrata SODA) and streaming XML feeds (European Central Bank FX Reference Rates), buffers through Apache Kafka (`acks=all`, idempotent producer), and loads into PostgreSQL 16 with **transactional offset tracking**, **idempotent source-hash upserts**, **online schema-drift evolution**, **dead-letter routing**, and **resumable crash recovery** (verified across 10 consecutive fault-injection runs and OS-level `SIGKILL` recovery).

---

## 1. System Architecture & Execution Flows

### 1.1 End-to-End Pipeline Topology

```mermaid
flowchart LR
    subgraph Sources["Public Data Sources"]
        RDW["RDW Open Data API<br/>(Socrata Keyset Pagination)"]
        ECB["ECB FX Rates XML<br/>(StAX Streaming Parser)"]
    end

    subgraph Engine["Extraction Engine (engine)"]
        EXT["ExtractionOrchestrator<br/>(Resilience4j Retry + CircuitBreaker)"]
        CHK[("PostgreSQL<br/>extract_checkpoint")]
    end

    subgraph Transport["Apache Kafka (KRaft)"]
        RAW[["Topics: ingest.raw.<stream><br/>(acks=all, enable.idempotence=true)"]]
        DLQ_TOPIC[["Topic: ingest.dlq"]]
    end

    subgraph Sink["PostgreSQL Transactional Loader (sink-postgres)"]
        CONS["KafkaBatchSinkConsumer<br/>(ConsumerSeekAware, auto.commit=false)"]
        WRITER["PostgresTransactionalWriter<br/>(Single ACID Transaction)"]
        PG_DATA[("Stream Tables<br/>(rdw_vehicles, ecb_exchange_rates)<br/>ON CONFLICT (pk) DO UPDATE<br/>WHERE source_hash IS DISTINCT")]
        PG_OFF[("consumer_offsets<br/>(topic, partition, next_offset)")]
        PG_SCHEMA[("stream_schema &<br/>schema_events")]
        PG_DLQ[("dead_letters<br/>(unique on topic, partition, offset)")]
    end

    RDW --> EXT
    ECB --> EXT
    EXT -- "1. Publish page & await all broker acks" --> RAW
    EXT -- "2. Commit continuation cursor" --> CHK
    RAW -- "3. Poll batch / seek from DB" --> CONS
    CONS --> WRITER
    WRITER -- "4a. Schema drift DDL" --> PG_SCHEMA
    WRITER -- "4b. Idempotent batch upsert" --> PG_DATA
    WRITER -- "4c. Poison records" --> PG_DLQ
    WRITER -- "4d. Commit Kafka offsets" --> PG_OFF
    WRITER -. "4e. Publish DLQ after commit" .-> DLQ_TOPIC
```

### 1.2 End-to-End Extraction, Crash Recovery & Atomic Sink Sequence

```mermaid
sequenceDiagram
    autonumber
    participant API as Source (RDW JSON / ECB XML)
    participant EXT as ExtractionOrchestrator
    participant K as Kafka (ingest.raw.<stream>)
    participant C as KafkaBatchSinkConsumer
    participant PG as PostgreSQL 16 (Single ACID Tx)

    EXT->>PG: SELECT continuation_token FROM extract_checkpoint
    PG-->>EXT: Last committed cursor (or null on initial run)

    loop Keyset / StAX Page Extraction
        EXT->>API: Fetch page > last_cursor (Resilience4j Retry + CircuitBreaker)
        API-->>EXT: Page of SourceRecords
        EXT->>K: Produce records (acks=all, enable.idempotence=true)
        K-->>EXT: Broker acks for entire page
        Note over EXT,PG: Crash window tested in M3CrashRecoveryIT:<br/>If process dies before cursor commit, page is re-extracted<br/>and deduplicated downstream via source_hash.
        EXT->>PG: UPSERT extract_checkpoint (continuation_token)
    end

    C->>PG: On partition assignment -> SELECT next_offset FROM consumer_offsets
    C->>K: seek(topicPartition, next_offset)

    loop Kafka Batch Poll (enable.auto.commit=false)
        K-->>C: Poll List<ConsumerRecord>
        rect rgb(235, 255, 240)
            Note over C,PG: PostgresTransactionalWriter (One ACID Transaction)
            C->>PG: Evolve schema if needed (ADD COLUMN / widen INTEGER->BIGINT->NUMERIC)
            C->>PG: JDBC Batch UPSERT -> ON CONFLICT (pk) DO UPDATE WHERE source_hash IS DISTINCT
            C->>PG: INSERT poison records INTO dead_letters ON CONFLICT (topic, partition, offset) DO NOTHING
            C->>PG: UPSERT consumer_offsets (topic, partition, last_offset + 1)
            C->>PG: COMMIT
        end
    end
```

### 1.3 Checkpoint Recovery & Online Schema-Drift State Machines

```mermaid
stateDiagram-v2
    direction LR
    state "Extractor & Consumer Recovery" as Rec {
        [*] --> EXTRACTING: Load cursor from extract_checkpoint
        EXTRACTING --> KAFKA_ACKED: Page published (acks=all)
        KAFKA_ACKED --> CURSOR_COMMITTED: extract_checkpoint updated
        KAFKA_ACKED --> CRASH_RESTART: SIGKILL / fault before cursor commit
        CURSOR_COMMITTED --> EXTRACTING: Next keyset page
        CRASH_RESTART --> EXTRACTING: Re-fetch from previous cursor (idempotent upsert absorbs replay)
        CURSOR_COMMITTED --> COMPLETED: hasMore == false
    }

    state "Online Schema Drift & DLQ Routing" as Schema {
        [*] --> INSPECT_RECORD: Compare incoming JSON vs stream_schema
        INSPECT_RECORD --> APPLY_UPSERT: Columns & types match
        INSPECT_RECORD --> DDL_ADD_COLUMN: New field detected (ALTER TABLE ADD COLUMN)
        INSPECT_RECORD --> DDL_WIDEN_TYPE: Safe widening (INTEGER -> BIGINT -> NUMERIC)
        INSPECT_RECORD --> ROUTE_TO_DLQ: Corrupt JSON or incompatible type (e.g. text in numeric column)
        DDL_ADD_COLUMN --> APPLY_UPSERT: Update stream_schema & schema_events
        DDL_WIDEN_TYPE --> APPLY_UPSERT: Update stream_schema & schema_events
        ROUTE_TO_DLQ --> COMMIT_BATCH: Persist in dead_letters + publish to ingest.dlq
    }
```

---

## 2. Repository Module Layout

| Module / Path | Responsibility | Key Classes / Files |
| :--- | :--- | :--- |
| **`core`** | Pure Java 21 SPI and domain records for connectors, checkpoints, canonical SHA-256 record hashing, and schema evolution actions. | `SourceConnector`, `SourceRecord`, `Checkpoint`, `CanonicalRecordHasher`, `SchemaField`, `SchemaEvolutionAction` |
| **`connector-rdw`** | Dutch RDW Socrata SODA JSON connector using deterministic keyset pagination (`WHERE kenteken > :cursor ORDER BY kenteken ASC`). | `RdwSocrataConnector`, `RdwVehicleRecordMapper` |
| **`connector-ecb-xml`** | Streaming StAX (`XMLStreamReader`) connector for European Central Bank FX Reference Rates processing 155 MB+ XML files in constant `O(1)` heap. | `EcbXmlConnector`, `EcbRateStaxParser` |
| **`engine`** | Extraction orchestrator with Resilience4j retry/circuit-breaker decorators, Kafka `acks=all` producer, and `extract_checkpoint` state persistence. | `ExtractionOrchestrator`, `ExtractCheckpointRepository`, `FaultInjectingCheckpointHook` |
| **`sink-postgres`** | Seek-aware Kafka batch consumer, online DDL schema evolution engine, dead-letter router, and single-transaction PostgreSQL JDBC batch writer. | `KafkaBatchSinkConsumer`, `PostgresTransactionalWriter`, `SchemaEvolutionService`, `DeadLetterPublisher` |
| **`app`** | Spring Boot 3.3 runtime, REST control plane, Flyway migrations, Prometheus metrics, and M1–M6 Testcontainers verification suites. | `IngestApplication`, `SyncController`, `V1__init_schema.sql`, `M2TransportAndLoaderIT`, `M3CrashRecoveryIT`, `M4SchemaDriftAndDlqIT`, `M5EcbEndToEndIT`, `M6BenchmarkIT` |

---

## 3. Crash-Recovery & Reliability Proof

| Verification Gate | Test / Script | Verified Behavior |
| :--- | :--- | :--- |
| **Idempotent 200k Replay (M2)** | `M2TransportAndLoaderIT` | Loading 200,000 records twice (`400,000` Kafka messages) produces `200,000` rows, identical MD5 dataset checksums, and `0` redundant row updates (`MAX(loaded_at)` unchanged). |
| **Mid-Sync Crash Recovery (M3)** | `M3CrashRecoveryIT` & `scripts/kill-and-resume.sh` | **0 lost rows and an identical MD5 content checksum across 10 consecutive runs** (2 injected crashes per run between Kafka ack and checkpoint commit), with replayed pages absorbed idempotently (`WHERE source_hash IS DISTINCT`). Also includes `scripts/kill-and-resume.sh` for OS-level `SIGKILL` testing. |
| **Online Schema Drift & DLQ (M4)** | `M4SchemaDriftAndDlqIT` | Executes `ADD COLUMN` and `INTEGER -> BIGINT -> NUMERIC` widening (with cache rollback eviction) while routing incompatible types and corrupt JSON to `dead_letters` (deduplicated by `(topic, partition, offset)`) and `ingest.dlq`. |
| **Constant-Heap XML & E2E Load (M5)** | `EcbXmlConnectorTest` & `M5EcbEndToEndIT` | Streams a **155 MB** (`3,200,000` records) synthetic ECB XML file inside a **`-Xmx64m`** JVM without `OutOfMemoryError`, and loads `ecb_exchange_rates` end-to-end through Kafka into PostgreSQL. |

---

## 4. REST Control Plane & Observability

### 4.1 Endpoints & Prometheus Metrics (`/actuator/prometheus`)

| Signal / Endpoint | Type | Description |
| :--- | :--- | :--- |
| `POST /api/v1/sync/{stream}` | REST Endpoint | Trigger an extraction run for a registered stream (`rdw_vehicles`, `ecb_exchange_rates`) |
| `GET /api/v1/sync/runs` | REST Endpoint | Inspect sync run status, extracted/loaded/dead-letter row counts, and checkpoints |
| `ingest_extracted_records_total` | Counter | Total records extracted per stream from upstream HTTP/XML sources |
| `ingest_loaded_records_total` | Counter | Records committed into PostgreSQL target tables by `PostgresTransactionalWriter` |
| `ingest_dead_letter_records_total` | Counter | Poison pills and incompatible schema records routed to `dead_letters` |
| `ingest_schema_drift_events_total` | Counter | Online `ADD COLUMN` and type-widening DDL executions |
| `ingest_batch_write_duration_seconds` | Timer | End-to-end latency of single-transaction PostgreSQL batch upsert + offset commit |

---

## 5. Quickstart

```bash
# 1. Start PostgreSQL 16, Apache Kafka (KRaft), Prometheus, and Grafana
docker compose -f deploy/docker-compose.yml up -d

# 2. Run the full unit + Testcontainers integration verification suite
mvn clean verify

# 3. Run the M3 kill-and-resume OS process crash-recovery script (10 consecutive SIGKILL runs)
./scripts/kill-and-resume.sh

# 4. Run the M6 benchmark script to reproduce all throughput, batch-size, and heap numbers
./scripts/benchmark.sh
```

---

## 6. Measured Benchmarks

Reproduced via `./scripts/benchmark.sh` on:
- **CPU:** `Intel(R) Core(TM) i3-1005G1 CPU @ 1.20GHz` (4 vCPUs)
- **RAM:** `4.2Gi` (WSL2 Ubuntu 24.04)
- **JDK:** `openjdk version 21.0.12.1 2026-08-18`

### 6.1 End-to-End Pipeline, XML Streaming & Crash Recovery

| Benchmark | Dataset | Heap Limit | Measured Result |
| :--- | :--- | :--- | :--- |
| **RDW JSON End-to-End (`Extract -> Kafka -> PostgreSQL`)** | `20,000` records (`pageSize=2500`) | `-Xmx512m` | **`3788` records/sec** |
| **ECB XML End-to-End (`Extract -> Kafka -> PostgreSQL`)** | `20,000` records (`pageSize=2500`) | `-Xmx512m` | **`5197` records/sec** |
| **ECB XML StAX Parser Extraction (`connector-ecb-xml` only)** | `155 MB` (`3,200,000` records) | `-Xmx64m` | **`560028` records/sec**, Peak Sampled Heap **`27` MB** |
| **Mid-Sync Crash-and-Resume Convergence (`M3`)** | `10,000` records (crash after page 3) | `-Xmx512m` | **`968` ms** |

### 6.2 Effect of Batch Size on PostgreSQL Transactional Upsert Throughput

| Batch Size (`records/tx`) | Dataset | Measured Upsert Throughput |
| :--- | :--- | :--- |
| **`100`** | `10,000` records | **`7012` records/sec** |
| **`500`** | `10,000` records | **`14164` records/sec** |
| **`1000`** | `10,000` records | **`15337` records/sec** |
| **`5000`** | `10,000` records | **`18450` records/sec** |

### 6.3 Benchmark & Operational Notes
- Figures above are single-run measurements on the stated WSL2 laptop environment using the local deterministic HTTP fixture (`RdwFixtureServer`) rather than the live internet RDW endpoint.
- The batch-size benchmark invokes `PostgresTransactionalWriter` directly to isolate PostgreSQL JDBC batch upsert scaling from Kafka poll latency.
- `dead_letters` in PostgreSQL is the transactional source of truth for rejected records; publishing to `ingest.dlq` is best-effort after transaction commit.
- `sync_runs.records_loaded` and `records_dead` are approximate per-run counters attributed to the latest run for that stream.

---

## 7. Architecture Decision Records (ADRs)

- [**ADR-0001: Transactional Offsets in PostgreSQL**](docs/adr/0001-transactional-offsets-in-postgresql.md)
- [**ADR-0002: Keyset Pagination vs. Offset Pagination**](docs/adr/0002-keyset-vs-offset-pagination.md)
- [**ADR-0003: Idempotent Upsert with Canonical Source Hash**](docs/adr/0003-idempotent-upsert-with-source-hash.md)
- [**ADR-0004: Effectively-Once Results Over At-Least-Once Transport**](docs/adr/0004-at-least-once-to-effectively-once.md)
- [**ADR-0005: JDBC Batch Writes Over JPA on the Hot Path**](docs/adr/0005-jdbc-batch-over-jpa.md)

---

## 8. Future Scope: Architecture at 100x Scale

1. **PostgreSQL `COPY` Staging & Table Partitioning:** At 100M+ rows per stream, `INSERT ... ON CONFLICT` index maintenance becomes I/O bound. Staging batches into an unlogged temporary table via PostgreSQL `COPY` and merging into hash/range-partitioned target tables reduces B-Tree index thrashing.
2. **Binary Wire Format (Protobuf / Avro):** Replace JSON-encoded `SourceRecord` envelopes on Kafka topics with Schema Registry-backed binary payloads to reduce broker network and disk usage.
3. **Partitioned Keyset Ranges Across Worker Instances:** Split keyset ranges across multiple extractor workers coordinated via PostgreSQL advisory locks (`pg_try_advisory_lock`).
