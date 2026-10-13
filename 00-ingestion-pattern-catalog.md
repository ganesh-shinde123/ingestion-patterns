# Ingestion Pattern Catalog

## Overview

This catalog defines the minimum set of common enterprise data ingestion patterns needed to cover most real-world scenarios across Azure, AWS, Snowflake, and Databricks. Rather than creating duplicate patterns for every technology permutation (e.g., S3 to Snowflake vs. ADLS to Databricks), we group patterns by their core architectural mechanism and ingestion mode.

Each pattern document follows a standardized enterprise structure containing: Pattern Overview, Pre-Ingestion Profiling, Reference Architecture (Mermaid), End-to-End Data Flow, Format Selection, Target-Side Implementation (code snippets for both Databricks and Snowflake), AWS & Azure Reference, Security & Governance (RBAC model), Reliability & Failure Recovery (matrix), Observability & Monitoring (mandatory alerts), Cost Model, Constraints & Red Lines, Trade-Offs, and a Pattern Selection Checklist for ARB submission.

## Pattern Taxonomy

| Pattern ID | Pattern Name | Core Mechanism | Version | Sections | Primary Use Case & Trade-off |
|:---:|:---|:---|:---:|:---:|:---|
| **01** | [Batch File / Object Drop](01-batch-file-object-drop.md) | File-based triggers / scheduled ELT | 2.0 | 16 | Standard bulk file ingestion. Addresses the small-file problem and schema evolution on object storage. |
| **02** | [Full Snapshot / Truncate-Reload](02-full-snapshot-truncate-reload.md) | Bulk export (JDBC/ODBC/Native) → Target | 2.0 | 16 | Small dimension tables or legacy systems with no reliable `updated_at` timestamps or CDC capabilities. |
| **03** | [Incremental Batch (Watermark)](03-incremental-batch-watermark.md) | SQL query filtering by `updated_at` / `ID` | 2.0 | 16 | Medium-scale operational tables where real-time is not needed, but full snapshots are too expensive. *Fails to capture hard deletes.* |
| **04** | [Trigger-Based Change Table](04-trigger-based-change-table.md) | DB Triggers write to a shadow change table | 2.0 | 16 | Legacy systems where log-based CDC is blocked by DBAs, but exact row history (and deletes) are required. |
| **05** | [Log-Based CDC → Object Storage](05-log-based-cdc-object-storage.md) | WAL/Binlog → ADLS/S3 → Auto Loader/Snowpipe | 2.0 | 16 | **Target-state for high-scale DBs.** Bypasses Kafka for cost/simplicity. Excellent for Auto Loader or Snowpipe. 5–10× cheaper than P06. |
| **06** | [Log-Based CDC → Kafka → Target](06-log-based-cdc-kafka.md) | WAL/Binlog → Kafka → Spark Structured Streaming | 2.0 | 16 | Enterprise nervous system. Use when multiple downstream subscribers need the same CDC events, or sub-second latency is required. |
| **07** | [Transactional Outbox](07-transactional-outbox.md) | Application writes event & state in same DB tx | 2.0 | 16 | Microservices architectures. Avoids dual-write bugs. The app controls the semantic business event, decoupled from DB schemas. |
| **08** | [Lakehouse Federation (Zero-Copy)](08-lakehouse-federation.md) | Databricks Federation / Snowflake External Tables | 2.0 | 16 | Leave data in the source. Query remotely. Use for low-volume ad-hoc queries, regulatory boundaries, or prototyping. Break-even at ~50 queries/day vs. physical replication. |
| **09** | [Hybrid: Bulk Snapshot + CDC Tail](09-hybrid-snapshot-cdc-tail.md) | Read Replica Bulk Export + WAL integration | 2.0 | 16 | **The heavy-lifter.** Moving massive multi-TB tables without downtime. Requires complex LSN coordination between snapshot and CDC. |
| **10** | [Event Streaming (Native Kafka)](10-event-streaming-native-kafka.md) | App telemetry → Kafka → Target | 2.0 | 17 | High-velocity, append-only log data (IoT, clickstream). Focuses on backpressure, partitioning, schema registry, and delivery guarantees. |

## Supporting Documents

| Document | Purpose |
|:---|:---|
| [Decision Matrix](99-ingestion-pattern-decision-matrix.md) | Primary routing guide: map business requirements to the correct pattern. Includes decision tree flowchart. |
| [README](README.md) | Framework overview, architecture principles, and ARB submission process. |

## How to Select a Pattern

1. **Start here** — Review the Pattern Taxonomy table above to understand what's available.
2. **Use the Decision Matrix** — Consult [99-ingestion-pattern-decision-matrix.md](99-ingestion-pattern-decision-matrix.md) to filter patterns based on latency, volume, update frequency, and delete handling.
3. **Review the Pattern** — Open the specific pattern document for reference architectures, code snippets, and implementation guidelines.
4. **Complete the Checklist** — Each pattern ends with a Pattern Selection Checklist that must be submitted to the Architecture Review Board (ARB) before pipeline provisioning.
