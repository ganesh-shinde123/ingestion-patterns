# Ingestion Pattern Catalog

## Overview

This catalog defines the minimum set of common enterprise data ingestion patterns needed to cover most real-world scenarios across Azure, AWS, Snowflake, and Databricks. Rather than creating duplicate patterns for every technology permutation (e.g., S3 to Snowflake vs. ADLS to Databricks), we group patterns by their core architectural mechanism and ingestion mode.

## Pattern Taxonomy

| Pattern ID | Pattern Name | Core Mechanism | Primary Use Case & Trade-off |
| :---- | :---- | :---- | :---- |
| **01** | **Batch File / Object Drop** | File-based triggers / scheduled ELT | Standard bulk file ingestion. Addresses the small-file problem and schema evolution on object storage. |
| **02** | **Full Snapshot / Truncate-Reload** | Bulk export (JDBC/ODBC/Native) \-\> Target | Small dimension tables or legacy systems with no reliable `updated_at` timestamps or CDC capabilities. |
| **03** | **Incremental Batch (Watermark)** | SQL query filtering by `updated_at` / `ID` | Medium-scale operational tables where real-time is not needed, but full snapshots are too expensive. *Fails to capture hard deletes.* |
| **04** | **Trigger-Based Change Table** | DB Triggers write to a shadow change table | Legacy systems where log-based CDC is blocked by DBAs, but exact row history (and deletes) are required. |
| **05** | **Log-Based CDC \-\> Object Storage** | WAL/Binlog \-\> ADLS/S3 \-\> Auto Loader/Snowpipe | **Target State for high-scale DBs.** Bypasses Kafka for cost/simplicity. Excellent for Databricks Auto Loader or Snowflake Cloud Storage integrations. |
| **06** | **Log-Based CDC \-\> Kafka \-\> Target** | WAL/Binlog \-\> Kafka \-\> Spark Structured Streaming | Enterprise nervous system. Use when multiple downstream subscribers need the same CDC events, or sub-second latency is required. |
| **07** | **Transactional Outbox** | Application writes event & state in same DB tx | Microservices architectures. Avoids dual-write bugs. The app controls the semantic business event, decoupled from DB schemas. |
| **08** | **Lakehouse Federation (Zero-Copy)** | Databricks Federation / Snowflake External Tables | **IP8.** Leave data in the source. Query remotely. Use for low-volume ad-hoc queries, regulatory boundaries, or prototyping. |
| **09** | **Hybrid: Bulk Snapshot \+ CDC Tail** | Read Replica Bulk Export \+ WAL integration | **The heavy-lifter.** Moving massive multi-TB tables without downtime. Requires complex watermark reconciliation between the snapshot end and CDC start. |
| **10** | **Event Streaming (Native Kafka)** | App telemetry \-\> Kafka \-\> Target | High-velocity, append-only log data (IoT, clickstream). Focuses on backpressure, partitioning, and exact-once semantics. |

