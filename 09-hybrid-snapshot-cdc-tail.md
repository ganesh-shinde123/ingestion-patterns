# Pattern 09: Hybrid: Bulk Snapshot + CDC Tail

## 1. Pattern Overview
- **Pattern Name:** Hybrid: Bulk Snapshot + CDC Tail
- **Pattern ID:** 09
- **Objective:** Ingest or migrate massive operational tables (10TB+ to PB scale) into the target platform with zero source downtime, combining a parallelized historical snapshot from a Read Replica with continuous transaction log (WAL/Binlog) tailing.
- **Applicable Sources:** High-volume Relational Databases (PostgreSQL, Oracle, SQL Server, MySQL).
- **Applicable Targets:** Snowflake, Databricks, Data Lake (ADLS/S3).
- **Typical Use Cases:** Large-scale cloud migrations, modernization cutovers, initial loads for mission-critical production OLTP databases where maintenance windows are impossible.
- **When to Use:** When tables are too large for a standard batch window (Pattern 02), transaction volume is continuous, and the pipeline must transition seamlessly into ongoing CDC (Pattern 05/06) without data gaps or source locking.
- **When NOT to Use:** For small-to-medium tables (< 100GB) that fit comfortably within off-peak batch windows, or when source databases do not support point-in-time log sequence tracking (LSN/SCN).

## 2. Pre-Ingestion Profiling
Execute on the source database to establish baseline boundaries:
- **Write-Ahead Log (WAL) Generation Rate:** Measure daily WAL generation in GB/hour to calculate required storage buffers while the initial snapshot is in transit.
- **Log Sequence Coordinate Support:** Confirm the database exposes transaction log coordinates:
  - PostgreSQL: Log Sequence Number (`LSN`) via `pg_current_wal_lsn()`
  - Oracle: System Change Number (`SCN`) via `DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER`
  - MySQL: Binary Log File & Position via `SHOW MASTER STATUS`
  - SQL Server: Log Sequence Number (`LSN`) via `fn_dblog`
- **Replication Slot Disk Cap:** Verify `max_slot_wal_keep_size` (PostgreSQL) or equivalent to prevent runaway disk growth during long extractions.

## 3. Reference Architecture

```mermaid
flowchart TD
    subgraph Source Environment
        Primary[(Primary DB\nOLTP Writes)] -->|Async Physical Replication| Replica[(Read Replica)]
        Primary -->|WAL Output| LogBuffer[Transaction Log Buffer\nReplication Slot at LSN X]
    end
    
    subgraph Initial Bulk Phase
        Replica -->|1. Split Boundary Queries| Engine[Spark / ADF Parallel Extract]
        Engine -->|2. High-Throughput Parquet| Stage[(Landing / Staging Zone)]
        Stage -->|3. Initial Bulk Copy| Target[(Target Table\nDelta / Snowflake)]
    end
    
    subgraph CDC Tail Catch-Up
        LogBuffer -->|4. Stream Changes >= LSN X| CDC[CDC Engine\nDebezium / DMS / Qlik]
        CDC -->|5. Continuous Delta Events| Buffer[(Object Storage / Kafka)]
        Buffer -->|6. Idempotent MERGE (Catch-Up)| Target
    end
```

## 4. End-to-End Data Flow
1. **Coordination Point (LSN X):** Create a logical replication slot on the Primary database without starting extraction yet. Capture the exact Log Sequence Number/SCN (e.g., `LSN_START = 0/16B3748`).
2. **Buffer CDC Tail:** Start the CDC capture process immediately from `LSN_START`. Buffer the incoming change events into object storage or Kafka. Do **not** merge into the target yet.
3. **Partitioned Bulk Extract:** Launch a distributed extraction job (Spark or ADF) against the **Read Replica**. Split the table into parallel chunks using immutable numeric primary key or indexed range boundaries (e.g., `WHERE id >= 1 AND id < 1000000`).
4. **Target Initial Load:** Load the extracted snapshot data into the target table in append/bulk-load mode with indexing/clustering applied.
5. **Reconciliation & Cutover:** Release the buffered CDC stream. Apply changes with an idempotent `MERGE` using the source primary key and sequence timestamp/LSN.
6. **Steady-State Handover:** Once consumer lag reaches sub-minute thresholds, transition the pipeline orchestration into permanent Pattern 05 (CDC to Storage) or Pattern 06 (CDC to Kafka).

## 5. Ingestion Mode
- **Hybrid (Two-Phase):**
  - Phase A: Parallel partitioned bulk snapshot.
  - Phase B: Micro-batch or streaming log tailing starting from the snapshot boundary.

## 6. Insert / Update / Delete Handling
- **Snapshot Phase:** Contains a point-in-time capture of all committed rows at the time of read execution.
- **Tail Phase (`>= LSN_START`):**
  - **Inserts:** Inserted via `MERGE`. If a row was inserted after the snapshot started, it will be added.
  - **Updates:** Updated via `MERGE`. Overwrites stale snapshot rows with the latest image.
  - **Deletes:** Explicitly captured as delete events in the transaction log. Translated into a target `DELETE` or soft-delete flag via the `MERGE` statement.
- **Idempotent Overlap:** Rows captured in both the snapshot and the CDC tail are resolved deterministically because CDC records carry higher LSN coordinates.

## 7. Target-Side Optimization (Databricks / Snowflake)
- **Databricks:**
  - Initial Load: Write using `.format("delta").mode("overwrite")` with `liquid clustering` configured on join/filter keys.
  - Tail Processing: Use **Delta Live Tables (DLT)** with `APPLY CHANGES INTO` specifying `SEQUENCE BY lsn_column` to automatically resolve out-of-order records. Enable **Deletion Vectors** to prevent full Parquet rewrites during row-level updates/deletes.
- **Snowflake:**
  - Initial Load: Export to stage as compressed Parquet, execute `COPY INTO` using dedicated multi-cluster warehouses.
  - Tail Processing: Stage CDC files, use Snowflake `MERGE` matching on `target.id = source.id` with `WHEN MATCHED AND source.__op = 'd' THEN DELETE`.

## 8. AWS and Azure Considerations
- **AWS:**
  - Bulk Phase: Extract from Amazon RDS / Aurora Read Replica using AWS Glue or EMR running Spark with JDBC partitioning.
  - Tail Phase: AWS Database Migration Service (DMS) reading from the Aurora primary, landing CDC files in S3.
- **Azure:**
  - Bulk Phase: Extract from Azure Database for PostgreSQL (Read Replica) via Azure Data Factory (ADF) copy activity with physical partitioning.
  - Tail Phase: Stream changes via ADF CDC connector or Debezium on Azure Container Instances landing in ADLS Gen2.

## 9. Reliability, Resumability, and Failure Handling
- **LSN Synchronization Failure:** If the snapshot process fails halfway, the replication slot retains WAL logs on the primary. Resume the bulk extract from the last successful partition without recreating the replication slot.
- **Overlap Deduplication:** To guarantee idempotent replay, all target `MERGE` statements must incorporate the source sequence number:
  ```sql
  MERGE INTO target t
  USING source_cdc s
  ON t.id = s.id
  WHEN MATCHED AND s.__op = 'd' AND s.lsn >= t.last_lsn THEN DELETE
  WHEN MATCHED AND s.lsn >= t.last_lsn THEN UPDATE SET ...
  WHEN NOT MATCHED AND s.__op != 'd' THEN INSERT ...;
  ```

## 10. Constraints & Red Lines
- **Red Line 1 - Primary DB Extraction:** Never run the initial snapshot query against the primary production node. Saturating storage I/O and network bandwidth on the primary will degrade transactional response times.
- **Red Line 2 - WAL Disk Space Exhaustion:** If the initial bulk extract takes 72 hours, the primary database must retain 72 hours of WAL files for the active replication slot. If the disk fills to capacity, the database crashes. Set a hard alert threshold at 75% disk utilization.
- **Red Line 3 - Replica Lag Drift:** If the Read Replica lags significantly behind the Primary when the snapshot is taken, the required WAL retention window on the primary expands accordingly.

## 11. Trade-Offs

| Decision | Option A | Option B | Trade-Off |
|---|---|---|---|
| Initial Load Target | Direct JDBC Stream | Bulk Parquet Files -> Target Stage | Direct JDBC requires zero intermediate storage but risks network drops on multi-day loads. Parquet staging requires 1x extra storage footprint but provides deterministic checkpointing. |
| Cutover Mechanism | Automated Continuous Lag Catch-Up | Scheduled Offline Maintenance Window | Continuous catch-up requires zero downtime but complex LSN deduplication logic. An offline maintenance window simplifies logic but requires production downtime. |

## 12. Pattern Selection Checklist
- [ ] Read Replica confirmed and provisioned with sufficient I/O bandwidth.
- [ ] LSN/SCN tracking verified on source engine.
- [ ] Database storage headroom validated for retained transaction logs during snapshot duration.
- [ ] Primary key or indexed numeric column available for chunked parallel extraction.
- [ ] Target table configured with Deletion Vectors (Delta) or appropriate clustering keys.
- [ ] Idempotent `MERGE` logic verified using LSN/SCN sequence ordering.