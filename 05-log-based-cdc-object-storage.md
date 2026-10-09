# Pattern 05: Log-Based CDC \-\> Object Storage

## 1\. Pattern Overview

- **Pattern Name:** Log-Based CDC to Object Storage  
- **Pattern ID:** 05  
- **Objective:** Replicate transactional database changes by reading the database's write-ahead log (WAL) or binlog, and writing the CDC events directly to Cloud Object Storage (S3/ADLS) without a Kafka middle-tier.  
- **Applicable Sources:** Relational Databases (PostgreSQL, MySQL, SQL Server, Oracle).  
- **Applicable Targets:** Snowflake, Databricks, Data Lake.  
- **Typical Use Cases:** Target state for high-volume enterprise databases where exact history (including hard deletes) is required, but sub-second streaming latency is not necessary.  
- **When to Use:** When you need a scalable, cost-effective CDC solution that leverages the cheap storage of S3/ADLS and batch/micro-batch ingestion engines (Auto Loader, Snowpipe).  
- **When NOT to Use:** When you have an enterprise requirement for sub-second, event-driven streaming to multiple subscriber applications (use Pattern 06 instead).

## 2\. Pre-Ingestion Profiling

- **Database Engine Constraints:** For PostgreSQL, ensure `wal_level = logical`. Monitor the replication slot size to ensure the WAL does not consume all primary database disk space if the CDC tool goes offline.  
- **Volume & Velocity:** How many GBs of WAL are generated per hour? This dictates the batch frequency of the CDC tool writing to S3/ADLS to avoid the small-file problem.

## 3\. Reference Architecture

flowchart LR

    subgraph Source Environment

        DB\[(Primary DB)\]

        WAL\[WAL / Binlog\]

        DB \-.-\> WAL

    end

    

    CDC\[CDC Tool\\nDMS, Fivetran, Qlik\]

    WAL \--\>|Logical Replication| CDC

    CDC \--\>|Write CDC Files\\nJSON/Parquet| Landing\[S3 / ADLS\]

    

    Landing \--\>|Auto Loader / Snowpipe| Engine\[Ingestion Engine\]

    Engine \--\>|MERGE| Target\[(Target Lakehouse/DW)\]

## 4\. End-to-End Data Flow

1. **Transaction Commit:** A transaction commits to the primary DB and is written to the WAL.  
2. **Replication:** A managed CDC tool (AWS DMS, Fivetran, Qlik Replicate) continuously reads the WAL asynchronously.  
3. **Landing:** The CDC tool writes micro-batches of change events to S3/ADLS in Parquet or JSON format. These files contain the data payload and metadata (operation type `I/U/D`, LSN/SCN timestamp).  
4. **Ingestion:** Databricks Auto Loader or Snowflake Snowpipe detects the new files and ingests them into a Raw/Bronze table.  
5. **Target MERGE:** A processing job reads the raw CDC events, deduplicates them, and `MERGE`s them into the final Silver/Target table.

## 5\. Insert / Update / Delete Handling

- **Inserts:** Replicated with an operation flag (e.g., `Op = I`).  
- **Updates:** Replicated with an operation flag (e.g., `Op = U`). The CDC tool can often be configured to capture both the `before` and `after` image of the row.  
- **Deletes:** Replicated with an operation flag (e.g., `Op = D`). The downstream `MERGE` statement translates this into a physical delete (or a soft delete via an `is_deleted` flag on the target table).

## 6\. Initial Load vs. Incremental Tail

- **The Cutover (Crucial Step):** The CDC tool must first execute a full snapshot of the primary database (or a Read Replica) *while simultaneously* buffering incoming WAL changes. Once the full load completes, it flushes the buffered WAL events to catch up to the current state.  
- **Recommendation:** Offload the initial full load to a Read Replica to prevent locking or CPU starvation on the primary DB.

## 7\. Format Selection & Target Optimization

- **File Format:** Configure the CDC tool to write **Parquet** files to S3/ADLS. JSON causes massive storage and compute overhead for large tables.  
- **Target Engine (Databricks):** Use **Auto Loader** combined with Delta Live Tables (DLT) `APPLY CHANGES INTO`. DLT natively handles out-of-sequence CDC events, deduplication, and SCD Type 1/2 processing without requiring complex manual `MERGE` logic.  
- **Target Engine (Snowflake):** Use Snowpipe to land data in a staging table, then use **Dynamic Tables** or Snowflake Streams & Tasks to process the CDC events into the final table.

## 8\. Constraints & Red Lines

- **Red Line 1 \- Primary DB Disk Exhaustion:** If the CDC tool crashes, the database cannot truncate the WAL. If left unchecked, the DB disk will fill up and crash the primary production database. Strict monitoring on replication slot size is mandatory.  
- **Red Line 2 \- The Small File Problem:** If the CDC tool flushes to S3/ADLS every 5 seconds, it will create thousands of tiny 10KB files. This will destroy Data Lake performance. Set the flush interval to optimize file sizes (e.g., 60 seconds or 64MB).

## 9\. Trade-Offs

| Decision | Option A (Pattern 05\) | Option B (Pattern 06\) | Trade-Off |
| :---- | :---- | :---- | :---- |
| Middleware | Direct to S3/ADLS | Through Kafka | S3/ADLS is vastly cheaper and simpler to operate. Kafka provides lower latency and allows multiple real-time consumers to read the same event stream. |
| Implementation | Qlik / Fivetran / DMS | Debezium \-\> Kafka | Managed tools are expensive but provide out-of-the-box initial load synchronization. Debezium requires heavy engineering but is open source and highly customizable. |

