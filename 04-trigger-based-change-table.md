# Pattern 04: Trigger-Based Change Table

## 1\. Pattern Overview

- **Pattern Name:** Trigger-Based Change Table  
- **Pattern ID:** 04  
- **Objective:** Capture row-level inserts, updates, and hard deletes using database triggers that write to a shadow "change table" within the same source database.  
- **Applicable Sources:** Legacy Relational DBs (Oracle, SQL Server, older PostgreSQL) where Log-Based CDC is blocked by policy or unsupported.  
- **Applicable Targets:** Snowflake, Databricks, Data Lake.  
- **Typical Use Cases:** Legacy applications where exact history and hard deletes must be tracked, but DBAs will not allow tools like Debezium or Qlik to read the transaction logs (WAL/Binlogs).  
- **When to Use:** As a last resort for CDC when native log-based replication is impossible, but the business requires hard delete capture and historical state tracking.  
- **When NOT to Use:** On high-throughput transactional systems. The double-write penalty of triggers will severely degrade primary database performance.

## 2\. Pre-Ingestion Profiling

Before implementing, evaluate the source DB performance overhead:

- **Transaction Volume:** How many TPS (Transactions Per Second) does the source table experience?  
- **Write Amplification:** Every `INSERT`/`UPDATE`/`DELETE` on the primary table forces a secondary `INSERT` into the change table. Can the storage IOPS handle a 2x write multiplier?

## 3\. Reference Architecture

flowchart LR

    subgraph Source Database

        Primary\[(Primary Table)\]

        Trigger{DB Trigger}

        Shadow\[(Change / Audit Table)\]

        Primary \--\>|I, U, D| Trigger

        Trigger \--\>|Insert Event| Shadow

    end

    

    subgraph Data Platform

        Ingest\[Batch/Micro-Batch Pull\]

        Target\[(Target Lakehouse)\]

    end

    

    Shadow \--\>|SELECT \> Watermark| Ingest

    Ingest \--\>|MERGE| Target

## 4\. End-to-End Data Flow

1. **Source Event:** Application modifies a row in the Primary Table.  
2. **Trigger Execution:** A database trigger automatically fires and inserts a record into the Change Table containing the payload, timestamp, and an operation flag (`I`, `U`, `D`).  
3. **Extraction:** An ingestion tool (ADF, Spark) queries the Change Table periodically (e.g., hourly) using a watermark on the trigger timestamp.  
4. **Target Load:** The ingestion tool applies the ordered changes to the Target Table using a `MERGE` statement.  
5. **Pruning:** A DB maintenance job periodically deletes old, successfully extracted records from the Change Table to prevent DB bloat.

## 5\. Insert / Update / Delete Handling

- **Inserts:** Trigger records `operation_type = 'I'`.  
- **Updates:** Trigger records `operation_type = 'U'` (often logging both before and after states).  
- **Deletes:** Trigger records `operation_type = 'D'`. **This successfully solves the hard-delete problem of Pattern 03\.**

## 6\. Target-Side Optimization (Databricks / Snowflake)

- Apply changes in strict chronological order using the trigger timestamp.  
- **Databricks:** Window functions (`row_number() over (partition by id order by timestamp desc)`) must be used on the extracted micro-batch to deduplicate and find the *latest* state of a row before executing the Delta `MERGE`.  
- **Snowflake:** Similar `QUALIFY` row-number deduplication is required before applying the `MERGE` statement.

## 7\. Reliability and Failure Handling

- **Idempotency:** The extraction from the Change Table must be idempotent. If a downstream pipeline fails, the watermark is not advanced, and the same change events are pulled again.  
- **Failure Isolation:** If the extraction tool fails, the primary application is unaffected, though the Change Table will grow until extraction resumes.

## 8\. Constraints & Red Lines

- **Red Line 1 \- DB CPU/IOPS:** Triggers execute synchronously with the primary transaction. If the trigger is slow, the application slows down. If the database is already running at \>70% CPU, this pattern is prohibited.  
- **Red Line 2 \- Schema Evolution:** If a column is added to the Primary Table, the Trigger and the Change Table must be manually updated by a DBA. Schema drift will break this pipeline.

## 9\. Trade-Offs

| Decision | Option A | Option B | Trade-Off |
| :---- | :---- | :---- | :---- |
| CDC Mechanism | Trigger-Based (P4) | Log-Based (P5/P6) | Triggers incur massive DB performance penalties but require zero external replication tools. Log-based is highly performant but requires specialized tools and DBA privileges. |
| Payload Storage | Full Row State | Primary Key Only | Storing the full row in the change table uses more storage but prevents race conditions. Storing just the PK requires joining back to the primary table during extraction, risking missing data if the row was deleted. |

