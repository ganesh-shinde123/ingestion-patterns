# Pattern 02: Full Snapshot / Truncate-Reload

## 1\. Pattern Overview

- **Pattern Name:** Full Snapshot / Truncate-Reload  
- **Pattern ID:** 02  
- **Objective:** Extract the entirety of a source table and completely overwrite the target table.  
- **Applicable Sources:** Relational DBs (Azure SQL, PostgreSQL, Oracle, MySQL).  
- **Applicable Targets:** Snowflake, Databricks, Data Lake.  
- **Typical Use Cases:** Small lookup/dimension tables, legacy systems lacking `updated_at` timestamps, or systems where physical deletes are common but not logged.  
- **When to Use:** When the table is small (\< 5-10 GB) and data volume constraints allow for full extraction within the SLA.  
- **When NOT to Use:** On large transaction tables. On systems where the primary database cannot handle heavy, unindexed read IO.

## 2\. Pre-Ingestion Profiling

Execute on source DB to validate pattern applicability:

\-- Check total rows and physical size (PostgreSQL example)

SELECT count(\*) FROM target\_table;

SELECT pg\_size\_pretty(pg\_total\_relation\_size('target\_table'));

*If the result is \> 10 million rows or \> 10 GB, strongly consider Pattern 03 or 05\.*

## 3\. Reference Architecture

flowchart LR

    Source\[(Source DB\\nPrimary or Replica)\] \--\>|JDBC/ODBC Full Read| Ingest\[Ingestion Engine\\nSpark/ADF\]

    Ingest \--\>|Overwrite| Target\[(Target Table\\nLakehouse/Snowflake)\]

    

    subgraph Control

        Log\[(Audit DB)\]

    end

    Ingest \-.-\> Log

## 4\. End-to-End Data Flow

1. **Extraction:** Ingestion engine connects via JDBC and executes `SELECT * FROM table`.  
2. **Network Transit:** Data is pulled into memory/compute cluster.  
3. **Target Load:** The target table is truncated (or partition overwritten) and the new data is inserted.  
4. **Validation:** Source row count is compared against target row count.

## 5\. Decision Criteria

- **Data Volume:** Small (MBs to single-digit GBs).  
- **Data Freshness:** Daily or Hourly. Not suitable for near-real-time due to extraction overhead.  
- **History:** Assumes current state only (SCD 1).

## 6\. Insert / Update / Delete Handling

- **Inserts / Updates:** Inherently handled because the target state becomes an exact mirror of the source at the time of extraction.  
- **Deletes:** **Major advantage.** Hard deletes in the source are perfectly replicated to the target because missing rows simply aren't extracted.

## 7\. SCD Strategy

- **SCD 1:** Default behavior. The target reflects the latest state.  
- **SCD 2:** Can be achieved *downstream* by comparing the incoming full snapshot against the previous snapshot in the target to generate synthetic insert/update/delete records (Snapshot isolation/comparison).

## 8\. Target-Specific Considerations

- **Databricks:** Use Spark JDBC read. Write using `.mode("overwrite")` to a Delta table.  
- **Snowflake:** Avoid JDBC directly from Snowflake. Use an orchestrator (e.g., ADF) to dump to ADLS/S3, then use `COPY INTO` with `TRUNCATECOLUMNS` or `OVERWRITE=TRUE`.

## 9\. Constraints & Red Lines

- **Red Line 1 \- Primary DB Impact:** Full table scans consume DB CPU/Memory and can cause locking. Always query against a **Read Replica** if the table is moderately active.  
- **Red Line 2 \- Network Cost:** Moving the same unchanged 5 GB of data across the WAN every hour incurs high data egress costs.

## 10\. Trade-Offs

| Decision | Option A | Option B | Trade-Off |
| :---- | :---- | :---- | :---- |
| Target Overwrite Method | Physical Truncate/Drop | Atomic Table Swap | Truncate causes downtime for concurrent readers. Atomic swap (Delta/Snowflake cloning) ensures zero downtime for downstream consumers. |

## 11\. Pattern Selection Checklist

* Table volume profiled and confirmed small (\< 10GB).  
* Read Replica availability confirmed (if impacting Prod).  
* Overwrite concurrency impact assessed (Atomic swap required?).  
* Acceptable that historical row-level changes between snapshots are lost.