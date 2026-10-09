# Pattern 03: Incremental Batch (Watermark)

## 1\. Pattern Overview

- **Pattern Name:** Incremental Batch (Watermark)  
- **Pattern ID:** 03  
- **Objective:** Extract only new or updated records from a source database using a high-watermark column (usually `updated_at` or a sequential `ID`).  
- **Applicable Sources:** Relational Databases (SQL Server, PostgreSQL, MySQL, Oracle).  
- **Applicable Targets:** Snowflake, Databricks, Data Lake.  
- **Typical Use Cases:** Medium to large operational tables (10GB \- 1TB+) where the application reliably updates a timestamp column on every change.  
- **When to Use:** When full snapshots are too expensive, log-based CDC (Pattern 05/06) is not available/approved, and SLA is batch/micro-batch (hourly to daily).  
- **When NOT to Use:** When the source hard-deletes records, or when the `updated_at` column is not reliably updated by the application.

## 2\. Pre-Ingestion Profiling

Validate the reliability of the watermark:

\-- Is the watermark column indexed? If not, the extraction query will cause a full table scan anyway.

EXPLAIN SELECT count(\*) FROM target\_table WHERE updated\_at \> '2023-01-01';

\-- Are there records updated without the timestamp changing? (App logic bug check)

\-- Are deletes happening? If yes, this pattern WILL MISS THEM.

## 3\. Reference Architecture

flowchart LR

    subgraph Source Environment

        SourceDB\[(Source DB)\]

    end

    

    subgraph Ingestion

        Meta\[(Control DB)\]

        Engine\[Spark / ADF\]

    end

    

    Meta \--\>|1. Get Last Watermark| Engine

    Engine \--\>|2. SELECT WHERE update \> WM| SourceDB

    SourceDB \--\>|3. Delta Rows| Engine

    Engine \--\>|4. MERGE (Upsert)| Target\[(Target Lakehouse/DW)\]

    Engine \--\>|5. Update Watermark| Meta

## 4\. End-to-End Data Flow

1. **Metadata Lookup:** Engine reads the last successful watermark (e.g., `2023-10-01 12:00:00`) from a Control Table.  
2. **Extraction:** Executes query: `SELECT * FROM table WHERE updated_at > '2023-10-01 12:00:00' AND updated_at <= '2023-10-01 13:00:00'`. (Using an upper bound prevents late-arriving data issues during the run).  
3. **Landing:** Delta records land in raw storage.  
4. **Target MERGE:** Data is merged into the target table using the Primary Key.  
5. **Checkpoint:** Control table is updated with the new watermark.

## 5\. Insert / Update / Delete Handling

- **Inserts:** Picked up by the watermark.  
- **Updates:** Picked up by the watermark. Processed via `MERGE`.  
- **Deletes:** **Cannot detect hard deletes.** If source records are hard-deleted, the target data warehouse will silently retain the orphaned records.  
  - *Mitigation:* Source must use Soft Deletes (e.g., `is_deleted = true`, which updates the `updated_at` column).

## 6\. Initial vs Incremental Load Strategy

- **Initial Load:** The watermark is initialized at `1900-01-01`. The first run behaves as a Full Snapshot. For large tables, this initial query must be partitioned manually by Year/Month to avoid memory limits and DB timeouts.  
- **Steady State:** Subsequent runs use the high-watermark from the previous run.

## 7\. Target-Side Optimization (Databricks / Snowflake)

- **Databricks:** Use `MERGE INTO target USING source ON target.id = source.id`. If the table is highly churned, enable **Deletion Vectors** to prevent rewriting entire Parquet files during updates, and use **Liquid Clustering** on the `id` column.  
- **Snowflake:** Land data in an external stage. Use `MERGE`. If dealing with massive concurrency, consider loading into a temporary table first, then executing a single massive `MERGE`.

## 8\. Reliability, Resumability, and Failure Handling

- **Idempotency:** The pipeline must be retryable. If step 4 (MERGE) fails, step 5 (Watermark Update) does not occur. The next run will extract the same data. Because we use `MERGE` and not `INSERT`, duplicates are naturally avoided.  
- **Late Arriving Data (The Clock Skew Problem):** If a source transaction commits late, its `updated_at` might be *older* than the current DB time. To prevent missing records, overlap the watermark (e.g., `WHERE updated_at > (last_watermark - 5 minutes)`).

## 9\. Constraints & Red Lines

- **Red Line 1 \- Hard Deletes:** If the business requires exact reconciliation and the source hard-deletes data, this pattern is structurally invalid. Move to Log-based CDC (Pattern 05/06).  
- **Red Line 2 \- Missing Indexes:** If the `updated_at` column is not indexed on the source, every incremental pull will execute a full table scan, degrading DB performance.

## 10\. Trade-Offs

| Decision | Option A | Option B | Trade-Off |
| :---- | :---- | :---- | :---- |
| Watermark Column | `updated_at` (Timestamp) | `ID` (Sequential Integer) | `ID` is faster and immutable, but fails to capture row updates. `updated_at` captures updates but is vulnerable to clock skew and app bugs. |
| Extract Bound | Unbounded (`> WM`) | Bounded (`> WM AND <= CurrentTime`) | Bounded ensures consistency and prevents missing long-running transactions, but requires stricter metadata management. |

