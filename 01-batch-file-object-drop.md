# Pattern 01: Batch File / Object Drop

## 1\. Pattern Overview

- **Pattern Name:** Batch File / Object Drop  
- **Pattern ID:** 01  
- **Objective:** Ingest file-based data arriving periodically or sporadically in object storage (S3/ADLS) into the data platform.  
- **Applicable Sources:** ADLS, Amazon S3, SFTP gateways, third-party file drops (CSV, JSON, XML, Parquet).  
- **Applicable Targets:** Snowflake, Databricks, ADLS/S3 Data Lake.  
- **Typical Use Cases:** End-of-day batch extracts, vendor data feeds, legacy system file dumps.  
- **When to Use:** When the source system cannot support direct database connections, when integrating with external third parties, or when SLAs allow for batch latency.  
- **When NOT to Use:** When sub-minute latency is required, or when the source is a highly transactional database capable of CDC.

## 2\. Pre-Ingestion Profiling

Before implementing, evaluate the source files:

- **Volume & Velocity:** How many files per day? What is the average file size? (Beware of the small-file problem: thousands of 10KB files will cripple processing).  
- **Change Semantics:** Are files full snapshots (replace all) or incremental deltas (append/upsert)?  
- **Schema Evolution:** Do upstream systems silently change column names or drop columns?

## 3\. Reference Architecture

flowchart LR

    Source\[External Source / SFTP\] \--\>|Drop| Landing\[Landing Zone S3/ADLS\]

    Landing \--\>|Event/Schedule| Raw\[Raw / Bronze Layer\]

    Raw \--\>|Validate/Clean| Target\[Curated / Silver Target\]

    

    subgraph Control Plane

        Audit\[(Audit / Metadata)\]

        Monitor\[Monitoring & Alerts\]

    end

    

    Raw \-.-\> Audit

    Target \-.-\> Audit

    Landing \-.-\> Monitor

## 4\. End-to-End Data Flow

1. **Source Drop:** External system writes files to an isolated Landing Zone.  
2. **Detection:** File arrival triggers an event (e.g., Azure Event Grid / AWS S3 Event Notifications) or is detected by a scheduled directory list.  
3. **Landing to Raw:** The ingestion engine (Databricks Auto Loader, Snowpipe, ADF) reads the file, infers/validates the schema, appends metadata, and writes to the Raw/Bronze layer.  
4. **Target Load:** Data is merged or appended into the final Curated/Silver target.  
5. **Archive:** Processed files are moved to an archive directory or transitioned to cool storage via lifecycle policies.

## 5\. Ingestion Mode

- **Incremental (Append):** Preferred. Only new files are processed.  
- **Micro-batch:** Processing files in chunks (e.g., every 15 minutes) using continuous file arrival detection.

## 6\. Decision Criteria

- **Data Volume:** Highly scalable. Cloud object storage and modern ingestion tools handle PB-scale, provided files are optimally sized (100MB \- 1GB).  
- **Data Freshness:** Typically hourly or daily. Can be pushed to near real-time (minutes) using event-triggered ingestion (Snowpipe / Auto Loader).  
- **Initial vs Incremental:** Initial load processes the historical backlog directory; incremental listens for new files.

## 7\. Insert / Update / Delete Handling

- **Inserts:** Appended directly.  
- **Updates/Deletes:** Handled downstream. If files contain incremental changes, they must include an operation flag (`I`, `U`, `D`). A subsequent `MERGE` operation applies these to the target.

## 8\. SCD Strategy

- **SCD 1:** Applied during the Target Load stage via a `MERGE` statement overwriting based on a primary key.  
- **SCD 2:** Applied if history is required. Target load calculates `effective_start_date` and `effective_end_date` based on file arrival timestamps.

## 9\. File Format Decision

- **Landing Zone:** Usually dictated by the source (CSV, JSON).  
- **Raw/Target Zone:**  
  - **Delta Lake:** Native for Databricks. Provides ACID compliance and `MERGE` capabilities.  
  - **Apache Iceberg:** Preferred if heavy multi-engine access (Snowflake \+ AWS Athena \+ Spark) is required.  
  - **Parquet:** For basic data lakes without complex update requirements.

## 10\. Format Selection Decision Matrix

| Requirement | Parquet | Delta | Iceberg | Hudi |
| :---- | :---- | :---- | :---- | :---- |
| Append-only logs | High | High | High | High |
| Frequent Updates/Deletes | Low | High | High | High |
| Multi-engine interchange | Excellent | Good (UniForm) | High | Medium |
| Databricks Native | Good | Excellent | Good | Medium |

## 11\. Target-Specific Considerations

- **Databricks:** Use **Auto Loader (`cloudFiles`)**. It automatically tracks processed files using RocksDB, handles schema evolution efficiently, and avoids expensive directory listings.  
- **Snowflake:** Use **Snowpipe** (event-driven) for continuous ingestion or `COPY INTO` for scheduled batches. Target Snowflake-managed tables or External Tables (Iceberg).  
- **ADLS / S3:** Ensure strict directory partitioning (e.g., `year=YYYY/month=MM/day=DD`) to optimize downstream reads.

## 12\. AWS and Azure Considerations

- **Azure:** Use Azure Event Grid to trigger Databricks or ADF pipelines. Store files in ADLS Gen2 with hierarchical namespaces enabled.  
- **AWS:** Use SQS/SNS combined with S3 Event Notifications.

## 13\. Security & Governance

- **Isolation:** Landing zone must be isolated from the rest of the Data Lake via strict IAM/RBAC. External vendors should write to dedicated buckets/containers.  
- **Encryption:** AES-256 encryption at rest (SSE-S3 / CMK).

## 14\. Reliability, Resumability, and Failure Handling

- **Checkpointing:** Auto Loader and Snowpipe maintain state. If a cluster fails, it resumes exactly where it left off.  
- **Poison Records:** Use Databricks `rescueDataColumn` or Snowflake `VALIDATION_MODE` to quarantine malformed rows without failing the entire batch.

## 15\. Constraints & Red Lines

- **Red Line:** Do NOT use this pattern for high-frequency (sub-minute) updates if the files are tiny (e.g., \< 1MB). This will cause severe metadata bloat and small-file problems on the target table.

## 16\. Trade-Offs

| Decision | Option A | Option B | Trade-Off |
| :---- | :---- | :---- | :---- |
| Trigger | Event-driven (SNS/EventGrid) | Scheduled (Cron) | Event-driven lowers latency but can cause compute fragmentation. Scheduled is cheaper for compute but increases latency. |
| Engine | Databricks Auto Loader | ADF Copy Activity | Auto Loader handles schema drift natively; ADF requires explicit mapping but is serverless. |

## 17\. Decision Tree

flowchart TD

    A\[File Source\] \--\> B{Continuous Arrival?}

    B \--\>|Yes| C\[Event-Triggered / Auto Loader / Snowpipe\]

    B \--\>|No| D\[Scheduled Batch COPY/Read\]

    C \--\> E{Schema Drift Expected?}

    E \--\>|Yes| F\[Enable Schema Evolution / Rescue Column\]

    E \--\>|No| G\[Strict Schema Enforcement\]