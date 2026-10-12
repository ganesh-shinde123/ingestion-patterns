# Master Ingestion Pattern Decision Matrix

## 1. Overview
This matrix serves as the primary routing guide for Data Architects and Platform Engineers. Before designing a custom data pipeline, use the dimensions below to select the appropriate standardized enterprise ingestion pattern.

## 2. Requirements & Pattern Mapping

| Requirement / Scenario | Source Characteristics | Recommended Pattern | Fallback / Alternative |
| :--- | :--- | :--- | :--- |
| **Standard File Drop** | Files (CSV, JSON, Parquet) in S3/ADLS | **P01: Batch File Drop** | N/A |
| **Small DB Table (< 10GB)** | Low volume, hard deletes occur | **P02: Full Snapshot** | P03: Incremental (if no deletes) |
| **Medium DB Table (10GB - 1TB)** | `updated_at` exists, soft deletes only | **P03: Incremental Batch** | P05: CDC to Object Storage |
| **Legacy DB (No CDC, No Timestamps)** | Exact history and hard deletes required | **P04: Trigger-Based Change Table**| P02: Full Snapshot |
| **High-Volume DB (Minutes SLA)** | Heavy update/delete churn, exact history | **P05: CDC to Object Storage** | P03: Incremental Batch |
| **High-Volume DB (Sub-minute SLA)** | Multiple downstream Kafka consumers | **P06: CDC to Kafka** | P05: CDC to Object Storage |
| **Domain-Driven Microservices** | Semantic business events needed | **P07: Transactional Outbox** | P06: CDC to Kafka (raw rows) |
| **Regulatory / Zero-Copy** | Data cannot physically move (Snowflake/DBX) | **P08: Lakehouse Federation** | N/A |
| **Massive Historical Migration** | Multi-TB initial load, zero downtime | **P09: Hybrid Bulk + CDC Tail** | P02: Full Snapshot (requires downtime) |
| **IoT / App Telemetry** | High-velocity, append-only Kafka feeds | **P10: Native Event Streaming** | P01: Batch File Drop (if latency allows) |

## 3. Cross-Pattern Capability Comparison

| Pattern | Ingestion Mode | Captures Hard Deletes? | Tracks Full History (SCD2)? | Operational Complexity | Compute/Storage Cost |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **P01: File Drop** | Batch | Handled Downstream | Handled Downstream | Low | Low |
| **P02: Full Snapshot** | Batch | Yes (Implicitly) | No (Latest state only) | Low | Medium (Network Egress) |
| **P03: Incremental** | Batch | **No** | No (Latest state only) | Low | Low |
| **P04: DB Triggers** | Micro-Batch | Yes | Yes | High (DBA Overhead) | Low |
| **P05: CDC to S3** | Micro-Batch | Yes | Yes | Medium | Medium |
| **P06: CDC to Kafka** | Streaming | Yes | Yes | **High** | **High** |
| **P07: Outbox** | Streaming/Batch | Yes (Logical) | Yes (Semantic) | High (App Dev) | Medium |
| **P08: Federation** | Real-Time Read | N/A | N/A | Low | Zero Storage, High Compute |
| **P09: Hybrid Load** | Batch + Stream | Yes | Yes (Post-cutover) | High (One-Time) | High (Temporary) |
| **P10: Telemetry** | Streaming | N/A (Append-only)| Yes | Medium | High (If 24/7 cluster) |

## 4. Master Architectural Decision Tree

```mermaid
flowchart TD
    Start([New Data Source Identified]) --> Q1{What is the Source Type?}
    
    %% File / Event Path
    Q1 -->|Object Storage / SFTP| P01[P01: Batch File Drop]
    Q1 -->|IoT / App Telemetry| P10[P10: Native Event Streaming]
    Q1 -->|Microservices (DDD)| P07[P07: Transactional Outbox]
    
    %% Zero Copy Path
    Q1 -->|Snowflake / Databricks / Ad-Hoc| P08[P08: Lakehouse Federation]
    
    %% Database Path
    Q1 -->|Relational Database| Q2{What is the Target SLA?}
    
    %% Database Path - Streaming
    Q2 -->|Sub-Minute / Real-Time| Q3{Are there multiple independent consumers?}
    Q3 -->|Yes| P06[P06: CDC to Kafka]
    Q3 -->|No| P05[P05: CDC to Object Storage]
    
    %% Database Path - Batch
    Q2 -->|Hourly / Daily (Batch)| Q4{Data Volume & Constraints?}
    
    Q4 -->|Multi-TB Initial Load (Zero Downtime)| P09[P09: Hybrid Bulk + CDC]
    Q4 -->|Small Lookup Tables (< 10GB)| P02[P02: Full Snapshot]
    Q4 -->|Medium / Large Operational| Q5{Does the source hard-delete records?}
    
    %% Hard Deletes & Log Access
    Q5 -->|Yes| Q6{Is Log-Based CDC Allowed?}
    Q6 -->|Yes| P05
    Q6 -->|No| P04[P04: Trigger-Based Change Table]
    
    Q5 -->|No (Append/Soft-Delete only)| Q7{Reliable updated_at timestamp?}
    Q7 -->|Yes| P03[P03: Incremental Batch Watermark]
    Q7 -->|No| P02
```

## 5. Multi-Cloud & Target Technology Mapping

While the patterns are vendor-agnostic, enterprise implementations typically utilize the following proven tech stacks:

| Capability | AWS Ecosystem | Azure Ecosystem | Target: Databricks | Target: Snowflake |
| :--- | :--- | :--- | :--- | :--- |
| **File Landing** | S3 + SNS/SQS | ADLS Gen2 + Event Grid | Auto Loader (`cloudFiles`) | Snowpipe |
| **Log-Based CDC** | AWS DMS | ADF CDC / Event Hubs | DLT `APPLY CHANGES INTO` | Dynamic Tables / Streams |
| **Event Broker** | Amazon MSK / Kinesis | Azure Event Hubs | Spark Structured Streaming | Snowpipe Streaming |
| **Federation** | Athena / Glue Catalog | Synapse Serverless | Unity Catalog Foreign Catalogs | External Tables (Iceberg) |
| **Orchestration** | MWAA (Airflow) / Step Functions | Azure Data Factory | Databricks Workflows | Snowflake Tasks |

## 6. Architecture Review Board Submission
To approve a new ingestion pipeline, engineers must provide:
1. The **Pattern ID** selected from this matrix.
2. The completed **Pattern Selection Checklist** (found at the bottom of the corresponding pattern document).
3. Written justification if choosing an "Alternative/Fallback" pattern over the "Recommended" pattern.