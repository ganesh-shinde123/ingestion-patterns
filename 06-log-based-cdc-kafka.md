# Pattern 06: Log-Based CDC \-\> Kafka \-\> Target

## 1\. Pattern Overview

- **Pattern Name:** Log-Based CDC to Kafka (Event Mesh)  
- **Pattern ID:** 06  
- **Objective:** Extract transactional database changes via the WAL/Binlog and publish them immediately to an event streaming platform (Kafka / Confluent / MSK / Event Hubs) for sub-second latency and multi-consumer distribution.  
- **Applicable Sources:** Relational Databases (PostgreSQL, MySQL, SQL Server, Oracle).  
- **Applicable Targets:** Snowflake, Databricks, Data Lake, Microservices, Real-Time Dashboards.  
- **Typical Use Cases:** The "Enterprise Nervous System". When database changes must instantly trigger downstream application logic, *and* be ingested into the Data Lakehouse simultaneously.  
- **When to Use:** When sub-second latency is a hard SLA, or when the data architecture dictates that multiple independent systems (not just the Data Warehouse) must react to the same database events.  
- **When NOT to Use:** When the sole consumer of the data is a Data Lake or Warehouse. Do not introduce the operational complexity and cost of Kafka if a 5-minute S3/ADLS batch (Pattern 05\) satisfies business requirements.

## 2\. Pre-Ingestion Profiling

- **Kafka Sizing:** Calculate the MB/sec of WAL generation to correctly size the Kafka partition count and retention policies.  
- **Payload Structure:** Will the Kafka topic contain the full row payload, or just the primary key? (Full payload is strongly recommended to decouple the consumer from the source DB).

## 3\. Reference Architecture

flowchart LR

    subgraph Source Environment

        DB\[(Primary DB)\]

        WAL\[WAL / Binlog\]

        DB \-.-\> WAL

    end

    

    Debezium\[Debezium\\nKafka Connect\]

    Kafka\[\[Apache Kafka\\nTopic per Table\]\]

    

    WAL \--\>|Replication| Debezium

    Debezium \--\>|Produce JSON/Avro| Kafka

    

    Kafka \--\>|Consume| Spark\[Spark Structured Streaming\\nDatabricks\]

    Kafka \--\>|Consume| Snowpipe\[Snowpipe Streaming\]

    Kafka \--\>|Consume| App\[Microservice App\]

    

    Spark \--\>|MERGE| Target\[(Target Lakehouse)\]

## 4\. End-to-End Data Flow

1. **Transaction Commit:** A transaction commits and is written to the WAL.  
2. **Debezium Connect:** A Kafka Connect connector (e.g., Debezium) continuously reads the WAL and converts the row changes into standardized event payloads (usually Avro or Protobuf).  
3. **Kafka Publish:** Events are published to Kafka topics, typically partitioned by the source table's Primary Key to guarantee ordering.  
4. **Streaming Consumption:** An engine like Spark Structured Streaming continuously polls Kafka.  
5. **Target MERGE (Micro-batch):** Spark processes the stream in micro-batches (e.g., every 10 seconds), deduplicates events by key, and executes a Delta `MERGE` against the target table.

## 5\. Insert / Update / Delete Handling

- **Inserts / Updates:** Replicated seamlessly. Using a Schema Registry (like Confluent Schema Registry) ensures that schema changes in the DB do not break downstream consumers.  
- **Deletes:** Captured as explicit delete events. Debezium handles this by producing a "tombstone" record (a message with the deleted row's key and a `null` payload), which signals downstream systems to remove the record.

## 6\. Target-Side Optimization (Databricks / Snowflake)

- **Databricks:** Use **Spark Structured Streaming** reading from Kafka. Write to Delta Lake using `.trigger(availableNow=True)` for cost-effective micro-batching, or continuous processing for true real-time. Use Delta Live Tables (DLT) `APPLY CHANGES INTO` to automatically handle the CDC `MERGE` logic.  
- **Snowflake:** Use **Snowpipe Streaming** (via the Snowflake Kafka Connector). It bypasses cloud storage entirely and streams data directly into Snowflake tables with exactly-once semantics and sub-second latency.

## 7\. Reliability and Failure Handling

- **Kafka Offsets & Checkpointing:** The streaming consumer (Spark/Snowflake) maintains a checkpoint of the last read Kafka offset. If the cluster crashes, it restarts and resumes reading from the exact offset, ensuring exactly-once processing and zero duplicates.  
- **Ordering Guarantees:** Kafka guarantees message ordering *only within a partition*. To ensure a row's updates are applied in the correct sequence, the Kafka producer MUST partition messages by the source table's Primary Key.

## 8\. Constraints & Red Lines

- **Red Line 1 \- Over-Engineering:** Operating Kafka, Zookeeper/KRaft, Schema Registry, and Kafka Connect requires a dedicated platform team. Do not default to this pattern for standard ETL.  
- **Red Line 2 \- Retention Limits:** Kafka is not a database. If the ingestion pipeline goes down for 4 days and Kafka's retention policy is 3 days, data is permanently lost.

## 9\. Trade-Offs

| Decision | Option A | Option B | Trade-Off |
| :---- | :---- | :---- | :---- |
| Serialization | Avro / Protobuf | JSON | JSON is human-readable but lacks schema enforcement and consumes massive network bandwidth. Avro/Protobuf requires a Schema Registry but is highly compressed and strictly typed. |
| DB Extraction | Debezium (Log-Based) | JDBC Source Connector | JDBC polling causes DB CPU overhead and misses hard deletes. Debezium reads logs natively, capturing all history with minimal overhead. |

