# Enterprise Data Ingestion Architecture Framework

## Objective

This framework provides a concise, technology-neutral, and reusable set of enterprise data ingestion patterns. It is designed for Data Architects, Platform Engineers, and Data Engineers to select the appropriate ingestion architecture based on data volume, freshness, SLA, history, and source characteristics.

## Scope

- **Sources:** ADLS, S3, Azure Databases, generic Relational DBs, Kafka, APIs, IoT devices, File systems.
- **Targets:** Snowflake, Databricks, ADLS / Data Lake (Iceberg, Delta Lake).
- **Environments:** AWS, Azure, Hybrid Multi-Cloud.

---

## Quick Start

1. **Start with the Catalog:** Review [`00-ingestion-pattern-catalog.md`](00-ingestion-pattern-catalog.md) to understand the available architectural patterns.
2. **Use the Decision Matrix:** Consult [`99-ingestion-pattern-decision-matrix.md`](99-ingestion-pattern-decision-matrix.md) to filter patterns based on your specific use case requirements (e.g., latency, volume, update frequency).
3. **Review the Pattern:** Open the specific pattern document for reference architectures, code snippets, and implementation guidelines.
4. **Complete the Checklist:** Each pattern ends with a Pattern Selection Checklist that must be submitted to the Architecture Review Board (ARB).

---

## Pattern Index

| ID | Pattern | File | Use Case |
|:---:|:---|:---|:---|
| 01 | Batch File / Object Drop | [`01-batch-file-object-drop.md`](01-batch-file-object-drop.md) | Standard bulk file ingestion |
| 02 | Full Snapshot / Truncate-Reload | [`02-full-snapshot-truncate-reload.md`](02-full-snapshot-truncate-reload.md) | Small dimension tables |
| 03 | Incremental Batch (Watermark) | [`03-incremental-batch-watermark.md`](03-incremental-batch-watermark.md) | Medium-scale operational tables |
| 04 | Trigger-Based Change Table | [`04-trigger-based-change-table.md`](04-trigger-based-change-table.md) | Legacy systems without CDC |
| 05 | Log-Based CDC → Object Storage | [`05-log-based-cdc-object-storage.md`](05-log-based-cdc-object-storage.md) | Target-state for high-scale DBs |
| 06 | Log-Based CDC → Kafka → Target | [`06-log-based-cdc-kafka.md`](06-log-based-cdc-kafka.md) | Multi-consumer real-time CDC |
| 07 | Transactional Outbox | [`07-transactional-outbox.md`](07-transactional-outbox.md) | Microservices event sourcing |
| 08 | Lakehouse Federation (Zero-Copy) | [`08-lakehouse-federation.md`](08-lakehouse-federation.md) | Ad-hoc queries, regulatory isolation |
| 09 | Hybrid: Bulk Snapshot + CDC Tail | [`09-hybrid-snapshot-cdc-tail.md`](09-hybrid-snapshot-cdc-tail.md) | Multi-TB migrations |
| 10 | Event Streaming (Native Kafka) | [`10-event-streaming-native-kafka.md`](10-event-streaming-native-kafka.md) | IoT, clickstream, telemetry |

**Supporting Documents:**
- [`00-ingestion-pattern-catalog.md`](00-ingestion-pattern-catalog.md) — Pattern taxonomy and overview
- [`99-ingestion-pattern-decision-matrix.md`](99-ingestion-pattern-decision-matrix.md) — Decision tree and cross-pattern comparison

---

## Architecture Principles

All patterns in this framework adhere to the following core principles:

| Principle | Description |
|:---|:---|
| **Technology Neutrality** | Patterns focus on the "how and why" before the "what" (specific tool). |
| **Cloud Agnostic** | Patterns apply across Azure and AWS, highlighting specific managed services where appropriate. |
| **Idempotency** | All ingestion pipelines must be restartable without creating duplicate data. |
| **Observability** | Centralized logging, metric tracking, and anomaly detection are embedded by default. |
| **Security by Default** | Private networking, secrets management, least-privilege IAM, and PII handling are mandatory in every pattern. |
| **Cost Transparency** | Every pattern includes a Cost Model section with optimization levers and break-even analysis. |

---

## Document Structure Standard

Every pattern document follows a standardized enterprise structure. New patterns or rewrites must include all of the following sections:

| # | Section | Purpose |
|:---:|:---|:---|
| 1 | **Pattern Overview** | Name, ID, version, objective, applicable sources/targets, when to use, red lines |
| 2 | **Pre-Ingestion Profiling** | Assessment queries, sizing calculations, pre-flight checks |
| 3 | **Reference Architecture** | Mermaid diagrams showing end-to-end flow, networking, and security boundaries |
| 4 | **End-to-End Data Flow** | Step-by-step narrative of data movement from source to target |
| 5 | **Insert / Update / Delete Handling** | How each operation type is processed |
| 6 | **Format Selection** | File format or serialization format decision matrix |
| 7 | **Target-Side Implementation** | Code snippets for Databricks AND Snowflake (SQL, Python, JSON configs) |
| 8 | **Schema Evolution** | How schema drift is handled across the pipeline |
| 9 | **AWS & Azure Reference** | Cloud-specific service mapping and configuration |
| 10 | **Security & Governance** | Mandatory controls table, RBAC model, PII handling |
| 11 | **Reliability & Failure Recovery** | Failure scenarios matrix with recovery procedures |
| 12 | **Observability & Monitoring** | Mandatory alerts table with thresholds, severities, and actions |
| 13 | **Cost Model** | Cost components, optimization levers, cost comparisons |
| 14 | **Constraints & Red Lines** | Non-negotiable architectural boundaries |
| 15 | **Trade-Offs** | Decision matrix for key architectural choices |
| 16 | **Pattern Selection Checklist** | ARB submission checklist (must be completed before provisioning) |

---

## Architecture Review Board (ARB) Submission Process

### When is ARB Approval Required?

ARB approval is required **before provisioning any new data ingestion pipeline** in production. This includes:
- New source system onboarding
- Migration from one pattern to another
- Significant changes to an existing pipeline's architecture (e.g., adding Kafka, changing CDC tools)

### Submission Requirements

1. **Pattern Selection:** Identify the pattern from the [Decision Matrix](99-ingestion-pattern-decision-matrix.md). Document the pattern ID and justification.
2. **Completed Checklist:** Fill out the Pattern Selection Checklist from the bottom of the selected pattern document. Every item must be checked or explicitly justified as N/A.
3. **Cost Estimate:** Use the Cost Model section to provide a monthly cost estimate. Include the execution mode decision (e.g., 24/7 streaming vs. micro-batch) and its cost implications.
4. **Security Review:** Confirm all controls from the Security & Governance section are implemented. Provide evidence of Private Link configuration, secrets management, and IAM policies.
5. **Monitoring Plan:** Confirm all mandatory alerts from the Observability section are configured. Provide PagerDuty/Opsgenie routing for P0/P1 alerts.
6. **Fallback Justification:** If selecting a "Fallback/Alternative" pattern instead of the "Recommended" pattern, provide written justification.

### Review Timeline

| Submission Type | Review SLA |
|:---|:---|
| Standard pattern (P01–P04) | 2 business days |
| CDC / Streaming pattern (P05–P07, P10) | 5 business days |
| Federation or Hybrid (P08, P09) | 3 business days |
| Non-standard / custom pipeline | 10 business days + architecture review meeting |

---

## Contributing Guidelines

### Adding a New Pattern

1. Create a new file following the naming convention: `NN-descriptive-name.md` (e.g., `11-api-polling-ingestion.md`).
2. Follow the [Document Structure Standard](#document-structure-standard) above — all 16 sections are mandatory.
3. Include at minimum:
   - 2+ Mermaid diagrams (architecture + one additional)
   - Code snippets for both Databricks and Snowflake
   - Failure recovery matrix with ≥ 5 scenarios
   - Cost model with monthly estimates
   - Pattern Selection Checklist with ≥ 10 items
4. Update [`00-ingestion-pattern-catalog.md`](00-ingestion-pattern-catalog.md) with the new pattern entry.
5. Update [`99-ingestion-pattern-decision-matrix.md`](99-ingestion-pattern-decision-matrix.md) to include the new pattern in all comparison tables.

### Updating an Existing Pattern

1. Increment the version number in the Pattern Overview section.
2. Preserve all existing sections — do not remove sections without ARB approval.
3. Update the Pattern Catalog if the pattern's description or trade-offs have changed.

### Quality Bar

Every pattern must meet the enterprise quality bar established by [Pattern 05](05-log-based-cdc-object-storage.md):
- ✅ Mermaid architecture diagrams (≥ 2)
- ✅ Actionable code snippets (SQL, Python, JSON configs)
- ✅ Security & Governance section with RBAC model
- ✅ Failure Recovery matrix
- ✅ Observability with mandatory alert thresholds
- ✅ Cost Model with optimization levers
- ✅ Pattern Selection Checklist (≥ 10 items)

---

## Version History

| Version | Date | Changes |
|:---|:---|:---|
| 2.0 | 2026-10-12 | All 10 patterns rewritten to enterprise standard. Added Security, Cost Model, Observability, and Failure Recovery sections to all patterns. Standardized document structure across all patterns. Updated Catalog and Decision Matrix. |
| 1.0 | 2026-10-01 | Initial draft of all 10 patterns. Catalog and Decision Matrix created. |