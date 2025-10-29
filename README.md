# Banking Data Warehouse (BDW) Project: Master Document

**Project Name:** Wolf Banking Data Warehouse (BDW)  
**Version:** 1.0  
**Status:** Inception  
**Last Update:** 11 October 2025  

---

## Table of Contents

1. [Project Charter & Executive Summary](#1-project-charter--executive-summary)  
2. [Vision & Business Objectives](#2-vision--business-objectives)  
3. [Architecture & Data Flow](#3-architecture--data-flow)  
4. [Technology Stack](#4-technology-stack)  
5. [Data Governance & Quality Framework](#5-data-governance--quality-framework)  
6. [Naming Conventions & Standards](#6-naming-conventions--standards)  
7. [Project Timeline & Milestones (Phased Approach)](#7-project-timeline--milestones-phased-approach)  
8. [Roles & Responsibilities](#8-roles--responsibilities)  
9. [Risks & Mitigations](#9-risks--mitigations)  
10. [Appendices](#10-appendices)  

---

## 1. Project Charter & Executive Summary

The Wolf Banking Data Warehouse (BDW) project is a strategic initiative to create a single source of truth for all banking data. This centralized, modern data platform will empower the organization with accurate, timely, and accessible data for regulatory reporting, advanced customer analytics, risk management, and operational intelligence. By moving away from siloed data sources and fragmented reports, we aim to drive efficiency, enhance decision-making, and maintain a competitive edge in the market.

---

## 2. Vision & Business Objectives

**Vision:**  
To enable a data-driven culture at Wolf Bank by providing a secure, scalable, and trusted data platform that turns raw data into a strategic asset.

**Key Business Objectives:**

- **Regulatory Compliance:** Streamline and automate the generation of critical reports (e.g., Basel III, IFRS 9, AML/KYC).
- **360-Degree Customer View:** Unify customer data from core banking, CRM, and web channels to enable personalized marketing and improve customer service.
- **Risk Management:** Enhance credit, market, and operational risk modeling with integrated and historical data.
- **Operational Efficiency:** Automate manual data consolidation processes, reducing reporting cycle times and resource costs.
- **Advanced Analytics:** Provide a clean, modeled data foundation for Data Science and BI teams to build ML models and discover new business insights.

---

## 3. Architecture & Data Flow

The BDW will follow a **Medallion Architecture** (Bronze, Silver, Gold layers) implemented in the SQL Server platform, combining the best of data lakes and data warehouses.

### Layers Explained:

- **Bronze (Raw):** Immutable, append-only storage of raw source data (Parquet files). Schema enforcement and basic metadata are added.
- **Silver (Cleansed & Integrated):** Data is cleansed, standardized, deduplicated, and integrated from various sources into a conformed, business-ready model.
- **Gold (Business Semantics):** Data is aggregated and structured into purpose-built data charts for specific business functions (e.g., Customer 360, Risk Management, Regulatory Reporting).

---

## 4. Technology Stack

| Category         | Technology       | Rationale                                      |
|------------------|------------------|------------------------------------------------|
| Platform         | SQL Server       | Unified platform for ETL, streaming, and analytics |
| Storage          | GitHub           | Low-cost, scalable, durable object storage for all data layers |
| Orchestration    | Apache Airflow   | Schedule, manage, and monitor complex data pipelines |
| BI & Analytics   | Power BI         | Primary tool for business reporting and dashboards |
| Version Control  | Git (GitHub)     | For all code (SQL, PySpark, Python)           |
| Data Catalog     | Unity Catalog    | Centralized governance, access control, and lineage |

---

## 5. Data Governance & Quality Framework

- **Data Ownership:** Each data domain (e.g., Customer, Transactions) will have a designated Business Data Owner.
- **Source System Documentation:** All data sources will be formally documented with metadata including:
  - Source System
  - Extraction Method
  - Extraction Frequency
  - Data Owner
  - Data Classification (PII, Confidential, Public, etc.)
  - Retention Policy
- **Data Quality (DQ):** DQ checks will be implemented at each layer.

---

## 6. Naming Conventions & Standards

Adherence to these conventions is mandatory to ensure consistency, improve readability, and simplify maintenance.

### General Principles

- **Case:** Use `snake_case` for all names.
- **Reserved Words:** Avoid SQL reserved keywords (e.g., use `transaction_date` instead of `date`).
- **Clarity & Brevity:** Names must be clear and descriptive without unnecessary abbreviation.

### Table Naming Conventions by Layer

- **Bronze Layer (Raw):** `[source_system]_[entity]`  
  Example: `core_banking_customer`, `finacle_transactions`

- **Silver Layer (Cleansed):** Use a clear, singular noun for the business entity. Use `fact` and `dim` prefixes for normalized tables.  
  Example: `customer`, `account`, `fact_customers`, `dim_customer_identifications`

- **Gold Layer (Business Semantics):** `[domain_entity_purpose]`  
  Example: `customer_360_wide`, `compliance_loan_summary`

### Column Naming Conventions

- **General Rule:** Use descriptive, singular nouns in `snake_case`.  
  Example: `first_name`, `account_balance`, `effective_date`

- **Surrogate Keys:** `[table_name_key]`  
  Example: `customer_key`

- **Technical Columns:**  
  - `created_timestamp`  
  - `modified_timestamp`  
  - `source_system`  
  - `batch_id`

### Stored Procedure & Script Naming

- **Structure: Procedure** `pcd_[action]_[target_object]_[layer]`  
  Example: `pcd_merge_dim_customer_silver`, `pcd_insert_into_fct_transactions_silver`

  - **Structure: Script** `[action]_[target_object]_[layer]`  
  Example: `merge_dim_customer_silver`, `insert_into_fct_transactions_silver`

---

## 7. Project Timeline & Milestones (Phased Approach)

### Phase 1: Foundation (Months 1–3)
- Set up SQL Server infrastructure.
- Digest 2–3 critical sources into Bronze.
- Build initial Silver layer conformed dimensions.
- Deliver first Gold data mart: `Customer_360_Wide`.

### Phase 2: Core Banking (Months 4–6)
- Ingest transactional data.
- Build `Account Transaction Fact` and related dimensions.
- Enable first set of operational reports.

### Phase 3: Advanced Analytics & Regulatory (Months 7–9)
- Ingest risk and compliance data.
- Build Gold data marts for IFRS 9 reporting.
- Onboard Data Science team for advanced analytics.

---

## 8. Roles & Responsibilities

| Role               | Key Responsibilities |
|--------------------|----------------------|
| Project Sponsor    | Provides funding, champions the project, resolves high-level business conflicts |
| Data Architect     | Designs overall architecture, data model, and selects technologies |
| Data Engineer      | Develops, tests, and maintains data pipelines (ETL/ELT) |
| BI Developer       | Develops reports, dashboards, and semantic models |
| Data Product Owner | Defines business requirements and prioritizes backlog |
| Business Data Owner| Defines data meaning, rules, and quality standards for their domain |

---

## 9. Risks & Mitigations

| Risk                           | Mitigation Strategy |
|--------------------------------|----------------------|
| Data Quality in Source Systems | Profile source data during discovery. Implement DQ checks in Silver layer. |
| Scope Creep                    | Use phased delivery. Strict change control process. |
| Lack of Business Adoption      | Involve business users early. Deliver high-value use cases first. |
| Security & Compliance Breach   | Design security from the start using Unity Catalog. Classify PII and enforce access controls. |

---

## 10. Appendices

### Document History

| Version | Date         | Author         | Changes                |
|---------|--------------|----------------|------------------------|
| 1.0     | 11 Oct 2025  | Innocent Nhamo | Initial version created |
