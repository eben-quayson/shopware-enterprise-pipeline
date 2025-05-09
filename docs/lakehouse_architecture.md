# Architecture - Shopware Enterprise Data Lakehouse

## 1. Overview

This architecture implements a modern **data lakehouse** pattern using the **Medallion Architecture** (Bronze, Silver, Gold layers) to structure data processing and analytics. It leverages **Delta Lake** for analytical tables on Amazon S3, with **AWS Lake Formation** enforcing fine-grained access control and **Amazon Redshift Spectrum** enabling interactive analytics.

Key Components:
- **Delta Lake**: Table format with ACID guarantees and schema evolution
- **Amazon S3**: Scalable data lake storage
- **AWS Lake Formation**: Fine-grained access control and data governance
- **AWS Glue**: ETL pipelines and table catalog registration
- **Amazon Redshift Spectrum**: Querying Delta tables without loading into Redshift

---

## 2. Medallion Architecture

+----------------+ +----------------+ +----------------+
| Bronze Layer | ---> | Silver Layer | ---> | Gold Layer |
+----------------+ +----------------+ +----------------+
| Raw Ingested | | Cleansed & | | Business KPIs |
| Data (raw S3) | | Enriched Data | | Aggregated, |
| | | (Iceberg) | | Optimized Data |
+----------------+ +----------------+ +----------------+


- **Bronze**: Immutable raw data from source systems
- **Silver**: Validated, cleaned, and enriched data with evolving schemas
- **Gold**: Aggregated datasets for downstream analytics

---

## 3. Data Flow

1. **Ingestion (Bronze Layer)**
   - Batch data (e.g., POS, Inventory) ingested into S3 using AWS Glue jobs
   - Streaming data (e.g., Web Logs, CRM) sent to S3 via Kinesis + Firehose
   - Data stored in Iceberg tables (append-only) in the `bronze/` S3 prefix

2. **Transformation (Silver Layer)**
   - Glue jobs validate schema, cast types, remove duplicates, and enrich data
   - Partitioned Iceberg tables stored in the `silver/` S3 prefix

3. **Aggregation (Gold Layer)**
   - Business-level aggregations (e.g., KPIs) calculated and stored as compact Iceberg tables in the `gold/` S3 prefix
   - Redshift Spectrum and BI tools connect to this layer

---

## 4. Storage Layout (S3)

s3://shopware-lakehouse/
├── bronze/
│ ├── pos/
│ ├── inventory/
│ └── web_logs/
├── silver/
│ ├── validated_pos/
│ ├── enriched_inventory/
│ └── sessionized_web_logs/
├── gold/
│ ├── sales_kpis/
│ ├── marketing_funnel/
│ └── customer_insights/
└── rejected/
└── [source]/bad_rows/

---

## 5. Iceberg Table Strategy

| Layer   | Table Type     | Partitioning               | Notes                                  |
|---------|----------------|----------------------------|----------------------------------------|
| Bronze  | Append-only     | `ingest_date`              | Schema inference with minimal changes  |
| Silver  | Merge-on-read   | `region`, `month`, `type`  | Schema evolution and enrichment logic  |
| Gold    | Compact + merge | `kpi_name`, `month`        | Business aggregates and time-series    |

---

## 6. Access Control

- **Lake Formation** policies govern:
  - Table, column, and row-level access
  - Role-based restrictions(Marketing Team, Operations Team, Sales Team, Customer Support Team)
- **IAM + S3** policies restrict Glue and Redshift service access
- **Redshift Spectrum** reads only from Delta tables registered in the catalog

---

## 7. Querying and Analytics

- **Redshift Spectrum**: Used for federated querying over Gold and Silver tables
- **Amazon Athena (Optional)**: Ad-hoc querying over Iceberg
- **BI Tools**: Connect via JDBC/ODBC to Redshift or Athena for dashboards

---

## 8. Data Quality & Monitoring

- **Data Quality Checks**: Nulls, duplicates, type mismatches in Glue
- **Audit Trails**: Access logs via Lake Formation + CloudTrail
- **Lineage**: Tracked via Delta Lake snapshots and metadata
- **Metrics Logging**: Via CloudWatch and S3 access logs

---

## 9. KPIs per Team

| Team       | Gold KPIs Produced                              |
|------------|--------------------------------------------------|
| Sales      | Revenue by store/region, product conversion rate |
| Marketing  | Campaign ROI, click-through rates                |
| Support    | Customer feedback trends, resolution time        |
| Finance    | Daily revenue, refunds, net sales                |

---

**Maintainer**: Ebenezer Quayson  