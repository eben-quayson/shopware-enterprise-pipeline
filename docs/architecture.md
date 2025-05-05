
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
  - Role-based restrictions (e.g., data scientist vs. analyst)
- **IAM + S3** policies restrict Glue and Redshift service access
- **Redshift Spectrum** reads only from Iceberg tables registered in the catalog

---

## 7. Querying and Analytics

- **Redshift Spectrum**: Used for federated querying over Gold and Silver tables
- **Amazon Athena (Optional)**: Ad-hoc querying over Iceberg
- **BI Tools**: Connect via JDBC/ODBC to Redshift or Athena for dashboards

---

## 8. Data Quality & Monitoring

- **Data Quality Checks**: Nulls, duplicates, type mismatches in Glue
- **Audit Trails**: Access logs via Lake Formation + CloudTrail
- **Lineage**: Tracked via Iceberg snapshots and metadata
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
