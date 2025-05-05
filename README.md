# 🛒 Shopware Enterprise Data Lakehouse

A scalable, secure, and governed data lakehouse built on **Apache Iceberg**, **AWS Lake Formation**, and **Redshift Spectrum**, designed to serve batch and real-time analytics needs across various business domains/departments at Shopware.

---

## 📐 Architecture

This project follows the **Medallion Architecture (Bronze → Silver → Gold)**:

- **Bronze Layer**: Raw, ingested data (streaming + batch)
- **Silver Layer**: Cleaned, validated, deduplicated data
- **Gold Layer**: Aggregated, business-consumable KPIs

Data is stored in **Apache Iceberg** table format across all layers and catalogued via **AWS Glue**. Access control is managed using **AWS Lake Formation**, and BI dashboards connect via **Amazon Redshift Spectrum** and **QuickSight**.

---

## 🚀 Features

- ✅ Real-time ingestion from web sources using Lambda + Kinesis
- ✅ Batch data support from Shopware operational systems
- ✅ Schema validation, deduplication, and enrichment via Glue jobs
- ✅ Table versioning, rollback, and optimization with Iceberg
- ✅ Row-/column-level access control with Lake Formation
- ✅ Queryable by Redshift Spectrum and visualized in QuickSight

---

## 👥 Team Contributions

| Member                    | Responsibilities                                                                 |
|--------------------------|-----------------------------------------------------------------------------------|
| **Ebenezer Quayson**     | Project Lead, Architecture, Governance, Security, Streaming Ingestion, Docs      |
| **Marzuk Sanni Entsie**  | Infrastructure as Code (IaC), Terraform Pipeline Provisioning                     |
| **Brempong Appiah Dankwah** | Glue ETL Development: Bronze → Silver → Gold Transformation                    |
| **Prince Kyeremeh**       | Data Visualization in QuickSight                                                |

---

## 🔒 Governance & Security

- **Lake Formation** enforces fine-grained access control.
- **KMS** is used for encryption at rest in S3.
- Audit trails are enabled via **CloudTrail**.
- **User ID Hashing** is implemented for PII protection.

For more details, refer to [docs/security.md](docs/security.md).

---

## 📊 Dashboards

Real-time and historical KPI dashboards are available via **Amazon QuickSight**, powered by **Redshift Spectrum** querying the **Gold Layer**.

---

## 🧪 Testing

- Pytest-based validation for ETL scripts
- Schema compliance tests using Glue’s dynamic frame checks
- Integration tested via sample ingestion-to-query flows

---

## 🧱 Infra as Code (Terraform)

Terraform is used to provision:

- S3 buckets (Bronze, Silver, Gold)
- Lake Formation resources
- IAM roles and Glue jobs
- Redshift Spectrum setup

---

## 📌 Requirements

- AWS CLI v2
- Python 3.9+
- Terraform >= 1.3
- PySpark for Glue job testing
- Boto3 (for automation and testing)

---

## 📄 Documentation

- [Architecture Overview](docs/architecture.md)
- [Pipeline Details](docs/pipeline_architecture.md)
- [Security](docs/security.md)
- [Progress Tracker](docs/progress_documentation.md)

---

## 🧠 License

MIT License. See `LICENSE.md` for details.


