# Security Guidelines – Shopware Enterprise Data Engineering Project

## Overview
This document outlines the security and compliance strategy for the Shopware enterprise data lakehouse platform. The platform consolidates batch and streaming data into a secure Iceberg-based architecture on S3, governed by AWS Lake Formation, and accessed via tools like Redshift Spectrum, Athena, and Glue.

---

## 1. Data Access Control

### 1.1. Role-Based Access Control (RBAC)
Access to data is restricted using **AWS Lake Formation** based on team roles:

| Team             | IAM Role                     | Accessible Tables                | Access Type         |
|------------------|------------------------------|----------------------------------|---------------------|
| Sales            | `ShopwareLFRole_Sales`       | `pos`, `inventory`               | SELECT              |
| Marketing        | `ShopwareLFRole_Marketing`   | `web_traffic_logs`, `crm_events`| SELECT              |
| Operations       | `ShopwareLFRole_Operations`  | `inventory`, `pos`               | SELECT, AD-HOC SQL  |
| Customer Support | `ShopwareLFRole_Support`     | `crm_events`                     | SELECT (filtered)   |

Permissions are granted at the table and column level where applicable.

### 1.2. Row-Level and Column-Level Security
- **Column-Level**: Only relevant columns are shared with specific teams.
- **Row-Level Filtering** (when needed): Filters restrict data to relevant regions or departments (e.g., `store_id = 103`).

---

## 2. Data Encryption

### 2.1. Encryption at Rest
- All data stored in Amazon S3 is encrypted using **SSE-S3** or **SSE-KMS**.
- Iceberg metadata files and table data are encrypted.

### 2.2. Encryption in Transit
- All data transfer between services (S3, Glue, Redshift Spectrum, Athena) uses **HTTPS and TLS 1.2+**.
- Kinesis and other streaming sources use secure endpoints.

---

## 3. Data Governance

### 3.1. Catalog Management
- Apache Iceberg tables are registered in AWS Glue Data Catalog.
- AWS Lake Formation governs access to databases, tables, and columns.

### 3.2. Metadata Security
- Glue catalog access is restricted to pipeline and analytical roles only.
- Audit logs track access to catalog and table metadata.

---

## 4. Monitoring and Logging

### 4.1. Pipeline Monitoring
- **Amazon CloudWatch** tracks Glue job status, ETL logs, and Redshift queries.
- **SNS alerts** are triggered on job failures or data delays.

### 4.2. Data Quality Monitoring
- Data is validated before loading into the warehouse or data marts.
- Invalid or corrupt records are stored in quarantine S3 prefixes (`/rejected/`).

### 4.3. Audit Logging
- **Lake Formation Audit Logs** are enabled to track:
  - Who accessed which table
  - What queries were run
  - What permissions were granted/denied
- Logs are stored in a central logging S3 bucket with restricted access.

---

## 5. Compliance

### 5.1. GDPR / CCPA Readiness
- Personally Identifiable Information (PII) such as `user_id` is **hashed** before storage using salted SHA-256.
- Data retention and deletion policies are in place for user-related records.

### 5.2. Least Privilege Principle
- Each IAM role or user is granted only the permissions necessary to perform its job.
- No user or service has full administrative access to all datasets.

---

## 6. Incident Response

### 6.1. Alerts
- Security and pipeline failures trigger SNS alerts sent to engineering leads.
- IAM role anomalies or unauthorized access attempts are logged and flagged.

### 6.2. Data Breach Protocol
- In case of suspected breach:
  1. Access to affected roles is revoked immediately.
  2. Logs are reviewed via CloudTrail and Lake Formation audit logs.
  3. Response coordinated with InfoSec and Compliance teams.

---

## 7. Future Enhancements
- Integration with **AWS Macie** for automated PII detection.
- Automated masking and tokenization for sensitive columns.
- Attribute-based access control (ABAC) for dynamic data filtering.

---

**Maintainer**: Ebenezer Quayson
