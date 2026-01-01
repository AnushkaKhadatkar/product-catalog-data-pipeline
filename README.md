# 🛒 Product Catalog Data Pipeline  
### Airflow • dbt • Snowflake • SCD Type 2

An **end-to-end data engineering pipeline** that ingests product catalog data, transforms it using **dbt**, orchestrates workflows with **Apache Airflow**, and stores analytics-ready data in **Snowflake**, including **SCD Type 2 history tracking** and **zero-copy cloning**.

---

## 📌 Project Overview

This project was built as part of **HW 3.2** to demonstrate modern data engineering practices using:

- Apache Airflow for orchestration
- dbt for transformation and testing
- Snowflake for cloud data warehousing

The pipeline processes a **Home Depot product catalog dataset (2,551 products)** and tracks historical changes using **Slowly Changing Dimension (Type 2)** logic.

---

## 🎯 Objectives

- Build a fully automated **ETL pipeline**
- Load raw product data into Snowflake staging tables
- Transform data using **dbt models**
- Implement **SCD Type 2** for product dimensions
- Validate data using **dbt tests**
- Capture historical changes using **dbt snapshots**
- Leverage **Snowflake zero-copy cloning** for dev, backup, and point-in-time analysis

---

## 📂 Dataset

- **Source:** Home Depot Products Dataset
- **Records:** 2,551 products
- **Format:** CSV
- **Storage:** Local filesystem → Snowflake staging

---

## 🔄 Airflow DAG: `product_catalog_etl`

The Airflow DAG orchestrates the complete pipeline:

### Tasks
1. **Extract Data**
   - Reads product catalog CSV from local filesystem

2. **Load to Snowflake**
   - Loads data into staging tables
   - Batch size: 500 rows (6 batches)

3. **dbt Run**
   - Executes dbt models for transformations

4. **dbt Test**
   - Validates primary keys and SCD behavior

5. **dbt Snapshot**
   - Captures historical changes in dimension tables

6. **Error Handling**
   - Logs failures for observability

---

## 🧱 dbt Modeling

### Staging Models
- Clean and standardize raw product data
- Drop null values
- Normalize column names

### Core Models
- Product dimension implemented with **SCD Type 2**
- Uses `LAG` and `LEAD` logic to track changes over time

### Tests
- Primary key uniqueness
- Valid SCD behavior
- Schema validation

---

## 📸 dbt Snapshots

- Tracks historical changes in product attributes
- Initially empty (no changes)
- Populated after modifying 20 product records and re-running pipeline

---

## ❄️ Snowflake Zero-Copy Cloning

This project demonstrates advanced Snowflake features:

- **Development Environment Cloning**
  - `dev_product_catalog` cloned from production

- **Point-in-Time Analysis**
  - Database clones created at specific timestamps

- **Rapid Prototyping**
  - Schema-level cloning for testing new transformations

- **Backup & Restore**
  - Daily backups automated via Airflow DAG
  - DAG: `snowflake_backup_management`

---

## 🛠️ Tools & Technologies

- **Orchestration:** Apache Airflow
- **Transformations:** dbt
- **Warehouse:** Snowflake
- **Programming:** Python
- **Querying:** SQL
- **Version Control:** GitHub

---

## 📈 Key Learnings

- Airflow enables robust scheduling and dependency management
- dbt simplifies SQL-based transformations and testing
- SCD Type 2 ensures full historical accuracy
- Snowflake zero-copy cloning enables fast, cost-effective environments
- Integrating Airflow and dbt creates a powerful automated pipeline

---

## 📄 Academic Report

📎 **[Product Catalog Data Pipeline Report](report/Product_catalog_ETL_report.pdf)**

---

## 👩‍💻 Contributors

- **Anushka Rajesh Khadatkar**
- Kanika Mamgain
- Samruddhi Suresh Chitnis
- Soroor Ghandali

📍 San Jose State University  
📘 MS Applied Data Intelligence

---

## ⭐ Why This Project Matters

This project demonstrates:
- Real-world **data orchestration**
- Production-style **dbt modeling**
- Historical data tracking with **SCD Type 2**
- Advanced Snowflake capabilities
- Skills aligned with **Data Engineer / Analytics Engineer roles**

---

