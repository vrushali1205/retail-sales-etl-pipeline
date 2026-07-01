# 🛒 Retail Sales ETL Pipeline

An end-to-end data engineering project built on AWS — ingesting raw retail transaction data, transforming it with PySpark on AWS Glue, loading it into a Redshift Star Schema, and orchestrating the entire pipeline with Apache Airflow.

---

## 🏗️ Architecture

```
Kaggle CSV
    │
    ▼
Amazon S3 (raw/)
    │
    ▼ AWS Glue Crawler
Glue Data Catalog ──► Amazon Athena (ad-hoc queries)
    │
    ▼ AWS Glue Job (PySpark)
Amazon S3 (processed/ — Parquet)
    │  • Bronze → Silver → Gold
    │  • SCD Type 2 on dim_customer
    ▼
Amazon Redshift Serverless
    │  Star Schema:
    │  fact_sales + dim_customer + dim_product + dim_date
    ▼
Apache Airflow DAG (orchestration)
```

---

## 🗂️ Project Structure

```
retail-sales-etl-pipeline/
├── glue_jobs/
│   ├── 01_raw_to_bronze.py       # Clean raw CSV → Parquet
│   ├── 02_bronze_to_silver.py    # Split into dim/fact tables + SCD2
│   └── 03_silver_to_gold.py      # Aggregate / business metrics
├── redshift_ddl/
│   └── star_schema.sql           # DDL for all tables + COPY commands
├── dags/
│   └── retail_pipeline_dag.py    # Airflow DAG (Glue + Redshift)
├── scripts/
│   ├── upload_to_s3.py           # Upload raw CSV to S3
│   └── setup_glue_crawler.py     # Create & run Glue Crawler via boto3
├── .github/
│   └── .gitignore
└── README.md
```

---

## 🧰 Tech Stack

| Layer        | Technology                          |
|-------------|--------------------------------------|
| Storage      | Amazon S3 (raw / staging / processed)|
| Catalog      | AWS Glue Data Catalog + Crawler      |
| Transform    | AWS Glue Jobs (PySpark)              |
| Warehouse    | Amazon Redshift Serverless           |
| Query        | Amazon Athena                        |
| Orchestrate  | Apache Airflow (Docker)              |
| Language     | Python 3, PySpark, SQL               |
| IaC / Utils  | boto3                                |

---

## 📦 Dataset

**Online Retail II** from [Kaggle](https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci)

- ~1M rows of UK e-commerce transactions (2009–2011)
- Columns: `InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `Price`, `CustomerID`, `Country`

---

## 🚀 How to Run

### Prerequisites
- AWS account with permissions for S3, Glue, Redshift, IAM
- Python 3.9+, boto3, Apache Airflow (Docker)

### Step 1 — Upload raw data
```bash
pip install boto3
python scripts/upload_to_s3.py --bucket yourname-retail-pipeline --file online_retail_II.csv
```

### Step 2 — Run Glue Crawler
```bash
python scripts/setup_glue_crawler.py --bucket yourname-retail-pipeline --db retail_db
```

### Step 3 — Run Glue ETL Jobs (in order)
Deploy each script in `glue_jobs/` as an AWS Glue Job (Python Shell or Spark), then run:
1. `01_raw_to_bronze.py`
2. `02_bronze_to_silver.py`
3. `03_silver_to_gold.py`

### Step 4 — Create Redshift tables
Connect to your Redshift Serverless cluster and run:
```sql
-- Run redshift_ddl/star_schema.sql
```

### Step 5 — Orchestrate with Airflow
```bash
docker-compose up airflow-init
docker-compose up
```
Then enable the `retail_pipeline` DAG in the Airflow UI.

---

## 📊 Star Schema

```
              dim_date
                 │
dim_customer ─── fact_sales ─── dim_product
```

| Table         | Key           | Description                        |
|--------------|---------------|-------------------------------------|
| fact_sales    | invoice_id    | One row per line item               |
| dim_customer  | customer_sk   | SCD Type 2 — tracks address changes |
| dim_product   | product_sk    | Product details                     |
| dim_date      | date_id       | Calendar attributes                 |

---

## ⚠️ Security Notes
- **Never commit AWS credentials** — use IAM roles or environment variables
- `.gitignore` excludes `.env`, `*.csv`, and credential files

---

## 👩‍💻 Author
Vrushali | Data Engineer  
[GitHub](https://github.com/zamba) | [@skill.data](https://instagram.com/skill.data)
