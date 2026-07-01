"""
Airflow DAG: retail_pipeline
-----------------------------
Orchestrates the full ETL pipeline:
  1. Trigger Glue Job 01 (Raw → Bronze)
  2. Trigger Glue Job 02 (Bronze → Silver)
  3. Trigger Glue Job 03 (Silver → Gold)
  4. Run Redshift COPY commands to load Star Schema
  5. Run validation checks

Prerequisites:
  - Airflow Connection 'aws_default' configured with your AWS credentials
  - Airflow Connection 'redshift_default' with Redshift Serverless endpoint
  - Install providers:  pip install apache-airflow-providers-amazon

Run Airflow locally with Docker:
  docker-compose up airflow-init && docker-compose up
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.operators.empty import EmptyOperator

# ── Configuration — update these to match your environment ───────────────────
S3_BUCKET       = "yourname-retail-pipeline"
IAM_ROLE_ARN    = "arn:aws:iam::123456789012:role/GlueRedshiftRole"
AWS_REGION      = "ap-south-1"   # Mumbai — closest to Pune
GLUE_JOB_01     = "retail-raw-to-bronze"
GLUE_JOB_02     = "retail-bronze-to-silver"
GLUE_JOB_03     = "retail-silver-to-gold"

# ── Default args ─────────────────────────────────────────────────────────────
default_args = {
    "owner":            "vrushali",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="retail_pipeline",
    description="Retail Sales ETL: S3 raw → Glue → Redshift Star Schema",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",   # Daily at 02:00 UTC
    catchup=False,
    tags=["retail", "etl", "glue", "redshift"],
) as dag:

    # ── Start sentinel ─────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Glue Job 01: Raw → Bronze ──────────────────────────────────────────────
    trigger_job_01 = GlueJobOperator(
        task_id="trigger_raw_to_bronze",
        job_name=GLUE_JOB_01,
        script_args={
            "--S3_INPUT_PATH":  f"s3://{S3_BUCKET}/raw/online_retail_II.csv",
            "--S3_OUTPUT_PATH": f"s3://{S3_BUCKET}/processed/bronze/",
            "--TempDir":        f"s3://{S3_BUCKET}/tmp/",
        },
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=False,   # use sensor below
    )

    wait_job_01 = GlueJobSensor(
        task_id="wait_raw_to_bronze",
        job_name=GLUE_JOB_01,
        run_id=trigger_job_01.output,
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=3600,
    )

    # ── Glue Job 02: Bronze → Silver ───────────────────────────────────────────
    trigger_job_02 = GlueJobOperator(
        task_id="trigger_bronze_to_silver",
        job_name=GLUE_JOB_02,
        script_args={
            "--S3_BRONZE_PATH": f"s3://{S3_BUCKET}/processed/bronze/",
            "--S3_SILVER_PATH": f"s3://{S3_BUCKET}/processed/silver/",
            "--TempDir":        f"s3://{S3_BUCKET}/tmp/",
        },
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=False,
    )

    wait_job_02 = GlueJobSensor(
        task_id="wait_bronze_to_silver",
        job_name=GLUE_JOB_02,
        run_id=trigger_job_02.output,
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=3600,
    )

    # ── Glue Job 03: Silver → Gold ─────────────────────────────────────────────
    trigger_job_03 = GlueJobOperator(
        task_id="trigger_silver_to_gold",
        job_name=GLUE_JOB_03,
        script_args={
            "--S3_SILVER_PATH": f"s3://{S3_BUCKET}/processed/silver/",
            "--S3_GOLD_PATH":   f"s3://{S3_BUCKET}/processed/gold/",
            "--TempDir":        f"s3://{S3_BUCKET}/tmp/",
        },
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=False,
    )

    wait_job_03 = GlueJobSensor(
        task_id="wait_silver_to_gold",
        job_name=GLUE_JOB_03,
        run_id=trigger_job_03.output,
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=3600,
    )

    # ── Redshift COPY: load Star Schema ────────────────────────────────────────
    load_dim_date = RedshiftSQLOperator(
        task_id="load_dim_date",
        sql=f"""
            TRUNCATE retail.dim_date;
            COPY retail.dim_date
            FROM 's3://{S3_BUCKET}/processed/silver/dim_date/'
            IAM_ROLE '{IAM_ROLE_ARN}'
            FORMAT AS PARQUET;
        """,
        redshift_conn_id="redshift_default",
    )

    load_dim_product = RedshiftSQLOperator(
        task_id="load_dim_product",
        sql=f"""
            TRUNCATE retail.dim_product;
            COPY retail.dim_product
            FROM 's3://{S3_BUCKET}/processed/silver/dim_product/'
            IAM_ROLE '{IAM_ROLE_ARN}'
            FORMAT AS PARQUET;
        """,
        redshift_conn_id="redshift_default",
    )

    load_dim_customer = RedshiftSQLOperator(
        task_id="load_dim_customer",
        sql=f"""
            TRUNCATE retail.dim_customer;
            COPY retail.dim_customer
            FROM 's3://{S3_BUCKET}/processed/silver/dim_customer/'
            IAM_ROLE '{IAM_ROLE_ARN}'
            FORMAT AS PARQUET;
        """,
        redshift_conn_id="redshift_default",
    )

    load_fact_sales = RedshiftSQLOperator(
        task_id="load_fact_sales",
        sql=f"""
            TRUNCATE retail.fact_sales;
            COPY retail.fact_sales
            FROM 's3://{S3_BUCKET}/processed/silver/fact_sales/'
            IAM_ROLE '{IAM_ROLE_ARN}'
            FORMAT AS PARQUET;
        """,
        redshift_conn_id="redshift_default",
    )

    # ── Validation ─────────────────────────────────────────────────────────────
    validate = RedshiftSQLOperator(
        task_id="validate_row_counts",
        sql="""
            SELECT tbl, rows FROM (
                SELECT 'dim_date'     AS tbl, COUNT(*) AS rows FROM retail.dim_date
                UNION ALL
                SELECT 'dim_product'  AS tbl, COUNT(*) AS rows FROM retail.dim_product
                UNION ALL
                SELECT 'dim_customer' AS tbl, COUNT(*) AS rows FROM retail.dim_customer
                UNION ALL
                SELECT 'fact_sales'   AS tbl, COUNT(*) AS rows FROM retail.fact_sales
            )
            ORDER BY tbl;
        """,
        redshift_conn_id="redshift_default",
    )

    # ── End sentinel ───────────────────────────────────────────────────────────
    end = EmptyOperator(task_id="end")

    # ── Dependencies ───────────────────────────────────────────────────────────
    (
        start
        >> trigger_job_01 >> wait_job_01
        >> trigger_job_02 >> wait_job_02
        >> trigger_job_03 >> wait_job_03
        >> [load_dim_date, load_dim_product, load_dim_customer]
        >> load_fact_sales
        >> validate
        >> end
    )
