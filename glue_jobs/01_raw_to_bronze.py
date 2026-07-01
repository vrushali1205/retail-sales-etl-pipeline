"""
Glue Job 01: Raw → Bronze
--------------------------
Reads raw CSV from S3 raw/, cleans it (drop nulls, duplicates,
fix data types), and writes Parquet to S3 processed/bronze/.

Deploy this script as an AWS Glue Spark Job.
Required Job Parameters (set in Glue console under Job Parameters):
  --S3_INPUT_PATH   s3://yourname-retail-pipeline/raw/online_retail_II.csv
  --S3_OUTPUT_PATH  s3://yourname-retail-pipeline/processed/bronze/
  --TempDir         s3://yourname-retail-pipeline/tmp/
"""

import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Glue boilerplate ─────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_INPUT_PATH", "S3_OUTPUT_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ── 1. Read raw CSV ───────────────────────────────────────────────────────────
logger.info(f"Reading raw CSV from: {args['S3_INPUT_PATH']}")
df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")          # handles quoted newlines in Description
    .option("escape", '"')
    .csv(args["S3_INPUT_PATH"])
)

logger.info(f"Raw row count: {df_raw.count()}")
logger.info("Raw schema:")
df_raw.printSchema()

# ── 2. Rename columns to snake_case ──────────────────────────────────────────
df = (
    df_raw
    .withColumnRenamed("Invoice",     "invoice_no")
    .withColumnRenamed("StockCode",   "stock_code")
    .withColumnRenamed("Description", "description")
    .withColumnRenamed("Quantity",    "quantity")
    .withColumnRenamed("InvoiceDate", "invoice_date")
    .withColumnRenamed("Price",       "unit_price")
    .withColumnRenamed("Customer ID", "customer_id")   # Online Retail II uses "Customer ID"
    .withColumnRenamed("Country",     "country")
)

# ── 3. Cast data types ────────────────────────────────────────────────────────
df = (
    df
    .withColumn("quantity",     F.col("quantity").cast(IntegerType()))
    .withColumn("unit_price",   F.col("unit_price").cast(DoubleType()))
    .withColumn("customer_id",  F.col("customer_id").cast(IntegerType()))
    # InvoiceDate comes as "2010-12-01 08:26:00" — parse as timestamp
    .withColumn("invoice_date", F.to_timestamp("invoice_date", "yyyy-MM-dd HH:mm:ss"))
)

# ── 4. Drop rows missing critical fields ──────────────────────────────────────
before = df.count()
df = df.dropna(subset=["customer_id", "invoice_no", "stock_code", "invoice_date"])
after_null = df.count()
logger.info(f"Dropped {before - after_null} rows with null critical fields")

# ── 5. Remove cancelled invoices (InvoiceNo starting with 'C') ───────────────
df = df.filter(~F.col("invoice_no").startswith("C"))
after_cancel = df.count()
logger.info(f"Dropped {after_null - after_cancel} cancelled invoices")

# ── 6. Filter bad quantity / price values ─────────────────────────────────────
df = df.filter((F.col("quantity") > 0) & (F.col("unit_price") > 0))
after_filter = df.count()
logger.info(f"Dropped {after_cancel - after_filter} rows with non-positive qty/price")

# ── 7. Deduplicate ───────────────────────────────────────────────────────────
df = df.dropDuplicates()
after_dedup = df.count()
logger.info(f"Dropped {after_filter - after_dedup} exact duplicate rows")
logger.info(f"Bronze row count: {after_dedup}")

# ── 8. Add derived columns ────────────────────────────────────────────────────
df = (
    df
    .withColumn("line_total",   F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("invoice_year", F.year("invoice_date"))
    .withColumn("invoice_month",F.month("invoice_date"))
    .withColumn("ingestion_ts", F.current_timestamp())
)

# ── 9. Write Parquet to Bronze layer ─────────────────────────────────────────
logger.info(f"Writing Bronze Parquet to: {args['S3_OUTPUT_PATH']}")
(
    df.write
    .mode("overwrite")
    .partitionBy("invoice_year", "invoice_month")
    .parquet(args["S3_OUTPUT_PATH"])
)

logger.info("Job 01 (Raw → Bronze) completed successfully.")
job.commit()
