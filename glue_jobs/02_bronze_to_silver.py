"""
Glue Job 02: Bronze → Silver
-----------------------------
Reads Bronze Parquet, splits into dimension and fact tables,
applies SCD Type 2 logic on dim_customer, and writes Silver Parquet.

Deploy this script as an AWS Glue Spark Job.
Required Job Parameters:
  --S3_BRONZE_PATH  s3://yourname-retail-pipeline/processed/bronze/
  --S3_SILVER_PATH  s3://yourname-retail-pipeline/processed/silver/
  --TempDir         s3://yourname-retail-pipeline/tmp/
"""

import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F, Window
from pyspark.sql.types import IntegerType, StringType, BooleanType, TimestampType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BRONZE_PATH", "S3_SILVER_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BRONZE = args["S3_BRONZE_PATH"]
SILVER = args["S3_SILVER_PATH"]

# ── 1. Read Bronze ────────────────────────────────────────────────────────────
logger.info(f"Reading Bronze from: {BRONZE}")
df = spark.read.parquet(BRONZE)
logger.info(f"Bronze row count: {df.count()}")

# ═══════════════════════════════════════════════════════════════════════════════
# DIM_DATE
# ═══════════════════════════════════════════════════════════════════════════════
logger.info("Building dim_date...")

dim_date = (
    df.select(F.to_date("invoice_date").alias("full_date"))
    .distinct()
    .withColumn("date_id",       F.date_format("full_date", "yyyyMMdd").cast(IntegerType()))
    .withColumn("year",          F.year("full_date"))
    .withColumn("quarter",       F.quarter("full_date"))
    .withColumn("month",         F.month("full_date"))
    .withColumn("month_name",    F.date_format("full_date", "MMMM"))
    .withColumn("week_of_year",  F.weekofyear("full_date"))
    .withColumn("day_of_month",  F.dayofmonth("full_date"))
    .withColumn("day_of_week",   F.dayofweek("full_date"))   # 1=Sun .. 7=Sat
    .withColumn("day_name",      F.date_format("full_date", "EEEE"))
    .withColumn("is_weekend",    F.when(F.dayofweek("full_date").isin(1, 7), True).otherwise(False))
)

dim_date.write.mode("overwrite").parquet(f"{SILVER}/dim_date/")
logger.info(f"dim_date rows: {dim_date.count()}")

# ═══════════════════════════════════════════════════════════════════════════════
# DIM_PRODUCT
# ═══════════════════════════════════════════════════════════════════════════════
logger.info("Building dim_product...")

# Keep most recent description per stock_code (prices change; take latest)
w_product = Window.partitionBy("stock_code").orderBy(F.desc("invoice_date"))

dim_product = (
    df.select("stock_code", "description", "unit_price", "invoice_date")
    .withColumn("rn", F.row_number().over(w_product))
    .filter(F.col("rn") == 1)
    .drop("rn", "invoice_date")
    .withColumn("product_sk", F.monotonically_increasing_id())
    .withColumnRenamed("unit_price", "current_price")
    .select("product_sk", "stock_code", "description", "current_price")
)

dim_product.write.mode("overwrite").parquet(f"{SILVER}/dim_product/")
logger.info(f"dim_product rows: {dim_product.count()}")

# ═══════════════════════════════════════════════════════════════════════════════
# DIM_CUSTOMER  —  SCD Type 2
# ═══════════════════════════════════════════════════════════════════════════════
logger.info("Building dim_customer with SCD Type 2...")

"""
SCD Type 2 Strategy
--------------------
The raw data has customer_id + country (the only changing attribute).
We detect when a customer changes country across invoices and create a
new surrogate key row for each version.

Columns added:
  customer_sk   : surrogate key (monotonically_increasing_id)
  effective_date: first invoice date in this country version
  expiry_date   : last invoice date in this country version
                  (9999-12-31 for the current/active row)
  is_current    : True for the latest row per customer
"""

# Get all distinct (customer_id, country) combos with their first/last date
w_cust = Window.partitionBy("customer_id").orderBy("first_seen")

customer_versions = (
    df.groupBy("customer_id", "country")
    .agg(
        F.min("invoice_date").alias("first_seen"),
        F.max("invoice_date").alias("last_seen"),
    )
    .withColumn("version", F.row_number().over(w_cust))
)

# Build effective/expiry dates using LEAD
w_lead = Window.partitionBy("customer_id").orderBy("first_seen")

dim_customer = (
    customer_versions
    .withColumn("effective_date", F.col("first_seen"))
    .withColumn(
        "expiry_date",
        F.coalesce(
            F.lead("first_seen", 1).over(w_lead) - F.expr("INTERVAL 1 SECOND"),
            F.lit("9999-12-31").cast(TimestampType()),
        ),
    )
    .withColumn(
        "is_current",
        F.when(F.col("expiry_date") > F.current_timestamp(), True).otherwise(False),
    )
    .withColumn("customer_sk", F.monotonically_increasing_id())
    .select(
        "customer_sk",
        "customer_id",
        "country",
        "version",
        "effective_date",
        "expiry_date",
        "is_current",
    )
)

dim_customer.write.mode("overwrite").parquet(f"{SILVER}/dim_customer/")
logger.info(f"dim_customer rows: {dim_customer.count()}")

# ═══════════════════════════════════════════════════════════════════════════════
# FACT_SALES
# ═══════════════════════════════════════════════════════════════════════════════
logger.info("Building fact_sales...")

# Join to get surrogate keys
fact = (
    df
    # date_id
    .withColumn("invoice_date_only", F.to_date("invoice_date"))
    .join(
        dim_date.select("date_id", "full_date"),
        F.col("invoice_date_only") == F.col("full_date"),
        "left",
    )
    # product_sk
    .join(
        dim_product.select("product_sk", "stock_code"),
        on="stock_code",
        how="left",
    )
    # customer_sk — join on current SCD2 version
    .join(
        dim_customer.filter(F.col("is_current") == True).select("customer_sk", "customer_id"),
        on="customer_id",
        how="left",
    )
    .select(
        F.monotonically_increasing_id().alias("fact_id"),
        F.col("invoice_no"),
        F.col("date_id"),
        F.col("customer_sk"),
        F.col("product_sk"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("line_total"),
        F.col("country"),
        F.col("invoice_date"),
        F.col("ingestion_ts"),
    )
)

fact.write.mode("overwrite").parquet(f"{SILVER}/fact_sales/")
logger.info(f"fact_sales rows: {fact.count()}")

logger.info("Job 02 (Bronze → Silver) completed successfully.")
job.commit()
