"""
Glue Job 03: Silver → Gold
---------------------------
Reads Silver Parquet tables and builds aggregated Gold-layer views
for business intelligence / reporting use cases.

Gold tables produced:
  gold_monthly_sales     - Revenue + orders by country + month
  gold_top_products      - Top products by revenue
  gold_customer_ltv      - Customer lifetime value summary

Deploy as an AWS Glue Spark Job.
Required Job Parameters:
  --S3_SILVER_PATH  s3://yourname-retail-pipeline/processed/silver/
  --S3_GOLD_PATH    s3://yourname-retail-pipeline/processed/gold/
  --TempDir         s3://yourname-retail-pipeline/tmp/
"""

import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_SILVER_PATH", "S3_GOLD_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER = args["S3_SILVER_PATH"]
GOLD   = args["S3_GOLD_PATH"]

# ── Load Silver tables ────────────────────────────────────────────────────────
fact         = spark.read.parquet(f"{SILVER}/fact_sales/")
dim_date     = spark.read.parquet(f"{SILVER}/dim_date/")
dim_product  = spark.read.parquet(f"{SILVER}/dim_product/")
dim_customer = spark.read.parquet(f"{SILVER}/dim_customer/")

# Enrich fact with date attributes
fact_enriched = (
    fact
    .join(dim_date.select("date_id", "year", "month", "month_name"), on="date_id", how="left")
    .join(dim_product.select("product_sk", "stock_code", "description"), on="product_sk", how="left")
    .join(
        dim_customer.filter(F.col("is_current") == True)
        .select("customer_sk", "customer_id"),
        on="customer_sk",
        how="left",
    )
)

# ═══════════════════════════════════════════════════════════════════════════════
# GOLD 1 — Monthly Sales Summary by Country
# ═══════════════════════════════════════════════════════════════════════════════
logger.info("Building gold_monthly_sales...")

gold_monthly = (
    fact_enriched
    .groupBy("year", "month", "month_name", "country")
    .agg(
        F.round(F.sum("line_total"), 2).alias("total_revenue"),
        F.countDistinct("invoice_no").alias("total_orders"),
        F.sum("quantity").alias("total_units_sold"),
        F.round(F.avg("line_total"), 2).alias("avg_order_value"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
    .orderBy("year", "month", F.desc("total_revenue"))
)

gold_monthly.write.mode("overwrite").parquet(f"{GOLD}/monthly_sales/")
logger.info(f"gold_monthly_sales rows: {gold_monthly.count()}")

# ═══════════════════════════════════════════════════════════════════════════════
# GOLD 2 — Top Products by Revenue
# ═══════════════════════════════════════════════════════════════════════════════
logger.info("Building gold_top_products...")

gold_products = (
    fact_enriched
    .groupBy("stock_code", "description")
    .agg(
        F.round(F.sum("line_total"), 2).alias("total_revenue"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("invoice_no").alias("times_ordered"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .orderBy(F.desc("total_revenue"))
)

gold_products.write.mode("overwrite").parquet(f"{GOLD}/top_products/")
logger.info(f"gold_top_products rows: {gold_products.count()}")

# ═══════════════════════════════════════════════════════════════════════════════
# GOLD 3 — Customer Lifetime Value
# ═══════════════════════════════════════════════════════════════════════════════
logger.info("Building gold_customer_ltv...")

gold_ltv = (
    fact_enriched
    .groupBy("customer_id", "country")
    .agg(
        F.round(F.sum("line_total"), 2).alias("lifetime_value"),
        F.countDistinct("invoice_no").alias("total_orders"),
        F.sum("quantity").alias("total_units"),
        F.min("invoice_date").alias("first_purchase_date"),
        F.max("invoice_date").alias("last_purchase_date"),
        F.round(F.avg("line_total"), 2).alias("avg_order_value"),
    )
    .withColumn(
        "days_as_customer",
        F.datediff(F.col("last_purchase_date"), F.col("first_purchase_date")),
    )
    .orderBy(F.desc("lifetime_value"))
)

gold_ltv.write.mode("overwrite").parquet(f"{GOLD}/customer_ltv/")
logger.info(f"gold_customer_ltv rows: {gold_ltv.count()}")

logger.info("Job 03 (Silver → Gold) completed successfully.")
job.commit()
