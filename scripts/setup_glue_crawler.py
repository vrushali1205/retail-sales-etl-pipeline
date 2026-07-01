"""
scripts/setup_glue_crawler.py
------------------------------
Creates an AWS Glue Crawler pointed at your S3 raw/ folder,
runs it, and waits until the Data Catalog table is ready.

Usage:
    python scripts/setup_glue_crawler.py \\
        --bucket yourname-retail-pipeline \\
        --db retail_db \\
        --role arn:aws:iam::123456789012:role/GlueServiceRole

The IAM role must have:
  - AmazonS3ReadOnlyAccess (or S3 bucket access)
  - AWSGlueServiceRole
"""

import argparse
import time
import boto3
from botocore.exceptions import ClientError


def create_database(glue_client, db_name: str) -> None:
    try:
        glue_client.create_database(DatabaseInput={"Name": db_name})
        print(f"✅ Created Glue database: {db_name}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"ℹ️  Glue database already exists: {db_name}")


def create_crawler(glue_client, crawler_name: str, role: str, db_name: str, s3_path: str) -> None:
    try:
        glue_client.create_crawler(
            Name=crawler_name,
            Role=role,
            DatabaseName=db_name,
            Description="Crawl raw retail CSV in S3",
            Targets={"S3Targets": [{"Path": s3_path}]},
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG",
            },
            RecrawlPolicy={"RecrawlBehavior": "CRAWL_EVERYTHING"},
            TablePrefix="raw_",
        )
        print(f"✅ Created crawler: {crawler_name}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"ℹ️  Crawler already exists: {crawler_name}")


def run_crawler(glue_client, crawler_name: str) -> None:
    print(f"🚀 Starting crawler: {crawler_name}")
    glue_client.start_crawler(Name=crawler_name)

    print("  Waiting for crawler to complete", end="", flush=True)
    while True:
        time.sleep(10)
        response = glue_client.get_crawler(Name=crawler_name)
        state = response["Crawler"]["State"]
        print(".", end="", flush=True)

        if state == "READY":
            last_run = response["Crawler"].get("LastCrawl", {})
            status = last_run.get("Status", "UNKNOWN")
            print(f"\n✅ Crawler finished with status: {status}")

            if status == "FAILED":
                error = last_run.get("ErrorMessage", "Unknown error")
                raise RuntimeError(f"Crawler failed: {error}")
            break


def list_tables(glue_client, db_name: str) -> None:
    response = glue_client.get_tables(DatabaseName=db_name)
    tables = response.get("TableList", [])
    print(f"\n📋 Tables in '{db_name}':")
    for t in tables:
        print(f"   • {t['Name']}  ({t.get('StorageDescriptor', {}).get('Location', '')})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create and run Glue Crawler")
    parser.add_argument("--bucket",       required=True, help="S3 bucket name")
    parser.add_argument("--db",           required=True, help="Glue database name (e.g. retail_db)")
    parser.add_argument("--role",         required=True, help="IAM role ARN for Glue")
    parser.add_argument("--region",       default="ap-south-1", help="AWS region")
    parser.add_argument("--crawler-name", default="retail-raw-crawler", help="Crawler name")
    args = parser.parse_args()

    glue = boto3.client("glue", region_name=args.region)

    s3_path = f"s3://{args.bucket}/raw/"

    create_database(glue, args.db)
    create_crawler(glue, args.crawler_name, args.role, args.db, s3_path)
    run_crawler(glue, args.crawler_name)
    list_tables(glue, args.db)

    print(f"\n🎉 Done! Open Athena and run:")
    print(f"   SELECT * FROM {args.db}.raw_online_retail_ii LIMIT 10;")
