"""
scripts/upload_to_s3.py
------------------------
Upload the Kaggle Online Retail II CSV to S3 raw/ folder.

Usage:
    python scripts/upload_to_s3.py \\
        --bucket yourname-retail-pipeline \\
        --file /path/to/online_retail_II.csv

AWS credentials: configure via `aws configure` or environment variables
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
"""

import argparse
import os
import boto3
from botocore.exceptions import ClientError


def upload_to_s3(local_file: str, bucket: str, s3_key: str) -> None:
    s3 = boto3.client("s3")
    file_size = os.path.getsize(local_file) / (1024 * 1024)
    print(f"Uploading {local_file} ({file_size:.1f} MB) → s3://{bucket}/{s3_key}")

    # Use multipart upload for large files
    config = boto3.s3.transfer.TransferConfig(
        multipart_threshold=10 * 1024 * 1024,   # 10 MB
        max_concurrency=10,
    )

    try:
        s3.upload_file(
            local_file,
            bucket,
            s3_key,
            Config=config,
            Callback=ProgressCallback(local_file),
        )
        print(f"\n✅ Upload complete: s3://{bucket}/{s3_key}")
    except ClientError as e:
        print(f"\n❌ Upload failed: {e}")
        raise


class ProgressCallback:
    """Print upload progress to console."""

    def __init__(self, filename: str):
        self._size = os.path.getsize(filename)
        self._seen = 0

    def __call__(self, bytes_transferred: int):
        self._seen += bytes_transferred
        pct = (self._seen / self._size) * 100
        print(f"\r  Progress: {pct:.1f}%", end="", flush=True)


def create_folder_markers(bucket: str) -> None:
    """Create the raw/, staging/, processed/ folder markers in S3."""
    s3 = boto3.client("s3")
    for folder in ["raw/", "staging/", "processed/", "tmp/"]:
        try:
            s3.put_object(Bucket=bucket, Key=folder)
            print(f"  Created folder: s3://{bucket}/{folder}")
        except ClientError as e:
            print(f"  Warning: could not create {folder}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload raw CSV to S3")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--file",   required=True, help="Local path to CSV file")
    parser.add_argument(
        "--key",
        default="raw/online_retail_II.csv",
        help="S3 key (default: raw/online_retail_II.csv)",
    )
    args = parser.parse_args()

    if not os.path.exists(args.file):
        raise FileNotFoundError(f"File not found: {args.file}")

    print(f"\n📦 Setting up S3 bucket structure in: {args.bucket}")
    create_folder_markers(args.bucket)

    print()
    upload_to_s3(args.file, args.bucket, args.key)
