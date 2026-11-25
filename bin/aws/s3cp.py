#!/usr/bin/env python3
"""Copies one file to or from the exchange bucket over HTTPS, for the bin/aws/*.sh scripts (no aws CLI needed).

Usage: s3cp.py put <local-file> <key>
       s3cp.py get <key> <local-file>
  Bucket and region come from $BUCKET and $AWS_REGION, credentials from boto3's usual chain.
"""
import os
import sys

import boto3
from botocore.exceptions import ClientError


def main(argv, s3=None):
    if len(argv) != 3 or argv[0] not in ("put", "get"):
        print(__doc__, file=sys.stderr)
        return 2
    bucket = os.environ.get("BUCKET")
    region = os.environ.get("AWS_REGION")
    if not bucket or not region:
        print("BUCKET and AWS_REGION are required", file=sys.stderr)
        return 2
    s3 = s3 or boto3.client("s3", region_name=region)
    verb, first, second = argv
    try:
        if verb == "put":
            s3.upload_file(first, bucket, second)
            print(f"uploaded {first} ({os.path.getsize(first) / 2**20:.1f} MiB) to s3://{bucket}/{second}", file=sys.stderr)
        else:
            s3.download_file(bucket, first, second)
            print(f"downloaded s3://{bucket}/{first} to {second}", file=sys.stderr)
        result = 0
    except (ClientError, boto3.exceptions.S3UploadFailedError) as error:
        print(f"s3cp {verb} failed: {error}", file=sys.stderr)
        result = 1
    return result


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
