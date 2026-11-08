import os

import boto3
from botocore.exceptions import ClientError


def client(access_key, secret_key):
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


def check_access(s3, bucket, should_succeed):
    try:
        s3.put_object(
            Bucket=bucket,
            Key="smoke-test.txt",
            Body=b"Access test",
        )
        succeeded = True
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "AccessDenied":
            raise
        succeeded = False

    assert succeeded == should_succeed, f"Unexpected access result for bucket {bucket}"
    print(f"{bucket}: {'ALLOWED' if succeeded else 'DENIED'}")


def check_delete_denied(s3, bucket):
    try:
        s3.delete_object(
            Bucket=bucket,
            Key="smoke-test.txt",
        )
    except ClientError as exc:
        assert exc.response["Error"]["Code"] == "AccessDenied"
        print(f"{bucket}: DELETE DENIED")
    else:
        raise AssertionError(f"DELETE unexpectedly allowed on {bucket}")


pipeline = client(
    os.environ["S3_PIPELINE_ACCESS_KEY"],
    os.environ["S3_PIPELINE_SECRET_KEY"],
)

backup = client(
    os.environ["S3_BACKUP_ACCESS_KEY"],
    os.environ["S3_BACKUP_SECRET_KEY"],
)


print("Pipeline user:")
check_access(pipeline, "landing", True)
check_access(pipeline, "quarantine", True)
check_access(pipeline, "backups", False)

print("Backup user:")
check_access(backup, "backups", True)
check_access(backup, "landing", False)
check_access(backup, "quarantine", False)

check_delete_denied(pipeline, "landing")
check_delete_denied(pipeline, "quarantine")
check_delete_denied(backup, "backups")
