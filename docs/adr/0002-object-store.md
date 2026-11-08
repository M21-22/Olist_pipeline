# ADR-0002: S3-Compatible Object Storage

## Status
Accepted

## Context
The platform requires persistent storage for extracted Parquet files,
batch manifests, quarantined data, and backups.

Storage must support bucket-level access controls, object versioning,
and lifecycle management.

## Decision
Use a maintained MinIO-compatible community fork, exposed through
the S3 API.

The Docker Compose service will be named `minio`.

Storage endpoints, credentials, and image versions will be
configured through environment variables.

## Alternatives Considered
- Upstream MinIO
- RustFS
- SeaweedFS
- Garage
- Cloud-managed S3 storage

## Rationale
- S3 compatibility allows standard Python libraries such as boto3.
- Local deployment avoids cloud infrastructure costs.
- Object versioning protects previous versions of overwritten files.
- Bucket policies support least-privilege access.
- The selected fork aims to preserve MinIO administration capabilities.

## Consequences
Positive:
- Portable S3-compatible integration
- Local, cost-free development
- Separate landing, quarantine, and backup buckets
- Support for replay and recovery

Negative:
- Community-fork maintenance and security risks
- Compatibility must be verified before deployment
- The storage service is a single point of failure

## Implementation Constraints
- Enable bucket versioning.
- Expire noncurrent object versions after 30 days.
- Airflow may read and write landing and quarantine, but not delete.
- Backup credentials have separate permissions.
- Verify the exact container image and client versions before Phase 1.
