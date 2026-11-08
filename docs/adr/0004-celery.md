# ADR-0004: Airflow CeleryExecutor

## Status
Accepted

## Context
The platform needs to orchestrate incremental extraction, validation,
loading, transformations, reconciliation, and recovery.

Some tasks are lightweight, while others are resource-intensive.

## Decision
Use Apache Airflow 3.3.2 with CeleryExecutor.

Deploy:
- 2 general workers listening to `default` and `ingest`
- 1 batch worker listening to `batch`

General workers have concurrency 4 each.
The batch worker has concurrency 2.

## Alternatives Considered
- SequentialExecutor
- LocalExecutor
- KubernetesExecutor

## Rationale
- CeleryExecutor distributes tasks across worker processes.
- Separate queues isolate ingestion from heavy batch workloads.
- Worker replicas demonstrate horizontal task distribution.
- Workers can be scaled independently.

## Consequences
Positive:
- Parallel task execution
- Queue-based workload isolation
- Worker-level failure testing
- Explicit concurrency management

Negative:
- Requires a message broker and result backend.
- Increases memory consumption and configuration complexity.
- All workers on one Docker host do not provide host-level fault tolerance.
- Tasks must tolerate retries and duplicate execution.

## Implementation Constraints
- Set Airflow parallelism to 12.
- Use PostgreSQL for the Celery result backend.
- Keep DAGs and dependencies available to every worker.
- Verify task execution across at least two workers.
- Make processing tasks idempotent.
