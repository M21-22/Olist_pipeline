# ADR-0001: Orchestrated Micro-Batch ELT

## Status
Accepted

## Context
The platform simulates an operational e-commerce database
using the Olist dataset.

Data must be incrementally extracted, validated, loaded,
transformed, and exposed through an API.

The platform runs locally using Docker Compose.

## Decision
Use orchestrated micro-batch ELT with Apache Airflow.

Incremental extraction runs every 15 minutes using
watermarks and an overlap window.

## Alternatives Considered
- Continuous streaming with Kafka and Spark Structured Streaming
- Full batch extraction without incremental processing

## Rationale
- The simulated source is a PostgreSQL database.
- Sub-second latency is not required.
- Airflow provides scheduling, retries, and task dependencies.
- Micro-batches simplify validation and deterministic replay.
- Incremental extraction avoids repeatedly loading all source data.

## Consequences
Positive:
- Simpler local infrastructure
- Lower resource requirements
- Easier debugging and replay
- Clear data-quality checkpoints

Negative:
- Data freshness depends on the scheduling interval.
- Late updates and deletes require additional handling.
- Watermark management introduces implementation complexity.
