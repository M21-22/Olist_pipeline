# ADR-0005: Valkey as Celery Message Broker

## Status
Accepted

## Context
CeleryExecutor requires a message broker to distribute tasks
from Airflow to worker processes.

The broker must be lightweight and suitable for local deployment.

## Decision
Use Valkey 8 with the Redis-compatible Celery transport.

Configure:
- Password authentication
- Append-only persistence (AOF)
- noeviction memory policy
- 21600-second visibility timeout
- Internal Docker networking only

## Alternatives Considered
- Redis 8
- RabbitMQ

## Rationale
- Valkey is Redis-protocol compatible.
- It has a small infrastructure footprint.
- It is open source under the BSD-3-Clause license.
- It integrates with Celery through the Redis transport.

## Consequences
Positive:
- Lightweight broker
- Simple configuration
- Persistent broker state
- No additional message-processing application required

Negative:
- The broker remains a single point of failure.
- Visibility timeout can cause task redelivery.
- AOF does not guarantee zero message loss.
- Worker crashes and retries require idempotent tasks.

## Implementation Constraints
- Do not publish the broker port.
- Never use an empty or default password.
- Use a persistent Docker volume.
- Verify that unauthenticated access is rejected.
- Monitor broker memory and task queues.
