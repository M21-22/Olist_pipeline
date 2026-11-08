# ADR-0006: Docker Network Isolation

## Status
Accepted

## Context
The platform contains databases, a message broker, object storage,
and services with web interfaces.

Internal infrastructure should not be directly accessible
from the host or external networks.

## Decision
Use two Docker Compose networks:

1. data-net: internal network for data services
2. app-net: network for services requiring host access

Bind published ports to 127.0.0.1.

## Alternatives Considered
- One shared Docker network
- Publish every service port to localhost
- Separate networks for every service

## Rationale
- Internal networking reduces unnecessary exposure.
- Docker DNS allows services to communicate by service name.
- Host access is only needed for selected user interfaces.
- The configuration remains manageable in Docker Compose.

## Consequences
Positive:
- Reduced network exposure
- No direct host access to PostgreSQL or Valkey
- Clear separation between internal and external interfaces

Negative:
- Network configuration is more complex.
- Some services require membership in both networks.
- Internal-only containers cannot directly access the internet.

## Implementation Constraints
- Set data-net internal: true.
- Publish only required ports.
- Bind published ports to 127.0.0.1.
- Use Docker service names for internal communication.
- Verify network isolation during Phase 1 testing.
