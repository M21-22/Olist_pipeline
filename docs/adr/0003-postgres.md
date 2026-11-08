# ADR-0003: PostgreSQL 17 and Database Isolation

## Status
Accepted

## Context
The platform needs an operational source database, an analytical
warehouse, and persistent Airflow metadata.

Running a separate PostgreSQL server for every database would
increase local resource consumption.

## Decision
Use PostgreSQL 17 with two containers:

1. source-db: database `source`
2. warehouse-db: databases `warehouse` and `airflow`

Use separate database roles and explicit privileges.

## Alternatives Considered
- One PostgreSQL container for everything
- Three independent PostgreSQL containers
- Separate analytical warehouse technology

## Rationale
- Separate source and warehouse containers simulate system boundaries.
- Airflow metadata and analytical data remain logically isolated.
- Sharing one server for warehouse and Airflow reduces resource usage.
- PostgreSQL supports transactions, schemas, roles, and extensions.

## Consequences
Positive:
- Lower infrastructure overhead
- Realistic source-to-warehouse extraction
- Independent source and warehouse access controls
- SQL-based administration and testing

Negative:
- Warehouse and Airflow metadata share server resources.
- A warehouse PostgreSQL failure also affects Airflow metadata.
- PostgreSQL is not a distributed analytical warehouse.

## Implementation Constraints
- Use least-privilege application roles.
- Restrict database connections.
- Do not expose PostgreSQL ports to the host.
- Use Docker named volumes for persistence.
- Configure warehouse PostgreSQL max_connections to 150.
