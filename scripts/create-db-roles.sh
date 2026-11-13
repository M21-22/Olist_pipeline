
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

set -a
source .env
set +a

COMPOSE=(docker compose --env-file .env -f infra/compose.yaml)

create_role() {
    local service="$1"
    local role="$2"
    local password="$3"

    echo "Configuring role: $role on $service"

    "${COMPOSE[@]}" exec -T "$service" \
        psql -X -v ON_ERROR_STOP=1 \
        -U postgres -d postgres \
        -v role_name="$role" \
        -v role_password="$password" <<'SQL'
SELECT format(
    'CREATE ROLE %I LOGIN PASSWORD %L',
    :'role_name',
    :'role_password'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles
    WHERE rolname = :'role_name'
)
\gexec

SELECT format(
    'ALTER ROLE %I WITH LOGIN PASSWORD %L',
    :'role_name',
    :'role_password'
)
\gexec
SQL
}

create_role source-db emulator_writer "$EMULATOR_WRITER_PASSWORD"
create_role source-db source_reader "$SOURCE_READER_PASSWORD"

create_role warehouse-db etl_loader "$ETL_LOADER_PASSWORD"
create_role warehouse-db ingest_audit_writer "$INGEST_AUDIT_WRITER_PASSWORD"
create_role warehouse-db dbt_runner "$DBT_RUNNER_PASSWORD"
create_role warehouse-db soda_reader "$SODA_READER_PASSWORD"
create_role warehouse-db api_reader "$API_READER_PASSWORD"
create_role warehouse-db grafana_reader "$GRAFANA_READER_PASSWORD"

echo "Database roles configured."
