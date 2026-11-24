COMPOSE := docker compose -f infra/compose.yaml
EMU := $(COMPOSE) --profile emulator run --rm -T emulator

.PHONY: up down ps logs \
        emulator-build emulator-profile emulator-migrate \
        emulator-seed emulator-backfill emulator-run \
        emulator-up emulator-down emulator-status \
        emulator-verify emulator-fingerprint emulator-reset

# Infrastructure
up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f

.PHONY: profile-soda-drift apply-soda-drift
profile-soda-drift apply-soda-drift:
	@test -n "$$CLEAN_FROM" && test -n "$$CLEAN_UNTIL" || \
	  { echo "Set CLEAN_FROM and CLEAN_UNTIL to the UTC bounds of a defect-free capture."; exit 2; }
	$(COMPOSE) exec -T warehouse-db psql -X -v ON_ERROR_STOP=1 \
	  -v clean_from="$$CLEAN_FROM" -v clean_until="$$CLEAN_UNTIL" \
	  -v apply=$(if $(filter apply-soda-drift,$@),true,false) \
	  -U postgres -d warehouse --single-transaction -f - < quality/soda/calibrate.sql

# Rebuild the warehouse database on the existing PostgreSQL instance.
AIRFLOW_SERVICES := airflow-api-server airflow-scheduler airflow-dag-processor \
                    airflow-worker-general-1 airflow-worker-general-2 airflow-worker-batch
WAREHOUSE_SQL := infra/postgres/warehouse-init
DBT_PROJECT := /opt/airflow/dbt
DBT_FLAGS := --project-dir $(DBT_PROJECT) --profiles-dir $(DBT_PROJECT) \
             --target-path /tmp/olist-bootstrap-target --log-path /tmp/olist-bootstrap-logs

.PHONY: warehouse-rebuild
warehouse-rebuild: SHELL := /bin/bash
warehouse-rebuild: COMPOSE := docker compose --env-file .env -f infra/compose.yaml
warehouse-rebuild:
	@test -s .env && test -r scripts/create-db-roles.sh && test -s $(WAREHOUSE_SQL)/01-airflow.sql
	$(COMPOSE) config --quiet
	$(COMPOSE) build airflow-worker-batch
	$(COMPOSE) up -d --no-recreate --no-deps --wait source-db warehouse-db valkey minio
	bash scripts/create-db-roles.sh
	$(COMPOSE) stop $(AIRFLOW_SERVICES)
	@set -e; set -a; source .env; set +a; : "$${AIRFLOW_DB_PASSWORD:?missing AIRFLOW_DB_PASSWORD}"; \
	  export LC_ALL=C; files=($(WAREHOUSE_SQL)/*.sql); \
	  for file in "$${files[@]}"; do test -s "$$file"; done; \
	  $(COMPOSE) exec -T warehouse-db dropdb -U postgres --maintenance-db=postgres --if-exists --force warehouse; \
	  $(COMPOSE) exec -T warehouse-db createdb -U postgres --maintenance-db=postgres warehouse; \
	  for file in "$${files[@]}"; do \
	    echo "Applying $$file"; \
	    $(COMPOSE) exec -T -e AIRFLOW_DB_PASSWORD warehouse-db \
	      psql -X -v ON_ERROR_STOP=1 -U postgres -d warehouse \
	      -c '\getenv airflow_db_password AIRFLOW_DB_PASSWORD' -f - < "$$file"; \
	  done
	$(COMPOSE) run --rm --no-deps -T airflow-init
	$(COMPOSE) run --rm --no-deps -T --entrypoint /home/airflow/dbt-venv/bin/dbt \
	  airflow-worker-batch run --full-refresh $(DBT_FLAGS)
	$(COMPOSE) run --rm --no-deps -T -e SODA_HOST=warehouse-db \
	  --entrypoint /home/airflow/soda-venv/bin/soda airflow-worker-batch \
	  test-connection -d warehouse -c /opt/airflow/soda/configuration.yml
	$(COMPOSE) up -d --no-deps --wait --wait-timeout 240 $(AIRFLOW_SERVICES)

# Emulator
emulator-build:
	$(COMPOSE) --profile emulator build emulator

emulator-profile:
	$(EMU) profile

emulator-migrate:
	$(EMU) migrate

emulator-seed:
	$(EMU) seed

emulator-backfill:
	$(EMU) run --backfill

emulator-run:
	$(EMU) run

emulator-up:
	$(COMPOSE) --profile emulator up -d emulator

emulator-down:
	$(COMPOSE) --profile emulator stop emulator

emulator-status:
	$(EMU) status

emulator-verify:
	$(EMU) verify

emulator-fingerprint:
	$(EMU) fingerprint

emulator-reset:
	@read -p "Delete all emulator data and state? [y/N] " answer; \
	if [ "$$answer" = "y" ]; then \
		$(EMU) reset; \
	else \
		echo "Cancelled"; \
	fi

# Ephemeral regression stack
TEST_COMPOSE := docker compose -f infra/compose.test.yaml

.PHONY: test test-unit test-integration test-clean test-d12 test-warehouse-quality

test:
	@set -e; trap '$(TEST_COMPOSE) down -v --remove-orphans' EXIT; \
	  $(TEST_COMPOSE) build test-runner; \
	  $(TEST_COMPOSE) up -d --wait test-db test-s3; \
	  $(TEST_COMPOSE) run --rm test-runner

test-unit:
	$(TEST_COMPOSE) build test-runner
	$(TEST_COMPOSE) up -d --wait test-db test-s3
	$(TEST_COMPOSE) run --rm test-runner -v -ra /opt/airflow/tests/unit

test-integration:
	$(TEST_COMPOSE) build test-runner
	$(TEST_COMPOSE) up -d --wait test-db test-s3
	$(TEST_COMPOSE) run --rm test-runner -v -ra /opt/airflow/tests/integration

test-clean:
	$(TEST_COMPOSE) down -v --remove-orphans

# Run the warehouse quality suite in a disposable Compose project.
test-d12: WAREHOUSE_QUALITY_SELECTION = -k d12
test-d12: test-warehouse-quality

test-warehouse-quality:
	@set -e; project="olist-warehouse-quality-$$$$"; \
	  trap '$(TEST_COMPOSE) -p "$$project" down -v --remove-orphans' EXIT; \
	  $(TEST_COMPOSE) -p "$$project" build test-runner; \
	  $(TEST_COMPOSE) -p "$$project" up -d --wait test-db test-s3; \
	  $(TEST_COMPOSE) -p "$$project" run --rm -e OLIST_TEST_WAREHOUSE_QUALITY=1 test-runner \
	    -v -ra /opt/airflow/tests/acceptance/test_warehouse_quality.py $(WAREHOUSE_QUALITY_SELECTION)
