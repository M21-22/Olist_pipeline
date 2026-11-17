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

.PHONY: check2

check2:
	bash scripts/check-phase2.sh

# Ephemeral regression stack
TEST_COMPOSE := docker compose -f infra/compose.test.yaml

.PHONY: test test-unit test-integration test-clean

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
