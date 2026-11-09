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
