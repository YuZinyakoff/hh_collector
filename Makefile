PYTHON ?= $(if $(wildcard ./.venv/bin/python),./.venv/bin/python,python3)
COMPOSE ?= docker compose

ARGS ?=

.PHONY: up up-observability up-scheduler down migrate migrate-compose test lint format \
	show-metrics serve-metrics run-once-v2 trigger-run-now scheduler-loop \
	compose-health compose-show-metrics backup restore

up:
	$(COMPOSE) up -d postgres redis metrics

up-observability:
	$(COMPOSE) --profile observability up -d postgres redis metrics prometheus grafana

up-scheduler:
	$(COMPOSE) --profile ops up -d postgres redis metrics scheduler

down:
	$(COMPOSE) down

migrate:
	$(PYTHON) -m alembic upgrade head

migrate-compose:
	$(COMPOSE) --profile ops run --rm --entrypoint python app -m alembic upgrade head

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m mypy src

format:
	$(PYTHON) -m ruff format .

show-metrics:
	PYTHONPATH=src $(PYTHON) -m hhru_platform.interfaces.cli.main show-metrics

serve-metrics:
	PYTHONPATH=src $(PYTHON) -m hhru_platform.interfaces.cli.main serve-metrics

run-once-v2:
	PYTHONPATH=src $(PYTHON) -m hhru_platform.interfaces.cli.main run-once-v2 $(ARGS)

trigger-run-now:
	PYTHONPATH=src $(PYTHON) -m hhru_platform.interfaces.cli.main trigger-run-now $(ARGS)

scheduler-loop:
	PYTHONPATH=src $(PYTHON) -m hhru_platform.interfaces.cli.main scheduler-loop $(ARGS)

compose-health:
	$(COMPOSE) --profile ops run --rm app health-check

compose-show-metrics:
	$(COMPOSE) --profile ops run --rm app show-metrics

backup:
	$(COMPOSE) --profile ops run --rm backup

restore:
	@test -n "$(BACKUP_FILE)" || (echo "BACKUP_FILE=/backups/<file>.dump is required" >&2; exit 1)
	$(COMPOSE) --profile ops run --rm \
		-e HHRU_RESTORE_FILE="$(BACKUP_FILE)" \
		-e HHRU_RESTORE_CONFIRM=yes \
		backup /usr/local/bin/restore_postgres.sh
