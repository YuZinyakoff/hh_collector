PYTHON ?= python3
COMPOSE ?= docker compose

.PHONY: up up-observability down migrate migrate-compose test lint format \
	show-metrics serve-metrics compose-health compose-show-metrics backup restore

up:
	$(COMPOSE) up -d postgres redis metrics

up-observability:
	$(COMPOSE) --profile observability up -d postgres redis metrics prometheus

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
