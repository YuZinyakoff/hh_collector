PYTHON ?= $(if $(wildcard ./.venv/bin/python),./.venv/bin/python,python3)
COMPOSE ?= docker compose

ARGS ?=

.PHONY: up up-observability up-scheduler down migrate migrate-compose test lint format \
	show-metrics serve-metrics run-once-v2 trigger-run-now scheduler-loop worker-detail \
	drain-first-detail-backlog run-housekeeping \
	run-backup verify-backup-file run-restore-drill compose-health compose-show-metrics \
	backup verify-backup restore restore-drill detail-worker-measurement \
	soak-test soak-test-no-build

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

drain-first-detail-backlog:
	PYTHONPATH=src $(PYTHON) -m hhru_platform.interfaces.cli.main drain-first-detail-backlog $(ARGS)

worker-detail:
	PYTHONPATH=src $(PYTHON) -m hhru_platform.interfaces.workers.detail_worker $(ARGS)

detail-worker-measurement:
	bash ./scripts/dev/launch_detail_worker_measurement_tmux.sh

run-housekeeping:
	PYTHONPATH=src $(PYTHON) -m hhru_platform.interfaces.cli.main run-housekeeping $(ARGS)

run-backup:
	PYTHONPATH=src $(PYTHON) -m hhru_platform.interfaces.cli.main run-backup $(ARGS)

verify-backup-file:
	@test -n "$(BACKUP_FILE)" || (echo "BACKUP_FILE=.state/backups/<file>.dump is required" >&2; exit 1)
	PYTHONPATH=src $(PYTHON) -m hhru_platform.interfaces.cli.main verify-backup-file --backup-file "$(BACKUP_FILE)" $(ARGS)

run-restore-drill:
	@test -n "$(BACKUP_FILE)" || (echo "BACKUP_FILE=.state/backups/<file>.dump is required" >&2; exit 1)
	PYTHONPATH=src $(PYTHON) -m hhru_platform.interfaces.cli.main run-restore-drill --backup-file "$(BACKUP_FILE)" $(ARGS)

compose-health:
	$(COMPOSE) --profile ops run --rm app health-check

compose-show-metrics:
	$(COMPOSE) --profile ops run --rm app show-metrics

backup:
	$(COMPOSE) --profile ops run --rm app run-backup $(ARGS)

verify-backup:
	@test -n "$(BACKUP_FILE)" || (echo "BACKUP_FILE=.state/backups/<file>.dump is required" >&2; exit 1)
	$(COMPOSE) --profile ops run --rm app verify-backup-file --backup-file "$(BACKUP_FILE)" $(ARGS)

restore:
	@test -n "$(BACKUP_FILE)" || (echo "BACKUP_FILE=/backups/<file>.dump is required" >&2; exit 1)
	$(COMPOSE) --profile ops run --rm \
		--entrypoint /usr/local/bin/restore_postgres.sh \
		-e HHRU_RESTORE_FILE="$(BACKUP_FILE)" \
		-e HHRU_RESTORE_CONFIRM=yes \
		backup

restore-drill:
	@test -n "$(BACKUP_FILE)" || (echo "BACKUP_FILE=.state/backups/<file>.dump is required" >&2; exit 1)
	$(COMPOSE) --profile ops run --rm app run-restore-drill --backup-file "$(BACKUP_FILE)" $(if $(TARGET_DB),--target-db "$(TARGET_DB)",) $(ARGS)

soak-test:
	$(COMPOSE) --profile ops --profile observability up -d postgres redis metrics prometheus grafana scheduler

soak-test-no-build:
	$(COMPOSE) --profile ops --profile observability up -d --no-build postgres redis metrics prometheus grafana scheduler
