PYTHON ?= $(if $(wildcard ./.venv/bin/python),./.venv/bin/python,python3)
COMPOSE ?= docker compose

ARGS ?=

.PHONY: up up-observability up-scheduler down migrate migrate-compose test lint format \
	show-metrics serve-metrics run-once-v2 trigger-run-now scheduler-loop worker-detail \
	drain-first-detail-backlog run-housekeeping \
	run-backup verify-backup-file run-restore-drill sync-backup-offsite verify-backup-offsite-cli \
	run-backup-offsite-restore-drill run-export-research-archive run-verify-research-archive \
	run-sync-research-archive-offsite run-verify-research-archive-offsite \
	compose-health compose-show-metrics \
	backup verify-backup restore restore-drill backup-offsite verify-backup-offsite backup-offsite-restore-drill export-research-archive verify-research-archive sync-research-archive-offsite verify-research-archive-offsite detail-worker-measurement \
	vps-first-detail-measurement \
	soak-test soak-test-no-build

up:
	$(COMPOSE) up -d postgres redis metrics

up-observability:
	$(COMPOSE) --profile observability up -d postgres redis metrics prometheus alertmanager alert-webhook grafana node-exporter cadvisor

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

vps-first-detail-measurement:
	bash ./scripts/dev/run_vps_first_detail_measurement.sh

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

sync-backup-offsite:
	PYTHONPATH=src $(PYTHON) -m hhru_platform.interfaces.cli.main sync-backup-offsite $(ARGS)

verify-backup-offsite-cli:
	PYTHONPATH=src $(PYTHON) -m hhru_platform.interfaces.cli.main verify-backup-offsite $(if $(BACKUP_FILE),--backup-file "$(BACKUP_FILE)",) $(ARGS)

run-backup-offsite-restore-drill:
	PYTHONPATH=src $(PYTHON) -m hhru_platform.interfaces.cli.main run-backup-offsite-restore-drill $(if $(BACKUP_FILE),--backup-file "$(BACKUP_FILE)",) $(if $(TARGET_DB),--target-db "$(TARGET_DB)",) $(ARGS)

run-export-research-archive:
	PYTHONPATH=src $(PYTHON) -m hhru_platform.interfaces.cli.main export-research-archive $(ARGS)

run-verify-research-archive:
	PYTHONPATH=src $(PYTHON) -m hhru_platform.interfaces.cli.main verify-research-archive $(ARGS)

run-sync-research-archive-offsite:
	PYTHONPATH=src $(PYTHON) -m hhru_platform.interfaces.cli.main sync-research-archive-offsite $(ARGS)

run-verify-research-archive-offsite:
	PYTHONPATH=src $(PYTHON) -m hhru_platform.interfaces.cli.main verify-research-archive-offsite $(ARGS)

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

backup-offsite:
	$(COMPOSE) --profile ops run --rm app sync-backup-offsite $(ARGS)

verify-backup-offsite:
	$(COMPOSE) --profile ops run --rm app verify-backup-offsite $(if $(BACKUP_FILE),--backup-file "$(BACKUP_FILE)",) $(ARGS)

backup-offsite-restore-drill:
	$(COMPOSE) --profile ops run --rm app run-backup-offsite-restore-drill $(if $(BACKUP_FILE),--backup-file "$(BACKUP_FILE)",) $(if $(TARGET_DB),--target-db "$(TARGET_DB)",) $(ARGS)

export-research-archive:
	$(COMPOSE) --profile ops run --rm app export-research-archive $(ARGS)

verify-research-archive:
	$(COMPOSE) --profile ops run --rm app verify-research-archive $(ARGS)

sync-research-archive-offsite:
	$(COMPOSE) --profile ops run --rm app sync-research-archive-offsite $(ARGS)

verify-research-archive-offsite:
	$(COMPOSE) --profile ops run --rm app verify-research-archive-offsite $(ARGS)

soak-test:
	$(COMPOSE) --profile ops --profile observability up -d postgres redis metrics prometheus alertmanager alert-webhook grafana node-exporter cadvisor scheduler

soak-test-no-build:
	$(COMPOSE) --profile ops --profile observability up -d --no-build postgres redis metrics prometheus alertmanager alert-webhook grafana node-exporter cadvisor scheduler
