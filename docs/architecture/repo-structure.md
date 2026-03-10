# Структура репозитория: hhru data collection platform

**Статус:** Draft v1

---

## 1. Цель

Структура репозитория должна:

- быть понятной человеку и агенту;
- разделять доменную логику, инфраструктуру и документацию;
- позволять запускать отдельные сервисы из одной кодовой базы;
- быть удобной для постепенной реализации MVP.

---

## 2. Предлагаемая структура

```text
hhru-data-platform/
├─ README.md
├─ Makefile
├─ pyproject.toml
├─ .env.example
├─ .gitignore
├─ docker-compose.yml
├─ docker/
│  ├─ app.Dockerfile
│  ├─ worker.Dockerfile
│  ├─ scheduler.Dockerfile
│  └─ backup.Dockerfile
├─ docs/
│  ├─ architecture/
│  │  ├─ hld.md
│  │  ├─ lld.md
│  │  ├─ adr/
│  │  │  ├─ 0001-weekly-global-sweep.md
│  │  │  ├─ 0002-state-tracking-required.md
│  │  │  ├─ 0003-selective-detail-fetch.md
│  │  │  ├─ 0004-raw-payload-retention.md
│  │  │  └─ 0005-single-vps-compose.md
│  │  ├─ repo-structure.md
│  │  └─ mvp-scope.md
│  ├─ data/
│  │  ├─ logical-data-model.md
│  │  └─ data-lifecycle.md
│  ├─ ops/
│  │  ├─ observability.md
│  │  ├─ backup-restore.md
│  │  └─ deployment.md
│  └─ api/
│     ├─ hh-api-notes.md
│     └─ api-contracts.md
├─ migrations/
│  ├─ env.py
│  └─ versions/
├─ scripts/
│  ├─ dev/
│  │  ├─ bootstrap.sh
│  │  ├─ reset_local_db.sh
│  │  └─ run_local_sweep.sh
│  ├─ ops/
│  │  ├─ create_run.py
│  │  ├─ rerun_partition.py
│  │  ├─ sync_dictionaries.py
│  │  └─ force_detail.py
│  └─ backup/
│     ├─ backup_postgres.sh
│     └─ restore_postgres.sh
├─ src/
│  └─ hhru_platform/
│     ├─ __init__.py
│     ├─ config/
│     │  ├─ settings.py
│     │  └─ logging.py
│     ├─ domain/
│     │  ├─ entities/
│     │  │  ├─ crawl_run.py
│     │  │  ├─ crawl_partition.py
│     │  │  ├─ vacancy.py
│     │  │  ├─ vacancy_snapshot.py
│     │  │  ├─ vacancy_seen_event.py
│     │  │  ├─ vacancy_current_state.py
│     │  │  ├─ employer.py
│     │  │  ├─ area.py
│     │  │  └─ professional_role.py
│     │  ├─ value_objects/
│     │  │  ├─ hashes.py
│     │  │  ├─ enums.py
│     │  │  └─ partition_key.py
│     │  └─ services/
│     │     ├─ change_detector.py
│     │     ├─ detail_policy.py
│     │     └─ state_reconciler.py
│     ├─ application/
│     │  ├─ commands/
│     │  │  ├─ create_crawl_run.py
│     │  │  ├─ plan_sweep.py
│     │  │  ├─ process_list_page.py
│     │  │  ├─ fetch_vacancy_detail.py
│     │  │  ├─ sync_dictionary.py
│     │  │  └─ reconcile_run.py
│     │  ├─ dto/
│     │  └─ policies/
│     ├─ infrastructure/
│     │  ├─ db/
│     │  │  ├─ base.py
│     │  │  ├─ models/
│     │  │  │  ├─ crawl_run.py
│     │  │  │  ├─ crawl_partition.py
│     │  │  │  ├─ api_request_log.py
│     │  │  │  ├─ raw_api_payload.py
│     │  │  │  ├─ vacancy.py
│     │  │  │  ├─ vacancy_seen_event.py
│     │  │  │  ├─ vacancy_current_state.py
│     │  │  │  ├─ vacancy_snapshot.py
│     │  │  │  ├─ detail_fetch_attempt.py
│     │  │  │  ├─ employer.py
│     │  │  │  ├─ area.py
│     │  │  │  └─ professional_role.py
│     │  │  ├─ repositories/
│     │  │  │  ├─ crawl_run_repo.py
│     │  │  │  ├─ crawl_partition_repo.py
│     │  │  │  ├─ vacancy_repo.py
│     │  │  │  └─ raw_payload_repo.py
│     │  │  └─ session.py
│     │  ├─ queue/
│     │  │  ├─ producer.py
│     │  │  ├─ consumer.py
│     │  │  └─ job_models.py
│     │  ├─ hh_api/
│     │  │  ├─ client.py
│     │  │  ├─ schemas.py
│     │  │  ├─ endpoints.py
│     │  │  └─ rate_limiter.py
│     │  ├─ normalization/
│     │  │  ├─ vacancy_short_normalizer.py
│     │  │  ├─ vacancy_detail_normalizer.py
│     │  │  ├─ employer_normalizer.py
│     │  │  └─ dictionary_normalizers.py
│     │  ├─ observability/
│     │  │  ├─ metrics.py
│     │  │  ├─ tracing.py
│     │  │  └─ error_reporting.py
│     │  ├─ storage/
│     │  │  └─ raw_storage.py
│     │  └─ backup/
│     │     └─ backup_service.py
│     ├─ interfaces/
│     │  ├─ cli/
│     │  │  ├─ main.py
│     │  │  └─ commands/
│     │  │     ├─ run.py
│     │  │     ├─ partition.py
│     │  │     ├─ dictionary.py
│     │  │     └─ health.py
│     │  └─ workers/
│     │     ├─ scheduler.py
│     │     ├─ planner_worker.py
│     │     ├─ list_worker.py
│     │     ├─ detail_worker.py
│     │     ├─ dictionary_worker.py
│     │     ├─ housekeeping_worker.py
│     │     └─ backup_worker.py
│     └─ tests/
│        ├─ unit/
│        ├─ integration/
│        ├─ contract/
│        └─ fixtures/
├─ monitoring/
│  ├─ prometheus/
│  │  └─ prometheus.yml
│  ├─ grafana/
│  │  ├─ datasources/
│  │  └─ dashboards/
│  ├─ loki/
│  │  └─ config.yml
│  └─ alerting/
│     └─ rules.yml
└─ .github/
   └─ workflows/
      ├─ ci.yml
      └─ lint-test.yml


````

---

## 3. Почему именно так

### `docs/`

Вся архитектура и операционная документация лежит рядом и версионируется вместе с кодом.

### `src/hhru_platform/domain`

Чистая доменная модель, которую проще держать максимально независимой от инфраструктуры.

### `src/hhru_platform/application`

Use cases / application services. Именно сюда удобно давать задачи Codex.

### `src/hhru_platform/infrastructure`

Внешние зависимости: БД, очередь, API, observability, backup.

### `src/hhru_platform/interfaces`

Entry points: CLI, workers, scheduler.

### `monitoring/`

Конфиги observability хранятся рядом с кодом.

### `scripts/`

Операционные скрипты, dev bootstrap и backup.

---

## 4. Что важно для Codex

Codex проще работать, если:

1. есть чёткие каталоги ответственности;
2. бизнес-логика не смешана с SQLAlchemy/HTTP;
3. есть отдельные application commands;
4. worker entrypoints тонкие и вызывают application layer;
5. tests и fixtures лежат рядом и предсказуемо.

---

## 5. Минимальные entrypoints v1

* `python -m hhru_platform.interfaces.workers.scheduler`
* `python -m hhru_platform.interfaces.workers.planner_worker`
* `python -m hhru_platform.interfaces.workers.list_worker`
* `python -m hhru_platform.interfaces.workers.detail_worker`
* `python -m hhru_platform.interfaces.workers.dictionary_worker`
* `python -m hhru_platform.interfaces.cli.main`      