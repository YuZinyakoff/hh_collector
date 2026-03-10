Да, это хороший следующий шаг.

Под **bootstrap проекта** я имею в виду не что-то сложное, а минимальный набор файлов и команд, который позволяет новому человеку или Codex сделать путь:

**git clone → поднять локальную среду → применить миграции → запустить первый smoke flow**

То есть bootstrap — это:

* `README.md` с понятным стартом,
* `AGENTS.md` с правилами для Codex,
* `.env.example`,
* `docker-compose.yml`,
* `Makefile` или набор команд,
* базовая DDL/миграция,
* минимальные entrypoints и smoke-команды.

Для самого Codex это особенно полезно, потому что CLI запускается в текущей директории, работает с кодом локально, а проектные инструкции лучше всего задавать через `AGENTS.md`; у Codex также есть пользовательский и проектный `config.toml` (`~/.codex/config.toml` и `.codex/config.toml`). Установка CLI официально описана через npm/Homebrew, запуск — просто `codex`, затем вход через аккаунт ChatGPT или API key. ([OpenAI Developers][1])

Я бы прямо сейчас добавил в репозиторий ещё **три стартовых файла**:

* `README.md`
* `AGENTS.md`
* `schema.sql` или первая Alembic migration

Ниже даю готовые заготовки.

---

## 1. `README.md`

````md
# hhru-data-platform

Stateful платформа длительного накопления данных о вакансиях hh.ru.

## Что делает проект

Платформа предназначена для:

- регулярного сбора вакансий через hh API;
- сохранения сырых API-ответов;
- накопления истории наблюдений вакансий во времени;
- выборочной детализации карточек вакансий;
- подготовки данных для последующего анализа.

На текущем этапе проект сфокусирован на **накоплении данных**, а не на исследовательском enrichment-слое.

---

## Архитектурная идея

Система работает как **stateful crawler**:

- запускает регулярный глобальный sweep пространства поиска;
- фиксирует факт наблюдения вакансии;
- обновляет текущее состояние наблюдения;
- выборочно запрашивает detail-карточки;
- хранит raw + normalized данные;
- сохраняет историю запусков и партиций.

---

## Основные документы

- `docs/architecture/hld.md`
- `docs/architecture/lld.md`
- `docs/data/logical-data-model.md`
- `docs/architecture/mvp-scope.md`
- `docs/architecture/adr/`

---

## Технологический стек v1

- Python 3.12+
- PostgreSQL
- Redis / Valkey
- Docker Compose
- Alembic
- Prometheus
- Grafana
- Loki
- GlitchTip

---

## Быстрый старт

### 1. Подготовить окружение

Скопировать файл окружения:

```bash
cp .env.example .env
````

Заполнить переменные:

* `DB_DSN`
* `REDIS_DSN`
* `HH_API_BASE_URL`
* `HH_USER_AGENT`

### 2. Поднять инфраструктуру

```bash
docker compose up -d postgres redis
```

### 3. Применить миграции

```bash
make migrate
```

### 4. Запустить dictionary sync

```bash
make sync-dictionaries
```

### 5. Запустить тестовый sweep

```bash
make run-local-sweep
```

---

## Полезные команды

```bash
make up
make down
make migrate
make test
make lint
make format
make create-run
make sync-dictionaries
make run-local-sweep
make worker-list
make worker-detail
make scheduler
```

---

## Структура проекта

См.:

* `docs/architecture/repo-structure.md`

---

## Статус

Проект в стадии активного проектирования и scaffold-реализации MVP.

---

````

---

## 2. `AGENTS.md`

Это очень советую добавить **до первого прогона Codex**, потому что Codex читает `AGENTS.md` до начала работы и использует его как долговременные инструкции по репозиторию. :contentReference[oaicite:1]{index=1}

```md
# AGENTS.md

## Project overview

This repository contains a stateful data collection platform for long-term accumulation of vacancy data from hh.ru.

Current focus:
- scaffolding
- operational data model
- crawl orchestration
- state tracking
- observability
- local/prod reproducibility

Do not implement research-specific enrichment unless explicitly requested.

---

## Architectural priorities

1. Keep the system stateful.
2. Preserve history of observations.
3. Keep raw API payloads.
4. Separate domain logic from infrastructure.
5. Prefer explicit code over clever abstractions.
6. Keep worker entrypoints thin.
7. Prefer incremental, testable changes.

---

## Code style expectations

- Python 3.12+
- Type hints required for public functions
- Small focused modules
- No giant god-classes
- Prefer dataclasses / pydantic models where appropriate
- SQLAlchemy models separated from domain entities
- Business logic should not live inside ORM models

---

## Repository rules

- Architecture docs live in `docs/`
- DB models live in `src/hhru_platform/infrastructure/db/models/`
- Domain entities live in `src/hhru_platform/domain/entities/`
- Application use cases live in `src/hhru_platform/application/commands/`
- CLI entrypoints live in `src/hhru_platform/interfaces/cli/`
- Worker entrypoints live in `src/hhru_platform/interfaces/workers/`

---

## Implementation rules

When implementing a feature:
1. read the relevant architecture docs first;
2. update or create tests;
3. avoid breaking existing contracts;
4. do not silently rename core concepts;
5. keep logging and metrics in mind for worker code.

---

## For scaffold tasks

When creating scaffold:
- include `pyproject.toml`
- include Docker Compose
- include Alembic setup
- include `.env.example`
- include Makefile commands
- include a minimal test structure

---

## For database tasks

Core tables are critical and should not be skipped:
- crawl_run
- crawl_partition
- api_request_log
- raw_api_payload
- vacancy
- vacancy_seen_event
- vacancy_current_state
- vacancy_snapshot

Do not over-optimize schema before MVP.

---

## For worker tasks

Workers must:
- be idempotent where possible;
- log structured context;
- expose metrics hooks;
- fail safely;
- avoid crashing the entire run because of one vacancy.

---

## Output preference

When asked to implement:
- first make the smallest correct version;
- then improve structure;
- avoid speculative complexity unless requested.

````

---

## 3. `schema.sql` — стартовая DDL-схема PostgreSQL

Это не финальная production-schema на все времена, а очень хороший **v1 baseline**, от которого уже можно делать первую Alembic migration.

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =========================================================
-- control plane
-- =========================================================

CREATE TABLE crawl_run (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_type TEXT NOT NULL,
    status TEXT NOT NULL,
    triggered_by TEXT NOT NULL DEFAULT 'system',
    config_snapshot_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    partitions_total INT NOT NULL DEFAULT 0,
    partitions_done INT NOT NULL DEFAULT 0,
    partitions_failed INT NOT NULL DEFAULT 0,
    notes TEXT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ NULL
);

CREATE INDEX idx_crawl_run_status ON crawl_run(status);
CREATE INDEX idx_crawl_run_started_at ON crawl_run(started_at DESC);

CREATE TABLE crawl_partition (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    crawl_run_id UUID NOT NULL REFERENCES crawl_run(id) ON DELETE CASCADE,
    partition_key TEXT NOT NULL,
    params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL,
    pages_total_expected INT NULL,
    pages_processed INT NOT NULL DEFAULT 0,
    items_seen INT NOT NULL DEFAULT 0,
    retry_count INT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NULL,
    finished_at TIMESTAMPTZ NULL,
    last_error_message TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX uq_crawl_partition_run_key
    ON crawl_partition(crawl_run_id, partition_key);

CREATE INDEX idx_crawl_partition_run_id
    ON crawl_partition(crawl_run_id);

CREATE INDEX idx_crawl_partition_status
    ON crawl_partition(status);

-- =========================================================
-- api logging / raw
-- =========================================================

CREATE TABLE api_request_log (
    id BIGSERIAL PRIMARY KEY,
    crawl_run_id UUID NULL REFERENCES crawl_run(id) ON DELETE SET NULL,
    crawl_partition_id UUID NULL REFERENCES crawl_partition(id) ON DELETE SET NULL,
    request_type TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    method TEXT NOT NULL DEFAULT 'GET',
    params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    request_headers_json JSONB NULL,
    status_code INT NOT NULL,
    latency_ms INT NOT NULL,
    attempt INT NOT NULL DEFAULT 1,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    response_received_at TIMESTAMPTZ NULL,
    error_type TEXT NULL,
    error_message TEXT NULL
);

CREATE INDEX idx_api_request_log_requested_at
    ON api_request_log(requested_at DESC);

CREATE INDEX idx_api_request_log_status_code
    ON api_request_log(status_code);

CREATE INDEX idx_api_request_log_run_id
    ON api_request_log(crawl_run_id);

CREATE INDEX idx_api_request_log_partition_id
    ON api_request_log(crawl_partition_id);

CREATE TABLE raw_api_payload (
    id BIGSERIAL PRIMARY KEY,
    api_request_log_id BIGINT NOT NULL REFERENCES api_request_log(id) ON DELETE CASCADE,
    endpoint_type TEXT NOT NULL,
    entity_hh_id TEXT NULL,
    payload_json JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_raw_api_payload_request_log_id
    ON raw_api_payload(api_request_log_id);

CREATE INDEX idx_raw_api_payload_entity_hh_id
    ON raw_api_payload(entity_hh_id);

CREATE INDEX idx_raw_api_payload_received_at
    ON raw_api_payload(received_at DESC);

-- =========================================================
-- dictionaries / reference data
-- =========================================================

CREATE TABLE area (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hh_area_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    parent_area_id UUID NULL REFERENCES area(id) ON DELETE SET NULL,
    level INT NULL,
    path_text TEXT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_area_parent_area_id
    ON area(parent_area_id);

CREATE TABLE professional_role (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hh_professional_role_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    category_name TEXT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE dictionary_sync_run (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dictionary_name TEXT NOT NULL,
    status TEXT NOT NULL,
    etag TEXT NULL,
    source_status_code INT NULL,
    notes TEXT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ NULL
);

CREATE INDEX idx_dictionary_sync_run_name
    ON dictionary_sync_run(dictionary_name);

CREATE INDEX idx_dictionary_sync_run_started_at
    ON dictionary_sync_run(started_at DESC);

-- =========================================================
-- domain core
-- =========================================================

CREATE TABLE employer (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hh_employer_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    alternate_url TEXT NULL,
    site_url TEXT NULL,
    area_id UUID NULL REFERENCES area(id) ON DELETE SET NULL,
    is_trusted BOOLEAN NULL,
    raw_first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_employer_name
    ON employer(name);

CREATE INDEX idx_employer_area_id
    ON employer(area_id);

CREATE TABLE vacancy (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hh_vacancy_id TEXT NOT NULL UNIQUE,
    employer_id UUID NULL REFERENCES employer(id) ON DELETE SET NULL,
    area_id UUID NULL REFERENCES area(id) ON DELETE SET NULL,
    name_current TEXT NOT NULL,
    published_at TIMESTAMPTZ NULL,
    created_at_hh TIMESTAMPTZ NULL,
    archived_at_hh TIMESTAMPTZ NULL,
    alternate_url TEXT NULL,
    employment_type_code TEXT NULL,
    schedule_type_code TEXT NULL,
    experience_code TEXT NULL,
    source_type TEXT NOT NULL DEFAULT 'hh_api',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_vacancy_employer_id
    ON vacancy(employer_id);

CREATE INDEX idx_vacancy_area_id
    ON vacancy(area_id);

CREATE INDEX idx_vacancy_published_at
    ON vacancy(published_at DESC);

CREATE TABLE vacancy_professional_role (
    vacancy_id UUID NOT NULL REFERENCES vacancy(id) ON DELETE CASCADE,
    professional_role_id UUID NOT NULL REFERENCES professional_role(id) ON DELETE CASCADE,
    PRIMARY KEY (vacancy_id, professional_role_id)
);

CREATE INDEX idx_vacancy_prof_role_role_id
    ON vacancy_professional_role(professional_role_id);

-- =========================================================
-- history / state tracking
-- =========================================================

CREATE TABLE vacancy_seen_event (
    id BIGSERIAL PRIMARY KEY,
    vacancy_id UUID NOT NULL REFERENCES vacancy(id) ON DELETE CASCADE,
    crawl_run_id UUID NOT NULL REFERENCES crawl_run(id) ON DELETE CASCADE,
    crawl_partition_id UUID NOT NULL REFERENCES crawl_partition(id) ON DELETE CASCADE,
    seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    list_position INT NULL,
    short_hash TEXT NOT NULL,
    short_payload_ref_id BIGINT NULL REFERENCES raw_api_payload(id) ON DELETE SET NULL
);

CREATE INDEX idx_vacancy_seen_event_vacancy_id
    ON vacancy_seen_event(vacancy_id);

CREATE INDEX idx_vacancy_seen_event_run_id
    ON vacancy_seen_event(crawl_run_id);

CREATE INDEX idx_vacancy_seen_event_partition_id
    ON vacancy_seen_event(crawl_partition_id);

CREATE INDEX idx_vacancy_seen_event_seen_at
    ON vacancy_seen_event(seen_at DESC);

CREATE TABLE vacancy_current_state (
    vacancy_id UUID PRIMARY KEY REFERENCES vacancy(id) ON DELETE CASCADE,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    seen_count INT NOT NULL DEFAULT 1,
    consecutive_missing_runs INT NOT NULL DEFAULT 0,
    is_probably_inactive BOOLEAN NOT NULL DEFAULT FALSE,
    last_seen_run_id UUID NULL REFERENCES crawl_run(id) ON DELETE SET NULL,
    last_short_hash TEXT NULL,
    last_detail_hash TEXT NULL,
    last_detail_fetched_at TIMESTAMPTZ NULL,
    detail_fetch_status TEXT NOT NULL DEFAULT 'not_requested',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_vacancy_current_state_last_seen_at
    ON vacancy_current_state(last_seen_at DESC);

CREATE INDEX idx_vacancy_current_state_inactive
    ON vacancy_current_state(is_probably_inactive);

CREATE INDEX idx_vacancy_current_state_detail_status
    ON vacancy_current_state(detail_fetch_status);

CREATE TABLE vacancy_snapshot (
    id BIGSERIAL PRIMARY KEY,
    vacancy_id UUID NOT NULL REFERENCES vacancy(id) ON DELETE CASCADE,
    snapshot_type TEXT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    crawl_run_id UUID NULL REFERENCES crawl_run(id) ON DELETE SET NULL,
    short_hash TEXT NULL,
    detail_hash TEXT NULL,
    short_payload_ref_id BIGINT NULL REFERENCES raw_api_payload(id) ON DELETE SET NULL,
    detail_payload_ref_id BIGINT NULL REFERENCES raw_api_payload(id) ON DELETE SET NULL,
    normalized_json JSONB NULL,
    change_reason TEXT NULL
);

CREATE INDEX idx_vacancy_snapshot_vacancy_id
    ON vacancy_snapshot(vacancy_id);

CREATE INDEX idx_vacancy_snapshot_captured_at
    ON vacancy_snapshot(captured_at DESC);

CREATE INDEX idx_vacancy_snapshot_detail_hash
    ON vacancy_snapshot(detail_hash);

CREATE TABLE detail_fetch_attempt (
    id BIGSERIAL PRIMARY KEY,
    vacancy_id UUID NOT NULL REFERENCES vacancy(id) ON DELETE CASCADE,
    crawl_run_id UUID NULL REFERENCES crawl_run(id) ON DELETE SET NULL,
    reason TEXT NOT NULL,
    attempt INT NOT NULL DEFAULT 1,
    status TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ NULL,
    error_message TEXT NULL
);

CREATE INDEX idx_detail_fetch_attempt_vacancy_id
    ON detail_fetch_attempt(vacancy_id);

CREATE INDEX idx_detail_fetch_attempt_status
    ON detail_fetch_attempt(status);

CREATE INDEX idx_detail_fetch_attempt_requested_at
    ON detail_fetch_attempt(requested_at DESC);
```

---

## Что ещё нужно **до** первого запуска Codex

Я бы не пускал Codex в совсем пустой репозиторий. Ему лучше дать минимальный контекст и рамки. Набор такой:

### Обязательно

* `README.md`
* `AGENTS.md`
* `docs/...` с HLD/LLD
* `schema.sql` или первая миграция
* `repo-structure.md`
* `.env.example`

### Очень желательно

* `pyproject.toml`
* `docker-compose.yml`
* `Makefile`
* пустой каркас каталогов `src/...`
* `alembic init` или уже готовый `migrations/`

---

## Что я бы попросил Codex сделать **первым прогоном**

Не всё сразу, а scaffold.

### Задача 1

Создать каркас репозитория:

* `pyproject.toml`
* `src/hhru_platform/...`
* `tests/...`
* `docker-compose.yml`
* `.env.example`
* `Makefile`

### Задача 2

Подключить:

* ruff
* mypy
* pytest
* alembic
* SQLAlchemy
* pydantic-settings
* asyncpg / psycopg
* redis

### Задача 3

Сгенерировать:

* SQLAlchemy models под текущую DDL
* Alembic migration `0001_initial`
* базовый config loader
* CLI entrypoint

---

## Как я бы сформулировал первый промпт для Codex

```text
You are scaffolding a Python monorepo for a stateful hh.ru vacancy data collection platform.

Context:
- The repository already contains architecture docs in docs/.
- Follow AGENTS.md strictly.
- Target stack: Python 3.12+, PostgreSQL, Redis/Valkey, Docker Compose, Alembic, SQLAlchemy, pytest, ruff, mypy.
- Do not implement the full crawler yet.
- First task is only repository scaffold.

Please:
1. Create the repository skeleton according to docs/architecture/repo-structure.md.
2. Add pyproject.toml with dependencies and dev dependencies.
3. Add Docker Compose with postgres and redis.
4. Add .env.example.
5. Add Makefile with common dev commands.
6. Add a minimal src package and CLI entrypoint.
7. Add Alembic initialization and configuration.
8. Add placeholder test structure.
9. Keep the code minimal, correct, and easy to extend.

Do not add speculative complexity.
```

---

## Как работать с Codex дальше

Официально Codex CLI поддерживает интерактивный режим, может запускаться прямо в текущей директории, а проектные инструкции стоит задавать через `AGENTS.md`; для проекта также можно завести `.codex/config.toml`, если захочешь закрепить поведение именно для этого репозитория. ([OpenAI Developers][2])

Практически я бы делал так:

1. ты создаёшь репозиторий и кладёшь туда docs + README + AGENTS + schema;
2. запускаешь Codex на scaffold;
3. присылаешь мне diff или список созданных файлов;
4. я проверяю и формулирую следующий узкий промпт;
5. дальше идём итерациями: scaffold → models → migration → config → workers.

Такой режим обычно сильно надёжнее, чем один гигантский запрос “сделай всю систему”.

Дальше у нас есть 3 очень естественных следующих шага:

1. я могу сразу написать тебе `pyproject.toml + docker-compose.yml + Makefile + .env.example`,
2. могу подготовить **первую Alembic migration** из этой DDL,
3. или могу дать тебе **пошаговый сценарий работы с Codex CLI/VS Code** именно для этого репозитория.

[1]: https://developers.openai.com/codex/quickstart/?utm_source=chatgpt.com "Quickstart"
[2]: https://developers.openai.com/codex/cli/features/?utm_source=chatgpt.com "Codex CLI features"
