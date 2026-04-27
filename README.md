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

- `docs/ops/project-status-roadmap.md`
- `docs/ops/current-readiness.md`
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
```

Заполнить переменные:

* `HHRU_DB_*`
* `HHRU_REDIS_*`
* `HHRU_HH_API_BASE_URL`
* `HHRU_HH_API_USER_AGENT`
* `HHRU_HH_API_APPLICATION_TOKEN`, если нужен token contour

### 2. Поднять инфраструктуру

```bash
make up
```

### 3. Применить миграции

```bash
make migrate-compose
```

### 4. Запустить dictionary sync

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main sync-dictionaries --name areas
```

### 5. Запустить короткий guarded sweep

```bash
make run-once-v2 ARGS="--sync-dictionaries no --detail-limit 0 --detail-refresh-ttl-days 30 --triggered-by local-smoke"
```

---

## Полезные команды

```bash
make up
make down
make migrate-compose
make test
make lint
make format
make compose-health
make run-once-v2
make drain-first-detail-backlog
make worker-detail
make scheduler-loop
make backup
make verify-backup
make restore-drill
```

---

## Структура проекта

См.:

* `docs/architecture/repo-structure.md`

---

## Статус

Проект в стадии активного проектирования и scaffold-реализации MVP.
