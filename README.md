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