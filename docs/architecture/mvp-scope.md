# MVP Scope: hhru data collection platform

**Статус:** Draft v1

---

## 1. Цель MVP

Собрать минимально полноценную production-like систему, которая:

- запускает weekly sweep;
- хранит raw и normalized данные;
- отслеживает историю наблюдений вакансий;
- выборочно тянет detail;
- имеет базовую observability;
- переживает рестарты и частичные сбои.

---

## 2. Что входит в MVP

### Обязательно
- PostgreSQL schema + migrations
- hh API client
- scheduler
- planner
- list worker
- detail worker
- dictionary sync
- crawl_run / crawl_partition lifecycle
- vacancy_seen_event
- vacancy_current_state
- vacancy_snapshot
- raw_api_payload
- admin CLI
- Prometheus metrics
- Grafana dashboard
- structured logging
- error tracking
- backup postgres

### Желательно
- post-run reconciliation
- TTL policy на detail refresh
- retention jobs
- force rerun partition

### Не входит
- research features
- ML/NLP enrichment
- UI
- multi-node infra
- BI dashboards по предметной аналитике

---

## 3. Порядок реализации

## Этап 1. Базовый каркас
- создать репозиторий
- настроить pyproject, lint, formatter, type checks
- поднять docker compose
- поднять PostgreSQL и Redis/Valkey
- настроить Alembic
- настроить logging/config

## Этап 2. База и доменная модель
- реализовать таблицы:
  - crawl_run
  - crawl_partition
  - api_request_log
  - raw_api_payload
  - vacancy
  - vacancy_seen_event
  - vacancy_current_state
  - vacancy_snapshot
  - employer
  - area
  - professional_role
- реализовать ORM models и repositories

## Этап 3. hh API client
- реализовать базовый async client
- обязательные headers
- retries/backoff
- rate limiter
- методы:
  - search vacancies
  - get vacancy detail
  - get dictionaries

## Этап 4. Planner + list worker
- создать scheduler-команду запуска run
- создать planner
- создать list worker
- реализовать запись raw/request logs
- реализовать seen events
- реализовать pagination flow

## Этап 5. Detail worker
- реализовать detail policy
- реализовать detail fetch
- реализовать snapshot builder
- реализовать update vacancy current state

## Этап 6. Reconciliation
- логика завершения run
- обновление consecutive_missing_runs
- пометка probably_inactive
- частичные сбои и rerun partitions

## Этап 7. Observability
- Prometheus metrics
- Grafana dashboard
- Loki logs
- error tracker
- алерты на критичные состояния

## Этап 8. Ops
- admin CLI
- backup/restore
- retention jobs
- health check commands

---

## 4. Definition of Done для MVP

MVP готов, если:

1. локально можно поднять систему одной командой;
2. можно запустить weekly sweep;
3. sweep обрабатывает партиции и страницы автоматически;
4. данные сохраняются в raw и core schema;
5. seen-state и snapshots корректны;
6. detail fetch работает по правилам;
7. есть dashboard и logs;
8. рестарт контейнеров не ломает состояние;
9. есть рабочий backup PostgreSQL.