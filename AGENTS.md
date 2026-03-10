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