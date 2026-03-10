PYTHON ?= python3

.PHONY: up down migrate test lint format

up:
	docker compose up -d

down:
	docker compose down

migrate:
	$(PYTHON) -m alembic upgrade head

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m mypy src

format:
	$(PYTHON) -m ruff format .
