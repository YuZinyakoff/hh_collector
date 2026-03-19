FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY pyproject.toml README.md alembic.ini ./
COPY migrations ./migrations
COPY src ./src

RUN pip install --no-cache-dir .

CMD ["python", "-m", "hhru_platform.interfaces.cli.main"]
