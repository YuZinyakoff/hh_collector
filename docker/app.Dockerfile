FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY pyproject.toml README.md alembic.ini ./
COPY migrations ./migrations
COPY scripts ./scripts
COPY src ./src

RUN apt-get update \
    && apt-get install --no-install-recommends -y bash postgresql-client \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir .

CMD ["python", "-m", "hhru_platform.interfaces.cli.main"]
