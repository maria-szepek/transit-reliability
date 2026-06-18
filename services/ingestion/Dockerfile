FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# Keep the virtual environment outside /app so bind mounts do not overwrite it.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY pyproject.toml uv.lock ./
COPY dbt/pyproject.toml ./dbt/
COPY services/api/pyproject.toml ./services/api/
COPY services/scoring/pyproject.toml ./services/scoring/
COPY services/ingestion/pyproject.toml ./services/ingestion/
COPY services/ui/pyproject.toml ./services/ui/
COPY services/ingestion ./services/ingestion

RUN uv sync --locked --package transit-reliability-ingestion --no-dev

WORKDIR /app/services/ingestion

# Commands are supplied by docker-compose or Airflow tasks.
