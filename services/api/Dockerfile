FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY pyproject.toml uv.lock ./
COPY dbt/pyproject.toml ./dbt/
COPY services/api/pyproject.toml ./services/api/
COPY services/scoring/pyproject.toml ./services/scoring/
COPY services/ingestion/pyproject.toml ./services/ingestion/
COPY services/flink/pyproject.toml ./services/flink/
COPY services/ui/pyproject.toml ./services/ui/
COPY services ./services

RUN uv sync --locked --package transit-reliability-api --no-dev

EXPOSE 8000

CMD ["uvicorn", "services.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
