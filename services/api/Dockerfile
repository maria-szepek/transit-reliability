FROM python:3.11-slim

WORKDIR /app

COPY services/api/pyproject.toml ./services/api/
COPY services ./services

RUN pip install --no-cache-dir -e ./services/api

EXPOSE 8000

CMD ["uvicorn", "services.api.main:app", "--host", "0.0.0.0", "--port", "8000"]