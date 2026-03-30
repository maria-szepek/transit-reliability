# FROM python:3.11-slim

# # install uv
# COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# WORKDIR /app

# # create venv explicitly
# # RUN uv venv

# # activate venv
# ENV PATH="/app/.venv/bin:$PATH"

# # copy dependency files
# COPY pyproject.toml .python-version uv.lock ./
# RUN uv sync --locked

# # install dependencies
# RUN uv sync --locked

# # copy source code
# COPY . .

# # ENTRYPOINT ["python", "gtfs/load_gtfs.py"]
# ENTRYPOINT [".venv/bin/python", "gtfs/load_gtfs.py"]

FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# Put the virtual environment OUTSIDE /app so bind mounts do not overwrite it
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy dependency files first
COPY pyproject.toml uv.lock .python-version ./

# Install dependencies into /opt/venv
RUN uv sync --locked

# Copy source code
COPY . .

# Run with the venv python explicitly
ENTRYPOINT ["/opt/venv/bin/python", "gtfs/load_gtfs.py"]