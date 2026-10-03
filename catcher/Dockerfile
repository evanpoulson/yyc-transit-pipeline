FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /bin/uv
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV UV_PYTHON_DOWNLOADS=never
ENV PATH="/src/.venv/bin:$PATH"

WORKDIR /src

# uv needs every workspace member's pyproject.toml to read the lockfile,
# but only the catcher's dependencies are installed.
COPY pyproject.toml uv.lock ./
COPY catcher/pyproject.toml ./catcher/
COPY compactor/pyproject.toml ./compactor/
RUN uv sync --frozen --no-dev --no-cache --package catcher

COPY config.py ./
COPY catcher/ ./catcher/

# Python runs as PID 1 and ignores SIGTERM, so stop with SIGINT instead:
# it raises KeyboardInterrupt and the thread pool shuts down cleanly.
STOPSIGNAL SIGINT

ENTRYPOINT [ "python", "-m", "catcher" ]
