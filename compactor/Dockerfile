FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /bin/uv
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV UV_PYTHON_DOWNLOADS=never
ENV PATH="/src/.venv/bin:$PATH"

WORKDIR /src

# uv needs every workspace member's pyproject.toml to read the lockfile,
# but only the compactor's dependencies are installed.
COPY pyproject.toml uv.lock ./
COPY catcher/pyproject.toml ./catcher/
COPY compactor/pyproject.toml ./compactor/
RUN uv sync --frozen --no-dev --no-cache --package compactor

RUN python -c "import duckdb; conn = duckdb.connect(); conn.execute('INSTALL httpfs; INSTALL aws;')"
COPY config.py ./
COPY compactor/ ./compactor/

ENTRYPOINT [ "python", "-m", "compactor" ]
