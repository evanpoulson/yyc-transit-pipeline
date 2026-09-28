FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /src

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN python -c "import duckdb; conn = duckdb.connect(); conn.execute('INSTALL httpfs; INSTALL aws;')"
COPY config.py ./
COPY compactor/ ./compactor/

ENTRYPOINT [ "python", "-m", "compactor" ]