FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends build-essential git && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir uv

WORKDIR /opt/potatocore
COPY . /opt/potatocore

RUN uv pip install --system dbt-core==1.8.8 dbt-postgres==1.8.2

ENV DBT_PROFILES_DIR=/opt/potatocore/analytics/dbt

CMD ["sleep", "infinity"]
