FROM apache/airflow:2.10.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/potatocore
COPY . /opt/potatocore

USER airflow

RUN pip install --no-cache-dir -e libs/core -e libs/ingestion -e apps/api -e apps/orchestrator pytest ruff black mypy

ENV PYTHONPATH="/opt/potatocore/libs/core/src:/opt/potatocore/libs/ingestion/src:/opt/potatocore/apps/api/src:/opt/potatocore/apps/orchestrator/src"
