FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir uv

WORKDIR /opt/potatocore
COPY . /opt/potatocore

RUN uv pip install --system -e libs/core -e apps/api

ENV PYTHONPATH="/opt/potatocore/libs/core/src:/opt/potatocore/apps/api/src"

CMD ["uvicorn", "potatocore.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
