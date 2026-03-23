# ETL Engenharia de Dados - Lotofacil com Airflow + dbt + FastAPI

<p align="left">

  <img src="https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white" alt="Docker Compose">
  <img src="https://img.shields.io/badge/Airflow-Orquestracao-017CEE?logo=apacheairflow&logoColor=white" alt="Airflow">
  <img src="https://img.shields.io/badge/dbt-Transformacoes-FF694B?logo=dbt&logoColor=white" alt="dbt">
  <img src="https://img.shields.io/badge/FastAPI-Serving-009688?logo=fastapi&logoColor=white" alt="FastAPI">
  <img src="https://img.shields.io/badge/PostgreSQL-Warehouse_+_API-4169E1?logo=postgresql&logoColor=white" alt="PostgreSQL">

</p>

Este projeto implementa uma plataforma de dados completa para os concursos da Lotofacil, com ingestao historica, transformacoes em camadas e serving via API REST read-only.

O sistema permite:
- Ingerir o arquivo `Data/Lotofacil.xlsx` para uma camada Bronze historica e idempotente
- Orquestrar o pipeline de dados com Apache Airflow
- Transformar os dados com dbt nas camadas Bronze, Silver e Gold
- Publicar dados analiticos em um banco de serving separado para consumo por API
- Expor consultas read-only com FastAPI

![Arquitetura](diagrama_arquitetura.png)

## Dataset

- **Fonte:** arquivo local `Data/Lotofacil.xlsx`
- **Dominio:** historico de concursos da Lotofacil
- **Estrutura de dados:** dezenas sorteadas, ganhadores por faixa, valores de rateio, arrecadacao, observacoes e acumulados

## Arquitetura do Sistema

O desenvolvimento segue uma arquitetura de dados em camadas:

- **Bronze:** historico bruto de ingestao com metadados de carga e idempotencia por hash do arquivo
- **Silver:** conversao de tipos, normalizacao de campos e padronizacao dos dados
- **Gold:** enriquecimento analitico para consumo por API e analise
- **Orquestracao:** Apache Airflow coordena ingestao, dbt, publicacao e smoke test
- **Serving:** FastAPI le dados do `postgres_api` e expoe endpoints GET-only


## Estrutura do Projeto

```bash
ETL-Engenharia-de-dados/
├── apps/
│   ├── api/
│   │   ├── pyproject.toml
│   │   └── src/potatocore/api/
│   │       ├── main.py
│   │       ├── routers/
│   │       ├── schemas.py
│   │       └── services/
│   │
│   └── orchestrator/
│       ├── pyproject.toml
│       └── src/potatocore/orchestrator/
│           ├── dags/
│           └── jobs/
│
├── libs/
│   ├── core/
│   │   ├── pyproject.toml
│   │   └── src/potatocore/core/
│   │       ├── config.py
│   │       ├── db.py
│   │       ├── logging.py
│   │       └── types.py
│   │
│   └── ingestion/
│       ├── pyproject.toml
│       └── src/potatocore/ingestion/
│           ├── bronze_loader.py
│           └── excel_reader.py
│
├── analytics/
│   └── dbt/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── models/
│       │   ├── bronze/
│       │   ├── silver/
│       │   └── gold/
│       └── tests/
│
├── infra/
│   └── docker/
│       ├── airflow/
│       ├── api/
│       └── dbt/
│
├── Data/
│   └── Lotofacil.xlsx
├── tests/
│   ├── contract/
│   ├── integration/
│   └── unit/
├── docker-compose.yaml
├── pyproject.toml
├── requiriment.txt
├── uv.lock
└── README.md
```

## Getting Started

### Requisitos

- Docker e Docker Compose
- Python 3.11+ (apenas como suporte local opcional)
- Git

### Instalacao

Clone o repositorio:

```bash
git clone https://github.com/AurelioGuilherme/ETL-Engenharia-de-dados.git
cd ETL-Engenharia-de-dados
```

Suba a stack completa:

```bash
docker compose up airflow-init
docker compose up -d
```

Servicos principais:
- Airflow UI: `http://localhost:8080`
- API: `http://localhost:8000`
- Postgres warehouse: `localhost:5433`
- Postgres api: `localhost:5434`

Credenciais do Airflow:
- usuario: `airflow`
- senha: `airflow`

## Pipeline Completo

```bash
# 1) Inicializar ambiente Airflow
docker compose up airflow-init

# 2) Subir stack completa
docker compose up -d

# 3) Executar ingestao Bronze manualmente
docker compose exec airflow-webserver python -c "from potatocore.ingestion.bronze_loader import load_lotofacil_to_bronze; print(load_lotofacil_to_bronze())"

# 4) Rodar dbt
docker compose exec airflow-webserver python -c "from potatocore.orchestrator.jobs.dbt_runner import run_dbt; run_dbt('run')"

# 5) Validar modelos dbt
docker compose exec airflow-webserver python -c "from potatocore.orchestrator.jobs.dbt_runner import run_dbt; run_dbt('test')"

# 6) Publicar dados Gold no banco da API
docker compose exec airflow-webserver python -c "from potatocore.orchestrator.jobs.publish_api import publish_gold_to_api; publish_gold_to_api()"

# 7) Validar endpoint de health
docker compose exec airflow-webserver python -c "from potatocore.orchestrator.jobs.api_smoke import check_api_health; check_api_health()"
```

## Variaveis de Ambiente

As principais variaveis ja estao configuradas no `docker-compose.yaml`, mas o projeto suporta sobrescrita via ambiente:

```bash
export SOURCE_XLSX_PATH="/opt/potatocore/Data/Lotofacil.xlsx"
export WAREHOUSE_DB_HOST="postgres_warehouse"
export WAREHOUSE_DB_PORT="5432"
export WAREHOUSE_DB_NAME="warehouse"
export API_DB_HOST="postgres_api"
export API_DB_PORT="5432"
export API_DB_NAME="api"
```

## Funcionamento das Camadas

**Bronze:** `bronze.lotofacil_ingestion_history`

> - Historico bruto de ingestao
> - Mantem metadados como `ingestion_id`, `ingested_at`, `source_file_hash` e `source_row_number`
> - Idempotente por hash do arquivo
> - Nao faz transformacao analitica

**Silver:** `silver.silver_lotofacil`

> - Faz a conversao de tipos
> - Normaliza datas, valores numericos e monetarios
> - Preserva o contrato tipado da ultima ingestao valida

**Gold:** `gold.gold_lotofacil_concursos` e `gold.gold_lotofacil_resumo_anual`

> - Aplica transformacoes analiticas e enriquecimentos
> - Calcula indicadores derivados para consumo
> - Organiza dados prontos para API e analise exploratoria

## Endpoints da API

Atualmente a API expoe:

- `GET /health`
- `GET /ready`
- `GET /v1/gold/concursos?limit=50&offset=0`

Exemplos:

```bash
curl http://localhost:8000/health
curl "http://localhost:8000/v1/gold/concursos?limit=5&offset=0"
```

## Funcionalidades

> * Arquitetura monorepo `libs + apps`
> * Execucao Docker-first
> * Orquestracao via Airflow
> * Transformacoes em camadas com dbt
> * Bronze historica e idempotente
> * Silver para tipagem e normalizacao
> * Gold para enriquecimento analitico
> * API FastAPI read-only sobre camada Gold publicada
> * Testes unitarios, de integracao e de contrato

## Qualidade de Codigo

O projeto utiliza validacoes dentro de containers:

```bash
# Lint + format check + testes
docker compose run --rm airflow-webserver bash -lc "ruff check libs apps tests && black --check libs apps tests && pytest -o cache_dir=/tmp/pytest-cache -q"

# Validar import da DAG
docker compose exec airflow-webserver python -c "import importlib; importlib.import_module('potatocore.orchestrator.dags.etl_lotofacil'); print('ok')"

# Validar dbt
docker compose exec airflow-webserver python -c "from potatocore.orchestrator.jobs.dbt_runner import run_dbt; run_dbt('run'); run_dbt('test')"
```

## Monitoramento Rapido

```bash
docker compose ps
docker compose logs -f airflow-scheduler
docker compose logs -f api
```
