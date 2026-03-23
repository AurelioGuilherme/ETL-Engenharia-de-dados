from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd
from potatocore.core.config import get_settings
from potatocore.core.db import get_warehouse_engine
from potatocore.ingestion.excel_reader import read_lotofacil_excel
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

LOGGER = logging.getLogger(__name__)

BRONZE_SCHEMA = "bronze"
BRONZE_TABLE = "lotofacil_ingestion_history"


def _compute_file_hash(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _ingestion_already_loaded(engine: Engine, file_hash: str) -> bool:
    inspector = inspect(engine)
    if not inspector.has_table(BRONZE_TABLE, schema=BRONZE_SCHEMA):
        return False

    stmt = text(
        f"""
        SELECT 1
        FROM {BRONZE_SCHEMA}.{BRONZE_TABLE}
        WHERE source_file_hash = :file_hash
        LIMIT 1
        """
    )

    with engine.begin() as connection:
        return connection.execute(stmt, {"file_hash": file_hash}).scalar() is not None


def _build_ingestion_history_frame(
    df: pd.DataFrame, source_path: str, file_hash: str
) -> pd.DataFrame:
    history_df = df.copy()
    ingestion_id = str(uuid4())
    ingested_at = datetime.now(UTC).replace(microsecond=0)

    history_df.insert(
        0, "source_row_number", pd.Series(range(1, len(history_df) + 1), dtype="int64")
    )
    history_df.insert(0, "source_file_hash", file_hash)
    history_df.insert(0, "source_file_path", source_path)
    history_df.insert(0, "ingested_at", ingested_at)
    history_df.insert(0, "ingestion_id", ingestion_id)
    return history_df


def load_lotofacil_to_bronze() -> int:
    settings = get_settings()
    engine = get_warehouse_engine()
    source_path = settings.source_xlsx_path
    source_hash = _compute_file_hash(source_path)

    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}"))

    if _ingestion_already_loaded(engine, source_hash):
        LOGGER.info(
            "Skipping bronze ingestion for %s because hash %s is already loaded",
            source_path,
            source_hash,
        )
        return 0

    raw_df = read_lotofacil_excel(source_path)
    history_df = _build_ingestion_history_frame(raw_df, source_path, source_hash)

    history_df.to_sql(
        name=BRONZE_TABLE,
        con=engine,
        schema=BRONZE_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
    )

    LOGGER.info(
        "Loaded %s raw rows into %s.%s with ingestion hash %s",
        len(history_df),
        BRONZE_SCHEMA,
        BRONZE_TABLE,
        source_hash,
    )
    return int(len(history_df))
