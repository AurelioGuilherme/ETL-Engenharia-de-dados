from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import potatocore.ingestion.bronze_loader as bronze_loader


@contextmanager
def _engine_connection(engine: MagicMock):
    yield engine.connection


def _make_engine() -> MagicMock:
    engine = MagicMock()
    engine.connection = MagicMock()
    engine.begin.side_effect = lambda: _engine_connection(engine)
    return engine


def test_build_ingestion_history_frame_adds_metadata() -> None:
    raw_df = pd.DataFrame([{"concurso": 1}, {"concurso": 2}])
    ingested_at = datetime(2026, 3, 23, 12, 0, tzinfo=UTC)

    with (
        patch.object(bronze_loader, "uuid4", return_value="batch-123"),
        patch.object(bronze_loader, "datetime") as datetime_mock,
    ):
        datetime_mock.now.return_value = ingested_at
        history_df = bronze_loader._build_ingestion_history_frame(
            raw_df,
            "/tmp/Lotofacil.xlsx",
            "hash-abc",
        )

    assert list(history_df.columns[:5]) == [
        "ingestion_id",
        "ingested_at",
        "source_file_path",
        "source_file_hash",
        "source_row_number",
    ]
    assert history_df["ingestion_id"].tolist() == ["batch-123", "batch-123"]
    assert history_df["source_file_hash"].tolist() == ["hash-abc", "hash-abc"]
    assert history_df["source_row_number"].tolist() == [1, 2]


def test_load_lotofacil_to_bronze_skips_duplicate_ingestion() -> None:
    engine = _make_engine()
    settings = SimpleNamespace(source_xlsx_path="/tmp/Lotofacil.xlsx")

    with (
        patch.object(bronze_loader, "get_settings", return_value=settings),
        patch.object(bronze_loader, "get_warehouse_engine", return_value=engine),
        patch.object(bronze_loader, "_compute_file_hash", return_value="hash-abc"),
        patch.object(bronze_loader, "_ingestion_already_loaded", return_value=True),
        patch.object(bronze_loader, "read_lotofacil_excel") as read_excel_mock,
    ):
        rows_loaded = bronze_loader.load_lotofacil_to_bronze()

    assert rows_loaded == 0
    read_excel_mock.assert_not_called()


def test_load_lotofacil_to_bronze_appends_history_rows() -> None:
    engine = _make_engine()
    settings = SimpleNamespace(source_xlsx_path="/tmp/Lotofacil.xlsx")
    raw_df = pd.DataFrame([{"concurso": 1}])
    history_df = pd.DataFrame(
        [
            {
                "ingestion_id": "batch-123",
                "ingested_at": datetime(2026, 3, 23, 12, 0, tzinfo=UTC),
                "source_file_path": "/tmp/Lotofacil.xlsx",
                "source_file_hash": "hash-abc",
                "source_row_number": 1,
                "concurso": 1,
            }
        ]
    )

    with (
        patch.object(bronze_loader, "get_settings", return_value=settings),
        patch.object(bronze_loader, "get_warehouse_engine", return_value=engine),
        patch.object(bronze_loader, "_compute_file_hash", return_value="hash-abc"),
        patch.object(bronze_loader, "_ingestion_already_loaded", return_value=False),
        patch.object(bronze_loader, "read_lotofacil_excel", return_value=raw_df),
        patch.object(bronze_loader, "_build_ingestion_history_frame", return_value=history_df),
        patch.object(pd.DataFrame, "to_sql") as to_sql_mock,
    ):
        rows_loaded = bronze_loader.load_lotofacil_to_bronze()

    assert rows_loaded == 1
    to_sql_mock.assert_called_once_with(
        name=bronze_loader.BRONZE_TABLE,
        con=engine,
        schema=bronze_loader.BRONZE_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
    )
