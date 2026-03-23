from __future__ import annotations

import logging

from potatocore.core.config import get_settings
from potatocore.core.db import get_api_engine, get_warehouse_engine
from sqlalchemy import text

LOGGER = logging.getLogger(__name__)


def publish_gold_to_api() -> None:
    settings = get_settings()
    warehouse_engine = get_warehouse_engine()
    api_engine = get_api_engine()

    query = text(
        f"""
        SELECT
            concurso,
            data_sorteio,
            arrecadacao_total,
            ganhadores_15_acertos,
            rateio_15_acertos
        FROM {settings.gold_schema}.gold_lotofacil_concursos
        """
    )

    with warehouse_engine.begin() as warehouse_conn:
        rows = warehouse_conn.execute(query).mappings().all()

    row_count = len(rows)

    with api_engine.begin() as api_conn:
        api_conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {settings.api_schema}"))
        api_conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {settings.api_schema}.gold_lotofacil_concursos (
                    concurso BIGINT PRIMARY KEY,
                    data_sorteio DATE,
                    arrecadacao_total DOUBLE PRECISION,
                    ganhadores_15_acertos BIGINT,
                    rateio_15_acertos DOUBLE PRECISION
                )
                """
            )
        )
        api_conn.execute(text(f"TRUNCATE TABLE {settings.api_schema}.gold_lotofacil_concursos"))
        if rows:
            api_conn.execute(
                text(
                    f"""
                    INSERT INTO {settings.api_schema}.gold_lotofacil_concursos (
                        concurso,
                        data_sorteio,
                        arrecadacao_total,
                        ganhadores_15_acertos,
                        rateio_15_acertos
                    ) VALUES (
                        :concurso,
                        :data_sorteio,
                        :arrecadacao_total,
                        :ganhadores_15_acertos,
                        :rateio_15_acertos
                    )
                    """
                ),
                rows,
            )

        inserted_count = api_conn.execute(
            text(f"SELECT COUNT(*) FROM {settings.api_schema}.gold_lotofacil_concursos")
        ).scalar_one()

    if inserted_count != row_count:
        raise RuntimeError(
            f"Publish row count mismatch: source={row_count} inserted={inserted_count}"
        )

    LOGGER.info(
        "Published %s rows to %s.gold_lotofacil_concursos", inserted_count, settings.api_schema
    )
