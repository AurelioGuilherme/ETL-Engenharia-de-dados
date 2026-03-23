from __future__ import annotations

from typing import Any

from potatocore.core.config import get_settings
from potatocore.core.db import get_api_engine
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError


def list_gold_concursos(limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
    settings = get_settings()
    engine = get_api_engine()

    stmt = text(
        f"""
        SELECT
            concurso,
            data_sorteio,
            arrecadacao_total,
            ganhadores_15_acertos,
            rateio_15_acertos
        FROM {settings.api_schema}.gold_lotofacil_concursos
        ORDER BY concurso DESC
        LIMIT :limit OFFSET :offset
        """
    )

    with engine.begin() as connection:
        try:
            rows = connection.execute(stmt, {"limit": limit, "offset": offset}).mappings().all()
        except ProgrammingError:
            return []

    return [dict(row) for row in rows]
