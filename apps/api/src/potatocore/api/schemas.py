from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class HealthSchema(BaseModel):
    status: str
    service: str


class ConcursoResumoSchema(BaseModel):
    concurso: int
    data_sorteio: date | None
    arrecadacao_total: float | None
    ganhadores_15_acertos: int | None
    rateio_15_acertos: float | None
