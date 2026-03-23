from __future__ import annotations

from fastapi import APIRouter, Query
from potatocore.api.schemas import ConcursoResumoSchema
from potatocore.api.services.gold_queries import list_gold_concursos

router = APIRouter(prefix="/v1/gold", tags=["gold"])


@router.get("/concursos", response_model=list[ConcursoResumoSchema])
def get_gold_concursos(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[ConcursoResumoSchema]:
    data = list_gold_concursos(limit=limit, offset=offset)
    return [ConcursoResumoSchema(**item) for item in data]
