from __future__ import annotations

from fastapi import APIRouter
from potatocore.api.schemas import HealthSchema

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthSchema)
def health() -> HealthSchema:
    return HealthSchema(status="ok", service="potatocore-api")


@router.get("/ready", response_model=HealthSchema)
def ready() -> HealthSchema:
    return HealthSchema(status="ready", service="potatocore-api")
