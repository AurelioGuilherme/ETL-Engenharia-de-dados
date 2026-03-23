from __future__ import annotations

from fastapi import FastAPI
from potatocore.api.routers.gold import router as gold_router
from potatocore.api.routers.health import router as health_router
from potatocore.core.config import get_settings
from potatocore.core.logging import configure_logging

settings = get_settings()
configure_logging(settings.app_log_level)

app = FastAPI(title="PotatoCore Gold API", version="1.0.0")
app.include_router(health_router)
app.include_router(gold_router)
