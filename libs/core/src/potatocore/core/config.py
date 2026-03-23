from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "potatocore"
    app_env: str = "dev"
    app_log_level: str = "INFO"

    source_xlsx_path: str = "/opt/potatocore/Data/Lotofacil.xlsx"

    warehouse_db_host: str = "postgres_warehouse"
    warehouse_db_port: int = 5432
    warehouse_db_name: str = "warehouse"
    warehouse_db_user: str = "warehouse"
    warehouse_db_password: str = "warehouse"

    api_db_host: str = "postgres_api"
    api_db_port: int = 5432
    api_db_name: str = "api"
    api_db_user: str = "api"
    api_db_password: str = "api"

    gold_schema: str = Field(default="gold")
    api_schema: str = Field(default="public")

    @property
    def warehouse_sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.warehouse_db_user}:{self.warehouse_db_password}"
            f"@{self.warehouse_db_host}:{self.warehouse_db_port}/{self.warehouse_db_name}"
        )

    @property
    def api_sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.api_db_user}:{self.api_db_password}"
            f"@{self.api_db_host}:{self.api_db_port}/{self.api_db_name}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
