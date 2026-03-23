from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

LOGGER = logging.getLogger(__name__)

DBT_PROJECT_DIR = Path("/opt/potatocore/analytics/dbt")
DBT_LOG_PATH = "/tmp/dbt/logs"
DBT_TARGET_PATH = "/tmp/dbt/target"


def _has_dbt_dependencies_file() -> bool:
    return any(
        (DBT_PROJECT_DIR / filename).exists() for filename in ("packages.yml", "dependencies.yml")
    )


def run_dbt(command: str) -> None:
    allowed_commands = {"deps", "run", "test"}
    if command not in allowed_commands:
        raise ValueError(f"Unsupported dbt command: {command}")

    os.makedirs(DBT_LOG_PATH, exist_ok=True)
    os.makedirs(DBT_TARGET_PATH, exist_ok=True)

    env = os.environ.copy()
    env.setdefault("DBT_LOG_PATH", DBT_LOG_PATH)
    env.setdefault("DBT_TARGET_PATH", DBT_TARGET_PATH)

    if command == "deps" and not _has_dbt_dependencies_file():
        LOGGER.info("Skipping dbt deps because no packages.yml or dependencies.yml was found")
        return

    LOGGER.info("Running dbt command: %s", command)
    subprocess.run(
        [
            "dbt",
            command,
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROJECT_DIR),
            "--target",
            "dev",
        ],
        check=True,
        timeout=3600,
        env=env,
    )
