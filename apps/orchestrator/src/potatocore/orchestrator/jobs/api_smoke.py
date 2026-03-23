from __future__ import annotations

import json
from urllib.request import urlopen


def check_api_health() -> None:
    with urlopen("http://api:8000/health", timeout=20) as response:
        body = response.read().decode("utf-8")
        if response.status != 200:
            raise RuntimeError(f"API health check failed: {response.status}")
        parsed_body = json.loads(body)
        if parsed_body.get("status") != "ok":
            raise RuntimeError("API health check body invalid")
