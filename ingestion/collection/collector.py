from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv

DEFAULT_BASE_URL = "https://free-api-live-football-data.p.rapidapi.com"
DEFAULT_ENDPOINT = "/football-league-all-seasons"
DEFAULT_HOST = "free-api-live-football-data.p.rapidapi.com"


def _load_env() -> None:
    load_dotenv()
    repo_env = Path(__file__).resolve().parents[2] / ".env"
    if repo_env.exists():
        load_dotenv(repo_env, override=False)
    local_env = Path(__file__).resolve().parent / ".env"
    if local_env.exists():
        load_dotenv(local_env, override=False)


def _parse_querystring(raw: Optional[str]) -> Dict[str, str]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
        if isinstance(value, dict):
            return {str(k): str(v) for k, v in value.items()}
    except json.JSONDecodeError:
        pass
    return {}


def fetch() -> Any:
    _load_env()

    base_url = os.getenv("API_BASE_URL", DEFAULT_BASE_URL).rstrip("/")
    endpoint = os.getenv("API_ENDPOINT", DEFAULT_ENDPOINT)
    url = f"{base_url}{endpoint}"

    api_key = os.getenv("API_KEY")
    api_host = os.getenv("API_HOST", DEFAULT_HOST)
    if not api_key or not api_host:
        raise ValueError("Missing API_KEY or API_HOST in environment variables.")

    querystring = _parse_querystring(os.getenv("API_QUERYSTRING"))

    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": api_host,
        "Content-Type": "application/json",
    }

    timeout = float(os.getenv("API_TIMEOUT", "30"))
    response = requests.get(url, headers=headers, params=querystring or None, timeout=timeout)
    response.raise_for_status()
    payload: Any = response.json()
    if isinstance(payload, dict) and "result" in payload:
        return payload["result"]
    return payload
