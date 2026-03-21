from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from .io_utils import default_headers, write_df, write_json

BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"


def load_manifest(config_path: Path) -> dict[str, Any]:
    return json.loads(config_path.read_text(encoding="utf-8"))


def _has_next(payload: dict[str, Any]) -> bool:
    links = payload.get("links", {}) or {}
    if links.get("next"):
        return True

    meta = payload.get("meta", {}) or {}
    current_candidates = ["page-number", "page_number", "current-page", "current_page"]
    total_candidates = ["total-pages", "total_pages", "page-count", "page_count"]
    current = None
    total = None
    for key in current_candidates:
        if key in meta:
            current = int(meta[key])
            break
    for key in total_candidates:
        if key in meta:
            total = int(meta[key])
            break
    if current is not None and total is not None:
        return current < total
    return False


def fetch_dataset(endpoint: str, params: dict[str, Any] | None = None, max_pages: int | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
    params = dict(params or {})
    params.setdefault("format", "json")
    params.setdefault("page[size]", "10000")

    rows: list[dict[str, Any]] = []
    page = 1
    pages_fetched = 0
    last_payload: dict[str, Any] = {}

    while True:
        query = dict(params)
        query["page[number]"] = page
        url = BASE_URL + endpoint.lstrip("/")
        response = requests.get(url, params=query, headers=default_headers(), timeout=120)
        response.raise_for_status()
        payload = response.json()
        last_payload = payload
        data = payload.get("data", []) or []
        rows.extend(data)

        pages_fetched += 1
        if max_pages is not None and pages_fetched >= max_pages:
            break
        if not _has_next(payload):
            break
        page += 1

    df = pd.DataFrame(rows)
    meta = {
        "endpoint": endpoint,
        "params": params,
        "pages_fetched": pages_fetched,
        "rows_fetched": len(df),
        "meta": last_payload.get("meta", {}),
        "links": last_payload.get("links", {}),
    }
    return df, meta


def save_dataset(df: pd.DataFrame, meta: dict[str, Any], csv_path: Path, meta_path: Path) -> None:
    write_df(df, csv_path)
    write_json(meta, meta_path)
