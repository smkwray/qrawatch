from __future__ import annotations

import io
import os
import time
from pathlib import Path
from typing import Mapping, Sequence

import pandas as pd
import requests

from .io_utils import default_headers, write_df, write_json

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
FRED_API_URL = "https://api.stlouisfed.org/fred/series/observations"


def _fetch_series_response(
    series_id: str,
    timeout: int,
    max_retries: int,
    backoff_seconds: float,
) -> requests.Response:
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if api_key:
        url = FRED_API_URL
        params = {
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "limit": 100000,
            "sort_order": "asc",
        }
    else:
        url = FRED_CSV_URL
        params = {"id": series_id}

    attempts = max(0, int(max_retries)) + 1
    response = None
    last_error: requests.RequestException | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(
                url,
                params=params,
                headers=default_headers(),
                timeout=timeout,
            )
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= attempts:
                raise
            time.sleep(backoff_seconds * attempt)

    if response is None:
        raise RuntimeError(f"Failed to fetch FRED series {series_id}: {last_error}")

    return response


def _parse_api_observations(payload: dict, series_id: str) -> pd.DataFrame:
    observations = payload.get("observations", [])
    if not observations:
        return pd.DataFrame(columns=["date", "value", "series_id"])
    df = pd.DataFrame(observations)
    if "date" not in df.columns or "value" not in df.columns:
        raise KeyError(f"FRED API response for {series_id} is missing required observation fields")
    df = df[["date", "value"]].copy()
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["series_id"] = series_id
    return df[["date", "value", "series_id"]]


def fetch_series(
    series_id: str,
    timeout: int = 60,
    max_retries: int = 2,
    backoff_seconds: float = 1.0,
) -> pd.DataFrame:
    response = _fetch_series_response(series_id, timeout, max_retries, backoff_seconds)
    if response.request.url and response.request.url.startswith(FRED_API_URL):
        return _parse_api_observations(response.json(), series_id)

    df = pd.read_csv(io.StringIO(response.text))
    if df.empty:
        return pd.DataFrame(columns=["date", "value", "series_id"])
    first_col, second_col = df.columns[:2]
    df = df.rename(columns={first_col: "date", second_col: "value"})
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["series_id"] = series_id
    return df[["date", "value", "series_id"]]


def fetch_bundle(series_ids: Sequence[str] | Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    if isinstance(series_ids, Mapping):
        series_map = dict(series_ids)
    else:
        series_map = {series_id: fetch_series(series_id) for series_id in series_ids}
    return build_bundle_from_frames(series_map)


def build_bundle_from_frames(series_frames: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    merged = None
    for series_id, frame in series_frames.items():
        s = frame.rename(columns={"value": series_id}).drop(columns=["series_id"])
        if merged is None:
            merged = s
        else:
            merged = merged.merge(s, on="date", how="outer")
    if merged is None:
        return pd.DataFrame(columns=["date"])
    return merged.sort_values("date").reset_index(drop=True)


def save_bundle(bundle: pd.DataFrame, series_meta: list[dict], out_dir: Path) -> None:
    write_df(bundle, out_dir / "core_wide.csv")
    write_json({"downloaded_series": series_meta}, out_dir / "core_manifest.json")
