from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd
import requests


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _sanitize_for_json(obj: object) -> object:
    """Replace NaN/Infinity with None so json.dumps emits valid JSON."""
    import math

    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(v) for v in obj]
    return obj


def write_json(obj: dict, path: Path) -> None:
    ensure_dir(path.parent)
    clean = _sanitize_for_json(obj)
    path.write_text(json.dumps(clean, indent=2, default=str) + "\n", encoding="utf-8")


def write_text(text: str, path: Path) -> None:
    ensure_dir(path.parent)
    path.write_text(text, encoding="utf-8")


def write_df(df: pd.DataFrame, path: Path) -> None:
    ensure_dir(path.parent)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df.to_csv(path, index=False)
    elif suffix == ".parquet":
        df.to_parquet(path, index=False)
    else:
        raise ValueError(f"Unsupported output format: {path}")


def read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported tabular format: {path}")


def coerce_numeric(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    )
    return pd.to_numeric(cleaned, errors="coerce")


def pick_first_existing(columns: Iterable[str], candidates: Sequence[str]) -> str:
    normalized = {str(col).lower(): str(col) for col in columns}
    for candidate in candidates:
        if candidate.lower() in normalized:
            return normalized[candidate.lower()]
    raise KeyError(
        "None of the candidate columns were found. "
        f"Candidates={list(candidates)} Available={list(columns)}"
    )


def slugify(text: str) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", text.strip())
    return re.sub(r"_+", "_", text).strip("_") or "file"


def default_headers() -> dict[str, str]:
    return {
        "User-Agent": "ati-shadow-policy-research-scaffold/0.1"
    }


def _should_retry_request(exc: requests.RequestException) -> bool:
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True
    response = getattr(exc, "response", None)
    if response is None:
        return False
    return response.status_code in {408, 425, 429, 500, 502, 503, 504}


def download_binary_with_metadata(
    url: str,
    path: Path,
    timeout: int = 60,
    max_retries: int = 2,
    backoff_seconds: float = 1.0,
    skip_existing: bool = False,
) -> dict[str, Any]:
    ensure_dir(path.parent)
    if skip_existing and path.exists() and path.stat().st_size > 0:
        return {
            "download_status": "skipped_existing",
            "download_attempts": 0,
            "http_status": None,
            "final_url": url,
            "content_type": None,
            "content_length": path.stat().st_size,
            "etag": None,
            "last_modified": None,
            "downloaded_at_utc": None,
            "bytes_written": 0,
            "skipped_existing": True,
            "error_type": None,
            "error_message": None,
        }

    attempts = max(0, int(max_retries)) + 1
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(
                url,
                headers=default_headers(),
                timeout=timeout,
                stream=True,
            )
            response.raise_for_status()
            bytes_written = 0
            tmp_path = path.with_name(f".{path.name}.part")
            with tmp_path.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        bytes_written += len(chunk)
                        handle.write(chunk)
            tmp_path.replace(path)
            return {
                "download_status": "ok",
                "download_attempts": attempt,
                "http_status": response.status_code,
                "final_url": response.url,
                "content_type": response.headers.get("Content-Type"),
                "content_length": response.headers.get("Content-Length"),
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
                "bytes_written": bytes_written,
                "skipped_existing": False,
                "error_type": None,
                "error_message": None,
            }
        except requests.RequestException as exc:
            if attempt < attempts and _should_retry_request(exc):
                time.sleep(backoff_seconds * attempt)
                continue
            return {
                "download_status": "error",
                "download_attempts": attempt,
                "http_status": getattr(getattr(exc, "response", None), "status_code", None),
                "final_url": url,
                "content_type": None,
                "content_length": None,
                "etag": None,
                "last_modified": None,
                "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
                "bytes_written": 0,
                "skipped_existing": False,
                "error_type": exc.__class__.__name__,
                "error_message": str(exc),
            }

    return {
        "download_status": "error",
        "download_attempts": attempts,
        "http_status": None,
        "final_url": url,
        "content_type": None,
        "content_length": None,
        "etag": None,
        "last_modified": None,
        "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        "bytes_written": 0,
        "skipped_existing": False,
        "error_type": "RuntimeError",
        "error_message": "Download failed unexpectedly without an exception",
    }


def download_binary(
    url: str,
    path: Path,
    timeout: int = 60,
    max_retries: int = 2,
    backoff_seconds: float = 1.0,
    skip_existing: bool = False,
) -> Path:
    metadata = download_binary_with_metadata(
        url=url,
        path=path,
        timeout=timeout,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        skip_existing=skip_existing,
    )
    if metadata["download_status"] == "error":
        raise RuntimeError(metadata["error_message"] or "Download failed")
    return path
