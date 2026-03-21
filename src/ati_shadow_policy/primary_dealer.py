from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import urlparse

import pandas as pd

from .io_utils import download_binary_with_metadata, pick_first_existing as _pick_first_existing
from .webscrape import extract_links, filter_links

URLS = [
    "https://www.newyorkfed.org/markets/primarydealers.html",
    "https://www.newyorkfed.org/markets/counterparties/primary-dealers-statistics",
]
RAW_DIR_NAME = "primary_dealer"
PROCESSED_FILENAME = "primary_dealer_inventory.csv"
PROCESSED_PANEL_FILENAME = "primary_dealer_panel.csv"

_DOWNLOADABLE_EXTENSIONS = [".csv", ".json", ".xml"]
_PRIMARY_DEALER_HREF_TOKENS = ["markets.newyorkfed.org/api/pd", "markets.newyorkfed.org/api/marketshare"]
_PANEL_DATASET_TYPES = {"latest_series_snapshot", "quarterly_marketshare", "ytd_marketshare", "series_catalog"}
_PANEL_SOURCE_PRIORITY = {
    "latest_series_snapshot": {".csv": 0, ".json": 1, ".xml": 2},
    "series_catalog": {".csv": 0, ".json": 1, ".xml": 2},
    "quarterly_marketshare": {".json": 0, ".xml": 1, ".csv": 2},
    "ytd_marketshare": {".json": 0, ".xml": 1, ".csv": 2},
}
_MARKETSHARE_METRIC_UNITS = {
    "dailyAvgVolInMillions": "USD millions",
}
_MARKETSHARE_PERCENT_FIELDS = {
    "percentFirstQuintMktShare",
    "percentSecondQuintMktShare",
    "percentThirdQuintMktShare",
    "percentFourthQuintMktShare",
    "percentFifthQuintMktShare",
}


def collect_links(urls: Sequence[str] = URLS) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for url in urls:
        try:
            links = extract_links(url)
        except Exception:
            continue
        links["start_url"] = url
        links["source_url"] = url
        frames.append(links)
    if not frames:
        return pd.DataFrame(columns=["source_page", "start_url", "text", "href"])
    links_df = pd.concat(frames, ignore_index=True).drop_duplicates()
    return links_df.reset_index(drop=True)


def build_manifest(links: pd.DataFrame) -> pd.DataFrame:
    required = {"href", "text"}
    missing = required - set(links.columns)
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(f"Primary dealer manifest requires columns: {missing_str}")

    manifest = filter_links(
        links,
        href_contains=_PRIMARY_DEALER_HREF_TOKENS,
        allowed_extensions=_DOWNLOADABLE_EXTENSIONS,
    ).copy()
    if manifest.empty:
        return manifest

    manifest["href"] = manifest["href"].fillna("").astype(str)
    manifest["text"] = manifest["text"].fillna("").astype(str)
    if "source_page" in manifest.columns:
        manifest["source_page"] = manifest["source_page"].fillna("").astype(str)
    else:
        manifest["source_page"] = ""
    if "start_url" in manifest.columns:
        manifest["start_url"] = manifest["start_url"].fillna("").astype(str)
    else:
        manifest["start_url"] = manifest["source_page"]

    manifest["source_url"] = manifest["start_url"].where(manifest["start_url"].ne(""), manifest["source_page"])
    manifest["href_extension"] = manifest["href"].map(_href_extension)
    manifest["source_href_sha1"] = manifest["href"].map(_sha1)
    manifest["dataset_type"] = manifest["href"].map(_dataset_type_from_href)
    manifest["file_family"] = manifest["href"].map(_file_family_from_href)
    manifest["series_break"] = manifest["href"].map(_series_break_from_href)
    manifest["release_scope"] = manifest["href"].map(_release_scope_from_href)
    manifest["normalized_href"] = manifest["href"].str.lower()
    manifest["normalized_text"] = manifest["text"].str.lower()

    return (
        manifest.sort_values(["dataset_type", "href"], ascending=[True, True])
        .drop_duplicates(subset=["href"], keep="first")
        .reset_index(drop=True)
        .drop(columns=["normalized_href", "normalized_text"])
    )


def build_inventory(manifest: pd.DataFrame, downloads: pd.DataFrame | None = None) -> pd.DataFrame:
    required = {"href", "text"}
    missing = required - set(manifest.columns)
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(f"Primary dealer inventory requires columns: {missing_str}")

    inventory = manifest.copy().drop_duplicates(subset=["href"]).reset_index(drop=True)
    if downloads is not None and not downloads.empty:
        download_rows = downloads.copy().drop_duplicates(subset=["href"]).reset_index(drop=True)
        inventory = inventory.merge(download_rows, on=["href"], how="left", suffixes=("", "_download"))

    if "source_url" not in inventory.columns:
        inventory["source_url"] = inventory.get("start_url", inventory.get("source_page", pd.NA))
    if "local_path" not in inventory.columns:
        inventory["local_path"] = pd.NA
    if "local_filename" not in inventory.columns:
        inventory["local_filename"] = pd.NA
    if "local_extension" not in inventory.columns:
        inventory["local_extension"] = pd.NA
    if "download_status" not in inventory.columns:
        inventory["download_status"] = pd.NA
    if "source_href_sha1" not in inventory.columns:
        inventory["source_href_sha1"] = inventory["href"].map(_sha1)
    else:
        inventory["source_href_sha1"] = inventory["source_href_sha1"].fillna(inventory["href"].map(_sha1))

    inventory["inventory_status"] = inventory["download_status"].fillna("manifest_only")
    inventory.loc[inventory["inventory_status"].isin(["ok", "skipped_existing"]), "inventory_status"] = "downloaded"
    inventory["file_available"] = inventory["inventory_status"].eq("downloaded")
    inventory["local_file_exists"] = inventory["local_path"].map(_path_exists)

    artifact_info = inventory["local_path"].map(_inspect_artifact)
    inventory["artifact_rows"] = artifact_info.map(lambda item: item["rows"] if item else pd.NA)
    inventory["artifact_columns"] = artifact_info.map(lambda item: item["columns"] if item else pd.NA)
    inventory["artifact_shape"] = artifact_info.map(lambda item: item["shape"] if item else pd.NA)
    inventory["artifact_detail"] = artifact_info.map(lambda item: item["detail"] if item else pd.NA)

    columns = [
        "source_url",
        "source_page",
        "start_url",
        "text",
        "href",
        "href_extension",
        "dataset_type",
        "file_family",
        "series_break",
        "release_scope",
        "source_href_sha1",
        "local_path",
        "local_filename",
        "local_extension",
        "download_status",
        "download_attempts",
        "http_status",
        "final_url",
        "content_type",
        "content_length",
        "etag",
        "last_modified",
        "downloaded_at_utc",
        "bytes_written",
        "skipped_existing",
        "error_type",
        "error_message",
        "inventory_status",
        "file_available",
        "local_file_exists",
        "artifact_rows",
        "artifact_columns",
        "artifact_shape",
        "artifact_detail",
    ]
    keep = [col for col in columns if col in inventory.columns]
    return inventory[keep].copy()


def build_panel(manifest: pd.DataFrame, downloads: pd.DataFrame | None = None) -> pd.DataFrame:
    required = {"href", "text"}
    missing = required - set(manifest.columns)
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(f"Primary dealer panel requires columns: {missing_str}")

    sources = _select_panel_sources(manifest, downloads)
    if sources.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "series_id",
                "metric_id",
                "series_label",
                "metric_label",
                "value",
                "units",
                "frequency",
                "source_file",
                "source_path",
                "source_quality",
                "source_url",
                "source_page",
                "source_dataset_type",
                "source_section",
                "source_title",
                "provenance_summary",
            ]
        )

    catalog_lookup = _build_series_catalog_lookup(sources)
    frames: list[pd.DataFrame] = []
    for source in sources.to_dict(orient="records"):
        dataset_type = str(source.get("dataset_type", "") or "")
        path = Path(str(source["local_path"]))
        if dataset_type == "latest_series_snapshot":
            frame = _normalize_snapshot_source(source, path, catalog_lookup)
        elif dataset_type in {"quarterly_marketshare", "ytd_marketshare"}:
            frame = _normalize_marketshare_source(source, path)
        else:
            frame = pd.DataFrame()
        if not frame.empty:
            frames.append(frame)

    if not frames:
        return pd.DataFrame(
            columns=[
                "date",
                "series_id",
                "metric_id",
                "series_label",
                "metric_label",
                "value",
                "units",
                "frequency",
                "source_file",
                "source_path",
                "source_quality",
                "source_url",
                "source_page",
                "source_dataset_type",
                "source_section",
                "source_title",
                "provenance_summary",
            ]
        )

    panel = pd.concat(frames, ignore_index=True)
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    panel = panel.sort_values(
        ["date", "source_dataset_type", "series_id", "metric_id", "source_file"],
        kind="stable",
    ).reset_index(drop=True)
    columns = [
        "date",
        "series_id",
        "metric_id",
        "series_label",
        "metric_label",
        "value",
        "units",
        "frequency",
        "source_file",
        "source_path",
        "source_quality",
        "source_url",
        "source_page",
        "source_dataset_type",
        "source_section",
        "source_title",
        "provenance_summary",
    ]
    return panel[[col for col in columns if col in panel.columns]].copy()


def summarize_panel(panel: pd.DataFrame) -> pd.DataFrame:
    if panel.empty:
        return pd.DataFrame(
            columns=[
                "source_dataset_type",
                "source_quality",
                "rows",
                "series_count",
                "metric_count",
                "first_date",
                "last_date",
            ]
        )

    summary = (
        panel.assign(
            series_id=panel["series_id"].fillna(""),
            metric_id=panel["metric_id"].fillna(""),
        )
        .groupby(["source_dataset_type", "source_quality"], dropna=False)
        .agg(
            rows=("value", "size"),
            series_count=("series_id", pd.Series.nunique),
            metric_count=("metric_id", pd.Series.nunique),
            first_date=("date", "min"),
            last_date=("date", "max"),
        )
        .reset_index()
        .sort_values(["source_dataset_type", "source_quality"])
        .reset_index(drop=True)
    )
    return summary


def download_manifest(manifest: pd.DataFrame, output_dir: Path, limit: int | None = None) -> pd.DataFrame:
    output_dir.mkdir(parents=True, exist_ok=True)
    records = []
    subset = manifest.head(limit) if limit is not None else manifest
    for _, row in subset.iterrows():
        href = str(row["href"])
        filename = _guess_download_filename(href)
        path = output_dir / filename
        record = dict(row)
        metadata = download_binary_with_metadata(href, path, skip_existing=True)
        record["local_path"] = str(path)
        record["local_filename"] = path.name
        record["local_extension"] = path.suffix.lower()
        record["filename_method"] = "stem_sha1_href_ext"
        record["source_href_sha1"] = hashlib.sha1(href.encode("utf-8")).hexdigest()
        record["download_status"] = metadata["download_status"]
        record["download_attempts"] = metadata["download_attempts"]
        record["http_status"] = metadata["http_status"]
        record["final_url"] = metadata["final_url"]
        record["content_type"] = metadata["content_type"]
        record["content_length"] = metadata["content_length"]
        record["etag"] = metadata["etag"]
        record["last_modified"] = metadata["last_modified"]
        record["downloaded_at_utc"] = metadata["downloaded_at_utc"]
        record["bytes_written"] = metadata["bytes_written"]
        record["skipped_existing"] = metadata["skipped_existing"]
        record["error_type"] = metadata["error_type"]
        record["error_message"] = metadata["error_message"]
        records.append(record)
    return pd.DataFrame(records)


def summarize_inventory(inventory: pd.DataFrame) -> pd.DataFrame:
    if inventory.empty:
        return pd.DataFrame(
            columns=[
                "dataset_type",
                "file_family",
                "total_links",
                "downloaded_links",
                "available_links",
                "observed_rows_total",
            ]
        )

    summary = (
        inventory.assign(
            downloaded_link=inventory["inventory_status"].eq("downloaded"),
            available_link=inventory["file_available"].astype(bool),
            observed_rows_numeric=pd.to_numeric(inventory["artifact_rows"], errors="coerce"),
        )
        .groupby(["dataset_type", "file_family"], dropna=False)
        .agg(
            total_links=("href", "size"),
            downloaded_links=("downloaded_link", "sum"),
            available_links=("available_link", "sum"),
            observed_rows_total=("observed_rows_numeric", "sum"),
        )
        .reset_index()
        .sort_values(["dataset_type", "file_family"])
        .reset_index(drop=True)
    )
    return summary


def _href_extension(href: str) -> str:
    return Path(str(href).split("?", 1)[0]).suffix.lower()


def _guess_download_filename(href: str) -> str:
    parsed = urlparse(href)
    stem = Path(parsed.path).stem or "primary_dealer"
    ext = _href_extension(href) or ".bin"
    href_hash = hashlib.sha1(href.encode("utf-8")).hexdigest()[:10]
    return f"{stem}_{href_hash}{ext}"


def _sha1(text: str) -> str:
    return hashlib.sha1(str(text).encode("utf-8")).hexdigest()


def _dataset_type_from_href(href: str) -> str:
    href_lower = str(href).lower()
    if "/api/pd/latest/" in href_lower and href_lower.endswith((".csv", ".xml", ".json")):
        return "latest_series_snapshot"
    if "/api/pd/list/timeseries" in href_lower:
        return "series_catalog"
    if "/api/marketshare/qtrly/latest" in href_lower:
        return "quarterly_marketshare"
    if "/api/marketshare/ytd/latest" in href_lower:
        return "ytd_marketshare"
    return "primary_dealer_download"


def _file_family_from_href(href: str) -> str:
    dataset_type = _dataset_type_from_href(href)
    if dataset_type == "latest_series_snapshot":
        return "dealer_statistics_snapshot"
    if dataset_type == "series_catalog":
        return "dealer_timeseries_catalog"
    if dataset_type == "quarterly_marketshare":
        return "dealer_marketshare_quarterly"
    if dataset_type == "ytd_marketshare":
        return "dealer_marketshare_ytd"
    return "dealer_download"


def _series_break_from_href(href: str) -> str:
    href_lower = str(href).lower()
    if "/api/pd/latest/" in href_lower:
        return Path(str(href).split("?", 1)[0]).stem
    if "/api/pd/list/timeseries" in href_lower:
        return "timeseries"
    if "/api/marketshare/qtrly/latest" in href_lower:
        return "quarterly"
    if "/api/marketshare/ytd/latest" in href_lower:
        return "ytd"
    return "unknown"


def _release_scope_from_href(href: str) -> str:
    dataset_type = _dataset_type_from_href(href)
    if dataset_type == "latest_series_snapshot":
        return "weekly"
    if dataset_type == "series_catalog":
        return "catalog"
    if dataset_type == "quarterly_marketshare":
        return "quarterly"
    if dataset_type == "ytd_marketshare":
        return "ytd"
    return "unknown"


def _path_exists(value: object) -> bool:
    if value is None or pd.isna(value):
        return False
    return Path(str(value)).exists()


def _inspect_artifact(value: object) -> dict[str, object] | None:
    if value is None or pd.isna(value):
        return None
    path = Path(str(value))
    if not path.exists() or path.stat().st_size == 0:
        return None

    suffix = path.suffix.lower()
    if suffix == ".csv":
        try:
            df = pd.read_csv(path)
        except Exception:
            return None
        rows, columns = df.shape
        return {
            "rows": int(rows),
            "columns": int(columns),
            "shape": f"{rows}x{columns}",
            "detail": "csv",
        }
    if suffix == ".json":
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {
                "rows": pd.NA,
                "columns": pd.NA,
                "shape": "invalid_json",
                "detail": "invalid_json",
            }
        rows, detail = _count_json_records(payload)
        columns = len(_json_top_level_keys(payload))
        shape = f"{rows}x{columns}" if columns else str(rows)
        return {
            "rows": int(rows),
            "columns": int(columns),
            "shape": shape,
            "detail": detail,
        }
    if suffix == ".xml":
        try:
            df = pd.read_xml(path)
        except Exception:
            return {
                "rows": pd.NA,
                "columns": pd.NA,
                "shape": "xml",
                "detail": "xml",
            }
        rows, columns = df.shape
        return {
            "rows": int(rows),
            "columns": int(columns),
            "shape": f"{rows}x{columns}",
            "detail": "xml",
        }
    return None


def _count_json_records(payload: object) -> tuple[int, str]:
    best = (0, "json")

    def walk(node: object, path: str = "root") -> None:
        nonlocal best
        if isinstance(node, list):
            if len(node) > best[0]:
                best = (len(node), path)
            for idx, item in enumerate(node):
                walk(item, f"{path}[{idx}]")
        elif isinstance(node, dict):
            for key, value in node.items():
                walk(value, f"{path}.{key}")

    walk(payload)
    return best


def _json_top_level_keys(payload: object) -> list[str]:
    if isinstance(payload, dict):
        return list(payload.keys())
    return []


def _select_panel_sources(manifest: pd.DataFrame, downloads: pd.DataFrame | None) -> pd.DataFrame:
    merged = manifest.copy()
    if downloads is not None and not downloads.empty:
        download_cols = [col for col in downloads.columns if col != "href"]
        merged = merged.merge(downloads[["href", *download_cols]], on="href", how="left", suffixes=("", "_download"))

    if "local_path" not in merged.columns:
        merged["local_path"] = pd.NA
    if "local_filename" not in merged.columns:
        merged["local_filename"] = pd.NA
    if "local_extension" not in merged.columns:
        merged["local_extension"] = merged["href"].map(_href_extension)

    merged["dataset_type"] = merged["dataset_type"].fillna("").astype(str)
    merged["local_extension"] = merged["local_extension"].fillna("").astype(str).str.lower()
    merged = merged[merged["dataset_type"].isin(_PANEL_DATASET_TYPES)].copy()
    if merged.empty:
        return merged

    merged["source_priority"] = merged.apply(
        lambda row: _PANEL_SOURCE_PRIORITY.get(row["dataset_type"], {}).get(str(row["local_extension"]).lower(), 99),
        axis=1,
    )
    merged["source_quality"] = merged.apply(_panel_source_quality, axis=1)
    merged = merged.sort_values(
        ["dataset_type", "source_priority", "href"],
        ascending=[True, True, True],
    )
    return merged.drop_duplicates(subset=["dataset_type"], keep="first").reset_index(drop=True)


def _panel_source_quality(row: pd.Series) -> str:
    ext = str(row.get("local_extension", "") or "").lower()
    if ext == ".csv":
        return "csv_canonical"
    if ext == ".json":
        path = row.get("local_path")
        if pd.isna(path) or not str(path).strip():
            return "json_canonical"
        try:
            _, quality = _load_json_payload(Path(str(path)))
        except Exception:
            return "json_unparseable"
        return quality
    if ext == ".xml":
        return "xml_fallback"
    return "unknown"


def _load_json_payload(path: Path) -> tuple[object, str]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    try:
        return json.loads(text), "json_canonical"
    except json.JSONDecodeError:
        repaired = re.sub(r":\s*\*(\s*[},])", r": null\1", text)
        if repaired != text:
            return json.loads(repaired), "json_repaired"
        raise


def _build_series_catalog_lookup(sources: pd.DataFrame) -> dict[str, str]:
    catalog_sources = sources.loc[sources["dataset_type"].eq("series_catalog")]
    if catalog_sources.empty:
        return {}

    row = catalog_sources.iloc[0]
    path = Path(str(row["local_path"]))
    ext = str(row.get("local_extension", "") or "").lower()
    lookup: dict[str, str] = {}
    if ext == ".csv":
        try:
            df = pd.read_csv(path)
        except Exception:
            return lookup
        key_col = _pick_first_existing(df.columns, ["Key Id", "keyid", "series_id"])
        label_col = _pick_first_existing(df.columns, ["Label", "description", "series_label"])
        for _, record in df[[key_col, label_col]].iterrows():
            lookup[str(record[key_col])] = str(record[label_col])
        return lookup
    if ext == ".json":
        try:
            payload, _ = _load_json_payload(path)
        except Exception:
            return lookup
        records = payload.get("pd", {}).get("timeseries", []) if isinstance(payload, dict) else []
        for record in records:
            keyid = str(record.get("keyid", "") or "")
            if keyid:
                lookup[keyid] = str(record.get("description", "") or "")
    return lookup


def _normalize_snapshot_source(source: dict[str, object], path: Path, catalog_lookup: dict[str, str]) -> pd.DataFrame:
    ext = str(source.get("local_extension", "") or "").lower()
    source_quality = str(source.get("source_quality", "") or "unknown")
    if ext not in {".csv", ".json"}:
        return pd.DataFrame()

    if ext == ".csv":
        df = pd.read_csv(path)
        series_col = _pick_first_existing(df.columns, ["Time Series", "keyid", "series_id"])
        date_col = _pick_first_existing(df.columns, ["As Of Date", "asofdate", "date"])
        value_col = _pick_first_existing(df.columns, ["Value (millions)", "value"])
    else:
        payload, quality = _load_json_payload(path)
        source_quality = quality
        records = payload.get("pd", {}).get("timeseries", []) if isinstance(payload, dict) else []
        df = pd.DataFrame.from_records(records)
        if df.empty:
            return df
        series_col = _pick_first_existing(df.columns, ["keyid", "series_id"])
        date_col = _pick_first_existing(df.columns, ["asofdate", "date"])
        value_col = _pick_first_existing(df.columns, ["value"])

    out = pd.DataFrame(
        {
            "date": pd.to_datetime(df[date_col], errors="coerce"),
            "series_id": df[series_col].astype(str),
            "metric_id": pd.NA,
            "series_label": df[series_col].astype(str).map(catalog_lookup).fillna(df[series_col].astype(str)),
            "metric_label": pd.NA,
            "value": pd.to_numeric(df[value_col], errors="coerce"),
            "units": "USD millions",
            "frequency": "weekly",
            "source_file": str(source.get("local_filename") or path.name),
            "source_path": str(source.get("local_path") or path),
            "source_quality": source_quality,
            "source_url": str(source.get("href") or ""),
            "source_page": str(source.get("source_page") or ""),
            "source_dataset_type": str(source.get("dataset_type") or "latest_series_snapshot"),
            "source_section": pd.NA,
            "source_title": "dealer_statistics_snapshot",
        }
    )
    out["provenance_summary"] = out.apply(
        lambda row: "; ".join(
            [
                f"source_file={row['source_file']}",
                f"quality={row['source_quality']}",
                f"series_id={row['series_id']}",
                f"dataset_type={row['source_dataset_type']}",
            ]
        ),
        axis=1,
    )
    out = out[out["value"].notna()].reset_index(drop=True)
    return out


def _normalize_marketshare_source(source: dict[str, object], path: Path) -> pd.DataFrame:
    payload, quality = _load_json_payload(path)
    source_quality = quality if str(source.get("local_extension", "")).lower() == ".json" else str(source.get("source_quality", "") or "unknown")
    if not isinstance(payload, dict):
        return pd.DataFrame()
    marketshare = payload.get("pd", {}).get("marketshare", {})
    if not isinstance(marketshare, dict):
        return pd.DataFrame()

    dataset_type = str(source.get("dataset_type") or "")
    scope = "quarterly" if dataset_type == "quarterly_marketshare" else "ytd"
    root = marketshare.get(scope, {})
    if not isinstance(root, dict):
        return pd.DataFrame()
    release_date = root.get("releaseDate")
    title = root.get("title")

    rows: list[dict[str, object]] = []
    for section in ["interDealerBrokers", "others", "totals"]:
        entries = root.get(section, [])
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            security_type = str(entry.get("securityType", "") or "")
            security = str(entry.get("security", "") or "")
            series_id = _slugify_series_id(section, security_type, security)
            series_label = " | ".join(part for part in [security_type, security] if part)
            for key, raw_value in entry.items():
                if key in {"securityType", "security", "percentFirstQuintRange", "percentSecondQuintRange", "percentThirdQuintRange", "percentFourthQuintRange", "percentFifthQuintRange"}:
                    continue
                value = pd.to_numeric(pd.Series([raw_value]), errors="coerce").iloc[0]
                if pd.isna(value):
                    continue
                metric_id = _normalize_marketshare_metric_id(key)
                rows.append(
                    {
                        "date": pd.to_datetime(release_date, errors="coerce"),
                        "series_id": series_id,
                        "metric_id": metric_id,
                        "series_label": series_label or section,
                        "metric_label": key,
                        "value": float(value),
                        "units": _marketshare_units(key),
                        "frequency": scope,
                        "source_file": str(source.get("local_filename") or path.name),
                        "source_path": str(source.get("local_path") or path),
                        "source_quality": source_quality,
                        "source_url": str(source.get("href") or ""),
                        "source_page": str(source.get("source_page") or ""),
                        "source_dataset_type": dataset_type,
                        "source_section": section,
                        "source_title": str(title or ""),
                        "provenance_summary": "; ".join(
                            [
                                f"source_file={source.get('local_filename') or path.name}",
                                f"quality={source_quality}",
                                f"section={section}",
                                f"series={series_label or section}",
                                f"metric={metric_id}",
                            ]
                        ),
                    }
                )
    return pd.DataFrame(rows)


def _slugify_series_id(section: str, security_type: str, security: str) -> str:
    text = " | ".join(part for part in [section, security_type, security] if part)
    text = re.sub(r"[^A-Za-z0-9]+", "_", text.lower())
    return re.sub(r"_+", "_", text).strip("_")


def _normalize_marketshare_metric_id(field: str) -> str:
    field = str(field)
    field = field.replace("percent", "percent_")
    field = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", field)
    field = re.sub(r"[^A-Za-z0-9]+", "_", field)
    return re.sub(r"_+", "_", field).strip("_").lower()


def _marketshare_units(field: str) -> str:
    if field in _MARKETSHARE_PERCENT_FIELDS:
        return "percent"
    if field in _MARKETSHARE_METRIC_UNITS:
        return _MARKETSHARE_METRIC_UNITS[field]
    return "unknown"
