from __future__ import annotations

import csv
import hashlib
import os
import re
from datetime import date, datetime, timezone
from io import TextIOWrapper
from pathlib import Path
from urllib.parse import urljoin, urlparse
from zipfile import BadZipFile, ZipFile

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .io_utils import download_binary_with_metadata, ensure_dir, slugify

URL = "https://www.sec.gov/data-research/sec-markets-data/dera-form-n-mfp-data-sets"
RAW_DIR_NAME = "sec_nmfp"
PROCESSED_FILENAME = "sec_nmfp_inventory.csv"
SUMMARY_PANEL_FILENAME = "sec_nmfp_summary_panel.csv"

_SEC_DATA_PATH_TOKEN = "/files/dera/data/form-n-mfp-data-sets/"
_NMFP_QUARTER_RE = re.compile(r"(?P<year>\d{4})q(?P<quarter>[1-4])_nmfp\.zip$", flags=re.IGNORECASE)
_NMFP_MONTHLY_RE = re.compile(
    r"(?P<start>\d{8})-(?P<end>\d{8})_nmfp\.zip$",
    flags=re.IGNORECASE,
)
_SUMMARY_COLUMNS = [
    "summary_type",
    "dataset_id",
    "dataset_version",
    "period_family",
    "period_label",
    "measure",
    "value",
    "units",
    "source_quality",
    "source_file",
    "source_href",
    "local_path",
    "parse_status",
    "parse_error",
    "generated_at_utc",
]


def collect_links(url: str = URL) -> pd.DataFrame:
    response = requests.get(url, headers=_sec_headers(), timeout=60)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")
    rows = []
    for anchor in soup.find_all("a", href=True):
        href_raw = str(anchor["href"]).strip()
        href = urljoin(url, href_raw)
        text = " ".join(anchor.get_text(" ", strip=True).split())
        rows.append(
            {
                "source_page": url,
                "start_url": url,
                "text": text,
                "href": href,
            }
        )
    if not rows:
        return pd.DataFrame(columns=["source_page", "start_url", "text", "href"])
    return pd.DataFrame(rows).drop_duplicates().reset_index(drop=True)


def build_manifest(links: pd.DataFrame) -> pd.DataFrame:
    required = {"href", "text"}
    missing = required - set(links.columns)
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(f"SEC N-MFP manifest requires columns: {missing_str}")

    df = links.copy()
    df["href"] = df["href"].fillna("").astype(str)
    df["text"] = df["text"].fillna("").astype(str)
    if "source_page" not in df.columns:
        df["source_page"] = URL
    if "start_url" not in df.columns:
        df["start_url"] = URL
    df["source_page"] = df["source_page"].fillna(URL).astype(str)
    df["start_url"] = df["start_url"].fillna(URL).astype(str)
    df["href_lower"] = df["href"].str.lower()
    df["text_lower"] = df["text"].str.lower()
    df["href_extension"] = df["href"].map(_href_extension)
    df["source_href_sha1"] = df["href"].map(_sha1)
    df["is_sec_domain"] = df["href_lower"].str.contains(r"^https?://(?:www\.)?sec\.gov/")
    df["is_nmfp_signal"] = (
        df["href_lower"].str.contains("nmfp")
        | df["text_lower"].str.contains(r"\bn-?mfp\b", regex=True)
        | df["href_lower"].str.contains(_SEC_DATA_PATH_TOKEN)
        | df["href_lower"].str.contains("formn-mfp")
    )
    keep = df["is_sec_domain"] & df["is_nmfp_signal"]
    manifest = df[keep].copy()
    if manifest.empty:
        return manifest.drop(columns=["href_lower", "text_lower", "is_sec_domain", "is_nmfp_signal"])

    manifest["resource_type"] = manifest.apply(_resource_type, axis=1)
    manifest["archive_type"] = manifest.apply(_archive_type, axis=1)
    manifest["readme_or_archive_type"] = manifest.apply(_readme_or_archive_type, axis=1)
    manifest["period_family"] = manifest["href_lower"].map(_period_family)
    manifest["dataset_version"] = manifest["href_lower"].map(_dataset_version)
    manifest["dataset_version_detail"] = manifest.apply(_dataset_version_detail, axis=1)
    manifest["period_start"] = manifest["href_lower"].map(_period_start)
    manifest["period_end"] = manifest["href_lower"].map(_period_end)
    manifest["period_label"] = manifest.apply(_period_label, axis=1)
    manifest["dataset_id"] = manifest["href_lower"].map(_dataset_id)
    manifest["period_sort_rank"] = manifest["period_family"].map(
        {
            "documentation": 0,
            "quarterly": 1,
            "monthly": 2,
        }
    ).fillna(9)
    manifest = (
        manifest.sort_values(
            ["period_sort_rank", "period_start", "period_end", "href"],
            ascending=[True, True, True, True],
        )
        .drop_duplicates(subset=["href"], keep="first")
        .reset_index(drop=True)
    )
    return manifest[
        [
            "source_page",
            "start_url",
            "text",
            "href",
            "href_extension",
            "resource_type",
            "archive_type",
            "readme_or_archive_type",
            "period_family",
            "dataset_version",
            "dataset_version_detail",
            "period_start",
            "period_end",
            "period_label",
            "dataset_id",
            "source_href_sha1",
        ]
    ].copy()


def download_manifest(
    manifest: pd.DataFrame,
    output_dir: Path,
    limit: int | None = None,
    skip_existing: bool = True,
) -> pd.DataFrame:
    ensure_dir(output_dir)
    records = []
    subset = manifest.head(limit) if limit is not None else manifest
    for _, row in subset.iterrows():
        href = str(row["href"])
        source_sha1 = str(row.get("source_href_sha1") or _sha1(href))
        filename = _local_filename(href, source_sha1[:10])
        local_path = output_dir / filename
        metadata = download_binary_with_metadata(
            url=href,
            path=local_path,
            skip_existing=skip_existing,
        )
        record = dict(row)
        record["local_path"] = str(local_path)
        record["local_filename"] = filename
        record["local_extension"] = local_path.suffix.lower()
        record["filename_method"] = "url_stem_sha1_ext"
        record["source_href_sha1"] = source_sha1
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


def build_inventory(manifest: pd.DataFrame, downloads: pd.DataFrame | None = None) -> pd.DataFrame:
    required = {"href", "text"}
    missing = required - set(manifest.columns)
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(f"SEC N-MFP inventory requires columns: {missing_str}")

    inventory = manifest.copy().drop_duplicates(subset=["href"]).reset_index(drop=True)
    if downloads is not None and not downloads.empty:
        download_rows = downloads.copy().drop_duplicates(subset=["href"]).reset_index(drop=True)
        inventory = inventory.merge(download_rows, on=["href"], how="left", suffixes=("", "_download"))
    if "source_href_sha1" not in inventory.columns:
        inventory["source_href_sha1"] = inventory["href"].map(_sha1)
    else:
        inventory["source_href_sha1"] = inventory["source_href_sha1"].fillna(inventory["href"].map(_sha1))
    if "download_status" not in inventory.columns:
        inventory["download_status"] = pd.NA
    if "local_path" not in inventory.columns:
        inventory["local_path"] = pd.NA
    if "local_filename" not in inventory.columns:
        inventory["local_filename"] = pd.NA
    inventory["inventory_status"] = inventory["download_status"].fillna("manifest_only")
    inventory.loc[inventory["inventory_status"].isin(["ok", "skipped_existing"]), "inventory_status"] = "downloaded"
    inventory["file_available"] = inventory["inventory_status"].isin(["downloaded"])
    inventory["local_file_exists"] = inventory["local_path"].map(_path_exists)

    columns = [
        "source_page",
        "start_url",
        "text",
        "href",
        "href_extension",
        "resource_type",
        "archive_type",
        "readme_or_archive_type",
        "period_family",
        "dataset_version",
        "dataset_version_detail",
        "period_start",
        "period_end",
        "period_label",
        "dataset_id",
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
    ]
    keep = [col for col in columns if col in inventory.columns]
    return inventory[keep].copy()


def build_summary_panel(inventory: pd.DataFrame) -> pd.DataFrame:
    required = {"dataset_version", "period_family", "period_label", "dataset_id"}
    missing = required - set(inventory.columns)
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(f"SEC N-MFP summary panel requires columns: {missing_str}")

    rows: list[dict[str, object]] = []
    generated_at = datetime.now(timezone.utc).isoformat()
    df = inventory.copy()
    if "resource_type" not in df.columns:
        df["resource_type"] = "dataset_archive"
    archives = df.loc[df["resource_type"].eq("dataset_archive")].copy()
    if archives.empty:
        return pd.DataFrame(columns=_SUMMARY_COLUMNS)

    archives = archives.sort_values(
        ["period_start", "period_end", "period_label", "dataset_id", "href"],
        ascending=[True, True, True, True, True],
        na_position="last",
    ).reset_index(drop=True)
    coverage_records: list[dict[str, object]] = []
    for _, row in archives.iterrows():
        base = _summary_base(row, generated_at)
        local_path = str(row.get("local_path", "") or "")
        local_exists = _path_exists(local_path)
        download_status = str(row.get("download_status", "") or "")
        download_ok = download_status in {"ok", "skipped_existing"}
        archive_info = _inspect_archive(Path(local_path) if local_exists else None)
        parse_status = _derive_parse_status(archive_info)
        parse_error = _derive_parse_error(archive_info)

        rows.append(
            _summary_row(
                base,
                summary_type="archive_status",
                measure="archive_listed",
                value=1,
                units="flag_0_1",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="archive_status",
                measure="local_file_available",
                value=int(local_exists),
                units="flag_0_1",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="archive_status",
                measure="download_ok",
                value=int(download_ok),
                units="flag_0_1",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="archive_status",
                measure="zip_readable",
                value=int(archive_info["zip_readable"]),
                units="flag_0_1",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="archive_file_availability",
                measure="archive_file_count",
                value=int(archive_info["archive_file_count"]),
                units="count",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="archive_file_availability",
                measure="has_submission_table",
                value=int(archive_info["has_submission_table"]),
                units="flag_0_1",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="archive_file_availability",
                measure="has_portfolio_table",
                value=int(archive_info["has_portfolio_table"]),
                units="flag_0_1",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="archive_file_availability",
                measure="has_metadata_json",
                value=int(archive_info["has_metadata_json"]),
                units="flag_0_1",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="archive_file_availability",
                measure="has_readme_html",
                value=int(archive_info["has_readme_html"]),
                units="flag_0_1",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="archive_counts",
                measure="report_count",
                value=int(archive_info["report_count"]),
                units="count",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="archive_counts",
                measure="accession_count",
                value=int(archive_info["accession_count"]),
                units="count",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="archive_counts",
                measure="series_count",
                value=int(archive_info["series_count"]),
                units="count",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="archive_counts",
                measure="filer_count",
                value=int(archive_info["filer_count"]),
                units="count",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="field_availability",
                measure="field_investment_category_present",
                value=int(archive_info["field_investment_category_present"]),
                units="flag_0_1",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="field_availability",
                measure="field_repo_open_flag_present",
                value=int(archive_info["field_repo_open_flag_present"]),
                units="flag_0_1",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="field_availability",
                measure="field_value_including_support_present",
                value=int(archive_info["field_value_including_support_present"]),
                units="flag_0_1",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="field_availability",
                measure="field_value_excluding_support_present",
                value=int(archive_info["field_value_excluding_support_present"]),
                units="flag_0_1",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="field_availability",
                measure="field_pct_net_assets_present",
                value=int(archive_info["field_pct_net_assets_present"]),
                units="flag_0_1",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="field_availability",
                measure="field_cusip_present",
                value=int(archive_info["field_cusip_present"]),
                units="flag_0_1",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )
        rows.append(
            _summary_row(
                base,
                summary_type="field_availability",
                measure="treasury_repo_exposure_fields_ready",
                value=int(
                    archive_info["field_investment_category_present"]
                    and archive_info["field_repo_open_flag_present"]
                    and archive_info["field_value_including_support_present"]
                ),
                units="flag_0_1",
                parse_status=parse_status,
                parse_error=parse_error,
            )
        )

        coverage_records.append(
            {
                "dataset_version": base["dataset_version"],
                "period_family": base["period_family"],
                "archive_count": 1,
                "local_file_count": int(local_exists),
                "zip_readable_count": int(archive_info["zip_readable"]),
                "submission_parse_ok_count": int(archive_info["submission_parse_ok"]),
                "portfolio_parse_ok_count": int(archive_info["portfolio_parse_ok"]),
                "report_count_total": int(archive_info["report_count"]),
                "series_count_total": int(archive_info["series_count"]),
                "period_label": base["period_label"],
                "parse_error_count": int(_has_value(parse_error)),
            }
        )

    if coverage_records:
        coverage_df = pd.DataFrame(coverage_records)
        grouped = (
            coverage_df.groupby(["dataset_version", "period_family"], dropna=False)
            .agg(
                archive_count=("archive_count", "sum"),
                local_file_count=("local_file_count", "sum"),
                zip_readable_count=("zip_readable_count", "sum"),
                submission_parse_ok_count=("submission_parse_ok_count", "sum"),
                portfolio_parse_ok_count=("portfolio_parse_ok_count", "sum"),
                report_count_total=("report_count_total", "sum"),
                series_count_total=("series_count_total", "sum"),
                periods_covered=("period_label", pd.Series.nunique),
                parse_error_count=("parse_error_count", "sum"),
            )
            .reset_index()
        )
        for _, row in grouped.iterrows():
            base = {
                "dataset_id": "all_archives",
                "dataset_version": row["dataset_version"],
                "period_family": row["period_family"],
                "period_label": "all_periods",
                "source_quality": "official_sec_archive_aggregate",
                "source_file": PROCESSED_FILENAME,
                "source_href": pd.NA,
                "local_path": pd.NA,
                "generated_at_utc": generated_at,
            }
            rows.append(
                _summary_row(
                    base,
                    summary_type="coverage_by_version_period",
                    measure="archive_count",
                    value=int(row["archive_count"]),
                    units="count",
                    parse_status="aggregate",
                    parse_error=pd.NA,
                )
            )
            rows.append(
                _summary_row(
                    base,
                    summary_type="coverage_by_version_period",
                    measure="local_file_count",
                    value=int(row["local_file_count"]),
                    units="count",
                    parse_status="aggregate",
                    parse_error=pd.NA,
                )
            )
            rows.append(
                _summary_row(
                    base,
                    summary_type="coverage_by_version_period",
                    measure="zip_readable_count",
                    value=int(row["zip_readable_count"]),
                    units="count",
                    parse_status="aggregate",
                    parse_error=pd.NA,
                )
            )
            rows.append(
                _summary_row(
                    base,
                    summary_type="coverage_by_version_period",
                    measure="submission_parse_ok_count",
                    value=int(row["submission_parse_ok_count"]),
                    units="count",
                    parse_status="aggregate",
                    parse_error=pd.NA,
                )
            )
            rows.append(
                _summary_row(
                    base,
                    summary_type="coverage_by_version_period",
                    measure="portfolio_parse_ok_count",
                    value=int(row["portfolio_parse_ok_count"]),
                    units="count",
                    parse_status="aggregate",
                    parse_error=pd.NA,
                )
            )
            rows.append(
                _summary_row(
                    base,
                    summary_type="coverage_by_version_period",
                    measure="report_count_total",
                    value=int(row["report_count_total"]),
                    units="count",
                    parse_status="aggregate",
                    parse_error=pd.NA,
                )
            )
            rows.append(
                _summary_row(
                    base,
                    summary_type="coverage_by_version_period",
                    measure="series_count_total",
                    value=int(row["series_count_total"]),
                    units="count",
                    parse_status="aggregate",
                    parse_error=pd.NA,
                )
            )
            rows.append(
                _summary_row(
                    base,
                    summary_type="coverage_by_version_period",
                    measure="periods_covered",
                    value=int(row["periods_covered"]),
                    units="count",
                    parse_status="aggregate",
                    parse_error=pd.NA,
                )
            )
            rows.append(
                _summary_row(
                    base,
                    summary_type="coverage_by_version_period",
                    measure="parse_error_count",
                    value=int(row["parse_error_count"]),
                    units="count",
                    parse_status="aggregate",
                    parse_error=pd.NA,
                )
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=_SUMMARY_COLUMNS)
    for col in _SUMMARY_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    out = out[_SUMMARY_COLUMNS].copy()
    out["summary_type"] = pd.Categorical(
        out["summary_type"],
        categories=[
            "archive_status",
            "archive_file_availability",
            "archive_counts",
            "field_availability",
            "coverage_by_version_period",
        ],
        ordered=True,
    )
    out = out.sort_values(
        [
            "dataset_version",
            "period_family",
            "period_label",
            "dataset_id",
            "summary_type",
            "measure",
        ],
        ascending=[True, True, True, True, True, True],
        na_position="last",
    ).reset_index(drop=True)
    out["summary_type"] = out["summary_type"].astype(str)
    return out


def _sec_headers() -> dict[str, str]:
    configured = os.getenv("SEC_USER_AGENT", "").strip()
    if configured:
        return {"User-Agent": configured}
    return {
        "User-Agent": "Mozilla/5.0 (compatible; qrawatch-sec-nmfp/0.1; +https://www.sec.gov)"
    }


def _href_extension(href: str) -> str:
    parsed = urlparse(str(href))
    return Path(parsed.path).suffix.lower()


def _sha1(value: str) -> str:
    return hashlib.sha1(str(value).encode("utf-8")).hexdigest()


def _resource_type(row: pd.Series) -> str:
    href = str(row.get("href_lower", ""))
    text = str(row.get("text_lower", ""))
    ext = str(row.get("href_extension", ""))
    if ext == ".zip" and _SEC_DATA_PATH_TOKEN in href:
        return "dataset_archive"
    if "readme" in href or "readme" in text:
        return "readme"
    if "formn-mfp" in href:
        return "form_spec"
    return "other"


def _archive_type(row: pd.Series) -> str:
    ext = str(row.get("href_extension", ""))
    if ext.startswith("."):
        return ext[1:] or "none"
    return "none"


def _readme_or_archive_type(row: pd.Series) -> str:
    resource_type = str(row.get("resource_type", "other"))
    archive_type = str(row.get("archive_type", "none"))
    if resource_type == "readme":
        return "readme"
    if resource_type == "dataset_archive":
        return f"archive_{archive_type}"
    if resource_type == "form_spec":
        return "form_spec"
    return "other"


def _period_family(href_lower: str) -> str:
    if _NMFP_MONTHLY_RE.search(href_lower):
        return "monthly"
    if _NMFP_QUARTER_RE.search(href_lower):
        return "quarterly"
    return "documentation"


def _dataset_version(href_lower: str) -> str:
    if _NMFP_MONTHLY_RE.search(href_lower):
        return "nmfp3"
    if _NMFP_QUARTER_RE.search(href_lower):
        return "legacy_nmfp"
    return "not_applicable"


def _dataset_version_detail(row: pd.Series) -> str:
    version = str(row.get("dataset_version", ""))
    period_family = str(row.get("period_family", ""))
    if version == "nmfp3":
        return "monthly_zip_filename_pattern"
    if version == "legacy_nmfp":
        return "quarterly_zip_filename_pattern"
    if period_family == "documentation":
        return "documentation_link"
    return "unknown"


def _period_start(href_lower: str) -> str | object:
    monthly = _NMFP_MONTHLY_RE.search(href_lower)
    if monthly:
        return _yyyymmdd_to_iso(monthly.group("start"))
    quarterly = _NMFP_QUARTER_RE.search(href_lower)
    if quarterly:
        q = int(quarterly.group("quarter"))
        month = 1 + (q - 1) * 3
        return date(int(quarterly.group("year")), month, 1).isoformat()
    return pd.NA


def _period_end(href_lower: str) -> str | object:
    monthly = _NMFP_MONTHLY_RE.search(href_lower)
    if monthly:
        return _yyyymmdd_to_iso(monthly.group("end"))
    quarterly = _NMFP_QUARTER_RE.search(href_lower)
    if quarterly:
        q = int(quarterly.group("quarter"))
        year = int(quarterly.group("year"))
        month = q * 3
        if month == 3:
            return date(year, 3, 31).isoformat()
        if month == 6:
            return date(year, 6, 30).isoformat()
        if month == 9:
            return date(year, 9, 30).isoformat()
        return date(year, 12, 31).isoformat()
    return pd.NA


def _period_label(row: pd.Series) -> str:
    period_family = str(row.get("period_family", ""))
    href = str(row.get("href_lower", ""))
    if period_family == "monthly":
        start = row.get("period_start")
        end = row.get("period_end")
        if pd.notna(start) and pd.notna(end):
            return f"{start}_to_{end}"
    if period_family == "quarterly":
        quarterly = _NMFP_QUARTER_RE.search(href)
        if quarterly:
            return f"{quarterly.group('year')}Q{quarterly.group('quarter')}"
    return "documentation"


def _dataset_id(href_lower: str) -> str:
    parsed = urlparse(href_lower)
    stem = Path(parsed.path).stem
    return stem or "sec_nmfp_resource"


def _yyyymmdd_to_iso(value: str) -> str:
    return f"{value[0:4]}-{value[4:6]}-{value[6:8]}"


def _local_filename(href: str, href_hash: str) -> str:
    parsed = urlparse(href)
    stem = Path(parsed.path).stem or "sec_nmfp_resource"
    ext = Path(parsed.path).suffix.lower() or ".bin"
    return f"{slugify(stem)[:120]}_{href_hash}{ext}"


def _path_exists(value: object) -> bool:
    if value is None or pd.isna(value):
        return False
    return Path(str(value)).exists()


def _summary_base(row: pd.Series, generated_at: str) -> dict[str, object]:
    local_path = str(row.get("local_path", "") or "")
    source_file = str(row.get("local_filename", "") or "")
    if not source_file and local_path:
        source_file = Path(local_path).name
    if not source_file:
        source_file = f"{row.get('dataset_id', 'sec_nmfp_archive')}.zip"
    return {
        "dataset_id": str(row.get("dataset_id", "") or ""),
        "dataset_version": str(row.get("dataset_version", "") or ""),
        "period_family": str(row.get("period_family", "") or ""),
        "period_label": str(row.get("period_label", "") or ""),
        "source_quality": "official_sec_archive",
        "source_file": source_file,
        "source_href": str(row.get("href", "") or ""),
        "local_path": local_path or pd.NA,
        "generated_at_utc": generated_at,
    }


def _summary_row(
    base: dict[str, object],
    *,
    summary_type: str,
    measure: str,
    value: int | float,
    units: str,
    parse_status: str,
    parse_error: str | object,
) -> dict[str, object]:
    row = dict(base)
    row["summary_type"] = summary_type
    row["measure"] = measure
    row["value"] = value
    row["units"] = units
    row["parse_status"] = parse_status
    row["parse_error"] = parse_error
    return row


def _inspect_archive(local_path: Path | None) -> dict[str, object]:
    info: dict[str, object] = {
        "zip_readable": False,
        "zip_error": pd.NA,
        "archive_file_count": 0,
        "has_submission_table": False,
        "has_portfolio_table": False,
        "has_metadata_json": False,
        "has_readme_html": False,
        "submission_parse_ok": False,
        "submission_parse_error": pd.NA,
        "portfolio_parse_ok": False,
        "portfolio_parse_error": pd.NA,
        "report_count": 0,
        "accession_count": 0,
        "series_count": 0,
        "filer_count": 0,
        "field_investment_category_present": False,
        "field_repo_open_flag_present": False,
        "field_value_including_support_present": False,
        "field_value_excluding_support_present": False,
        "field_pct_net_assets_present": False,
        "field_cusip_present": False,
    }
    if local_path is None:
        info["zip_error"] = "local_file_missing"
        return info

    try:
        with ZipFile(local_path) as archive:
            members = archive.namelist()
            members_upper = [name.upper() for name in members]
            info["zip_readable"] = True
            info["archive_file_count"] = len(members)
            info["has_metadata_json"] = any(name.endswith("NMFP_METADATA.JSON") for name in members_upper)
            info["has_readme_html"] = any(name.endswith("NMFP_README.HTM") for name in members_upper)
            submission_name = _member_name(members, "NMFP_SUBMISSION.tsv")
            portfolio_name = _member_name(members, "NMFP_SCHPORTFOLIOSECURITIES.tsv")
            info["has_submission_table"] = submission_name is not None
            info["has_portfolio_table"] = portfolio_name is not None

            if submission_name is not None:
                try:
                    report_count, accession_count, series_count, filer_count = _parse_submission_counts(
                        archive,
                        submission_name,
                    )
                    info["submission_parse_ok"] = True
                    info["report_count"] = report_count
                    info["accession_count"] = accession_count
                    info["series_count"] = series_count
                    info["filer_count"] = filer_count
                except Exception as exc:
                    info["submission_parse_error"] = f"{type(exc).__name__}: {exc}"

            if portfolio_name is not None:
                try:
                    header = set(_read_tsv_header(archive, portfolio_name))
                    info["portfolio_parse_ok"] = True
                    info["field_investment_category_present"] = "INVESTMENTCATEGORY" in header
                    info["field_repo_open_flag_present"] = "REPURCHASEAGREEMENTOPENFLAG" in header
                    info["field_value_including_support_present"] = "INCLUDINGVALUEOFANYSPONSORSUPP" in header
                    info["field_value_excluding_support_present"] = "EXCLUDINGVALUEOFANYSPONSORSUPP" in header
                    info["field_pct_net_assets_present"] = "PERCENTAGEOFMONEYMARKETFUNDNET" in header
                    info["field_cusip_present"] = "CUSIP_NUMBER" in header
                except Exception as exc:
                    info["portfolio_parse_error"] = f"{type(exc).__name__}: {exc}"
    except (BadZipFile, OSError) as exc:
        info["zip_error"] = f"{type(exc).__name__}: {exc}"
    return info


def _derive_parse_status(info: dict[str, object]) -> str:
    if not bool(info.get("zip_readable")):
        return "zip_unreadable"
    submission_present = bool(info.get("has_submission_table"))
    portfolio_present = bool(info.get("has_portfolio_table"))
    submission_ok = bool(info.get("submission_parse_ok"))
    portfolio_ok = bool(info.get("portfolio_parse_ok"))
    if submission_present and portfolio_present and submission_ok and portfolio_ok:
        return "parsed"
    if submission_present or portfolio_present:
        if submission_ok or portfolio_ok:
            return "parsed_partial"
        return "parse_failed"
    return "zip_no_expected_tables"


def _derive_parse_error(info: dict[str, object]) -> str | object:
    errors = []
    for key in ["zip_error", "submission_parse_error", "portfolio_parse_error"]:
        value = info.get(key)
        if value is None or pd.isna(value) or str(value).strip() == "":
            continue
        errors.append(str(value))
    if not errors:
        return pd.NA
    return " | ".join(errors)


def _has_value(value: object) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    return str(value).strip() != ""


def _member_name(members: list[str], target_name: str) -> str | None:
    target = target_name.upper()
    for member in members:
        if member.upper().endswith(target):
            return member
    return None


def _parse_submission_counts(archive: ZipFile, member_name: str) -> tuple[int, int, int, int]:
    report_count = 0
    accession_numbers: set[str] = set()
    series_ids: set[str] = set()
    filer_ids: set[str] = set()
    with archive.open(member_name) as raw:
        reader = csv.DictReader(
            TextIOWrapper(raw, encoding="utf-8", errors="ignore", newline=""),
            delimiter="\t",
        )
        for row in reader:
            report_count += 1
            accession = str(row.get("ACCESSION_NUMBER", "") or "").strip()
            series = str(row.get("SERIESID", "") or "").strip()
            filer = str(row.get("FILER_CIK", "") or row.get("CIK", "") or "").strip()
            if accession:
                accession_numbers.add(accession)
            if series:
                series_ids.add(series)
            if filer:
                filer_ids.add(filer)
    return report_count, len(accession_numbers), len(series_ids), len(filer_ids)


def _read_tsv_header(archive: ZipFile, member_name: str) -> list[str]:
    with archive.open(member_name) as raw:
        header = raw.readline().decode("utf-8", errors="ignore").strip()
    if not header:
        return []
    return [part.strip().upper() for part in header.split("\t")]
