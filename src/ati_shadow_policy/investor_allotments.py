from __future__ import annotations

import hashlib
import re
from pathlib import Path
from urllib.parse import unquote, urlparse
import zipfile

import pandas as pd

from .io_utils import download_binary_with_metadata
from .webscrape import extract_links, filter_links

URL = "https://home.treasury.gov/data/investor-class-auction-allotments"
RAW_DIR_NAME = "investor_allotments"
PROCESSED_FILENAME = "investor_allotments.csv"
PANEL_FILENAME = "investor_allotments_panel.csv"

_DOWNLOADABLE_EXTENSIONS = [".pdf", ".xls", ".xlsx", ".csv", ".zip"]
_RELEVANT_HREF_TOKENS = ["/system/files/276/"]
_RELEVANT_TEXT_TOKENS = [
    "investor class auction allotments",
    "coupon auctions",
    "bill auctions",
    "investor class category descriptions",
]
_OLE_XLS_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
_ZIP_MAGIC = b"PK\x03\x04"
_PDF_MAGIC = b"%PDF-"
_PANEL_COLUMNS = [
    "auction_date",
    "security_family",
    "investor_class",
    "measure",
    "value",
    "units",
    "provenance",
    "summary_type",
    "as_of_date",
    "source_quality",
    "source_file",
    "source_href",
]


def collect_links(url: str = URL) -> pd.DataFrame:
    links = extract_links(url)
    links["start_url"] = url
    return links


def build_manifest(links: pd.DataFrame) -> pd.DataFrame:
    required = {"href", "text"}
    missing = required - set(links.columns)
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(f"Investor allotments manifest requires columns: {missing_str}")

    manifest = filter_links(
        links,
        href_contains=_RELEVANT_HREF_TOKENS,
        text_contains=_RELEVANT_TEXT_TOKENS,
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
    if "start_url" not in manifest.columns:
        manifest["start_url"] = URL
    manifest["href_extension"] = manifest["href"].map(_href_extension)
    manifest["source_href_sha1"] = manifest["href"].map(_sha1)
    manifest["resource_family"] = manifest.apply(_resource_family, axis=1)
    return manifest.drop_duplicates(subset=["href"]).reset_index(drop=True)


def build_inventory(manifest: pd.DataFrame, downloads: pd.DataFrame | None = None) -> pd.DataFrame:
    required = {"href", "text"}
    missing = required - set(manifest.columns)
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(f"Investor allotments inventory requires columns: {missing_str}")

    inventory = manifest.copy().drop_duplicates(subset=["href"]).reset_index(drop=True)
    if downloads is not None and not downloads.empty:
        download_rows = downloads.copy().drop_duplicates(subset=["href"]).reset_index(drop=True)
        inventory = inventory.merge(download_rows, on=["href"], how="left", suffixes=("", "_download"))
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
    inventory["file_available"] = inventory["inventory_status"].isin(["downloaded"])
    inventory["local_file_exists"] = inventory["local_path"].map(_path_exists)
    inventory["resource_family"] = inventory.apply(_resource_family, axis=1)
    artifact_info = inventory["local_path"].map(_inspect_from_path_value)
    inventory["detected_content_kind"] = artifact_info.map(lambda item: item.get("detected_content_kind"))
    inventory["detected_extension"] = artifact_info.map(lambda item: item.get("detected_extension"))
    inventory["parser_ready"] = artifact_info.map(lambda item: bool(item.get("parser_ready")))
    columns = [
        "source_page",
        "start_url",
        "text",
        "href",
        "href_extension",
        "resource_family",
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
        "detected_content_kind",
        "detected_extension",
        "parser_ready",
    ]
    keep = [col for col in columns if col in inventory.columns]
    return inventory[keep].copy()


def download_manifest(manifest: pd.DataFrame, output_dir: Path, limit: int | None = None) -> pd.DataFrame:
    output_dir.mkdir(parents=True, exist_ok=True)
    records = []
    subset = manifest.head(limit) if limit is not None else manifest
    for _, row in subset.iterrows():
        href = str(row.get("href", ""))
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


def build_normalized_panel(inventory: pd.DataFrame) -> pd.DataFrame:
    if inventory.empty:
        return pd.DataFrame(columns=_PANEL_COLUMNS)

    rows: list[pd.DataFrame] = []
    for _, item in inventory.iterrows():
        path_value = item.get("local_path")
        if _is_missing(path_value):
            continue
        path = Path(str(path_value))
        if not path.exists():
            continue

        inspection = _inspect_local_artifact(path)
        detected_kind = inspection["detected_content_kind"]
        if detected_kind not in {"excel_xls", "excel_xlsx"}:
            continue

        parsed = _read_investor_workbook(path, detected_kind)
        if parsed.empty:
            continue
        normalized = _normalize_investor_rows(parsed, item, detected_kind)
        if normalized.empty:
            continue
        rows.append(normalized)

    if not rows:
        return pd.DataFrame(columns=_PANEL_COLUMNS)

    panel = pd.concat(rows, ignore_index=True)
    panel = (
        panel.sort_values(["auction_date", "security_family", "investor_class", "source_file"], kind="stable")
        .reset_index(drop=True)
        .copy()
    )
    return panel


def _href_extension(href: str) -> str:
    return Path(str(href).split("?", 1)[0]).suffix.lower()


def _sha1(text: str) -> str:
    return hashlib.sha1(str(text).encode("utf-8")).hexdigest()


def _resource_family(row: pd.Series) -> str:
    text = str(row.get("text", "")).lower()
    href = str(row.get("href", "")).lower()
    if "investor class category descriptions" in text or "descriptions" in href:
        return "category_descriptions"
    if "coupon" in text:
        return "coupon_auctions"
    if "bill" in text:
        return "bill_auctions"
    return "other"


def _path_exists(value: object) -> bool:
    if value is None or pd.isna(value):
        return False
    return Path(str(value)).exists()


def _guess_download_filename(href: str) -> str:
    parsed = urlparse(str(href))
    stem = Path(unquote(parsed.path)).stem or "investor_allotments"
    ext = _href_extension(href) or ".bin"
    safe_stem = re.sub(r"[^a-z0-9]+", "-", stem.lower()).strip("-") or "investor-allotments"
    href_hash = hashlib.sha1(str(href).encode("utf-8")).hexdigest()[:10]
    return f"{safe_stem}_{href_hash}{ext}"


def _inspect_local_artifact(path: Path) -> dict[str, object]:
    try:
        head = path.read_bytes()[:4096]
    except Exception:
        return {
            "detected_content_kind": "unreadable",
            "detected_extension": pd.NA,
            "parser_ready": False,
        }

    detected_kind = _detect_content_kind(path, head)
    detected_extension = {
        "excel_xls": ".xls",
        "excel_xlsx": ".xlsx",
        "pdf": ".pdf",
        "csv": ".csv",
        "html": ".html",
    }.get(detected_kind, pd.NA)
    parser_ready = detected_kind in {"excel_xls", "excel_xlsx"}
    return {
        "detected_content_kind": detected_kind,
        "detected_extension": detected_extension,
        "parser_ready": parser_ready,
    }


def _inspect_from_path_value(path_value: object) -> dict[str, object]:
    if _is_missing(path_value):
        return {
            "detected_content_kind": "missing",
            "detected_extension": pd.NA,
            "parser_ready": False,
        }
    path = Path(str(path_value))
    if not path.exists():
        return {
            "detected_content_kind": "missing",
            "detected_extension": pd.NA,
            "parser_ready": False,
        }
    return _inspect_local_artifact(path)


def _detect_content_kind(path: Path, head: bytes) -> str:
    if head.startswith(_OLE_XLS_MAGIC):
        return "excel_xls"
    if head.startswith(_ZIP_MAGIC):
        try:
            with zipfile.ZipFile(path) as zf:
                members = [name.lower() for name in zf.namelist()]
            if any(name.startswith("xl/") for name in members):
                return "excel_xlsx"
            return "zip"
        except Exception:
            return "zip"
    if head.startswith(_PDF_MAGIC):
        return "pdf"
    decoded = head.decode("utf-8", errors="ignore").lower()
    if "<html" in decoded or "<!doctype html" in decoded:
        return "html"
    if "," in decoded and "\n" in decoded:
        return "csv"
    return "binary_unknown"


def _read_investor_workbook(path: Path, detected_kind: str) -> pd.DataFrame:
    engine = "xlrd" if detected_kind == "excel_xls" else "openpyxl"
    try:
        return pd.read_excel(path, header=None, engine=engine)
    except Exception:
        return pd.DataFrame()


def _normalize_investor_rows(raw: pd.DataFrame, inventory_row: pd.Series, detected_kind: str) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=_PANEL_COLUMNS)
    header_idx = _find_header_row(raw)
    if header_idx is None:
        return pd.DataFrame(columns=_PANEL_COLUMNS)

    units, scale = _extract_units_and_scale(raw)
    body = raw.iloc[header_idx + 1 :].copy()
    headers = _sanitize_headers(raw.iloc[header_idx].tolist())
    body.columns = headers
    body = body.dropna(how="all").copy()

    issue_col = _find_issue_date_column(headers)
    security_col = _find_security_column(headers)
    if issue_col is None:
        return pd.DataFrame(columns=_PANEL_COLUMNS)

    body["auction_date"] = pd.to_datetime(body[issue_col], errors="coerce").dt.date
    body = body.loc[body["auction_date"].notna()].copy()
    if body.empty:
        return pd.DataFrame(columns=_PANEL_COLUMNS)

    security_fallback = str(inventory_row.get("resource_family", "")).lower()
    if security_col is None:
        body["security_family"] = _classify_security_family("", security_fallback)
    else:
        body["security_family"] = body[security_col].map(lambda value: _classify_security_family(value, security_fallback))

    investor_columns = _find_investor_columns(headers)
    if not investor_columns:
        return pd.DataFrame(columns=_PANEL_COLUMNS)

    long_frames: list[pd.DataFrame] = []
    for column_name, investor_class in investor_columns.items():
        series = pd.to_numeric(body[column_name], errors="coerce")
        part = pd.DataFrame(
            {
                "auction_date": body["auction_date"].astype(str),
                "security_family": body["security_family"],
                "investor_class": investor_class,
                "measure": "allotment_amount",
                "value": series * scale,
                "units": units,
            }
        )
        part = part.loc[part["value"].notna()].copy()
        if part.empty:
            continue
        long_frames.append(part)

    if not long_frames:
        return pd.DataFrame(columns=_PANEL_COLUMNS)

    out = pd.concat(long_frames, ignore_index=True)
    source_href = str(inventory_row.get("href", ""))
    source_file = str(inventory_row.get("local_filename", ""))
    source_path = str(inventory_row.get("local_path", ""))
    out["provenance"] = (
        "href="
        + source_href
        + "|local_path="
        + source_path
        + "|detected_content_kind="
        + detected_kind
    )
    out["summary_type"] = "auction_observation"
    out["as_of_date"] = out["auction_date"]
    out["source_quality"] = "official_treasury_download"
    out["source_file"] = source_file
    out["source_href"] = source_href
    return out[_PANEL_COLUMNS].copy()


def _find_header_row(raw: pd.DataFrame) -> int | None:
    scan_limit = min(len(raw), 40)
    for idx in range(scan_limit):
        values = [str(value) for value in raw.iloc[idx].tolist() if not _is_missing(value)]
        if not values:
            continue
        text = _normalize_text(" ".join(values))
        if "issue date" in text and ("security" in text or "cusip" in text) and "total issue" in text:
            return idx
    return None


def _sanitize_headers(row_values: list[object]) -> list[str]:
    headers: list[str] = []
    seen: dict[str, int] = {}
    for idx, value in enumerate(row_values):
        raw = "" if _is_missing(value) else str(value)
        normalized = _normalize_text(raw)
        header = normalized if normalized else f"col_{idx}"
        counter = seen.get(header, 0) + 1
        seen[header] = counter
        if counter > 1:
            header = f"{header}_{counter}"
        headers.append(header)
    return headers


def _find_issue_date_column(headers: list[str]) -> str | None:
    for header in headers:
        if "issue" in header and "date" in header:
            return header
    return None


def _find_security_column(headers: list[str]) -> str | None:
    preferred = [
        "security term",
        "security type",
    ]
    for token in preferred:
        for header in headers:
            if token in header:
                return header
    for header in headers:
        if "security" in header and ("term" in header or "type" in header):
            return header
    return None


def _find_investor_columns(headers: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for header in headers:
        label = _investor_class_from_header(header)
        if label is not None:
            out[header] = label
    return out


def _investor_class_from_header(header: str) -> str | None:
    if "total issue" in header:
        return "total_issue"
    if "soma" in header or ("federal" in header and "reserve" in header):
        return "soma_federal_reserve_banks"
    if "depository" in header or "deposi tory" in header:
        return "depository_institutions"
    if "individual" in header:
        return "individuals"
    if "dealer" in header and "broker" in header:
        return "dealers_and_brokers"
    if "pension" in header or "retirement" in header:
        return "pension_and_retirement_funds"
    if "investment" in header and "fund" in header:
        return "investment_funds"
    if "foreign" in header or "international" in header or "interna tional" in header:
        return "foreign_and_international"
    if header.startswith("other") or " other " in f" {header} ":
        return "other"
    return None


def _extract_units_and_scale(raw: pd.DataFrame) -> tuple[str, float]:
    scan_rows = min(len(raw), 8)
    text = " ".join(
        str(value)
        for value in raw.iloc[:scan_rows].to_numpy().flatten().tolist()
        if not _is_missing(value)
    )
    normalized = _normalize_text(text)
    if "in millions of dollars" in normalized:
        return "USD billions", 0.001
    if "in billions of dollars" in normalized:
        return "USD billions", 1.0
    return "reported Treasury allotment units", 1.0


def _classify_security_family(value: object, fallback_resource_family: str) -> str:
    text = _normalize_text(value)
    if "bill" in text or re.search(r"\b\d+\s*(week|wk)\b", text):
        return "bill"
    if "frn" in text or "floating rate" in text:
        return "frn"
    if "tips" in text or "inflation" in text:
        return "tips"
    if "note" in text or "bond" in text:
        return "nominal_coupon"
    if fallback_resource_family == "bill_auctions":
        return "bill"
    if fallback_resource_family == "coupon_auctions":
        return "nominal_coupon"
    return "other"


def _normalize_text(value: object) -> str:
    text = str(value).replace("\n", " ")
    text = re.sub(r"[\(\)\[\]\{\}]", " ", text)
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text)
    return " ".join(text.lower().split())


def _is_missing(value: object) -> bool:
    return value is None or (isinstance(value, float) and pd.isna(value)) or pd.isna(value)
