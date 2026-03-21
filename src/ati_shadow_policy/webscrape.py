from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import TYPE_CHECKING, Iterable
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

from .io_utils import (
    default_headers,
    download_binary_with_metadata,
    ensure_dir,
    slugify,
)

if TYPE_CHECKING:
    from bs4 import BeautifulSoup


def fetch_soup(url: str) -> "BeautifulSoup":
    from bs4 import BeautifulSoup

    response = requests.get(url, headers=default_headers(), timeout=60)
    response.raise_for_status()
    return BeautifulSoup(response.text, "lxml")


def extract_links(url: str) -> pd.DataFrame:
    soup = fetch_soup(url)
    rows = []
    for a in soup.find_all("a", href=True):
        href = urljoin(url, a["href"])
        rows.append({
            "source_page": url,
            "text": " ".join(a.get_text(" ", strip=True).split()),
            "href": href,
        })
    return pd.DataFrame(rows).drop_duplicates()


def filter_links(
    links: pd.DataFrame,
    href_contains: Iterable[str] | None = None,
    text_contains: Iterable[str] | None = None,
    allowed_extensions: Iterable[str] | None = None,
) -> pd.DataFrame:
    df = links.copy()
    if href_contains:
        mask = False
        for token in href_contains:
            mask = mask | df["href"].str.contains(token, case=False, na=False)
        df = df[mask]
    if text_contains:
        mask = False
        for token in text_contains:
            mask = mask | df["text"].str.contains(token, case=False, na=False)
        df = df[mask]
    if allowed_extensions:
        exts = {
            ext.lower() if ext.startswith(".") else f".{ext.lower()}"
            for ext in allowed_extensions
        }
        df = df[df["href"].map(_href_extension).isin(exts)]
    return df.drop_duplicates().reset_index(drop=True)


_QRA_DOWNLOADABLE_EXTENSIONS = {".pdf", ".xlsx", ".xls", ".csv", ".zip"}
_QRA_LANDING_PAGE_PATTERNS = (
    r"/quarterly-refunding/?$",
    r"/quarterly-refunding-archives/?$",
    r"/news/press-releases/?$",
    r"/news/press-releases/statements-remarks/?$",
    r"/resource-center/data-chart-center/quarterly-refunding/pages/default\.aspx$",
)
_QRA_COLLECTION_PAGE_PATTERNS = (
    "most-recent-quarterly-refunding-documents",
    "quarterly-refunding-financing-estimates-by-calendar-year",
    "office-of-economic-policy-statements-to-tbac",
    "tbac-recommended-financing-tables-by-calendar-year",
    "official-remarks-on-quarterly-refunding-by-calendar-year",
    "financing-estimates-by-calendar-year",
)
_QRA_SOURCE_CONTEXT_PATTERNS = (
    "quarterly-refunding-financing-estimates-by-calendar-year",
    "office-of-economic-policy-statements-to-tbac",
    "official-remarks-on-quarterly-refunding-by-calendar-year",
    "tbac-recommended-financing-tables-by-calendar-year",
    "most-recent-quarterly-refunding-documents",
)
_QRA_QUARTER_TERMS = (
    "quarterly refunding",
    "quarterly financing estimates",
    "financing estimates",
    "borrowing estimates",
    "refunding statement",
    "office of economic policy statements to tbac",
    "statement to tbac",
    "tbac recommended financing tables",
    "treasury borrowing advisory committee",
)
_QRA_NEGATIVE_TERMS = (
    "view all",
    "remarks and statements",
    "press releases",
    "liquidity regulation",
    "economic club",
    "financial literacy",
)


def build_qra_manifest(links: pd.DataFrame, min_relevance_score: int = 5) -> pd.DataFrame:
    required = {"href", "text"}
    missing = required - set(links.columns)
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(f"QRA manifest requires columns: {missing_str}")

    df = links.copy()
    df["text"] = df["text"].fillna("").astype(str)
    df["href"] = df["href"].fillna("").astype(str)
    if "source_page" in df.columns:
        df["source_page"] = df["source_page"].fillna("").astype(str)
    else:
        df["source_page"] = ""
    df["normalized_text"] = df["text"].str.lower()
    df["normalized_href"] = df["href"].str.lower()
    df["normalized_source_page"] = df["source_page"].str.lower()
    df["href_extension"] = df["href"].map(_href_extension)
    df["is_downloadable_extension"] = df["href_extension"].isin(_QRA_DOWNLOADABLE_EXTENSIONS)
    df["is_press_release_detail"] = df["normalized_href"].str.contains(
        r"/news/press-releases/[a-z0-9-]+/?$",
        regex=True,
        na=False,
    ) & ~df["normalized_href"].str.contains(
        r"/news/press-releases/(?:statements-remarks)?/?$",
        regex=True,
        na=False,
    )

    landing_pattern = "|".join(_QRA_LANDING_PAGE_PATTERNS)
    df["is_landing_page"] = df["normalized_href"].str.contains(landing_pattern, regex=True, na=False)
    df["is_collection_page"] = df["normalized_href"].apply(
        lambda href: any(token in href for token in _QRA_COLLECTION_PAGE_PATTERNS)
    )
    df["is_source_qra_context"] = df["normalized_source_page"].apply(
        lambda href: any(token in href for token in _QRA_SOURCE_CONTEXT_PATTERNS)
    )
    df["has_qra_term"] = _contains_any(df["normalized_text"], _QRA_QUARTER_TERMS) | _contains_any(
        df["normalized_href"], _QRA_QUARTER_TERMS
    )
    df["has_negative_term"] = _contains_any(df["normalized_text"], _QRA_NEGATIVE_TERMS) | _contains_any(
        df["normalized_href"], _QRA_NEGATIVE_TERMS
    )
    df["has_quarter_or_year_marker"] = df["normalized_text"].str.contains(
        r"\bq[1-4]\b|\b20\d{2}\b",
        regex=True,
        na=False,
    ) | df["normalized_href"].str.contains(r"\b20\d{2}\b", regex=True, na=False)
    df["has_explicit_quarter_label"] = df["normalized_text"].str.contains(
        r"\b(?:[1-4](?:st|nd|rd|th)\s+quarter|q[1-4])\b",
        regex=True,
        na=False,
    ) | df["normalized_href"].str.contains(r"\b(?:q[1-4]|[1-4](?:st|nd|rd|th)-quarter)\b", regex=True, na=False)
    df["is_treasury_domain"] = df["normalized_href"].str.contains(r"(?:^https?://)?(?:[a-z0-9-]+\.)*treasury\.gov")

    score = (
        df["is_downloadable_extension"].astype(int) * 4
        + df["is_press_release_detail"].astype(int) * 2
        + df["has_qra_term"].astype(int) * 4
        + df["has_explicit_quarter_label"].astype(int) * 2
        + df["has_quarter_or_year_marker"].astype(int) * 1
        + df["is_source_qra_context"].astype(int) * 2
        + df["is_collection_page"].astype(int) * 2
        - df["is_landing_page"].astype(int) * 4
        - df["has_negative_term"].astype(int) * 2
    )
    df["relevance_score"] = score.astype(int)
    df["quarter_relevant"] = df["relevance_score"] >= int(min_relevance_score)
    df["doc_type"] = df.apply(_classify_qra_doc_type, axis=1)
    df["quality_tier"] = df.apply(_qra_quality_tier, axis=1)
    df["quality_rank"] = df["quality_tier"].map(
        {
            "primary_document": 0,
            "official_release_page": 1,
            "collection_page": 2,
            "other": 3,
        }
    ).fillna(3).astype(int)

    is_quarter_document = (
        (
            df["is_downloadable_extension"]
            | df["is_press_release_detail"]
        )
        & (
            df["has_qra_term"]
            | df["has_explicit_quarter_label"]
            | (df["is_downloadable_extension"] & df["is_source_qra_context"])
        )
    )

    keep = (
        df["is_treasury_domain"]
        & df["quarter_relevant"]
        & ~df["is_landing_page"]
        & (is_quarter_document | df["is_collection_page"])
    )

    manifest = (
        df[keep]
        .sort_values(["quality_rank", "relevance_score", "href"], ascending=[True, False, True])
        .drop_duplicates(subset=["href"], keep="first")
        .reset_index(drop=True)
    )
    return manifest.drop(columns=["normalized_text", "normalized_href", "normalized_source_page", "quality_rank"])


def _href_extension(href: str) -> str:
    parsed = urlparse(str(href))
    return Path(parsed.path).suffix.lower()


def _contains_any(series: pd.Series, tokens: Iterable[str]) -> pd.Series:
    pattern = "|".join(re.escape(token) for token in tokens)
    return series.str.contains(pattern, case=False, na=False, regex=True)


def _classify_qra_doc_type(row: pd.Series) -> str:
    text = str(row.get("normalized_text", ""))
    href = str(row.get("normalized_href", ""))
    source_page = str(row.get("normalized_source_page", ""))
    combined = f"{text} {href} {source_page}"
    if "financing estimates" in combined or "borrowing estimates" in combined:
        return "financing_estimates"
    if "office-of-economic-policy-statements-to-tbac" in href or "statement to tbac" in combined:
        return "oep_statement_to_tbac"
    if "tbac-recommended-financing" in href:
        return "tbac_recommended_financing_tables"
    if row.get("is_downloadable_extension"):
        if "financing-estimates" in source_page or "financing estimates" in combined:
            return "financing_estimates_attachment"
        if "office-of-economic-policy-statements-to-tbac" in source_page:
            return "oep_statement_attachment"
        if "tbac" in combined:
            return "tbac_attachment"
        if "quarterly refunding" in combined or "refunding statement" in combined:
            return "quarterly_refunding_document"
        return "treasury_attachment"
    if row.get("is_press_release_detail"):
        if "quarterly refunding" in combined or bool(row.get("has_explicit_quarter_label")) or bool(row.get("is_source_qra_context")):
            return "quarterly_refunding_press_release"
        return "press_release"
    if row.get("is_collection_page"):
        return "quarterly_refunding_document_index"
    return "other"


def _qra_quality_tier(row: pd.Series) -> str:
    if bool(row.get("is_downloadable_extension")):
        return "primary_document"
    if bool(row.get("is_press_release_detail")):
        return "official_release_page"
    if bool(row.get("is_collection_page")):
        return "collection_page"
    return "other"


def _extractor_compatible_extension(href: str, text: str) -> str:
    href_lower = str(href).lower()
    text_lower = str(text).lower()
    ext = _href_extension(href_lower)
    if ext == ".pdf" or ".pdf" in href_lower or " pdf" in f" {text_lower}":
        return ".pdf"
    return ".html"


def _guess_filename(row: pd.Series) -> str:
    href = str(row["href"])
    text = str(row.get("text", "") or "")
    parsed = urlparse(href)
    slug_source = Path(parsed.path).stem or text or "document"
    slug = slugify(slug_source)[:80] or "document"
    href_hash = hashlib.sha1(href.encode("utf-8")).hexdigest()[:10]
    ext = _extractor_compatible_extension(href, text)
    return f"{slug}_{href_hash}{ext}"


def download_link_manifest(
    df: pd.DataFrame,
    output_dir: Path,
    limit: int | None = None,
    skip_existing: bool = True,
) -> pd.DataFrame:
    ensure_dir(output_dir)
    records = []
    subset = df.head(limit) if limit is not None else df
    for _, row in subset.iterrows():
        filename = _guess_filename(row)
        path = output_dir / filename
        record = dict(row)
        metadata = download_binary_with_metadata(
            str(row["href"]),
            path,
            skip_existing=skip_existing,
        )
        record["local_path"] = str(path)
        record["local_filename"] = path.name
        record["local_extension"] = path.suffix.lower()
        record["filename_method"] = "slug_sha1_href_ext"
        record["source_href_sha1"] = hashlib.sha1(str(row["href"]).encode("utf-8")).hexdigest()
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
