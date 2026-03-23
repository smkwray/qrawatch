from __future__ import annotations

from pathlib import Path
import re
import sys
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import argparse
from pathlib import Path

import pandas as pd

from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import RAW_DIR, ensure_project_dirs
from ati_shadow_policy.webscrape import build_qra_manifest, download_link_manifest, extract_links

START_URLS = [
    "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives",
    "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/quarterly-refunding-financing-estimates-by-calendar-year",
    "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/office-of-economic-policy-statements-to-tbac",
    "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/tbac-recommended-financing-tables-by-calendar-year",
    "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/primary-dealer-auction-size-survey",
    "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/2010-and-before-quarterly-refunding-charts-data",
    "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding",
]


def _normalize_qra_metadata_text(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def _contains_token(text: str, tokens: tuple[str, ...]) -> bool:
    haystack = _normalize_qra_metadata_text(text)
    return all(token in haystack for token in tokens)


def _benchmark_context_slug(row: pd.Series) -> str:
    combined = " ".join(
        str(row.get(column, "") or "")
        for column in ("start_url", "source_page", "href")
    ).lower()
    if "primary-dealer-auction-size-survey" in combined:
        return "primary_dealer_auction_size_survey_archive"
    if "tbac-recommended-financing-tables" in combined:
        return "tbac_recommended_financing_tables_archive"
    if "quarterly-refunding-financing-estimates-by-calendar-year" in combined:
        return "financing_estimates_archive"
    if "office-of-economic-policy-statements-to-tbac" in combined:
        return "oep_statements_to_tbac_archive"
    if "official-remarks-on-quarterly-refunding-by-calendar-year" in combined:
        return "official_refunding_statement_archive"
    if "2010-and-before-quarterly-refunding-charts-data" in combined:
        return "legacy_charts_archive"
    if "quarterly-refunding" in combined:
        return "quarterly_refunding_archive"
    return "other_qra_context"


def _benchmark_candidate_family(row: pd.Series) -> str:
    context_slug = _benchmark_context_slug(row)
    doc_type = str(row.get("doc_type", "") or "").strip().lower()
    source_family = str(row.get("source_family", "") or "").strip().lower()
    text_blob = " ".join(
        str(row.get(column, "") or "")
        for column in ("text", "href", "source_page", "start_url")
    )
    if (
        "primary_dealer_auction_size_survey" in context_slug
        or _contains_token(text_blob, ("primary", "dealer", "survey"))
    ):
        return "primary_dealer_auction_size_survey"
    if (
        "tbac_recommended_financing_tables" in context_slug
        or _contains_token(text_blob, ("recommended", "financing"))
        or doc_type == "tbac_recommended_financing_tables"
    ):
        return "tbac_recommended_financing_tables"
    if doc_type == "financing_estimates_attachment" or (
        context_slug == "financing_estimates_archive"
        and bool(row.get("is_downloadable_extension", False))
    ):
        return "financing_estimates_attachment"
    if doc_type in {"oep_statement_to_tbac", "oep_statement_attachment"} or context_slug == "oep_statements_to_tbac_archive":
        return "oep_statement_to_tbac"
    if context_slug == "official_refunding_statement_archive" or source_family == "official_refunding_statement_archive":
        return "official_refunding_statement_archive"
    if context_slug in {"legacy_charts_archive", "quarterly_refunding_archive"}:
        return "legacy_qra_archive"
    return "non_benchmark_qra"


def _benchmark_candidate_kind(row: pd.Series) -> str:
    family = _benchmark_candidate_family(row)
    downloadable = bool(row.get("is_downloadable_extension", False))
    if family == "primary_dealer_auction_size_survey":
        return "primary_dealer_survey_file" if downloadable else "primary_dealer_survey_archive_page"
    if family == "tbac_recommended_financing_tables":
        return "tbac_financing_tables_file" if downloadable else "tbac_financing_tables_archive_page"
    if family == "financing_estimates_attachment":
        return "financing_estimates_file" if downloadable else "financing_estimates_archive_page"
    if family == "oep_statement_to_tbac":
        return "oep_statement_file" if downloadable else "oep_statement_archive_page"
    if family == "official_refunding_statement_archive":
        return "official_refunding_statement_page"
    if family == "legacy_qra_archive":
        return "legacy_archive_page"
    return "other_qra_document"


def _benchmark_candidate_rank(row: pd.Series) -> int:
    family = _benchmark_candidate_family(row)
    return {
        "primary_dealer_auction_size_survey": 0,
        "tbac_recommended_financing_tables": 1,
        "financing_estimates_attachment": 2,
        "oep_statement_to_tbac": 3,
        "official_refunding_statement_archive": 4,
        "legacy_qra_archive": 5,
        "non_benchmark_qra": 9,
    }.get(family, 9)


def _benchmark_candidate_key(row: pd.Series) -> str:
    family = _benchmark_candidate_family(row)
    quarter = str(row.get("quarter", "") or "").strip()
    if quarter:
        suffix = _normalize_qra_metadata_text(quarter)
    else:
        href = str(row.get("href", "") or "")
        stem = Path(urlparse(href).path).stem
        suffix = _normalize_qra_metadata_text(stem) or "unknown"
    return f"{family}__{suffix}"


def _enrich_benchmark_candidate_metadata(df: pd.DataFrame) -> pd.DataFrame:
    enriched = df.copy()
    enriched["benchmark_context_slug"] = enriched.apply(_benchmark_context_slug, axis=1)
    enriched["benchmark_candidate_family"] = enriched.apply(_benchmark_candidate_family, axis=1)
    enriched["benchmark_candidate_kind"] = enriched.apply(_benchmark_candidate_kind, axis=1)
    enriched["benchmark_candidate_priority"] = enriched.apply(_benchmark_candidate_rank, axis=1)
    enriched["benchmark_candidate_key"] = enriched.apply(_benchmark_candidate_key, axis=1)
    enriched["benchmark_candidate_flag"] = (
        enriched["benchmark_candidate_family"].astype(str).str.strip().ne("non_benchmark_qra")
    )
    preferred = enriched.get("preferred_for_download", pd.Series(False, index=enriched.index)).fillna(False).astype(bool)
    downloadable = enriched.get("is_downloadable_extension", pd.Series(False, index=enriched.index)).fillna(False).astype(bool)
    enriched["benchmark_download_candidate"] = enriched["benchmark_candidate_flag"].astype(bool) & (
        preferred | downloadable
    )
    return enriched

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect QRA / TBAC source links from Treasury pages.")
    parser.add_argument("--download-files", action="store_true", help="Also download linked documents")
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=None,
        help="Optional file download limit (must be a positive integer)",
    )
    return parser.parse_args()


def _positive_int(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("--limit must be a positive integer")
    return value

def main() -> None:
    args = parse_args()
    ensure_project_dirs()
    out_dir = RAW_DIR / "qra"
    out_dir.mkdir(parents=True, exist_ok=True)

    all_links = []
    for url in START_URLS:
        try:
            links = extract_links(url)
            links["start_url"] = url
            all_links.append(links)
            print(f"Collected links from {url}: {len(links):,}")
        except Exception as exc:
            print(f"Failed to collect links from {url}: {exc}")

    if not all_links:
        raise SystemExit("No links collected from QRA pages.")

    links_df = pd.concat(all_links, ignore_index=True).drop_duplicates()
    filtered = build_qra_manifest(links_df).drop_duplicates()
    filtered = _enrich_benchmark_candidate_metadata(filtered)

    write_df(links_df, out_dir / "all_links.csv")
    write_df(filtered, out_dir / "manifest.csv")
    print(f"Saved manifest with {len(filtered):,} candidate links")

    if args.download_files:
        files_dir = out_dir / "files"
        download_manifest = filtered.copy()
        if "preferred_for_download" in download_manifest.columns:
            preferred = download_manifest.loc[
                download_manifest["preferred_for_download"].fillna(False).astype(bool)
            ].copy()
            if not preferred.empty:
                download_manifest = preferred
        sort_columns: list[str] = []
        ascending: list[bool] = []
        if "benchmark_download_candidate" in download_manifest.columns:
            sort_columns.append("benchmark_download_candidate")
            ascending.append(False)
        if "benchmark_candidate_priority" in download_manifest.columns:
            sort_columns.append("benchmark_candidate_priority")
            ascending.append(True)
        if "download_priority" in download_manifest.columns:
            sort_columns.append("download_priority")
            ascending.append(True)
        if "relevance_score" in download_manifest.columns:
            sort_columns.append("relevance_score")
            ascending.append(False)
        if "href" in download_manifest.columns:
            sort_columns.append("href")
            ascending.append(True)
        if sort_columns:
            download_manifest = download_manifest.sort_values(sort_columns, ascending=ascending, kind="stable")
        downloaded = download_link_manifest(download_manifest, files_dir, limit=args.limit)
        write_df(downloaded, out_dir / "downloads.csv")
        print(
            "Downloaded "
            f"{len(downloaded):,} preferred candidate files to {files_dir} "
            f"(manifest candidates={len(filtered):,})"
        )

if __name__ == "__main__":
    main()
