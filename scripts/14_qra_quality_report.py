from __future__ import annotations

from collections.abc import Sequence
import argparse
import json
import sys
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
import pandas as pd

from ati_shadow_policy.paths import PROCESSED_DIR, RAW_DIR

DEFAULT_DOWNLOADS_PATH = RAW_DIR / "qra" / "downloads.csv"
DEFAULT_CAPTURE_PATH = PROCESSED_DIR / "official_quarterly_refunding_capture.csv"
REQUIRED_CAPTURE_FIELDS = (
    "quarter",
    "qra_release_date",
    "market_pricing_marker_minus_1d",
    "total_financing_need_bn",
    "net_bill_issuance_bn",
    "source_url",
    "source_doc_local",
    "source_doc_type",
    "qa_status",
)
OFFICIAL_QA_STATUSES = {"manual_official_capture", "parser_verified"}
OFFICIAL_PROVENANCE_FIELDS = ("source_url", "source_doc_local", "source_doc_type")
PDF_EXTENSIONS = {".pdf"}
HTML_EXTENSIONS = {".html", ".htm"}
DOWNLOAD_PROVENANCE_FIELDS = (
    "quarter",
    "doc_type",
    "source_family",
    "quality_tier",
    "preferred_for_download",
)
OFFICIAL_SOURCE_FAMILIES = {
    "financing_estimates_archive",
    "official_quarterly_refunding_statement_archive",
    "quarterly_refunding_press_release",
    "quarterly_refunding_main_page",
}
EXACT_OFFICIAL_SOURCE_DOC_TYPES = {
    "official_auction_reconstruction",
    "official_quarterly_refunding_statement",
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report concise backend quality counts for QRA download and capture assets."
    )
    parser.add_argument(
        "--downloads",
        default=str(DEFAULT_DOWNLOADS_PATH),
        help="Path to QRA downloads CSV (default: %(default)s).",
    )
    parser.add_argument(
        "--official-capture",
        default=str(DEFAULT_CAPTURE_PATH),
        help="Path to official QRA capture CSV (default: %(default)s).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the report as compact JSON instead of human-readable text.",
    )
    parser.add_argument(
        "--fail-on-contract",
        action="store_true",
        help=(
            "Exit non-zero if any official QA rows are missing required provenance fields "
            "or exact-official linkage checks fail."
        ),
    )
    return parser.parse_args(argv)


def _safe_read_csv(path: Path) -> tuple[pd.DataFrame, str | None]:
    if not path.exists():
        return pd.DataFrame(), f"missing:{path}"
    if path.stat().st_size == 0:
        return pd.DataFrame(), f"empty:{path}"
    try:
        return pd.read_csv(path), None
    except Exception as exc:
        return pd.DataFrame(), f"read_error:{exc}"


def _normalize_ext(raw_ext: str | None) -> str:
    if raw_ext is None or (isinstance(raw_ext, float) and pd.isna(raw_ext)):
        return ""
    candidate = str(raw_ext).strip().lower()
    if candidate in {"", "nan", "none", "null"}:
        return ""
    if candidate:
        return candidate if candidate.startswith(".") else f".{candidate}"
    return ""


def _infer_extension(value: str) -> str:
    value = str(value).strip().lower()
    if value.endswith(".pdf"):
        return ".pdf"
    if value.endswith(".html") or value.endswith(".htm"):
        return Path(value).suffix.lower()
    if "application/pdf" in value:
        return ".pdf"
    if "text/html" in value or "html" in value:
        return ".html"
    return ""


def _coalesce_extension(row: pd.Series) -> str:
    ext = _normalize_ext(row.get("local_extension"))
    if ext:
        return ext

    ext = _normalize_ext(row.get("local_filename"))
    if ext:
        return Path(ext).suffix.lower()

    href_ext = _normalize_ext(Path(str(row.get("local_path", ""))).suffix)
    if href_ext:
        return href_ext

    content_type = str(row.get("content_type", "") or "").lower()
    inferred = _infer_extension(content_type)
    if inferred:
        return inferred

    final_url = str(row.get("final_url", "") or "")
    if final_url:
        parsed = urlparse(final_url)
        return _normalize_ext(Path(parsed.path).suffix)
    return ""


def summarize_downloads(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "status": "empty",
            "rows": 0,
            "pdf": 0,
            "html": 0,
            "other": 0,
            "extensions": {},
            "source_family_counts": {},
            "provenance_missing_counts": {
                field: {"missing": 0, "pct": 0.0} for field in DOWNLOAD_PROVENANCE_FIELDS
            },
            "official_source_family_rows": 0,
            "preferred_for_download_rows": 0,
        }

    ext = df.apply(_coalesce_extension, axis=1)
    counts = ext.value_counts(dropna=False).to_dict()
    total = len(df)
    pdf = int((ext.isin(PDF_EXTENSIONS)).sum())
    html = int((ext.isin(HTML_EXTENSIONS)).sum())
    source_family_counts: dict[str, int]
    if "source_family" in df.columns:
        source_family_counts = (
            df["source_family"]
            .fillna("missing")
            .astype(str)
            .str.strip()
            .replace("", "missing")
            .value_counts(dropna=False)
            .to_dict()
        )
        source_family_counts = {k: int(v) for k, v in source_family_counts.items()}
    else:
        source_family_counts = {}
    provenance_missing_counts: dict[str, dict[str, float]]
    provenance_missing_counts = {}
    for field in DOWNLOAD_PROVENANCE_FIELDS:
        if field in df.columns:
            missing = int(_is_missing_value(df[field]).sum())
            provenance_missing_counts[field] = {
                "missing": missing,
                "pct": (missing / total * 100) if total else 0.0,
            }
        else:
            provenance_missing_counts[field] = {"missing": total, "pct": 100.0}
    official_source_family_rows = 0
    if "source_family" in df.columns:
        official_source_family_rows = int(
            df["source_family"].fillna("").astype(str).str.strip().isin(OFFICIAL_SOURCE_FAMILIES).sum()
        )
    preferred_rows = 0
    if "preferred_for_download" in df.columns:
        preferred_rows = int(
            df["preferred_for_download"]
            .map(_is_truthy)
            .sum()
        )
    return {
        "status": "ok",
        "rows": total,
        "pdf": pdf,
        "html": html,
        "other": total - pdf - html,
        "extensions": {k: int(v) for k, v in sorted(counts.items())},
        "source_family_counts": source_family_counts,
        "provenance_missing_counts": provenance_missing_counts,
        "official_source_family_rows": official_source_family_rows,
        "preferred_for_download_rows": preferred_rows,
    }


def _is_missing_value(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series([], index=series.index, dtype=bool)
    s = series.fillna("")
    return s.astype(str).str.strip().eq("")


def _is_missing_value_scalar(value: object) -> bool:
    if value is pd.NA:
        return True
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    return str(value).strip() in {"", "nan", "none", "null"}


def _string_or_empty(value: object) -> str:
    if _is_missing_value_scalar(value):
        return ""
    return str(value).strip()


def _split_pipe_values(value: object) -> list[str]:
    if _is_missing_value_scalar(value):
        return []
    return [part.strip() for part in str(value).split("|") if part.strip()]


def _normalize_url(value: object) -> str:
    if _is_missing_value_scalar(value):
        return ""
    return str(value).strip().rstrip("/")


def _is_truthy(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _build_download_lookup(df: pd.DataFrame) -> dict[str, dict[str, object]]:
    lookup: dict[str, dict[str, object]] = {}
    if df.empty:
        return lookup

    for _, row in df.iterrows():
        record = row.to_dict()
        for key in ("href", "final_url"):
            url = _normalize_url(record.get(key))
            if url and url not in lookup:
                lookup[url] = record
    return lookup


def _infer_source_url_family(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()

    if "fiscaldata.treasury.gov" in host and "auctions-query" in path:
        return "official_auction_reconstruction"
    if "home.treasury.gov" in host:
        if "/news/press-releases/" in path:
            return "quarterly_refunding_press_release"
        if "official-remarks-on-quarterly-refunding-by-calendar-year" in path:
            return "official_quarterly_refunding_statement_archive"
        if "quarterly-refunding-financing-estimates-by-calendar-year" in path:
            return "financing_estimates_archive"
        if "/policy-issues/financing-the-government/quarterly-refunding" in path:
            return "quarterly_refunding_main_page"
    return "unknown"


def summarize_official_capture(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "status": "empty",
            "rows": 0,
            "quarter_coverage": {"rows": 0, "filled": 0, "pct": 0.0, "unique": 0},
            "required_fields": {
                field: {"missing": 0, "pct": 0.0} for field in REQUIRED_CAPTURE_FIELDS
            },
            "document_type_counts": {},
            "qa_status_counts": {},
        }

    total = len(df)
    quarter = df.get("quarter", pd.Series([pd.NA] * total))
    filled_quarters = int((~_is_missing_value(quarter)).sum())
    normalized_quarters = (
        quarter.fillna("")
        .astype(str)
        .str.strip()
        .replace("nan", "")
        .replace("None", "")
        .astype(str)
        .str.strip()
    )

    document_type_counts: dict[str, int]
    if "source_doc_type" in df.columns:
        document_type_counts = (
            df["source_doc_type"]
            .fillna("missing")
            .astype(str)
            .str.strip()
            .replace("", "missing")
            .value_counts(dropna=False)
            .to_dict()
        )
        document_type_counts = {k: int(v) for k, v in document_type_counts.items()}
    else:
        document_type_counts = {}

    if "qa_status" in df.columns:
        qa_status_counts = (
            df["qa_status"]
            .fillna("missing")
            .astype(str)
            .str.strip()
            .replace("", "missing")
            .value_counts(dropna=False)
            .to_dict()
        )
        qa_status_counts = {k: int(v) for k, v in qa_status_counts.items()}
    else:
        qa_status_counts = {}

    required = {}
    for field in REQUIRED_CAPTURE_FIELDS:
        if field in df.columns:
            missing = int(_is_missing_value(df[field]).sum())
            required[field] = {"missing": missing, "pct": (missing / total * 100) if total else 0.0}
        else:
            required[field] = {"missing": total, "pct": 100.0}

    return {
        "status": "ok",
        "rows": total,
        "quarter_coverage": {
            "rows": total,
            "filled": filled_quarters,
            "pct": (filled_quarters / total * 100) if total else 0.0,
            "unique": int((normalized_quarters[normalized_quarters != ""]).nunique()),
        },
        "required_fields": required,
        "document_type_counts": document_type_counts,
        "qa_status_counts": qa_status_counts,
    }


def _is_exact_official_capture_row(row: pd.Series) -> bool:
    qa_status = _string_or_empty(row.get("qa_status"))
    if qa_status not in OFFICIAL_QA_STATUSES:
        return False
    if _is_missing_value_scalar(row.get("total_financing_need_bn")):
        return False
    if _is_missing_value_scalar(row.get("net_bill_issuance_bn")):
        return False
    source_doc_types = set(_split_pipe_values(row.get("source_doc_type")))
    return EXACT_OFFICIAL_SOURCE_DOC_TYPES.issubset(source_doc_types)


def _validate_capture_contract(df: pd.DataFrame, downloads: pd.DataFrame) -> list[str]:
    if df.empty:
        return []
    if "qa_status" not in df.columns:
        return ["official_capture missing qa_status column"]

    violations: list[str] = []
    download_lookup = _build_download_lookup(downloads)
    for idx, row in df.iterrows():
        qa_status = str(row.get("qa_status", "")).strip()
        if qa_status not in OFFICIAL_QA_STATUSES:
            continue
        row_num = idx + 2
        for field in OFFICIAL_PROVENANCE_FIELDS:
            if field not in df.columns or _is_missing_value_scalar(row.get(field, pd.NA)):
                violations.append(
                    f"row {row_num}: qa_status='{qa_status}' requires non-empty '{field}'"
                )

        if not _is_exact_official_capture_row(row):
            continue

        source_urls = _split_pipe_values(row.get("source_url"))
        source_doc_types = set(_split_pipe_values(row.get("source_doc_type")))
        if not source_urls:
            violations.append(
                f"row {row_num}: exact-official capture row requires at least one source_url"
            )
            continue

        validated_official_source = False
        for source_url in source_urls:
            url_family = _infer_source_url_family(source_url)
            if url_family == "official_auction_reconstruction":
                if "official_auction_reconstruction" not in source_doc_types:
                    violations.append(
                        f"row {row_num}: source_url='{source_url}' is an official-auctions link "
                        "but source_doc_type is missing 'official_auction_reconstruction'"
                    )
                else:
                    validated_official_source = True
                continue
            if url_family == "quarterly_refunding_press_release":
                if "quarterly_refunding_press_release" not in source_doc_types:
                    violations.append(
                        f"row {row_num}: source_url='{source_url}' is a Treasury press-release link "
                        f"but source_doc_type='{_string_or_empty(row.get('source_doc_type'))}' is not press-release typed"
                    )
                else:
                    validated_official_source = True
                continue

            download_row = download_lookup.get(_normalize_url(source_url))
            if download_row is None:
                violations.append(
                    f"row {row_num}: source_url='{source_url}' is not linked to a preferred official download row"
                )
                continue

            download_family = _string_or_empty(download_row.get("source_family"))
            if download_family not in OFFICIAL_SOURCE_FAMILIES:
                violations.append(
                    f"row {row_num}: source_url='{source_url}' links to source_family='{download_family}' "
                    f"which is not in allowed official families {sorted(OFFICIAL_SOURCE_FAMILIES)}"
                )

            if not _is_truthy(download_row.get("preferred_for_download")):
                violations.append(
                    f"row {row_num}: source_url='{source_url}' links to a download row that is not preferred_for_download"
                )
            else:
                validated_official_source = True

            if url_family == "official_quarterly_refunding_statement_archive":
                if "official_quarterly_refunding_statement" not in source_doc_types:
                    violations.append(
                        f"row {row_num}: source_url='{source_url}' is a statement-archive link "
                        f"but source_doc_type='{_string_or_empty(row.get('source_doc_type'))}' is not statement typed"
                    )
            elif url_family == "quarterly_refunding_main_page":
                if not (
                    "quarterly_refunding_press_release" in source_doc_types
                    or "official_quarterly_refunding_statement" in source_doc_types
                ):
                    violations.append(
                        f"row {row_num}: source_url='{source_url}' is a quarterly-refunding page link "
                        f"but source_doc_type='{_string_or_empty(row.get('source_doc_type'))}' is not statement/press-release typed"
                    )
            elif url_family == "financing_estimates_archive":
                if not (
                    "quarterly_refunding_press_release" in source_doc_types
                    or "official_quarterly_refunding_statement" in source_doc_types
                ):
                    violations.append(
                        f"row {row_num}: source_url='{source_url}' is a financing-estimates archive link "
                        f"but source_doc_type='{_string_or_empty(row.get('source_doc_type'))}' is not statement/press-release typed"
                    )

        if not validated_official_source:
            violations.append(
                f"row {row_num}: exact-official capture row requires at least one validated official source URL or preferred download linkage"
            )
    return violations


def build_qra_quality_report(
    downloads_path: Path, capture_path: Path, *, check_contract: bool = False
) -> dict:
    downloads, downloads_status = _safe_read_csv(downloads_path)
    capture, capture_status = _safe_read_csv(capture_path)

    downloads_summary = summarize_downloads(downloads)
    capture_summary = summarize_official_capture(capture)

    contract_violations: list[str] = []
    if check_contract:
        if capture_status is not None:
            contract_violations.append(f"official_capture status is not valid: {capture_status}")
        else:
            contract_violations = _validate_capture_contract(capture, downloads)
    if downloads_status is not None:
        downloads_summary["status"] = downloads_status
    if capture_status is not None:
        capture_summary["status"] = capture_status

    return {
        "downloads": downloads_summary,
        "official_capture": capture_summary,
        "contract_violations": contract_violations,
    }


def _to_text(report: dict) -> str:
    lines: list[str] = []
    lines.append("QRA quality report")

    downloads = report["downloads"]
    lines.append(f"- downloads: rows={downloads['rows']}")
    if downloads["status"] == "ok":
        lines.append(f"- pdf_docs={downloads['pdf']} html_docs={downloads['html']} other_docs={downloads['other']}")
        lines.append(
            "- download provenance coverage: "
            + ", ".join(
                f"{field}={values['missing']} missing ({values['pct']:.1f}%)"
                for field, values in downloads["provenance_missing_counts"].items()
            )
        )
        lines.append(
            "- preferred/official rows: "
            f"preferred={downloads['preferred_for_download_rows']} "
            f"official_source_family={downloads['official_source_family_rows']}"
        )
        if downloads["source_family_counts"]:
            lines.append("- download source_family coverage:")
            for item, count in sorted(downloads["source_family_counts"].items(), key=lambda x: x[0]):
                lines.append(f"  - {item}: {count}")
    else:
        lines.append(f"  - note: {downloads['status']}")

    capture = report["official_capture"]
    lines.append(f"- official_capture: rows={capture['rows']} status={capture['status']}")
    if capture["status"] == "ok":
        quarter = capture["quarter_coverage"]
        lines.append(
            f"- quarter coverage: {quarter['filled']}/{quarter['rows']} ({quarter['pct']:.1f}%) unique={quarter['unique']}"
        )
        lines.append("- required-field missingness:")
        for field, values in capture["required_fields"].items():
            lines.append(f"  - {field}: {values['missing']} ({values['pct']:.1f}%)")
        if capture["document_type_counts"]:
            lines.append("- document_type coverage:")
            for item, count in sorted(capture["document_type_counts"].items(), key=lambda x: x[0]):
                lines.append(f"  - {item}: {count}")
        else:
            lines.append("- document_type coverage: unavailable")
        if capture["qa_status_counts"]:
            lines.append("- qa_status distribution:")
            for item, count in sorted(capture["qa_status_counts"].items(), key=lambda x: x[0]):
                lines.append(f"  - {item}: {count}")
        else:
            lines.append("- qa_status distribution: unavailable")

    if report.get("contract_violations"):
        lines.append(f"- contract_violations: {len(report['contract_violations'])}")
        for message in report["contract_violations"]:
            lines.append(f"  - {message}")

    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    downloads_path = Path(args.downloads)
    capture_path = Path(args.official_capture)
    report = build_qra_quality_report(
        downloads_path=downloads_path,
        capture_path=capture_path,
        check_contract=args.fail_on_contract,
    )

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=False))
    else:
        print(_to_text(report))

    if args.fail_on_contract and report["contract_violations"]:
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
