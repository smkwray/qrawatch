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
PDF_EXTENSIONS = {".pdf"}
HTML_EXTENSIONS = {".html", ".htm"}


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
        }

    ext = df.apply(_coalesce_extension, axis=1)
    counts = ext.value_counts(dropna=False).to_dict()
    total = len(df)
    pdf = int((ext.isin(PDF_EXTENSIONS)).sum())
    html = int((ext.isin(HTML_EXTENSIONS)).sum())
    return {
        "status": "ok",
        "rows": total,
        "pdf": pdf,
        "html": html,
        "other": total - pdf - html,
        "extensions": {k: int(v) for k, v in sorted(counts.items())},
    }


def _is_missing_value(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series([], index=series.index, dtype=bool)
    s = series.fillna("")
    return s.astype(str).str.strip().eq("")


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


def build_qra_quality_report(downloads_path: Path, capture_path: Path) -> dict:
    downloads, downloads_status = _safe_read_csv(downloads_path)
    capture, capture_status = _safe_read_csv(capture_path)

    downloads_summary = summarize_downloads(downloads)
    capture_summary = summarize_official_capture(capture)

    if downloads_status is not None:
        downloads_summary["status"] = downloads_status
    if capture_status is not None:
        capture_summary["status"] = capture_status

    return {
        "downloads": downloads_summary,
        "official_capture": capture_summary,
    }


def _to_text(report: dict) -> str:
    lines: list[str] = []
    lines.append("QRA quality report")

    downloads = report["downloads"]
    lines.append(f"- downloads: rows={downloads['rows']}")
    if downloads["status"] == "ok":
        lines.append(f"- pdf_docs={downloads['pdf']} html_docs={downloads['html']} other_docs={downloads['other']}")
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

    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    downloads_path = Path(args.downloads)
    capture_path = Path(args.official_capture)
    report = build_qra_quality_report(downloads_path=downloads_path, capture_path=capture_path)

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=False))
    else:
        print(_to_text(report))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
