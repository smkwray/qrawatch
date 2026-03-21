from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import argparse
from datetime import datetime
import json
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ati_shadow_policy.paths import MANUAL_DIR, OUTPUT_DIR, PROCESSED_DIR
from ati_shadow_policy.qra_capture import ALLOWED_QA_STATUSES, OFFICIAL_QA_STATUSES

DEFAULT_OFFICIAL_CAPTURE = PROCESSED_DIR / "official_quarterly_refunding_capture.csv"
DEFAULT_OFFICIAL_ATI = PROCESSED_DIR / "ati_index_official_capture.csv"
DEFAULT_MANUAL_TEMPLATE = MANUAL_DIR / "official_quarterly_refunding_capture_template.csv"
DEFAULT_PUBLISH_DIR = OUTPUT_DIR / "publish"

OFFICIAL_REQUIRED_FIELDS = (
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

QUARTER_PATTERN = re.compile(r"^\d{4}Q[1-4]$")

REQUIRED_PUBLISH_SCHEMAS: dict[str, list[str]] = {
    "ati_quarter_table.csv": [
        "quarter",
        "financing_need_bn",
        "ati_baseline_bn",
        "source_quality",
        "public_role",
    ],
    "ati_seed_forecast_table.csv": [
        "quarter",
        "financing_need_bn",
        "ati_baseline_bn",
        "seed_source",
        "seed_quality",
        "non_headline_reason",
        "public_role",
    ],
    "official_qra_capture.csv": [
        "quarter",
        "qra_release_date",
        "market_pricing_marker_minus_1d",
        "total_financing_need_bn",
        "net_bill_issuance_bn",
        "qa_status",
        "source_quality",
    ],
    "official_capture_readiness.csv": [
        "quarter",
        "readiness_tier",
        "source_quality",
        "headline_ready",
        "fallback_only",
    ],
    "official_capture_completion.csv": [
        "quarter",
        "completion_tier",
        "qa_status",
        "is_headline_ready",
    ],
    "ati_seed_vs_official.csv": [
        "quarter",
        "ati_seed_bn",
        "ati_official_bn",
        "comparison_status",
    ],
    "qra_event_table.csv": [
        "event_id",
        "event_label",
        "event_date_aligned",
        "expected_direction",
    ],
    "qra_event_summary.csv": [
        "expected_direction",
        "DGS10_d1",
        "DGS10_d3",
    ],
    "qra_event_robustness.csv": [
        "sample_variant",
        "event_date_type",
        "expected_direction",
        "n_events",
    ],
    "plumbing_regression_summary.csv": [
        "dependent_variable",
        "term",
        "coef",
        "p_value",
    ],
    "plumbing_robustness.csv": [
        "dependent_variable",
        "variant_id",
        "proxy_role",
        "term",
        "p_value",
    ],
    "duration_supply_summary.csv": [
        "date",
        "coupon_like_total",
        "qt_proxy",
        "buybacks_accepted",
        "provisional_public_duration_supply",
    ],
    "duration_supply_comparison.csv": [
        "date",
        "construction_id",
        "construction_family",
        "value",
    ],
    "data_sources_summary.csv": [
        "source_family",
        "raw_dir_exists",
        "manifest_exists",
        "downloads_exists",
    ],
    "extension_status.csv": [
        "extension",
        "backend_status",
        "headline_ready",
        "raw_dir_exists",
        "manifest_exists",
        "downloads_exists",
        "processed_exists",
        "public_role",
    ],
    "investor_allotments_summary.csv": [
        "summary_type",
        "security_family",
        "investor_class",
        "measure",
        "value",
    ],
    "primary_dealer_summary.csv": [
        "summary_type",
        "dataset_type",
        "series_id",
        "measure",
        "value",
    ],
    "sec_nmfp_summary.csv": [
        "summary_type",
        "dataset_version",
        "period_family",
        "measure",
        "value",
    ],
    "dataset_status.csv": [
        "dataset",
        "readiness_tier",
        "source_quality",
        "headline_ready",
        "fallback_only",
        "public_role",
    ],
    "series_metadata_catalog.csv": [
        "dataset",
        "series_id",
        "frequency",
        "value_units",
        "source_quality",
        "series_role",
        "public_role",
    ],
}
REQUIRED_EXTENSION_SUMMARY_READY = ("investor_allotments", "primary_dealer", "sec_nmfp")
PUBLIC_HYGIENE_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"/Users/", "absolute_local_path"),
    (r"\b(?:orca|dairy|tandy|subagent)\b|\bagent[_ -]?id\b", "internal_agent_reference"),
    (r"\b(?:memory-system\.md|handoff\.md|todo\.md|dontdo\.md|changes\.md)\b", "internal_workflow_reference"),
    (r"data/manual/", "manual_data_reference"),
)


def _coerce_timestamp(value: object) -> datetime | None:
    try:
        if pd.isna(value):
            return None
        if isinstance(value, (pd.Timestamp, datetime)):
            return pd.Timestamp(value).to_pydatetime()
        text = str(value).strip()
        if not text:
            return None
        return pd.to_datetime(text, errors="raise").to_pydatetime()
    except Exception:
        return None


def _safe_read_csv(path: Path) -> tuple[pd.DataFrame, str | None]:
    if not path.exists():
        return pd.DataFrame(), f"missing:{path}"
    if path.stat().st_size == 0:
        return pd.DataFrame(), f"empty:{path}"
    try:
        return pd.read_csv(path), None
    except Exception as exc:  # pragma: no cover - exercised via malformed data in tests
        return pd.DataFrame(), f"read_error:{exc}"


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    text = str(value).strip().lower()
    return text in {"", "nan", "none", "null"}


def _normalize_qa_status(value: object) -> str:
    return str(value or "").strip()


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _has_seed_dependency(*values: object) -> bool:
    for value in values:
        if _is_missing(value):
            continue
        text = str(value).lower()
        for part in text.replace(",", "|").replace(";", "|").split("|"):
            if part.strip() == "seed_csv":
                return True
    return False


def _quarter_coverage(frame: pd.DataFrame) -> dict[str, int | float]:
    if frame.empty:
        return {"total": 0, "filled": 0, "pct": 0.0}
    quarter_series = frame["quarter"].astype(str).str.strip().replace({"nan": "", "None": ""})
    quarter_series = quarter_series.where(quarter_series != "None", "")
    filled = quarter_series.ne("").sum()
    return {
        "total": int(len(frame)),
        "filled": int(filled),
        "pct": float((filled / len(frame)) * 100) if len(frame) else 0.0,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate backend readiness before publish.")
    parser.add_argument(
        "--official-capture",
        default=str(DEFAULT_OFFICIAL_CAPTURE),
        help="Path to processed official QRA capture file.",
    )
    parser.add_argument(
        "--official-ati",
        default=str(DEFAULT_OFFICIAL_ATI),
        help="Path to processed ATI official capture file.",
    )
    parser.add_argument(
        "--manual-template",
        default=str(DEFAULT_MANUAL_TEMPLATE),
        help="Path to official capture template CSV.",
    )
    parser.add_argument(
        "--publish-dir",
        default=str(DEFAULT_PUBLISH_DIR),
        help="Path to output/publish directory.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit structured JSON output.",
    )
    return parser.parse_args(argv)


@dataclass
class BackendValidationResult:
    errors: list[str]
    warnings: list[str]
    summaries: dict[str, object]


def validate_official_capture(capture: pd.DataFrame) -> tuple[list[str], list[str], dict[str, int]]:
    errors: list[str] = []
    warnings: list[str] = []

    if capture.empty:
        errors.append("official_capture_missing_rows")
        return errors, warnings, {
            "rows": 0,
            "exact_official_rows": 0,
            "semi_hybrid_rows": 0,
            "seed_only_rows": 0,
            "quarter_coverage": _quarter_coverage(capture),
        }

    if "qa_status" not in capture.columns:
        errors.append("official_capture_missing_qa_status_column")
        return errors, warnings, {
            "rows": len(capture),
            "exact_official_rows": 0,
            "semi_hybrid_rows": 0,
            "seed_only_rows": 0,
            "quarter_coverage": _quarter_coverage(capture),
        }

    if "quarter" in capture.columns:
        invalid_quarters = sorted(
            {
                str(value)
                for value in capture["quarter"].dropna().astype(str)
                if str(value).strip() and not QUARTER_PATTERN.match(str(value).strip())
            }
        )
        if invalid_quarters:
            errors.append(
                "official_capture_invalid_quarter_format:" + ",".join(invalid_quarters)
            )

    exact_rows = 0
    semi_rows = 0
    seed_rows = 0
    for row in capture.to_dict("records"):
        quarter = row.get("quarter", "")
        qa_status = _normalize_qa_status(row.get("qa_status"))
        seed_dependency = _has_seed_dependency(row.get("source_doc_type"), row.get("source_doc_local"))
        if qa_status in OFFICIAL_QA_STATUSES:
            exact_rows += 1
            if seed_dependency:
                errors.append(f"official_capture_seed_contamination:{quarter}")
            for field in OFFICIAL_REQUIRED_FIELDS:
                if _is_missing(row.get(field)):
                    errors.append(
                        f"official_capture_missing_required_field:{field}:quarter={quarter}"
                    )
            release = _coerce_timestamp(row.get("qra_release_date"))
            marker = _coerce_timestamp(row.get("market_pricing_marker_minus_1d"))
            if release is None or marker is None or (release - marker).days != 1:
                errors.append(f"official_capture_invalid_release_marker:{quarter}")
        elif qa_status == "semi_automated_capture":
            semi_rows += 1
            if seed_dependency:
                warnings.append(f"official_capture_seed_dependency:{quarter}")
        elif qa_status == "seed_only":
            seed_rows += 1
            if not seed_dependency:
                warnings.append(f"official_capture_seed_row_without_seed_doc:{quarter}")
        elif qa_status:
            errors.append(f"official_capture_unknown_status:{qa_status}:quarter={quarter}")

    if exact_rows == 0:
        errors.append("official_capture_no_exact_rows")

    return errors, warnings, {
        "rows": int(len(capture)),
        "exact_official_rows": int(exact_rows),
        "semi_hybrid_rows": int(semi_rows),
        "seed_only_rows": int(seed_rows),
        "quarter_coverage": _quarter_coverage(capture),
    }


def validate_official_ati_path(path: Path) -> tuple[list[str], list[str], dict[str, int]]:
    errors: list[str] = []
    warnings: list[str] = []
    official_rows = 0

    if not path.exists():
        warnings.append(f"official_ati_missing:{path}")
        return errors, warnings, {"rows": 0, "official_rows": 0, "official_seed_rows": 0}

    frame, status = _safe_read_csv(path)
    if status is not None:
        warnings.append(f"official_ati_{status.replace(':', '_')}")
        return errors, warnings, {"rows": 0, "official_rows": 0, "official_seed_rows": 0}

    if frame.empty:
        warnings.append(f"official_ati_empty:{path}")
        return errors, warnings, {"rows": 0, "official_rows": 0, "official_seed_rows": 0}

    rows = frame.to_dict("records")
    for row in rows:
        qa_status = _normalize_qa_status(row.get("qa_status"))
        if qa_status in OFFICIAL_QA_STATUSES:
            official_rows += 1
            if _has_seed_dependency(row.get("source_doc_type"), row.get("source_doc_local")):
                errors.append(
                    f"official_ati_seed_contamination:{row.get('quarter','')}"
                )

    official_seed_rows = sum(
        1
        for row in rows
        if _normalize_qa_status(row.get("qa_status")) in OFFICIAL_QA_STATUSES
        and _has_seed_dependency(row.get("source_doc_type"), row.get("source_doc_local"))
    )

    return errors, warnings, {
        "rows": int(len(frame)),
        "official_rows": int(official_rows),
        "official_seed_rows": int(official_seed_rows),
    }


def validate_publish_artifacts(publish_dir: Path) -> tuple[list[str], list[str], dict[str, int]]:
    errors: list[str] = []
    warnings: list[str] = []

    if not publish_dir.exists():
        errors.append(f"publish_dir_missing:{publish_dir}")
        return errors, warnings, {"missing_files": 1}

    missing_files = 0
    for csv_name, required_columns in REQUIRED_PUBLISH_SCHEMAS.items():
        for ext in (".csv", ".json", ".md"):
            artifact = publish_dir / f"{Path(csv_name).stem}{ext}"
            if not artifact.exists():
                errors.append(f"publish_artifact_missing:{artifact.name}")
                missing_files += 1

        csv_path = publish_dir / csv_name
        if not csv_path.exists():
            continue
        try:
            csv_frame = pd.read_csv(csv_path)
        except Exception as exc:
            errors.append(f"publish_artifact_read_error:{csv_name}:{exc}")
            continue
        missing_cols = [c for c in required_columns if c not in csv_frame.columns]
        if missing_cols:
            errors.append(
                f"publish_artifact_missing_columns:{csv_name}:{','.join(missing_cols)}"
            )

    index_path = publish_dir / "index.json"
    if not index_path.exists():
        errors.append("publish_index_missing:index.json")
        return errors, warnings, {"missing_files": missing_files, "index_artifacts": 0}

    try:
        payload = json.loads(index_path.read_text(encoding="utf-8"))
        artifacts = [str(item) for item in payload.get("artifacts", [])]
    except Exception as exc:
        errors.append(f"publish_index_read_error:{exc}")
        return errors, warnings, {"missing_files": missing_files, "index_artifacts": 0}

    required = set(REQUIRED_PUBLISH_SCHEMAS)
    required.update(name.replace(".csv", ".json") for name in REQUIRED_PUBLISH_SCHEMAS)
    required.update(name.replace(".csv", ".md") for name in REQUIRED_PUBLISH_SCHEMAS)
    required.add("index.json")
    missing_from_index = sorted(required - set(artifacts))
    if missing_from_index:
        errors.append(f"publish_index_missing_artifacts:{','.join(missing_from_index)}")

    dataset_status_path = publish_dir / "dataset_status.csv"
    extension_status_path = publish_dir / "extension_status.csv"
    series_catalog_path = publish_dir / "series_metadata_catalog.csv"
    if dataset_status_path.exists():
        try:
            dataset_status = pd.read_csv(dataset_status_path)
        except Exception as exc:
            errors.append(f"publish_artifact_read_error:dataset_status.csv:{exc}")
            dataset_status = pd.DataFrame()
        for name in REQUIRED_EXTENSION_SUMMARY_READY:
            dataset_name = f"extension_{name}"
            match = dataset_status.loc[dataset_status.get("dataset", pd.Series(dtype=str)) == dataset_name]
            if match.empty:
                errors.append(f"dataset_status_missing_extension:{dataset_name}")
                continue
            readiness = str(match.iloc[0].get("readiness_tier", "")).strip()
            if readiness != "summary_ready":
                errors.append(f"dataset_status_not_summary_ready:{dataset_name}:{readiness}")
            if _coerce_bool(match.iloc[0].get("headline_ready")):
                errors.append(f"dataset_status_extension_marked_headline:{dataset_name}")
            public_role = str(match.iloc[0].get("public_role", "")).strip()
            if public_role != "supporting":
                errors.append(f"dataset_status_extension_public_role_invalid:{dataset_name}:{public_role}")
    if extension_status_path.exists():
        try:
            extension_status = pd.read_csv(extension_status_path)
        except Exception as exc:
            errors.append(f"publish_artifact_read_error:extension_status.csv:{exc}")
            extension_status = pd.DataFrame()
        for name in REQUIRED_EXTENSION_SUMMARY_READY:
            match = extension_status.loc[extension_status.get("extension", pd.Series(dtype=str)) == name]
            if match.empty:
                errors.append(f"extension_status_missing:{name}")
                continue
            readiness = str(match.iloc[0].get("readiness_tier", "")).strip()
            if readiness != "summary_ready":
                errors.append(f"extension_status_not_summary_ready:{name}:{readiness}")
            if _coerce_bool(match.iloc[0].get("headline_ready")):
                errors.append(f"extension_status_marked_headline:{name}")
            public_role = str(match.iloc[0].get("public_role", "")).strip()
            if public_role != "supporting":
                errors.append(f"extension_status_public_role_invalid:{name}:{public_role}")
    if series_catalog_path.exists():
        try:
            series_catalog = pd.read_csv(series_catalog_path)
        except Exception as exc:
            errors.append(f"publish_artifact_read_error:series_metadata_catalog.csv:{exc}")
            series_catalog = pd.DataFrame()
        if "series_role" not in series_catalog.columns:
            errors.append("publish_artifact_missing_columns:series_metadata_catalog.csv:series_role")
        if "public_role" not in series_catalog.columns:
            errors.append("publish_artifact_missing_columns:series_metadata_catalog.csv:public_role")
        for name in REQUIRED_EXTENSION_SUMMARY_READY:
            match = series_catalog.loc[series_catalog.get("dataset", pd.Series(dtype=str)) == name]
            if match.empty:
                errors.append(f"series_metadata_missing_extension:{name}")
                continue
            if "series_role" in match.columns:
                invalid_role = match.loc[match["series_role"].astype(str).str.strip() != "supporting"]
                if not invalid_role.empty:
                    errors.append(f"series_metadata_extension_role_invalid:{name}")
            if "public_role" in match.columns:
                invalid_public_role = match.loc[match["public_role"].astype(str).str.strip() != "supporting"]
                if not invalid_public_role.empty:
                    errors.append(f"series_metadata_extension_public_role_invalid:{name}")

    return errors, warnings, {"missing_files": missing_files, "index_artifacts": len(artifacts)}


def validate_publish_contract_against_official_ati(
    publish_dir: Path,
    official_ati_path: Path,
) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    official_ati_frame, status = _safe_read_csv(official_ati_path)
    if status is not None or official_ati_frame.empty:
        return errors, warnings

    ati_quarter_table, ati_status = _safe_read_csv(publish_dir / "ati_quarter_table.csv")
    if ati_status is not None:
        errors.append(f"publish_artifact_{ati_status.replace(':', '_')}:ati_quarter_table.csv")
        return errors, warnings
    ati_seed_forecast, seed_status = _safe_read_csv(publish_dir / "ati_seed_forecast_table.csv")
    if seed_status is not None:
        errors.append(f"publish_artifact_{seed_status.replace(':', '_')}:ati_seed_forecast_table.csv")
        return errors, warnings

    official = official_ati_frame.copy()
    official["quarter"] = official.get("quarter", pd.Series(dtype=str)).astype(str).str.strip()
    official = official.loc[official["quarter"].str.match(QUARTER_PATTERN)]
    if "qa_status" in official.columns:
        official = official.loc[official["qa_status"].astype(str).str.strip().isin(OFFICIAL_QA_STATUSES)]
    if not official.empty:
        official = official.loc[
            ~official.apply(
                lambda row: _has_seed_dependency(row.get("source_doc_type", ""), row.get("source_doc_local", "")),
                axis=1,
            )
        ].copy()
    if official.empty:
        return errors, warnings

    official_quarters = set(official["quarter"])
    official_by_quarter = official.drop_duplicates(subset=["quarter"], keep="first").set_index("quarter")

    published = ati_quarter_table.copy()
    published["quarter"] = published.get("quarter", pd.Series(dtype=str)).astype(str).str.strip()
    nonempty_quarter = published["quarter"].astype(str).str.len() > 0
    published = published.loc[nonempty_quarter].copy()
    invalid_quarters = sorted(set(published.loc[~published["quarter"].str.match(QUARTER_PATTERN), "quarter"]))
    if invalid_quarters:
        errors.append("ati_quarter_table_invalid_quarters:" + ",".join(invalid_quarters))
    published = published.loc[published["quarter"].str.match(QUARTER_PATTERN)].copy()
    published_quarters = set(published["quarter"])
    if published_quarters != official_quarters:
        errors.append(
            "ati_quarter_table_quarter_mismatch:"
            f"published={','.join(sorted(published_quarters))}:official={','.join(sorted(official_quarters))}"
        )

    if "source_quality" in published.columns:
        invalid_quality = published.loc[
            published["source_quality"].astype(str).str.strip() != "exact_official_numeric"
        ]
        if not invalid_quality.empty:
            errors.append("ati_quarter_table_source_quality_invalid")

    seed_cols = [col for col in ("seed_source", "seed_quality") if col in published.columns]
    for col in seed_cols:
        has_seed_value = published[col].fillna("").astype(str).str.strip().ne("").any()
        if has_seed_value:
            errors.append(f"ati_quarter_table_contains_seed_values:{col}")

    compare_cols = [
        col
        for col in ("financing_need_bn", "net_bills_bn", "bill_share", "ati_baseline_bn")
        if col in official_by_quarter.columns and col in published.columns
    ]
    merged = published.merge(
        official_by_quarter[compare_cols].reset_index(),
        on="quarter",
        how="outer",
        suffixes=("_published", "_official"),
        indicator=True,
    )
    for _, row in merged.iterrows():
        quarter = str(row.get("quarter", "")).strip()
        if row["_merge"] != "both":
            continue
        for col in compare_cols:
            left = row.get(f"{col}_published")
            right = row.get(f"{col}_official")
            if _is_missing(left) or _is_missing(right):
                errors.append(f"ati_quarter_table_missing_value:{quarter}:{col}")
                continue
            try:
                left_num = float(left)
                right_num = float(right)
            except Exception:
                errors.append(f"ati_quarter_table_non_numeric_value:{quarter}:{col}")
                continue
            if abs(left_num - right_num) > 1e-9:
                errors.append(f"ati_quarter_table_value_mismatch:{quarter}:{col}")

    if "public_role" in published.columns:
        invalid_public_role = published.loc[published["public_role"].astype(str).str.strip() != "headline"]
        if not invalid_public_role.empty:
            errors.append("ati_quarter_table_public_role_invalid")

    if not ati_seed_forecast.empty:
        seed_frame = ati_seed_forecast.copy()
        seed_frame["quarter"] = seed_frame.get("quarter", pd.Series(dtype=str)).astype(str).str.strip()
        seed_frame = seed_frame.loc[seed_frame["quarter"].str.len() > 0].copy()
        seed_quarters = set(seed_frame["quarter"].dropna().astype(str))
        overlap = sorted(seed_quarters & official_quarters)
        if overlap:
            errors.append("ati_seed_forecast_contains_official_quarters:" + ",".join(overlap))
        if "public_role" in seed_frame.columns:
            invalid_public_role = seed_frame.loc[
                seed_frame["public_role"].astype(str).str.strip() != "supporting"
            ]
            if not invalid_public_role.empty:
                errors.append("ati_seed_forecast_public_role_invalid")
        if "headline_ready" in seed_frame.columns:
            if seed_frame["headline_ready"].apply(_coerce_bool).any():
                errors.append("ati_seed_forecast_marked_headline")

    for artifact in sorted(publish_dir.glob("*")):
        if not artifact.is_file():
            continue
        try:
            text = artifact.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for pattern, label in PUBLIC_HYGIENE_PATTERNS:
            if re.search(pattern, text, flags=re.IGNORECASE):
                errors.append(f"publish_hygiene_violation:{label}:{artifact.name}")
                break

    return errors, warnings


def validate_backend(
    *,
    official_capture_path: Path,
    official_ati_path: Path,
    manual_capture_path: Path,
    publish_dir: Path,
) -> BackendValidationResult:
    errors: list[str] = []
    warnings: list[str] = []
    summaries: dict[str, object] = {}

    if not manual_capture_path.exists():
        warnings.append(f"official_capture_template_missing:{manual_capture_path}")

    official_capture, status = _safe_read_csv(official_capture_path)
    if status is not None:
        errors.append(f"official_capture_{status.replace(':', '_')}")
        summaries["official_capture"] = {
            "rows": 0,
            "exact_official_rows": 0,
            "semi_hybrid_rows": 0,
            "seed_only_rows": 0,
            "quarter_coverage": _quarter_coverage(pd.DataFrame()),
        }
    else:
        capture_errors, capture_warnings, capture_summary = validate_official_capture(official_capture)
        errors.extend(capture_errors)
        warnings.extend(capture_warnings)
        summaries["official_capture"] = capture_summary

    ati_errors, ati_warnings, ati_summary = validate_official_ati_path(official_ati_path)
    errors.extend(ati_errors)
    warnings.extend(ati_warnings)
    summary_capture = summaries.get("official_capture", {})
    if isinstance(summary_capture, dict):
        exact_rows = int(summary_capture.get("exact_official_rows", 0))
        if exact_rows > 0 and ati_summary["rows"] == 0:
            errors.append("official_ati_missing_required_with_exact_rows")
        if exact_rows > 0 and ati_summary["official_seed_rows"] > 0:
            errors.append("official_ati_contains_seed_contaminated_exact_rows")
    summaries["official_ati"] = ati_summary

    publish_errors, publish_warnings, publish_summary = validate_publish_artifacts(publish_dir)
    errors.extend(publish_errors)
    warnings.extend(publish_warnings)
    summaries["publish"] = publish_summary
    contract_errors, contract_warnings = validate_publish_contract_against_official_ati(
        publish_dir,
        official_ati_path,
    )
    errors.extend(contract_errors)
    warnings.extend(contract_warnings)

    return BackendValidationResult(errors=errors, warnings=warnings, summaries=summaries)


def _to_text(payload: BackendValidationResult) -> str:
    lines: list[str] = []
    lines.append("OK" if not payload.errors else "FAILURE")
    if payload.errors:
        lines.append("Errors:")
        lines.extend(f"- {message}" for message in payload.errors)
    if payload.warnings:
        lines.append("Warnings:")
        lines.extend(f"- {message}" for message in payload.warnings)
    lines.append("Summaries:")
    for name, summary in payload.summaries.items():
        lines.append(f"- {name}: {summary}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = validate_backend(
        official_capture_path=Path(args.official_capture),
        official_ati_path=Path(args.official_ati),
        manual_capture_path=Path(args.manual_template),
        publish_dir=Path(args.publish_dir),
    )

    if args.json:
        print(json.dumps({
            "ok": not result.errors,
            "errors": result.errors,
            "warnings": result.warnings,
            "summaries": result.summaries,
        }, indent=2, sort_keys=True))
    else:
        print(_to_text(result))

    return 1 if result.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
