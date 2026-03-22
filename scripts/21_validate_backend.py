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
UNANCHORED_QUARTER_PATTERN = r"\d{4}Q[1-4]"

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
    "official_capture_history_status.csv": [
        "quarter",
        "qra_release_date",
        "qa_status",
        "source_quality",
        "readiness_tier",
        "headline_ready",
        "source_completeness",
    ],
    "ati_seed_vs_official.csv": [
        "quarter",
        "ati_seed_bn",
        "ati_official_bn",
        "comparison_status",
    ],
    "qra_event_table.csv": [
        "quarter",
        "event_id",
        "event_label",
        "event_date_aligned",
        "policy_statement_url",
        "current_quarter_action",
        "forward_guidance_bias",
        "headline_bucket",
        "classification_confidence",
        "classification_review_status",
    ],
    "qra_event_summary.csv": [
        "headline_bucket",
        "DGS10_d1",
        "DGS10_d3",
    ],
    "qra_event_robustness.csv": [
        "sample_variant",
        "event_date_type",
        "headline_bucket",
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
OPTIONAL_PUBLISH_SCHEMAS: dict[str, list[str]] = {
    "qra_event_elasticity.csv": [
        "quarter",
        "event_id",
        "event_date_type",
        "headline_bucket",
        "classification_review_status",
        "shock_review_status",
        "series",
        "window",
        "shock_bn",
        "schedule_diff_10y_eq_bn",
        "schedule_diff_dynamic_10y_eq_bn",
        "schedule_diff_dv01_usd",
        "shock_construction",
        "elasticity_bp_per_100bn",
    ],
    "qra_event_elasticity_diagnostic.csv": [
        "quarter",
        "event_id",
        "event_date_type",
        "headline_bucket",
        "classification_review_status",
        "shock_review_status",
        "series",
        "window",
        "shock_bn",
        "schedule_diff_10y_eq_bn",
        "schedule_diff_dynamic_10y_eq_bn",
        "schedule_diff_dv01_usd",
        "shock_construction",
        "elasticity_bp_per_100bn",
    ],
    "qra_event_shock_summary.csv": [
        "quarter",
        "event_id",
        "event_date_type",
        "headline_bucket",
        "overlap_severity",
        "classification_review_status",
        "shock_review_status",
        "shock_bn",
        "schedule_diff_10y_eq_bn",
        "schedule_diff_dynamic_10y_eq_bn",
        "schedule_diff_dv01_usd",
        "shock_construction",
        "usable_for_headline",
    ],
    "qra_event_shock_components.csv": [
        "event_id",
        "quarter",
        "previous_event_id",
        "previous_quarter",
        "tenor",
        "issue_type",
        "current_total_bn",
        "previous_total_bn",
        "delta_bn",
        "yield_date",
        "yield_curve_source",
        "tenor_yield_pct",
        "tenor_modified_duration",
        "duration_factor_source",
        "dynamic_10y_eq_weight",
        "contribution_dynamic_10y_eq_bn",
        "dv01_per_1bn_usd",
        "dv01_contribution_usd",
        "tenor_weight_10y_eq",
        "contribution_10y_eq_bn",
    ],
}
QRA_EVENT_REGISTRY_V2_COLUMNS = [
    "event_id",
    "quarter",
    "release_timestamp_et",
    "release_bundle_type",
    "policy_statement_url",
    "financing_estimates_url",
    "timing_quality",
    "overlap_severity",
    "overlap_label",
    "financing_need_news_flag",
    "composition_news_flag",
    "forward_guidance_flag",
    "reviewer",
    "review_date",
    "quality_tier",
    "eligibility_blockers",
    "timestamp_precision",
    "separability_status",
    "expectation_status",
    "contamination_status",
    "release_component_count",
    "causal_eligible_component_count",
    "treatment_version_id",
    "headline_eligibility_reason",
]

QRA_SHOCK_CROSSWALK_V1_COLUMNS = [
    "event_id",
    "event_date_type",
    "canonical_shock_id",
    "shock_bn",
    "schedule_diff_10y_eq_bn",
    "schedule_diff_dynamic_10y_eq_bn",
    "schedule_diff_dv01_usd",
    "shock_source",
    "manual_override_reason",
    "shock_review_status",
    "treatment_version_id",
    "usable_for_headline_reason",
]

EVENT_USABILITY_TABLE_REQUIRED_GROUPS = [
    {"headline_bucket", "bucket"},
    {"classification_review_status", "event_classification_review_status"},
    {"shock_review_status", "shock_status"},
    {"event_date_type", "date_type"},
    {"overlap_severity", "overlap_level"},
    {"headline_usable_count", "headline_usable", "usable_events"},
    {"event_count", "total_events", "n_events"},
]

LEAVE_ONE_EVENT_OUT_TABLE_REQUIRED_GROUPS = [
    {"left_out_event_id", "dropped_event_id", "excluded_event_id", "excluded_event"},
    {"event_date_type", "date_type"},
    {"headline_bucket", "bucket"},
    {"series", "dependent_variable"},
    {"window", "term"},
    {"estimate", "coef", "elasticity_bp_per_100bn"},
    {"p_value", "pval"},
    {"n_obs", "n_events", "sample_size"},
]

OPTIONAL_PUBLISH_SCHEMAS.update(
    {
        "qra_event_registry_v2.csv": QRA_EVENT_REGISTRY_V2_COLUMNS,
        "qra_shock_crosswalk_v1.csv": QRA_SHOCK_CROSSWALK_V1_COLUMNS,
        "treatment_comparison_table.csv": [
            "spec_id",
            "event_date_type",
            "series",
            "window",
            "treatment_variant",
            "comparison_family",
            "n_events",
            "mean_elasticity_value",
            "headline_recommendation_status",
            "primary_treatment_variant",
        ],
        "event_usability_table.csv": ["headline_bucket", "event_date_type", "event_count"],
        "leave_one_event_out_table.csv": ["left_out_event_id", "series", "window", "estimate"],
        "auction_absorption_table.csv": [
            "qra_event_id",
            "quarter",
            "auction_date",
            "security_family",
            "investor_class",
            "measure",
            "value",
            "units",
            "source_quality",
            "provenance_summary",
        ],
        "qra_release_component_registry.csv": [
            "release_component_id",
            "event_id",
            "quarter",
            "component_type",
            "release_timestamp_et",
            "timestamp_precision",
            "source_url",
            "benchmark_source_family",
            "benchmark_timing_status",
            "external_benchmark_ready",
            "expectation_status",
            "contamination_status",
            "quality_tier",
            "eligibility_blockers",
        ],
        "qra_benchmark_coverage.csv": [
            "scope",
            "metric",
            "value",
            "notes",
        ],
        "qra_causal_qa_ledger.csv": [
            "event_id",
            "quality_tier",
            "eligibility_blockers",
            "timestamp_precision",
            "separability_status",
            "expectation_status",
            "contamination_status",
            "release_component_count",
            "causal_eligible_component_count",
        ],
        "event_design_status.csv": [
            "metric",
            "value",
            "notes",
        ],
    }
)

QRA_EVENT_SHOCK_DRIFT_THRESHOLD_BN = 10.0
QRA_README_COVERAGE_PATTERNS = (
    re.compile(
        r"Exact official quarter coverage\s+currently\s+spans\s+(.+)\.",
        re.IGNORECASE,
    ),
    re.compile(
        r"exact official quarter history is still short and currently covers(?:\s+only)?\s+(.+)\.",
        re.IGNORECASE,
    ),
)
REQUIRED_EXTENSION_SUMMARY_READY = ("investor_allotments", "primary_dealer", "sec_nmfp")
PUBLIC_HYGIENE_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"/Users/", "absolute_local_path"),
    (r"\b(?:orca|dairy|tandy|subagent)\b|\bagent[_ -]?id\b", "internal_agent_reference"),
    (r"\b(?:memory-system\.md|handoff\.md|todo\.md|dontdo\.md|changes\.md)\b", "internal_workflow_reference"),
    (r"data/manual/", "manual_data_reference"),
)
QRA_ACTION_VALUES = {"tightening", "easing", "hold", "mixed", "pending"}
QRA_GUIDANCE_VALUES = {"hawkish", "neutral", "dovish", "pending"}
QRA_HEADLINE_BUCKET_VALUES = {"tightening", "easing", "control_hold", "exclude", "pending"}
QRA_CONFIDENCE_VALUES = {"exact_statement", "table_diff", "hybrid", "heuristic", "pending"}
QRA_REVIEW_VALUES = {"reviewed", "provisional", "pending"}


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


def _coerce_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (float, int)):
        return float(value)
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _coerce_int(value: object) -> int | None:
    float_value = _coerce_float(value)
    if float_value is None:
        return None
    if pd.isna(float_value):
        return None
    try:
        return int(float_value)
    except Exception:
        return None


def _parse_coverage_range(start: str, end: str) -> set[str]:
    start_int = _quarter_to_int(start)
    end_int = _quarter_to_int(end)
    if start_int is None or end_int is None:
        return set()
    if end_int < start_int:
        return set()

    quarters: set[str] = set()
    for value in range(start_int, end_int + 1):
        year = value // 4
        q = value % 4
        if q == 0:
            year -= 1
            q = 4
        quarters.add(f"{year}Q{q}")
    return quarters


def _quarter_to_int(quarter: object) -> int | None:
    text = str(quarter).strip()
    if not re.match(QUARTER_PATTERN, text):
        return None
    year = int(text[:4])
    q = int(text[5])
    return year * 4 + q


def _normalize_quarter(quarter: object) -> str:
    value = str(quarter or "").strip()
    if value.lower() in {"", "nan", "none", "null"}:
        return ""
    return value


def _quarters_to_ranges(quarters: set[str]) -> list[str]:
    if not quarters:
        return []
    ordered = sorted(
        q for q in quarters if re.match(QUARTER_PATTERN, str(q).strip())
    )
    if not ordered:
        return []

    ordinals = [_quarter_to_int(q) for q in ordered if _quarter_to_int(q) is not None]
    ordinals = sorted(set(int(v) for v in ordinals))
    if not ordinals:
        return []

    ranges: list[str] = []
    start = ordinals[0]
    end = ordinals[0]
    for value in ordinals[1:]:
        if value == end + 1:
            end = value
            continue
        ranges.append((start, end))
        start = value
        end = value
    ranges.append((start, end))
    rendered = []
    for left, right in ranges:
        left_year = left // 4
        right_year = right // 4
        left_q = left % 4
        if left_q == 0:
            left_year -= 1
            left_q = 4
        right_q = right % 4
        if right_q == 0:
            right_year -= 1
            right_q = 4
        left_q_label = f"{left_year}Q{left_q}"
        right_q_label = f"{right_year}Q{right_q}"
        if left == right:
            rendered.append(left_q_label)
        else:
            rendered.append(f"{left_q_label} through {right_q_label}")
    return rendered


def _extract_readme_coverage_claim(readme_text: str) -> str | None:
    for pattern in QRA_README_COVERAGE_PATTERNS:
        match = pattern.search(readme_text)
        if match:
            return match.group(1).strip()
    return None


def _parse_exact_coverage_statement(statement: str) -> set[str]:
    if not statement:
        return set()
    cleaned = str(statement).replace("`", "")

    declared = set()
    for statement_start, statement_end in re.findall(
        rf"({UNANCHORED_QUARTER_PATTERN})\s+through\s+({UNANCHORED_QUARTER_PATTERN})",
        cleaned,
        flags=re.IGNORECASE,
    ):
        declared |= _parse_coverage_range(statement_start, statement_end)

    declared |= set(re.findall(UNANCHORED_QUARTER_PATTERN, cleaned))

    return {quarter for quarter in declared if QUARTER_PATTERN.match(quarter)}


def _validate_exact_coverages(
    readiness_frame: pd.DataFrame | None,
    completion_frame: pd.DataFrame | None,
) -> tuple[set[str], set[str]]:
    if readiness_frame is None:
        readiness = pd.DataFrame()
    else:
        readiness = readiness_frame
    if completion_frame is None:
        completion = pd.DataFrame()
    else:
        completion = completion_frame

    if readiness.empty:
        readiness_exact = set()
    else:
        readiness_tier = readiness.get("readiness_tier", pd.Series(dtype=str)).astype(str).str.strip()
        source_quality = readiness.get("source_quality", pd.Series(dtype=str)).astype(str).str.strip()
        readiness_exact = set(
            _normalize_quarter(value)
            for value in readiness.loc[
                readiness_tier.str.contains("exact_official", na=False)
                | source_quality.str.contains("exact_official", na=False),
                "quarter",
            ]
            if QUARTER_PATTERN.match(_normalize_quarter(value))
        )

    if completion.empty:
        completion_exact = set()
    else:
        completion_tier = completion.get("completion_tier", pd.Series(dtype=str)).astype(str).str.strip()
        completion_exact = set(
            _normalize_quarter(value)
            for value in completion.loc[
                completion_tier.str.contains("exact_official", na=False),
                "quarter",
            ]
            if QUARTER_PATTERN.match(_normalize_quarter(value))
        )

    return readiness_exact, completion_exact


def _validate_alias_required_columns(
    frame: pd.DataFrame,
    alias_groups: list[set[str]],
) -> list[str]:
    missing: list[str] = []
    for aliases in alias_groups:
        if not any(alias in frame.columns for alias in aliases):
            missing.append(f"missing_any:{{{','.join(sorted(aliases))}}}")
    return missing


def _validate_alias_compatibility(csv_name: str, frame: pd.DataFrame, *, errors: list[str]) -> None:
    if csv_name == "event_usability_table.csv":
        missing = _validate_alias_required_columns(frame, EVENT_USABILITY_TABLE_REQUIRED_GROUPS)
        for item in missing:
            errors.append(f"publish_artifact_missing_columns:{csv_name}:{item}")
    elif csv_name == "leave_one_event_out_table.csv":
        missing = _validate_alias_required_columns(frame, LEAVE_ONE_EVENT_OUT_TABLE_REQUIRED_GROUPS)
        for item in missing:
            errors.append(f"publish_artifact_missing_columns:{csv_name}:{item}")


def _validate_shock_drift_alerts(
    csv_name: str,
    frame: pd.DataFrame,
    *,
    warnings: list[str],
) -> None:
    if frame.empty or csv_name != "qra_shock_crosswalk_v1.csv":
        return

    working = frame.copy()
    if "treatment_variant" in working.columns:
        canonical = working.loc[working["treatment_variant"].astype(str).str.strip() == "canonical_shock_bn"].copy()
        if not canonical.empty:
            working = canonical
    if "event_date_type" in working.columns:
        official = working.loc[working["event_date_type"].astype(str).str.strip() == "official_release_date"].copy()
        if not official.empty:
            working = official
    dedupe_subset = [
        column
        for column in (
            "event_id",
            "event_date_type",
            "shock_review_status",
            "shock_bn",
            "schedule_diff_10y_eq_bn",
            "schedule_diff_dynamic_10y_eq_bn",
        )
        if column in working.columns
    ]
    if dedupe_subset:
        working = working.drop_duplicates(subset=dedupe_subset, keep="first")

    for source_key in ("schedule_diff_10y_eq_bn", "schedule_diff_dynamic_10y_eq_bn"):
        if source_key not in working.columns:
            continue
        for _, row in working.iterrows():
            event_id = str(row.get("event_id", "")).strip()
            if not event_id:
                continue
            if _coerce_bool(row.get("ignore_drift_alert", False)):
                continue
            schedule = _coerce_float(row.get(source_key))
            if schedule is None or abs(schedule) < QRA_EVENT_SHOCK_DRIFT_THRESHOLD_BN:
                continue
            shock = _coerce_float(row.get("shock_bn"))
            reviewed = str(row.get("shock_review_status", "")).strip().lower()
            alternative_complete = _coerce_bool(row.get("alternative_treatment_complete", False))
            missing_alt_reason = str(row.get("alternative_treatment_missing_reason", "") or "").strip()
            missing_alt_fields = str(row.get("alternative_treatment_missing_fields", "") or "").strip()
            shock_source = str(row.get("shock_source", "") or "").strip().lower()
            manual_override_reason = str(row.get("manual_override_reason", "") or "").strip().lower()
            needs_manual_review = (
                reviewed == "reviewed"
                and not alternative_complete
                and (
                    bool(missing_alt_reason)
                    or bool(missing_alt_fields)
                    or "manual" in shock_source
                    or "manual" in manual_override_reason
                )
            )
            if not needs_manual_review:
                continue
            if shock is None or abs(shock) <= 1e-9:
                warnings.append(
                    "qra_publish_shock_drift_alert:"
                    f"{csv_name}:{event_id}:{source_key}:{schedule}:{reviewed or 'missing_review_status'}"
                )
                continue


def _validate_overlap_excluded_noop(
    csv_name: str,
    frame: pd.DataFrame,
    *,
    errors: list[str],
    warnings: list[str],
) -> None:
    required = {"sample_variant", "event_date_type", "headline_bucket", "n_events"}
    missing = sorted(required - set(frame.columns))
    if missing:
        errors.append(f"qra_overlap_missing_columns:{csv_name}:{','.join(missing)}")
        return
    if frame.empty:
        errors.append(f"qra_overlap_missing_rows:{csv_name}")
        return

    variants = set(frame["sample_variant"].dropna().astype(str))
    for required_variant in ("all_events", "overlap_excluded"):
        if required_variant not in variants:
            errors.append(f"qra_overlap_missing_sample_variant:{csv_name}:{required_variant}")

    note_column = "overlap_exclusion_note" if "overlap_exclusion_note" in frame.columns else None
    grouped = frame.groupby(["event_date_type", "headline_bucket"], dropna=False)
    for (event_date_type, headline_bucket), group in grouped:
        sample_counts = {
            str(row.get("sample_variant")): _coerce_int(row.get("n_events"))
            for row in group.to_dict("records")
        }
        all_events = sample_counts.get("all_events")
        overlap_excluded = sample_counts.get("overlap_excluded")
        if all_events is None or overlap_excluded is None:
            continue
        if overlap_excluded > all_events:
            errors.append(
                "qra_overlap_invalid_count_order:"
                f"{csv_name}:{event_date_type}:{headline_bucket}:{overlap_excluded}>{all_events}"
            )
        if overlap_excluded == all_events:
            note_values = []
            if note_column is not None:
                note_values = [
                    str(value).strip().lower()
                    for value in group.loc[group["sample_variant"].astype(str) == "overlap_excluded", note_column]
                    if str(value).strip()
                ]
            if not any(
                ("no overlap-annotated events were flagged" in value)
                or ("no reviewed overlaps found" in value)
                for value in note_values
            ):
                errors.append(
                    "qra_overlap_identical_without_audit_note:"
                    f"{csv_name}:{event_date_type}:{headline_bucket}"
                )


def _validate_readme_exact_coverage_consistency(
    readme_path: Path,
    readiness_exact: set[str],
    completion_exact: set[str],
    *,
    errors: list[str],
    warnings: list[str],
) -> None:
    if not readme_path.exists():
        warnings.append(f"validation_readme_missing:{readme_path}")
        return

    readme_text = readme_path.read_text(encoding="utf-8", errors="replace")
    statement = _extract_readme_coverage_claim(readme_text)
    if not statement:
        errors.append("readme_missing_exact_official_coverage_claim")
        return

    statement_quarters = _parse_exact_coverage_statement(statement)
    statement_ranges = _quarters_to_ranges(statement_quarters)

    if statement_quarters and readiness_exact != statement_quarters:
        warnings.append(
            "readme_official_coverage_mismatch:"
            f"statement={','.join(sorted(statement_quarters))}:"
            f"readiness={','.join(sorted(readiness_exact))}"
        )
    if readiness_exact != completion_exact and readiness_exact and completion_exact:
        warnings.append(
            "official_coverage_exact_source_mismatch:"
            f"readiness={','.join(sorted(readiness_exact))}:"
            f"completion={','.join(sorted(completion_exact))}"
        )
    if readiness_exact and len(_quarters_to_ranges(readiness_exact)) > 1:
        # Non-contiguous coverage can’t be represented by a single-through span.
        if "through" in statement.lower() and len(statement_ranges) == 1:
            warnings.append("official_coverage_statement_contiguous_while_data_noncontiguous")


def _has_seed_dependency(*values: object) -> bool:
    for value in values:
        if _is_missing(value):
            continue
        text = str(value).lower()
        for part in text.replace(",", "|").replace(";", "|").split("|"):
            if part.strip() == "seed_csv":
                return True
    return False


def _invalid_enum_values(frame: pd.DataFrame, column: str, allowed: set[str]) -> list[str]:
    if column not in frame.columns:
        return []
    return sorted(
        {
            str(value).strip()
            for value in frame[column].dropna().astype(str)
            if str(value).strip() and str(value).strip() not in allowed
        }
    )


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


def _has_absolute_local_path(value: object) -> bool:
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none"}:
        return False
    for part in text.split("|"):
        candidate = part.strip()
        if not candidate or "://" in candidate:
            continue
        if Path(candidate).is_absolute():
            return True
    return False


def validate_manual_capture_template(path: Path) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    frame, status = _safe_read_csv(path)
    if status is not None:
        warnings.append(f"official_capture_template_{status.replace(':', '_')}")
        return errors, warnings
    if frame.empty:
        warnings.append(f"official_capture_template_empty:{path}")
        return errors, warnings
    if {"quarter", "source_doc_local"}.issubset(frame.columns):
        flagged = frame.loc[
            frame["source_doc_local"].apply(_has_absolute_local_path)
        ]
        for row in flagged.to_dict("records"):
            errors.append(
                f"official_capture_template_absolute_source_doc_local:{row.get('quarter', '')}"
            )
    return errors, warnings


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


def _validate_qra_publish_frame(csv_name: str, frame: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    if frame.empty:
        return errors

    if csv_name == "qra_event_registry_v2.csv":
        for _, row in frame.iterrows():
            event_id = str(row.get("event_id", "")).strip()
            if not event_id:
                errors.append("qra_publish_missing_columns:qra_event_registry_v2.csv:event_id")
            if "release_timestamp_et" in frame.columns:
                parsed_timestamp = _coerce_timestamp(row.get("release_timestamp_et"))
                if parsed_timestamp is None:
                    errors.append(f"qra_publish_invalid_timestamp:qra_event_registry_v2.csv:{event_id}")
                elif str(row.get("timestamp_precision", "")).strip() == "exact_time":
                    release_time = parsed_timestamp.strftime("%H:%M:%S")
                    if release_time == "00:00:00":
                        errors.append(
                            f"qra_publish_inexact_event_timestamp_claim:qra_event_registry_v2.csv:{event_id}"
                        )
            elif str(row.get("timestamp_precision", "")).strip() == "exact_time":
                if "release_timestamp_kind" not in frame.columns or "timestamp_with_time" not in str(
                    row.get("release_timestamp_kind", "")
                ):
                    errors.append(
                        f"qra_publish_inexact_event_timestamp_claim:qra_event_registry_v2.csv:{event_id}"
                    )
        if "headline_eligibility_reason" not in frame.columns:
            errors.append("qra_publish_missing_columns:qra_event_registry_v2.csv:headline_eligibility_reason")

    if csv_name == "qra_shock_crosswalk_v1.csv":
        for required in ("canonical_shock_id", "shock_source"):
            if required not in frame.columns:
                errors.append(f"qra_publish_missing_columns:qra_shock_crosswalk_v1.csv:{required}")

    if "quarter" in frame.columns and frame["quarter"].fillna("").astype(str).str.strip().eq("").any():
        errors.append(f"qra_publish_null_quarter:{csv_name}")

    for column, allowed in (
        ("current_quarter_action", QRA_ACTION_VALUES),
        ("forward_guidance_bias", QRA_GUIDANCE_VALUES),
        ("headline_bucket", QRA_HEADLINE_BUCKET_VALUES),
        ("classification_confidence", QRA_CONFIDENCE_VALUES),
        ("classification_review_status", QRA_REVIEW_VALUES),
        ("shock_review_status", QRA_REVIEW_VALUES),
    ):
        invalid = _invalid_enum_values(frame, column, allowed)
        if invalid:
            errors.append(f"qra_publish_invalid_enum:{csv_name}:{column}:{','.join(invalid)}")

    duplicate_key = ["event_id", "event_date_type", "series", "window"]
    if "treatment_variant" in frame.columns:
        duplicate_key.append("treatment_variant")
    if set(duplicate_key).issubset(frame.columns):
        if frame.duplicated(subset=duplicate_key).any():
            errors.append(f"qra_publish_duplicate_event_series_window:{csv_name}")

    if csv_name == "qra_event_elasticity.csv":
        invalid_date_type = frame.loc[frame["event_date_type"].astype(str).str.strip() != "official_release_date"]
        if not invalid_date_type.empty:
            errors.append("qra_publish_noncanonical_date_type:qra_event_elasticity.csv")
    if csv_name == "qra_event_shock_summary.csv":
        invalid_date_type = frame.loc[frame["event_date_type"].astype(str).str.strip() != "official_release_date"]
        if not invalid_date_type.empty:
            errors.append("qra_publish_noncanonical_date_type:qra_event_shock_summary.csv")
        if frame.duplicated(subset=["event_id", "event_date_type"]).any():
            errors.append("qra_publish_duplicate_event_summary:qra_event_shock_summary.csv")

    if {"usable_for_headline", "event_date_type", "headline_bucket", "classification_review_status", "shock_review_status"}.issubset(frame.columns):
        flagged = frame.loc[frame["usable_for_headline"].map(_coerce_bool)]
        if not flagged.empty:
            invalid = flagged.loc[
                (flagged["event_date_type"].astype(str).str.strip() != "official_release_date")
                | (~flagged["headline_bucket"].astype(str).str.strip().isin({"tightening", "easing", "control_hold"}))
                | (flagged["classification_review_status"].astype(str).str.strip() != "reviewed")
                | (flagged["shock_review_status"].astype(str).str.strip() != "reviewed")
            ]
            if not invalid.empty:
                errors.append(f"qra_publish_invalid_headline_semantics:{csv_name}")

    _validate_alias_compatibility(csv_name, frame, errors=errors)

    return errors


def _validate_overlap_excluded_for_file(
    csv_name: str,
    csv_frame: pd.DataFrame,
    *,
    errors: list[str],
    warnings: list[str],
) -> None:
    if csv_name == "qra_event_robustness.csv":
        _validate_overlap_excluded_noop(csv_name, csv_frame, errors=errors, warnings=warnings)


def _validate_causal_component_registry(component_registry: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    if component_registry.empty:
        return errors

    exact_time = component_registry.loc[
        component_registry.get("timestamp_precision", pd.Series(dtype=object))
        .fillna("")
        .astype(str)
        .str.strip()
        .eq("exact_time")
    ].copy()
    if not exact_time.empty:
        missing_expectation = exact_time.loc[
            exact_time.get("expectation_status", pd.Series(dtype=object))
            .fillna("")
            .astype(str)
            .str.strip()
            .eq("")
        ]
        if not missing_expectation.empty:
            errors.append(
                "qra_component_registry_missing_expectation_status:"
                + ",".join(sorted(missing_expectation["release_component_id"].astype(str).unique()))
            )
        missing_contamination = exact_time.loc[
            exact_time.get("contamination_status", pd.Series(dtype=object))
            .fillna("")
            .astype(str)
            .str.strip()
            .eq("")
        ]
        if not missing_contamination.empty:
            errors.append(
                "qra_component_registry_missing_contamination_status:"
                + ",".join(sorted(missing_contamination["release_component_id"].astype(str).unique()))
            )
        if "benchmark_timing_status" in exact_time.columns:
            missing_benchmark_timing = exact_time.loc[
                exact_time["benchmark_timing_status"]
                .fillna("")
                .astype(str)
                .str.strip()
                .eq("")
            ]
        else:
            missing_benchmark_timing = exact_time.copy()
        if not missing_benchmark_timing.empty:
            errors.append(
                "qra_component_registry_missing_benchmark_timing_status:"
                + ",".join(sorted(missing_benchmark_timing["release_component_id"].astype(str).unique()))
            )

    causal_claims = component_registry.loc[
        component_registry.get("causal_eligible", pd.Series(dtype=bool)).fillna(False).astype(bool)
    ].copy()
    if not causal_claims.empty:
        invalid_claims: list[str] = []
        for _, row in causal_claims.iterrows():
            if str(row.get("quality_tier", "")).strip() != "Tier A":
                invalid_claims.append(str(row.get("release_component_id", "")))
                continue
            if not _is_missing(row.get("eligibility_blockers", "")):
                invalid_claims.append(str(row.get("release_component_id", "")))
                continue
            if str(row.get("timestamp_precision", "")).strip() != "exact_time":
                invalid_claims.append(str(row.get("release_component_id", "")))
                continue
            if str(row.get("separability_status", "")).strip() != "separable_component":
                invalid_claims.append(str(row.get("release_component_id", "")))
                continue
            if str(row.get("expectation_status", "")).strip() != "reviewed_surprise_ready":
                invalid_claims.append(str(row.get("release_component_id", "")))
                continue
            if str(row.get("benchmark_timing_status", "")).strip() != "pre_release_external":
                invalid_claims.append(str(row.get("release_component_id", "")))
                continue
            if not _coerce_bool(row.get("external_benchmark_ready")):
                invalid_claims.append(str(row.get("release_component_id", "")))
                continue
            if str(row.get("contamination_status", "")).strip() != "reviewed_clean":
                invalid_claims.append(str(row.get("release_component_id", "")))
                continue
            if not str(row.get("source_url", "")).strip():
                invalid_claims.append(str(row.get("release_component_id", "")))
        if invalid_claims:
            errors.append(
                "qra_component_registry_invalid_causal_eligible_claim:"
                + ",".join(sorted(set(invalid_claims)))
            )

    tier_a = component_registry.loc[
        component_registry.get("quality_tier", pd.Series(dtype=object))
        .fillna("")
        .astype(str)
        .str.strip()
        .eq("Tier A")
    ].copy()
    if not tier_a.empty:
        tier_a_without_flag = tier_a.loc[
            ~tier_a.get("causal_eligible", pd.Series(dtype=bool)).fillna(False).astype(bool)
        ]
        if not tier_a_without_flag.empty:
            errors.append(
                "qra_component_registry_tier_a_not_marked_causal_eligible:"
                + ",".join(sorted(tier_a_without_flag["release_component_id"].astype(str).unique()))
            )
    return errors


def _validate_event_design_status_against_components(
    component_registry: pd.DataFrame,
    event_design_status: pd.DataFrame,
) -> list[str]:
    errors: list[str] = []
    if component_registry.empty or event_design_status.empty:
        return errors

    actual_metrics = {
        "release_component_count": int(len(component_registry)),
        "tier_a_count": int(component_registry.get("quality_tier", pd.Series(dtype=object)).astype(str).eq("Tier A").sum()),
        "tier_b_count": int(component_registry.get("quality_tier", pd.Series(dtype=object)).astype(str).eq("Tier B").sum()),
        "tier_c_count": int(component_registry.get("quality_tier", pd.Series(dtype=object)).astype(str).eq("Tier C").sum()),
        "tier_d_count": int(component_registry.get("quality_tier", pd.Series(dtype=object)).astype(str).eq("Tier D").sum()),
        "exact_time_component_count": int(component_registry.get("timestamp_precision", pd.Series(dtype=object)).astype(str).eq("exact_time").sum()),
        "reviewed_surprise_ready_count": int(component_registry.get("expectation_status", pd.Series(dtype=object)).astype(str).eq("reviewed_surprise_ready").sum()),
        "reviewed_clean_component_count": int(component_registry.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("reviewed_clean").sum()),
    }
    reported = {
        str(row.get("metric", "")).strip(): int(pd.to_numeric(pd.Series([row.get("value")]), errors="coerce").fillna(0).iloc[0])
        for _, row in event_design_status.iterrows()
    }
    mismatched = [
        metric
        for metric, actual in actual_metrics.items()
        if metric in reported and reported[metric] != actual
    ]
    if mismatched:
        errors.append("qra_event_design_status_metric_mismatch:" + ",".join(sorted(mismatched)))
    return errors


def _canonical_qra_review_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    working = frame.copy()
    if "treatment_variant" in working.columns:
        canonical = working.loc[working["treatment_variant"].astype(str).str.strip() == "canonical_shock_bn"].copy()
        if not canonical.empty:
            working = canonical
    if "event_date_type" in working.columns:
        official = working.loc[working["event_date_type"].astype(str).str.strip() == "official_release_date"].copy()
        if not official.empty:
            working = official
    if "event_id" in working.columns:
        dedupe = [column for column in ("event_id", "event_date_type") if column in working.columns]
        working = working.drop_duplicates(subset=dedupe, keep="first").reset_index(drop=True)
    return working


def _validate_qra_publish_consistency(qra_frames: dict[str, pd.DataFrame]) -> list[str]:
    errors: list[str] = []
    registry = qra_frames.get("qra_event_registry_v2.csv", pd.DataFrame())
    component_registry = qra_frames.get("qra_release_component_registry.csv", pd.DataFrame())
    event_design_status = qra_frames.get("event_design_status.csv", pd.DataFrame())
    crosswalk = _canonical_qra_review_frame(qra_frames.get("qra_shock_crosswalk_v1.csv", pd.DataFrame()))
    shock_summary = _canonical_qra_review_frame(qra_frames.get("qra_event_shock_summary.csv", pd.DataFrame()))
    elasticity = _canonical_qra_review_frame(qra_frames.get("qra_event_elasticity.csv", pd.DataFrame()))
    usability = qra_frames.get("event_usability_table.csv", pd.DataFrame())
    if registry.empty or crosswalk.empty or shock_summary.empty or usability.empty:
        if registry.empty or component_registry.empty:
            return errors

    errors.extend(_validate_causal_component_registry(component_registry))
    errors.extend(_validate_event_design_status_against_components(component_registry, event_design_status))

    if not registry.empty and not component_registry.empty:
        registry = registry.drop_duplicates(subset=["event_id"], keep="first").copy()
        quality_order = {"Tier A": 0, "Tier B": 1, "Tier C": 2, "Tier D": 3}
        expected_rows = []
        for event_id, group in component_registry.groupby("event_id", sort=False, dropna=False):
            working = group.copy()
            working["_quality_order"] = working.get("quality_tier", pd.Series(dtype=object)).map(
                lambda value: quality_order.get(str(value), 99)
            )
            working["_release_sort_ts"] = pd.to_datetime(
                working.get("release_timestamp_et", pd.Series(dtype=object)),
                errors="coerce",
            )
            working = working.sort_values(
                by=["_quality_order", "_release_sort_ts", "release_component_id"],
                kind="stable",
            ).reset_index(drop=True)
            best = working.iloc[0]
            expected_rows.append(
                {
                    "event_id": event_id,
                    "expected_release_timestamp_et": best.get("release_timestamp_et", ""),
                    "expected_timestamp_precision": best.get("timestamp_precision", ""),
                }
            )
        expected = pd.DataFrame(expected_rows)
        joined_registry = registry.merge(expected, on="event_id", how="inner")
        if not joined_registry.empty:
            precision_mismatch = joined_registry.loc[
                joined_registry["timestamp_precision"].fillna("").astype(str).str.strip()
                != joined_registry["expected_timestamp_precision"].fillna("").astype(str).str.strip()
            ]
            if not precision_mismatch.empty:
                errors.append(
                    "qra_publish_consistency_registry_component_precision_mismatch:"
                    + ",".join(sorted(precision_mismatch["event_id"].astype(str).unique()))
                )
            timestamp_check = joined_registry.loc[
                joined_registry["expected_release_timestamp_et"].fillna("").astype(str).str.strip() != ""
            ].copy()
            timestamp_mismatch = timestamp_check.loc[
                timestamp_check["release_timestamp_et"].fillna("").astype(str).str.strip()
                != timestamp_check["expected_release_timestamp_et"].fillna("").astype(str).str.strip()
            ]
            if not timestamp_mismatch.empty:
                errors.append(
                    "qra_publish_consistency_registry_component_timestamp_mismatch:"
                    + ",".join(sorted(timestamp_mismatch["event_id"].astype(str).unique()))
                )

    if registry.empty or crosswalk.empty or shock_summary.empty or usability.empty:
        return errors

    registry = registry.drop_duplicates(subset=["event_id"], keep="first").copy()
    if "headline_eligibility_reason" in registry.columns and "usable_for_headline_reason" in crosswalk.columns:
        merged = registry.merge(
            crosswalk[["event_id", "usable_for_headline_reason"]],
            on="event_id",
            how="inner",
        )
        mismatch = merged.loc[
            merged["headline_eligibility_reason"].fillna("").astype(str).str.strip()
            != merged["usable_for_headline_reason"].fillna("").astype(str).str.strip()
        ]
        if not mismatch.empty:
            errors.append(
                "qra_publish_consistency_registry_crosswalk_reason_mismatch:"
                + ",".join(sorted(mismatch["event_id"].astype(str).unique()))
            )

    if not elasticity.empty:
        joined = shock_summary.merge(
            elasticity[["event_id", "usable_for_headline_reason", "spec_id", "treatment_variant"]],
            on="event_id",
            how="inner",
            suffixes=("_summary", "_elasticity"),
        )
        if not joined.empty:
            reason_mismatch = joined.loc[
                joined["usable_for_headline_reason_summary"].fillna("").astype(str).str.strip()
                != joined["usable_for_headline_reason_elasticity"].fillna("").astype(str).str.strip()
            ]
            if not reason_mismatch.empty:
                errors.append(
                    "qra_publish_consistency_summary_elasticity_reason_mismatch:"
                    + ",".join(sorted(reason_mismatch["event_id"].astype(str).unique()))
                )

    joined = shock_summary.merge(
        crosswalk[["event_id", "usable_for_headline_reason", "spec_id", "treatment_variant"]],
        on="event_id",
        how="inner",
        suffixes=("_summary", "_crosswalk"),
    )
    if not joined.empty:
        reason_mismatch = joined.loc[
            joined["usable_for_headline_reason_summary"].fillna("").astype(str).str.strip()
            != joined["usable_for_headline_reason_crosswalk"].fillna("").astype(str).str.strip()
        ]
        if not reason_mismatch.empty:
            errors.append(
                "qra_publish_consistency_summary_crosswalk_reason_mismatch:"
                + ",".join(sorted(reason_mismatch["event_id"].astype(str).unique()))
            )
        if "spec_id_summary" in joined.columns and "spec_id_crosswalk" in joined.columns:
            spec_mismatch = joined.loc[
                joined["spec_id_summary"].fillna("").astype(str).str.strip()
                != joined["spec_id_crosswalk"].fillna("").astype(str).str.strip()
            ]
            if not spec_mismatch.empty:
                errors.append(
                    "qra_publish_consistency_summary_crosswalk_spec_mismatch:"
                    + ",".join(sorted(spec_mismatch["event_id"].astype(str).unique()))
                )
        if "treatment_variant_summary" in joined.columns and "treatment_variant_crosswalk" in joined.columns:
            variant_mismatch = joined.loc[
                joined["treatment_variant_summary"].fillna("").astype(str).str.strip()
                != joined["treatment_variant_crosswalk"].fillna("").astype(str).str.strip()
            ]
            if not variant_mismatch.empty:
                errors.append(
                    "qra_publish_consistency_summary_crosswalk_variant_mismatch:"
                    + ",".join(sorted(variant_mismatch["event_id"].astype(str).unique()))
                )

    if {
        "event_date_type",
        "headline_bucket",
        "classification_review_status",
        "shock_review_status",
        "overlap_severity",
        "usable_for_headline",
        "usable_for_headline_reason",
        "event_id",
    }.issubset(shock_summary.columns) and {
        "event_date_type",
        "headline_bucket",
        "classification_review_status",
        "shock_review_status",
        "overlap_severity",
        "usable_for_headline",
        "usable_for_headline_reason",
        "event_count",
    }.issubset(usability.columns):
        usability = _canonical_qra_review_frame(usability)
        summary_group = (
            shock_summary.groupby(
                [
                    "event_date_type",
                    "headline_bucket",
                    "classification_review_status",
                    "shock_review_status",
                    "overlap_severity",
                    "usable_for_headline",
                    "usable_for_headline_reason",
                ],
                dropna=False,
            )["event_id"]
            .nunique()
            .reset_index(name="event_count_summary")
        )
        merged_counts = usability.merge(
            summary_group,
            on=[
                "event_date_type",
                "headline_bucket",
                "classification_review_status",
                "shock_review_status",
                "overlap_severity",
                "usable_for_headline",
                "usable_for_headline_reason",
            ],
            how="outer",
        )
        mismatch = merged_counts.loc[
            merged_counts["event_count"].fillna(-1).astype(float)
            != merged_counts["event_count_summary"].fillna(-1).astype(float)
        ]
        if not mismatch.empty:
            errors.append("qra_publish_consistency_usability_count_mismatch")
    return errors


def validate_publish_artifacts(
    publish_dir: Path,
    readme_path: Path | None = None,
) -> tuple[list[str], list[str], dict[str, int]]:
    errors: list[str] = []
    warnings: list[str] = []
    if readme_path is None:
        readme_path = ROOT / "README.md"

    readiness_frame: pd.DataFrame | None = None
    completion_frame: pd.DataFrame | None = None
    qra_frames: dict[str, pd.DataFrame] = {}

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
        if csv_name.startswith("qra_event_"):
            errors.extend(_validate_qra_publish_frame(csv_name, csv_frame))
        if csv_name in {"qra_event_shock_summary.csv", "qra_event_elasticity.csv", "qra_shock_crosswalk_v1.csv"}:
            _validate_shock_drift_alerts(csv_name, csv_frame, warnings=warnings)
        if csv_name.startswith("qra_event_"):
            _validate_overlap_excluded_for_file(csv_name, csv_frame, errors=errors, warnings=warnings)
        if csv_name.startswith("qra_") or csv_name in {"event_usability_table.csv", "event_design_status.csv"}:
            qra_frames[csv_name] = csv_frame.copy()
        if csv_name == "official_capture_readiness.csv":
            readiness_frame = csv_frame
        if csv_name == "official_capture_completion.csv":
            completion_frame = csv_frame

    optional_required = set()
    for csv_name, required_columns in OPTIONAL_PUBLISH_SCHEMAS.items():
        siblings = [publish_dir / f"{Path(csv_name).stem}{ext}" for ext in (".csv", ".json", ".md")]
        if not any(path.exists() for path in siblings):
            continue
        optional_required.add(csv_name)
        for artifact in siblings:
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
        if csv_name.startswith("qra_event_"):
            errors.extend(_validate_qra_publish_frame(csv_name, csv_frame))
        if csv_name in {"qra_event_shock_summary.csv", "qra_shock_crosswalk_v1.csv", "qra_event_elasticity.csv"}:
            _validate_shock_drift_alerts(csv_name, csv_frame, warnings=warnings)
        if csv_name.startswith("qra_event_"):
            _validate_overlap_excluded_for_file(csv_name, csv_frame, errors=errors, warnings=warnings)
        if csv_name.startswith("qra_") or csv_name in {"event_usability_table.csv", "event_design_status.csv"}:
            qra_frames[csv_name] = csv_frame.copy()
        if csv_name == "official_capture_readiness.csv":
            readiness_frame = csv_frame
        if csv_name == "official_capture_completion.csv":
            completion_frame = csv_frame

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
    required.update(optional_required)
    required.update(name.replace(".csv", ".json") for name in optional_required)
    required.update(name.replace(".csv", ".md") for name in optional_required)
    required.add("index.json")
    missing_from_index = sorted(required - set(artifacts))
    if missing_from_index:
        errors.append(f"publish_index_missing_artifacts:{','.join(missing_from_index)}")

    dataset_status_path = publish_dir / "dataset_status.csv"
    event_design_status_path = publish_dir / "event_design_status.csv"
    extension_status_path = publish_dir / "extension_status.csv"
    series_catalog_path = publish_dir / "series_metadata_catalog.csv"
    event_design_status = pd.DataFrame()
    if event_design_status_path.exists():
        try:
            event_design_status = pd.read_csv(event_design_status_path)
        except Exception as exc:
            errors.append(f"publish_artifact_read_error:event_design_status.csv:{exc}")
            event_design_status = pd.DataFrame()
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
        if not event_design_status.empty:
            metrics = {
                str(row.get("metric", "")).strip(): int(pd.to_numeric(pd.Series([row.get("value")]), errors="coerce").fillna(0).iloc[0])
                for _, row in event_design_status.iterrows()
            }
            causal_design_ready = metrics.get("tier_a_count", 0) > 0 and metrics.get("reviewed_surprise_ready_count", 0) > 0
            for dataset_name in (
                "qra_event_registry_v2",
                "qra_release_component_registry",
                "qra_causal_qa_ledger",
                "event_design_status",
            ):
                match = dataset_status.loc[dataset_status.get("dataset", pd.Series(dtype=str)) == dataset_name]
                if match.empty:
                    continue
                readiness = str(match.iloc[0].get("readiness_tier", "")).strip()
                if not causal_design_ready and readiness == "supporting_ready":
                    errors.append(f"dataset_status_overstates_causal_readiness:{dataset_name}")
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

    readiness_exact, completion_exact = _validate_exact_coverages(readiness_frame, completion_frame)
    errors.extend(_validate_qra_publish_consistency(qra_frames))
    _validate_readme_exact_coverage_consistency(
        readme_path=readme_path,
        readiness_exact=readiness_exact,
        completion_exact=completion_exact,
        errors=errors,
        warnings=warnings,
    )

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
    readme_path: Path | None = None,
) -> BackendValidationResult:
    errors: list[str] = []
    warnings: list[str] = []
    summaries: dict[str, object] = {}

    if not manual_capture_path.exists():
        warnings.append(f"official_capture_template_missing:{manual_capture_path}")
    else:
        template_errors, template_warnings = validate_manual_capture_template(manual_capture_path)
        errors.extend(template_errors)
        warnings.extend(template_warnings)

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

    if readme_path is None:
        readme_path = ROOT / "README.md"
    publish_errors, publish_warnings, publish_summary = validate_publish_artifacts(
        publish_dir,
        readme_path=readme_path,
    )
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
