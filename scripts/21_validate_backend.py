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
OFFICIAL_ROLE_PREFIXES = (
    "financing",
    "refunding_statement",
    "auction_reconstruction",
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
        "financing_provenance_ready",
        "refunding_statement_provenance_ready",
        "auction_reconstruction_ready",
        "numeric_official_capture_ready",
        "source_completeness",
    ],
    "official_capture_backfill_queue.csv": [
        "quarter",
        "source_quality",
        "readiness_tier",
        "numeric_official_capture_ready",
        "financing_provenance_ready",
        "refunding_statement_provenance_ready",
        "auction_reconstruction_ready",
        "missing_numeric_fields",
        "next_action",
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
    "pricing_regression_summary.csv": [
        "model_id",
        "model_family",
        "panel_key",
        "window_definition",
        "dependent_variable",
        "outcome_role",
        "term",
        "coef",
        "std_err",
        "p_value",
        "nobs",
        "effective_shock_count",
    ],
    "pricing_spec_registry.csv": [
        "spec_id",
        "spec_family",
        "headline_flag",
        "anchor_role",
        "window_definition",
        "sample_start",
        "sample_end",
        "outcome",
        "predictor_set",
        "control_set",
        "frequency",
        "notes",
    ],
    "pricing_subsample_grid.csv": [
        "spec_id",
        "spec_family",
        "variant_id",
        "variant_family",
        "frequency",
        "window_definition",
        "dependent_variable",
        "term",
        "coef",
        "p_value",
        "nobs",
        "effective_shock_count",
    ],
    "pricing_regression_robustness.csv": [
        "dependent_variable",
        "model_id",
        "model_family",
        "variant_id",
        "variant_family",
        "term",
        "coef",
        "p_value",
    ],
    "pricing_scenario_translation.csv": [
        "scenario_id",
        "scenario_label",
        "scenario_role",
        "scenario_shock_bn",
        "model_id",
        "dependent_variable",
        "term",
        "coef_bp_per_100bn",
        "implied_bp_change",
        "p_value",
    ],
    "pricing_release_flow_panel.csv": [
        "release_id",
        "quarter",
        "source_quarters",
        "release_row_count",
        "qra_release_date",
        "market_pricing_marker_minus_1d",
        "delta_dgs10_release_plus_63bd",
        "delta_threefytp10_release_plus_63bd",
        "delta_dff_release_plus_63bd",
        "delta_dgs10_release_minus_21bd_to_minus_1bd",
        "delta_threefytp10_release_minus_21bd_to_minus_1bd",
    ],
    "pricing_release_flow_leave_one_out.csv": [
        "spec_id",
        "window_definition",
        "dependent_variable",
        "omitted_release_id",
        "coef",
        "p_value",
        "nobs",
        "effective_shock_count",
    ],
    "pricing_tau_sensitivity_grid.csv": [
        "tau",
        "model_id",
        "dependent_variable",
        "term",
        "coef",
        "p_value",
        "nobs",
        "effective_shock_count",
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
    "qra_long_rate_translation_panel.csv": [
        "event_id",
        "quarter",
        "event_date_type",
        "translation_variant",
        "translation_value",
        "translation_value_units",
        "translation_method",
        "duration_assumption_source",
        "translation_review_status",
        "quality_tier",
        "long_rate_pilot_ready",
        "long_rate_pilot_blocker",
        "public_role",
        "claim_scope",
    ],
    "qra_benchmark_evidence_registry.csv": [
        "release_component_id",
        "event_id",
        "quarter",
        "component_type",
        "benchmark_source_family",
        "benchmark_timing_status",
        "external_benchmark_ready",
        "expectation_status",
        "benchmark_search_disposition",
        "contamination_status",
        "macro_crosswalk_status",
        "quality_tier",
        "causal_eligible",
        "terminal_disposition",
        "claim_scope",
    ],
    "causal_claims_status.csv": [
        "claim_id",
        "claim_name",
        "claim_scope",
        "readiness_tier",
        "public_role",
        "headline_ready",
        "causal_pilot_ready",
        "source_quality",
        "current_sample_financing_component_count",
        "benchmark_ready_count",
        "tier_a_count",
        "context_only_count",
        "post_release_invalid_count",
        "source_family_exhausted_count",
        "open_candidate_count",
        "can_claim",
        "cannot_claim",
        "boundary_reason",
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
        "claim_scope",
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
        "claim_scope",
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
        "descriptive_headline_reason",
        "usable_for_descriptive_headline",
        "usable_for_headline",
        "claim_scope",
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
    "claim_scope",
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
            "claim_scope",
        ],
        "event_usability_table.csv": ["headline_bucket", "event_date_type", "event_count", "claim_scope"],
        "leave_one_event_out_table.csv": ["left_out_event_id", "series", "window", "estimate", "claim_scope"],
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
            "claim_scope",
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
            "benchmark_search_disposition",
            "contamination_status",
            "macro_crosswalk_status",
            "quality_tier",
            "eligibility_blockers",
        ],
        "qra_benchmark_coverage.csv": [
            "scope",
            "metric",
            "value",
            "notes",
        ],
        "qra_benchmark_blockers_by_event.csv": [
            "event_id",
            "quarter",
            "release_component_count",
            "pre_release_external_count",
            "external_timing_unverified_count",
            "same_release_placeholder_count",
            "post_release_invalid_count",
            "benchmark_verification_incomplete_count",
            "reviewed_surprise_ready_count",
            "tier_a_count",
            "benchmark_blockers",
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
QRA_README_PILOT_PATTERNS = (
    re.compile(
        r"current-sample financing pilot with\s+(\d+)\s+current-sample financing components,\s+(\d+)\s+verified pre-release external benchmarks,\s+(\d+)\s+Tier A components,\s+(\d+)\s+source-family-exhausted blocked rows,\s+and\s+(\d+)\s+open benchmark candidates",
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
CLAIM_SCOPE_VALUES = {"descriptive_only", "causal_pilot_only", "headline"}
BENCHMARK_SEARCH_DISPOSITION_VALUES = {
    "upgraded_pre_release_external",
    "blocked_source_family_exhausted",
    "blocked_open_candidate",
}
MACRO_CROSSWALK_STATUS_VALUES = {
    "reviewed_no_external_overlap",
    "reviewed_external_overlap",
    "local_only_absent",
    "pending_external_crosswalk",
}
SUPPORTING_CLAIM_SCOPE_FILES = {
    "qra_shock_crosswalk_v1.csv",
    "qra_event_elasticity.csv",
    "qra_event_elasticity_diagnostic.csv",
    "qra_event_shock_summary.csv",
    "treatment_comparison_table.csv",
    "event_usability_table.csv",
    "leave_one_event_out_table.csv",
    "qra_long_rate_translation_panel.csv",
    "auction_absorption_table.csv",
    "qra_benchmark_evidence_registry.csv",
}
CLAIM_SCOPE_VALIDATED_NON_QRA_FILES = {
    "event_usability_table.csv",
    "leave_one_event_out_table.csv",
    "treatment_comparison_table.csv",
    "auction_absorption_table.csv",
}


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


def _extract_readme_current_sample_pilot_counts(readme_text: str) -> dict[str, int] | None:
    cleaned = str(readme_text).replace("`", "")
    for pattern in QRA_README_PILOT_PATTERNS:
        match = pattern.search(cleaned)
        if not match:
            continue
        return {
            "current_sample_financing_component_count": int(match.group(1)),
            "benchmark_ready_count": int(match.group(2)),
            "tier_a_count": int(match.group(3)),
            "source_family_exhausted_count": int(match.group(4)),
            "open_candidate_count": int(match.group(5)),
        }
    return None


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


def _validate_readme_causal_claims_consistency(
    readme_path: Path,
    causal_claims_status: pd.DataFrame,
    *,
    errors: list[str],
    warnings: list[str],
) -> None:
    if causal_claims_status.empty:
        return
    if not readme_path.exists():
        warnings.append(f"validation_readme_missing:{readme_path}")
        return
    claim_row = causal_claims_status.loc[
        causal_claims_status.get("claim_id", pd.Series(dtype=object)).astype(str).eq("current_sample_financing_pilot")
    ]
    if claim_row.empty:
        return

    readme_text = readme_path.read_text(encoding="utf-8", errors="replace")
    counts = _extract_readme_current_sample_pilot_counts(readme_text)
    if counts is None:
        errors.append("readme_missing_current_sample_financing_pilot_counts")
        return

    row = claim_row.iloc[0]
    expected = {
        "current_sample_financing_component_count": _coerce_int(row.get("current_sample_financing_component_count")) or 0,
        "benchmark_ready_count": _coerce_int(row.get("benchmark_ready_count")) or 0,
        "tier_a_count": _coerce_int(row.get("tier_a_count")) or 0,
        "source_family_exhausted_count": _coerce_int(row.get("source_family_exhausted_count")) or 0,
        "open_candidate_count": _coerce_int(row.get("open_candidate_count")) or 0,
    }
    mismatched = [key for key, value in expected.items() if counts.get(key) != value]
    if mismatched:
        errors.append("readme_current_sample_financing_pilot_mismatch:" + ",".join(sorted(mismatched)))


def _validate_neutral_maturity_tilt_language(
    readme_path: Path,
    pricing_methods_path: Path,
    pricing_results_path: Path | None = None,
    *,
    errors: list[str],
    warnings: list[str],
) -> None:
    targets = (
        ("README", readme_path),
        ("PRICING_METHODS", pricing_methods_path),
        ("PRICING_RESULTS", pricing_results_path),
    )
    for label, path in targets:
        if path is None:
            continue
        if not path.exists():
            warnings.append(f"validation_doc_missing:{path}")
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        relevant = True
        if label == "README":
            lowered = text.lower()
            relevant = any(token in lowered for token in ("pricing", "maturity", "term premium", "yield", "ati"))
        if relevant and "Maturity Tilt" not in text:
            errors.append(f"{label.lower()}_missing_maturity_tilt_label")
        if re.search(r"\bATI\b", text):
            errors.append(f"{label.lower()}_uses_ati_as_primary_public_label")


def _validate_release_level_anchor_language(
    readme_path: Path,
    pricing_results_path: Path,
    *,
    errors: list[str],
    warnings: list[str],
) -> None:
    targets = (
        ("README", readme_path),
        ("PRICING_RESULTS", pricing_results_path),
    )
    for label, path in targets:
        if not path.exists():
            warnings.append(f"validation_doc_missing:{path}")
            continue
        text = path.read_text(encoding="utf-8", errors="replace").lower()
        if "release-level" not in text and "release level" not in text:
            errors.append(f"{label.lower()}_missing_release_level_anchor_language")


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
            for prefix in OFFICIAL_ROLE_PREFIXES:
                doc_type = str(row.get(f"{prefix}_source_doc_type", "") or "").strip()
                if not doc_type:
                    errors.append(
                        f"official_capture_missing_required_role_doc_type:{prefix}:quarter={quarter}"
                    )
                if not _has_role_locator(row, prefix):
                    errors.append(
                        f"official_capture_missing_required_role_locator:{prefix}:quarter={quarter}"
                    )
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


def _has_role_locator(row: dict[str, object] | pd.Series, prefix: str) -> bool:
    return bool(str(row.get(f"{prefix}_source_url", "") or "").strip()) or bool(
        str(row.get(f"{prefix}_source_doc_local", "") or "").strip()
    )


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
    for prefix in OFFICIAL_ROLE_PREFIXES:
        column = f"{prefix}_source_doc_local"
        if {"quarter", column}.issubset(frame.columns):
            flagged = frame.loc[frame[column].apply(_has_absolute_local_path)]
            for row in flagged.to_dict("records"):
                errors.append(
                    f"official_capture_template_absolute_{prefix}_source_doc_local:{row.get('quarter', '')}"
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
        ("claim_scope", CLAIM_SCOPE_VALUES),
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

    headline_flag_column = None
    if "usable_for_descriptive_headline" in frame.columns:
        headline_flag_column = "usable_for_descriptive_headline"
    elif "usable_for_headline" in frame.columns:
        headline_flag_column = "usable_for_headline"
    if headline_flag_column and {"event_date_type", "headline_bucket", "classification_review_status", "shock_review_status"}.issubset(frame.columns):
        flagged = frame.loc[frame[headline_flag_column].map(_coerce_bool)]
        if not flagged.empty:
            invalid = flagged.loc[
                (flagged["event_date_type"].astype(str).str.strip() != "official_release_date")
                | (~flagged["headline_bucket"].astype(str).str.strip().isin({"tightening", "easing", "control_hold"}))
                | (flagged["classification_review_status"].astype(str).str.strip() != "reviewed")
                | (flagged["shock_review_status"].astype(str).str.strip() != "reviewed")
            ]
            if not invalid.empty:
                errors.append(f"qra_publish_invalid_headline_semantics:{csv_name}")

    if csv_name in SUPPORTING_CLAIM_SCOPE_FILES and "claim_scope" in frame.columns:
        invalid_headline_scope = frame.loc[
            frame["claim_scope"].fillna("").astype(str).str.strip().eq("headline")
        ]
        if not invalid_headline_scope.empty:
            errors.append(f"qra_publish_invalid_claim_scope:{csv_name}")

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

    indexed_empty_object = pd.Series("", index=component_registry.index)
    indexed_empty_bool = pd.Series(False, index=component_registry.index)
    exact_time = component_registry.loc[
        component_registry.get("timestamp_precision", indexed_empty_object)
        .fillna("")
        .astype(str)
        .str.strip()
        .eq("exact_time")
    ].copy()
    if not exact_time.empty:
        exact_time_empty_object = pd.Series("", index=exact_time.index)
        missing_expectation = exact_time.loc[
            exact_time.get("expectation_status", exact_time_empty_object)
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
            exact_time.get("contamination_status", exact_time_empty_object)
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
        reviewed_mask = (
            exact_time.get("review_status", pd.Series("", index=exact_time.index))
            .fillna("")
            .astype(str)
            .str.strip()
            .eq("reviewed")
        )
        current_sample_mask = (
            exact_time.get("quarter", pd.Series("", index=exact_time.index))
            .map(_normalize_quarter)
            .map(_quarter_to_int)
            .fillna(0)
            .ge(_quarter_to_int("2022Q3") or 0)
        )
        reviewed_current = exact_time.loc[reviewed_mask & current_sample_mask].copy()
        if not reviewed_current.empty:
            missing_source_method = (
                reviewed_current.get("release_timestamp_source_method", pd.Series("", index=reviewed_current.index))
                .fillna("")
                .astype(str)
                .str.strip()
                .eq("")
            )
            missing_url = (
                reviewed_current.get("timestamp_evidence_url", pd.Series("", index=reviewed_current.index))
                .fillna("")
                .astype(str)
                .str.strip()
                .eq("")
            )
            missing_note = (
                reviewed_current.get("timestamp_evidence_note", pd.Series("", index=reviewed_current.index))
                .fillna("")
                .astype(str)
                .str.strip()
                .eq("")
            )
            missing_evidence = reviewed_current.loc[missing_source_method | (missing_url & missing_note)]
            if not missing_evidence.empty:
                errors.append(
                    "qra_component_registry_missing_timestamp_evidence:"
                    + ",".join(sorted(missing_evidence["release_component_id"].astype(str).unique()))
                )
    benchmark_ready_mask = (
        component_registry.get("external_benchmark_ready", pd.Series(False, index=component_registry.index))
        .fillna(False)
        .astype(bool)
    )
    benchmark_ready = component_registry.loc[benchmark_ready_mask].copy()
    if not benchmark_ready.empty:
        invalid_benchmark_ready = benchmark_ready.loc[
            benchmark_ready.get(
                "benchmark_pre_release_verified_flag",
                pd.Series("", index=benchmark_ready.index),
            ).map(_coerce_bool).ne(True)
            | benchmark_ready.get(
                "benchmark_observed_before_component_flag",
                pd.Series("", index=benchmark_ready.index),
            ).map(_coerce_bool).ne(True)
            | benchmark_ready.get(
                "benchmark_timestamp_source_method",
                pd.Series("", index=benchmark_ready.index),
            ).fillna("").astype(str).str.strip().eq("")
            | benchmark_ready.get(
                "benchmark_timing_status",
                pd.Series("", index=benchmark_ready.index),
            ).fillna("").astype(str).str.strip().ne("pre_release_external")
        ]
        if not invalid_benchmark_ready.empty:
            errors.append(
                "qra_component_registry_invalid_external_benchmark_ready:"
                + ",".join(sorted(invalid_benchmark_ready["release_component_id"].astype(str).unique()))
            )
    financing_mask = (
        component_registry.get("component_type", pd.Series("", index=component_registry.index))
        .fillna("")
        .astype(str)
        .str.strip()
        .eq("financing_estimates")
    )
    current_sample_quarter_mask = (
        component_registry.get("quarter", pd.Series("", index=component_registry.index))
        .map(_normalize_quarter)
        .map(_quarter_to_int)
        .fillna(0)
        .ge(_quarter_to_int("2022Q3") or 0)
    )
    current_sample_financing = component_registry.loc[financing_mask & current_sample_quarter_mask].copy()
    if not current_sample_financing.empty:
        pending_contamination = current_sample_financing.loc[
            current_sample_financing.get(
                "contamination_status",
                pd.Series("", index=current_sample_financing.index),
            ).fillna("").astype(str).str.strip().eq("pending_review")
        ]
        if not pending_contamination.empty:
            errors.append(
                "qra_component_registry_pending_financing_contamination_review:"
                + ",".join(sorted(pending_contamination["release_component_id"].astype(str).unique()))
            )

    causal_claims = component_registry.loc[
        component_registry.get("causal_eligible", indexed_empty_bool).fillna(False).astype(bool)
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
            if not str(row.get("release_timestamp_source_method", "")).strip():
                invalid_claims.append(str(row.get("release_component_id", "")))
                continue
            if not str(row.get("timestamp_evidence_url", "")).strip() and not str(row.get("timestamp_evidence_note", "")).strip():
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
        component_registry.get("quality_tier", indexed_empty_object)
        .fillna("")
        .astype(str)
        .str.strip()
        .eq("Tier A")
    ].copy()
    if not tier_a.empty:
        tier_a_without_flag = tier_a.loc[
            ~tier_a.get("causal_eligible", pd.Series(False, index=tier_a.index)).fillna(False).astype(bool)
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

    current_sample_financing = component_registry.loc[
        component_registry.get("component_type", pd.Series(dtype=object)).astype(str).eq("financing_estimates")
        & component_registry.get("quarter", pd.Series(dtype=object)).map(_normalize_quarter).map(_quarter_to_int).fillna(0).ge(_quarter_to_int("2022Q3") or 0)
    ].copy()
    actual_metrics = {
        "release_component_count": int(len(component_registry)),
        "tier_a_count": int(component_registry.get("quality_tier", pd.Series(dtype=object)).astype(str).eq("Tier A").sum()),
        "tier_b_count": int(component_registry.get("quality_tier", pd.Series(dtype=object)).astype(str).eq("Tier B").sum()),
        "tier_c_count": int(component_registry.get("quality_tier", pd.Series(dtype=object)).astype(str).eq("Tier C").sum()),
        "tier_d_count": int(component_registry.get("quality_tier", pd.Series(dtype=object)).astype(str).eq("Tier D").sum()),
        "exact_time_component_count": int(component_registry.get("timestamp_precision", pd.Series(dtype=object)).astype(str).eq("exact_time").sum()),
        "reviewed_surprise_ready_count": int(component_registry.get("expectation_status", pd.Series(dtype=object)).astype(str).eq("reviewed_surprise_ready").sum()),
        "reviewed_clean_component_count": int(component_registry.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("reviewed_clean").sum()),
        "current_sample_financing_component_count": int(len(current_sample_financing)),
        "current_sample_financing_exact_time_count": int(current_sample_financing.get("timestamp_precision", pd.Series(dtype=object)).astype(str).eq("exact_time").sum()),
        "current_sample_financing_reviewed_clean_count": int(current_sample_financing.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("reviewed_clean").sum()),
        "current_sample_financing_pre_release_external_count": int(current_sample_financing.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("pre_release_external").sum()),
        "current_sample_financing_reviewed_surprise_ready_count": int(current_sample_financing.get("expectation_status", pd.Series(dtype=object)).astype(str).eq("reviewed_surprise_ready").sum()),
        "current_sample_financing_post_release_invalid_count": int(current_sample_financing.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("post_release_invalid").sum()),
        "current_sample_financing_external_timing_unverified_count": int(current_sample_financing.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("external_timing_unverified").sum()),
        "current_sample_financing_same_release_placeholder_count": int(current_sample_financing.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("same_release_placeholder").sum()),
        "current_sample_financing_pending_contamination_review_count": int(current_sample_financing.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("pending_review").sum()),
        "current_sample_financing_reviewed_contaminated_context_only_count": int(current_sample_financing.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("reviewed_contaminated_context_only").sum()),
        "current_sample_financing_reviewed_contaminated_exclude_count": int(current_sample_financing.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("reviewed_contaminated_exclude").sum()),
        "current_sample_financing_tier_a_count": int(current_sample_financing.get("quality_tier", pd.Series(dtype=object)).astype(str).eq("Tier A").sum()),
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


def _validate_qra_benchmark_coverage_against_components(
    component_registry: pd.DataFrame,
    benchmark_coverage: pd.DataFrame,
) -> list[str]:
    errors: list[str] = []
    if component_registry.empty or benchmark_coverage.empty:
        return errors

    current_sample = component_registry.loc[
        component_registry.get("quarter", pd.Series(dtype=object)).map(_normalize_quarter).map(_quarter_to_int).fillna(0).ge(_quarter_to_int("2022Q3") or 0)
    ].copy()
    if current_sample.empty:
        return errors

    scopes = {
        "current_sample_all_components": current_sample,
        "current_sample_financing_estimates": current_sample.loc[
            current_sample.get("component_type", pd.Series(dtype=object)).astype(str).eq("financing_estimates")
        ].copy(),
    }
    reported = {
        (str(row.get("scope", "")).strip(), str(row.get("metric", "")).strip()): int(
            pd.to_numeric(pd.Series([row.get("value")]), errors="coerce").fillna(0).iloc[0]
        )
        for _, row in benchmark_coverage.iterrows()
    }
    mismatched: list[str] = []
    for scope, frame in scopes.items():
        if frame.empty:
            continue
        actual_metrics = {
            "release_component_count": int(len(frame)),
            "pre_release_external_benchmark_count": int(frame.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("pre_release_external").sum()),
            "same_release_placeholder_count": int(frame.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("same_release_placeholder").sum()),
            "post_release_invalid_count": int(frame.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("post_release_invalid").sum()),
            "external_timing_unverified_count": int(frame.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("external_timing_unverified").sum()),
            "external_benchmark_ready_count": int(frame.get("external_benchmark_ready", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()),
            "reviewed_surprise_ready_count": int(frame.get("expectation_status", pd.Series(dtype=object)).astype(str).eq("reviewed_surprise_ready").sum()),
            "reviewed_clean_count": int(frame.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("reviewed_clean").sum()),
            "reviewed_contaminated_context_only_count": int(frame.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("reviewed_contaminated_context_only").sum()),
            "reviewed_contaminated_exclude_count": int(frame.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("reviewed_contaminated_exclude").sum()),
            "pending_review_count": int(frame.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("pending_review").sum()),
            "causal_eligible_count": int(frame.get("causal_eligible", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()),
        }
        for metric, actual in actual_metrics.items():
            key = (scope, metric)
            if key in reported and reported[key] != actual:
                mismatched.append(f"{scope}:{metric}")
    if mismatched:
        errors.append("qra_benchmark_coverage_metric_mismatch:" + ",".join(sorted(mismatched)))
    return errors


def _validate_qra_benchmark_evidence_registry_against_components(
    component_registry: pd.DataFrame,
    benchmark_evidence_registry: pd.DataFrame,
) -> list[str]:
    errors: list[str] = []
    if component_registry.empty or benchmark_evidence_registry.empty:
        return errors

    financing = component_registry.loc[
        component_registry.get("component_type", pd.Series(dtype=object)).astype(str).eq("financing_estimates")
        & component_registry.get("quarter", pd.Series(dtype=object)).map(_normalize_quarter).map(_quarter_to_int).fillna(0).ge(_quarter_to_int("2022Q3") or 0)
    ].copy()
    if financing.empty:
        return errors

    expected_ids = set(financing.get("release_component_id", pd.Series(dtype=object)).dropna().astype(str))
    actual_ids = set(benchmark_evidence_registry.get("release_component_id", pd.Series(dtype=object)).dropna().astype(str))
    missing_ids = sorted(expected_ids - actual_ids)
    extra_ids = sorted(actual_ids - expected_ids)
    if missing_ids:
        errors.append("qra_benchmark_evidence_registry_missing_rows:" + ",".join(missing_ids))
    if extra_ids:
        errors.append("qra_benchmark_evidence_registry_unexpected_rows:" + ",".join(extra_ids))

    if "release_component_id" not in benchmark_evidence_registry.columns:
        return errors

    merged = financing.merge(
        benchmark_evidence_registry,
        on="release_component_id",
        how="inner",
        suffixes=("_component", "_registry"),
    )
    if merged.empty:
        return errors

    mismatched: list[str] = []
    for base_name in (
        "benchmark_timing_status",
        "external_benchmark_ready",
        "expectation_status",
        "benchmark_search_disposition",
        "contamination_status",
        "macro_crosswalk_status",
        "quality_tier",
        "causal_eligible",
    ):
        component_col = f"{base_name}_component"
        registry_col = f"{base_name}_registry"
        if component_col not in merged.columns or registry_col not in merged.columns:
            continue
        component_vals = merged[component_col].fillna("").astype(str).str.strip()
        registry_vals = merged[registry_col].fillna("").astype(str).str.strip()
        if base_name in {"external_benchmark_ready", "causal_eligible"}:
            component_vals = component_vals.str.lower()
            registry_vals = registry_vals.str.lower()
        mismatch_rows = merged.loc[component_vals != registry_vals]
        if not mismatch_rows.empty:
            mismatched.append(base_name)
    if "claim_scope" in benchmark_evidence_registry.columns:
        expected_claim_scope = merged.get("causal_eligible_component", pd.Series(dtype=bool)).fillna(False).astype(bool).map(
            lambda eligible: "causal_pilot_only" if eligible else "descriptive_only"
        )
        actual_claim_scope = merged["claim_scope"].fillna("").astype(str).str.strip()
        if actual_claim_scope.ne(expected_claim_scope).any():
            mismatched.append("claim_scope")
    if mismatched:
        errors.append("qra_benchmark_evidence_registry_metric_mismatch:" + ",".join(sorted(mismatched)))
    return errors


def _validate_causal_claims_status_against_components(
    component_registry: pd.DataFrame,
    causal_claims_status: pd.DataFrame,
) -> list[str]:
    errors: list[str] = []
    if component_registry.empty or causal_claims_status.empty:
        return errors
    claim_row = causal_claims_status.loc[
        causal_claims_status.get("claim_id", pd.Series(dtype=object)).astype(str).eq("current_sample_financing_pilot")
    ]
    if claim_row.empty:
        errors.append("causal_claims_status_missing_current_sample_financing_pilot")
        return errors
    row = claim_row.iloc[0]
    financing = component_registry.loc[
        component_registry.get("component_type", pd.Series(dtype=object)).astype(str).eq("financing_estimates")
        & component_registry.get("quarter", pd.Series(dtype=object)).map(_normalize_quarter).map(_quarter_to_int).fillna(0).ge(_quarter_to_int("2022Q3") or 0)
    ].copy()
    actual = {
        "current_sample_financing_component_count": int(len(financing)),
        "benchmark_ready_count": int(financing.get("expectation_status", pd.Series(dtype=object)).astype(str).eq("reviewed_surprise_ready").sum()),
        "tier_a_count": int(financing.get("quality_tier", pd.Series(dtype=object)).astype(str).eq("Tier A").sum()),
        "context_only_count": int(financing.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("reviewed_contaminated_context_only").sum()),
        "post_release_invalid_count": int(financing.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("post_release_invalid").sum()),
        "source_family_exhausted_count": int(financing.get("benchmark_search_disposition", pd.Series(dtype=object)).astype(str).eq("blocked_source_family_exhausted").sum()),
        "open_candidate_count": int(financing.get("benchmark_search_disposition", pd.Series(dtype=object)).astype(str).eq("blocked_open_candidate").sum()),
    }
    mismatched = [
        key
        for key, expected in actual.items()
        if _coerce_int(row.get(key)) != expected
    ]
    if mismatched:
        errors.append("causal_claims_status_metric_mismatch:" + ",".join(sorted(mismatched)))
    expected_claim_scope = "causal_pilot_only" if actual["benchmark_ready_count"] > 0 and actual["tier_a_count"] > 0 else "descriptive_only"
    actual_claim_scope = str(row.get("claim_scope", "")).strip()
    if actual_claim_scope != expected_claim_scope:
        errors.append("causal_claims_status_invalid_claim_scope:current_sample_financing_pilot")
    if _coerce_bool(row.get("headline_ready")):
        errors.append("causal_claims_status_invalid_headline_ready:current_sample_financing_pilot")
    if str(row.get("public_role", "")).strip() != "supporting":
        errors.append("causal_claims_status_invalid_public_role:current_sample_financing_pilot")
    if not str(row.get("can_claim", "")).strip() or not str(row.get("cannot_claim", "")).strip():
        errors.append("causal_claims_status_missing_plain_language_boundary:current_sample_financing_pilot")
    return errors


def validate_manual_causal_review_inputs(manual_dir: Path) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    registry_path = manual_dir / "qra_release_component_registry.csv"
    expectation_path = manual_dir / "qra_component_expectation_template.csv"
    contamination_path = manual_dir / "qra_event_contamination_reviews.csv"
    overlap_path = manual_dir / "qra_event_overlap_annotations.csv"
    if not registry_path.exists():
        warnings.append(f"manual_causal_registry_missing:{registry_path}")
        return errors, warnings
    registry, status = _safe_read_csv(registry_path)
    if status is not None:
        warnings.append(f"manual_causal_registry_{status.replace(':', '_')}")
        return errors, warnings
    def _cell_text(value: object) -> str:
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except TypeError:
            pass
        return str(value).strip()

    registry_ids = set(registry.get("release_component_id", pd.Series(dtype=object)).dropna().astype(str))
    current_sample_financing_ids = set(
        registry.loc[
            registry.get("component_type", pd.Series(dtype=object)).fillna("").astype(str).str.strip().eq("financing_estimates")
            & registry.get("quarter", pd.Series(dtype=object)).map(_normalize_quarter).map(_quarter_to_int).fillna(0).ge(_quarter_to_int("2022Q3") or 0),
            "release_component_id",
        ].dropna().astype(str)
    )
    current_sample_financing_event_ids = set(
        (
            registry.get("event_id", pd.Series(index=registry.index, dtype=object))
            if "event_id" in registry.columns
            else registry.get("release_component_id", pd.Series(index=registry.index, dtype=object)).astype(str).map(
                lambda value: value.split("__", 1)[0] if "__" in value else ""
            )
        ).loc[
            registry.get("release_component_id", pd.Series(dtype=object)).astype(str).isin(current_sample_financing_ids)
        ].dropna().astype(str)
    )
    for name, path in (
        ("expectation", expectation_path),
        ("contamination", contamination_path),
    ):
        if not path.exists():
            warnings.append(f"manual_causal_{name}_missing:{path}")
            continue
        frame, status = _safe_read_csv(path)
        if status is not None:
            warnings.append(f"manual_causal_{name}_{status.replace(':', '_')}")
            continue
        if "release_component_id" not in frame.columns:
            errors.append(f"manual_causal_{name}_missing_release_component_id")
            continue
        frame_ids = set(frame["release_component_id"].dropna().astype(str))
        orphan_ids = sorted(frame_ids - registry_ids)
        if orphan_ids:
            errors.append(f"manual_causal_{name}_orphans:" + ",".join(orphan_ids))
        if current_sample_financing_ids:
            missing_financing_ids = sorted(current_sample_financing_ids - frame_ids)
            if missing_financing_ids:
                errors.append(
                    f"manual_causal_{name}_missing_current_sample_financing_rows:"
                    + ",".join(missing_financing_ids)
                )
            financing_rows = frame.loc[
                frame.get("release_component_id", pd.Series(dtype=object)).astype(str).isin(current_sample_financing_ids)
            ].copy()
            incomplete_ids: list[str] = []
            if name == "expectation":
                for _, row in financing_rows.iterrows():
                    required = (
                        _cell_text(row.get("benchmark_timing_status")),
                        _cell_text(row.get("expectation_review_status")),
                        _cell_text(row.get("expectation_notes")),
                    )
                    if any(not value for value in required):
                        incomplete_ids.append(str(row.get("release_component_id", "")))
                        continue
                    if _cell_text(row.get("expectation_review_status")) != "reviewed":
                        incomplete_ids.append(str(row.get("release_component_id", "")))
                        continue
                    disposition = _cell_text(row.get("benchmark_search_disposition"))
                    status = _cell_text(row.get("benchmark_timing_status"))
                    if disposition not in BENCHMARK_SEARCH_DISPOSITION_VALUES:
                        incomplete_ids.append(str(row.get("release_component_id", "")))
                        continue
                    if status == "pre_release_external" and disposition != "upgraded_pre_release_external":
                        incomplete_ids.append(str(row.get("release_component_id", "")))
                        continue
                    if status != "pre_release_external" and disposition == "upgraded_pre_release_external":
                        incomplete_ids.append(str(row.get("release_component_id", "")))
                        continue
                    if disposition in {"blocked_source_family_exhausted", "blocked_open_candidate"} and not _cell_text(row.get("benchmark_search_note")):
                        incomplete_ids.append(str(row.get("release_component_id", "")))
                        continue
                    if disposition == "blocked_open_candidate":
                        note = _cell_text(row.get("benchmark_search_note")).lower()
                        if not any(token in note for token in ("primary dealer", "tbac", "treasury")):
                            incomplete_ids.append(str(row.get("release_component_id", "")))
                            continue
                    if _cell_text(row.get("benchmark_timing_status")) == "pre_release_external":
                        has_benchmark_time = bool(
                            _cell_text(row.get("benchmark_timestamp_et"))
                            or _cell_text(row.get("benchmark_release_timestamp_et"))
                        )
                        if (
                            not has_benchmark_time
                            or not _cell_text(row.get("benchmark_source"))
                            or not _cell_text(row.get("benchmark_source_family"))
                            or not _coerce_bool(row.get("benchmark_pre_release_verified_flag"))
                            or not _coerce_bool(row.get("benchmark_observed_before_component_flag"))
                        ):
                            incomplete_ids.append(str(row.get("release_component_id", "")))
                if incomplete_ids:
                    errors.append(
                        "manual_causal_expectation_incomplete_current_sample_financing_rows:"
                        + ",".join(sorted(set(incomplete_ids)))
                    )
            if name == "contamination":
                for _, row in financing_rows.iterrows():
                    required = (
                        _cell_text(row.get("contamination_status")),
                        _cell_text(row.get("contamination_review_status")),
                        _cell_text(row.get("contamination_notes")),
                    )
                    if any(not value for value in required):
                        incomplete_ids.append(str(row.get("release_component_id", "")))
                        continue
                    if _cell_text(row.get("contamination_review_status")) != "reviewed":
                        incomplete_ids.append(str(row.get("release_component_id", "")))
                        continue
                    if not _cell_text(row.get("decision_rule")):
                        incomplete_ids.append(str(row.get("release_component_id", "")))
                        continue
                    if not _cell_text(row.get("decision_confidence")):
                        incomplete_ids.append(str(row.get("release_component_id", "")))
                        continue
                    if _cell_text(row.get("exclude_from_causal_pool")) == "":
                        incomplete_ids.append(str(row.get("release_component_id", "")))
                        continue
                    status = _cell_text(row.get("contamination_status"))
                    if status in {"reviewed_contaminated", "reviewed_contaminated_exclude", "reviewed_contaminated_context_only"}:
                        if not _cell_text(row.get("decision_rule")):
                            incomplete_ids.append(str(row.get("release_component_id", "")))
                if incomplete_ids:
                    errors.append(
                        "manual_causal_contamination_incomplete_current_sample_financing_rows:"
                        + ",".join(sorted(set(incomplete_ids)))
                    )
    if current_sample_financing_event_ids:
        if not overlap_path.exists():
            warnings.append(f"manual_causal_overlap_missing:{overlap_path}")
            return errors, warnings
        overlap, status = _safe_read_csv(overlap_path)
        if status is not None:
            warnings.append(f"manual_causal_overlap_{status.replace(':', '_')}")
            return errors, warnings
        if "event_id" not in overlap.columns:
            errors.append("manual_causal_overlap_missing_event_id")
            return errors, warnings
        overlap_ids = set(overlap["event_id"].dropna().astype(str))
        missing_event_ids = sorted(current_sample_financing_event_ids - overlap_ids)
        if missing_event_ids:
            errors.append("manual_causal_overlap_missing_current_sample_financing_events:" + ",".join(missing_event_ids))
        overlap_rows = overlap.loc[
            overlap.get("event_id", pd.Series(dtype=object)).astype(str).isin(current_sample_financing_event_ids)
        ].copy()
        invalid_overlap_ids: list[str] = []
        for _, row in overlap_rows.iterrows():
            event_id = _cell_text(row.get("event_id"))
            macro_status = _cell_text(row.get("macro_crosswalk_status"))
            macro_note = _cell_text(row.get("macro_crosswalk_note"))
            overlap_flag = _coerce_bool(row.get("overlap_flag"))
            if macro_status not in MACRO_CROSSWALK_STATUS_VALUES or not macro_note:
                invalid_overlap_ids.append(event_id)
                continue
            if overlap_flag and macro_status != "reviewed_external_overlap":
                invalid_overlap_ids.append(event_id)
                continue
            if not overlap_flag and macro_status == "reviewed_external_overlap":
                invalid_overlap_ids.append(event_id)
        if invalid_overlap_ids:
            errors.append(
                "manual_causal_overlap_incomplete_current_sample_financing_events:"
                + ",".join(sorted(set(invalid_overlap_ids)))
            )
    return errors, warnings


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
    benchmark_coverage = qra_frames.get("qra_benchmark_coverage.csv", pd.DataFrame())
    benchmark_evidence_registry = qra_frames.get("qra_benchmark_evidence_registry.csv", pd.DataFrame())
    causal_claims_status = qra_frames.get("causal_claims_status.csv", pd.DataFrame())
    crosswalk = _canonical_qra_review_frame(qra_frames.get("qra_shock_crosswalk_v1.csv", pd.DataFrame()))
    shock_summary = _canonical_qra_review_frame(qra_frames.get("qra_event_shock_summary.csv", pd.DataFrame()))
    elasticity = _canonical_qra_review_frame(qra_frames.get("qra_event_elasticity.csv", pd.DataFrame()))
    usability = qra_frames.get("event_usability_table.csv", pd.DataFrame())

    errors.extend(_validate_causal_component_registry(component_registry))
    errors.extend(_validate_event_design_status_against_components(component_registry, event_design_status))
    errors.extend(_validate_qra_benchmark_coverage_against_components(component_registry, benchmark_coverage))
    errors.extend(_validate_qra_benchmark_evidence_registry_against_components(component_registry, benchmark_evidence_registry))
    errors.extend(_validate_causal_claims_status_against_components(component_registry, causal_claims_status))

    if registry.empty or crosswalk.empty or shock_summary.empty or usability.empty:
        if registry.empty or component_registry.empty:
            return errors

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
        if csv_name.startswith("qra_") or csv_name in CLAIM_SCOPE_VALIDATED_NON_QRA_FILES:
            errors.extend(_validate_qra_publish_frame(csv_name, csv_frame))
        if csv_name in {"qra_event_shock_summary.csv", "qra_event_elasticity.csv", "qra_shock_crosswalk_v1.csv"}:
            _validate_shock_drift_alerts(csv_name, csv_frame, warnings=warnings)
        if csv_name.startswith("qra_event_"):
            _validate_overlap_excluded_for_file(csv_name, csv_frame, errors=errors, warnings=warnings)
        if csv_name.startswith("qra_") or csv_name in {"event_usability_table.csv", "event_design_status.csv", "causal_claims_status.csv"}:
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
        if csv_name.startswith("qra_") or csv_name in CLAIM_SCOPE_VALIDATED_NON_QRA_FILES:
            errors.extend(_validate_qra_publish_frame(csv_name, csv_frame))
        if csv_name in {"qra_event_shock_summary.csv", "qra_shock_crosswalk_v1.csv", "qra_event_elasticity.csv"}:
            _validate_shock_drift_alerts(csv_name, csv_frame, warnings=warnings)
        if csv_name.startswith("qra_event_"):
            _validate_overlap_excluded_for_file(csv_name, csv_frame, errors=errors, warnings=warnings)
        if csv_name.startswith("qra_") or csv_name in {"event_usability_table.csv", "event_design_status.csv", "causal_claims_status.csv"}:
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
        for dataset_name, series_ids in {
            "pricing": {
                "ati_baseline_bn",
                "stock_excess_bills_bn",
                "headline_public_duration_supply",
                "THREEFYTP10",
                "DGS10",
                "pricing_release_flow_panel",
                "pricing_release_flow_leave_one_out",
                "pricing_tau_sensitivity_grid",
            },
            "pricing_spec_registry": {"pricing_spec_registry"},
            "pricing_subsample_grid": {"pricing_subsample_grid"},
        }.items():
            match = series_catalog.loc[series_catalog.get("dataset", pd.Series(dtype=str)) == dataset_name]
            if match.empty:
                errors.append(f"series_metadata_missing_dataset:{dataset_name}")
                continue
            present = set(match.get("series_id", pd.Series(dtype=str)).astype(str))
            if not series_ids.issubset(present):
                errors.append(f"series_metadata_missing_series:{dataset_name}:{','.join(sorted(series_ids - present))}")

    if dataset_status_path.exists() and 'dataset_status' in locals():
        for dataset_name in (
            "pricing",
            "pricing_spec_registry",
            "pricing_subsample_grid",
            "pricing_scenario_translation",
            "pricing_release_flow_panel",
            "pricing_release_flow_leave_one_out",
            "pricing_tau_sensitivity_grid",
        ):
            match = dataset_status.loc[dataset_status.get("dataset", pd.Series(dtype=str)) == dataset_name]
            if match.empty:
                errors.append(f"dataset_status_missing:{dataset_name}")
                continue
            readiness = str(match.iloc[0].get("readiness_tier", "")).strip()
            if readiness not in {"supporting_provisional", "not_started"}:
                errors.append(f"dataset_status_invalid_pricing_readiness:{dataset_name}:{readiness}")

    readiness_exact, completion_exact = _validate_exact_coverages(readiness_frame, completion_frame)
    errors.extend(_validate_qra_publish_consistency(qra_frames))
    _validate_readme_exact_coverage_consistency(
        readme_path=readme_path,
        readiness_exact=readiness_exact,
        completion_exact=completion_exact,
        errors=errors,
        warnings=warnings,
    )
    _validate_readme_causal_claims_consistency(
        readme_path=readme_path,
        causal_claims_status=qra_frames.get("causal_claims_status.csv", pd.DataFrame()),
        errors=errors,
        warnings=warnings,
    )
    _validate_neutral_maturity_tilt_language(
        readme_path=readme_path,
        pricing_methods_path=ROOT / "docs" / "PRICING_METHODS.md",
        pricing_results_path=ROOT / "docs" / "PRICING_RESULTS_MEMO.md",
        errors=errors,
        warnings=warnings,
    )
    _validate_release_level_anchor_language(
        readme_path=readme_path,
        pricing_results_path=ROOT / "docs" / "PRICING_RESULTS_MEMO.md",
        errors=errors,
        warnings=warnings,
    )

    pricing_scenario_publish = publish_dir / "pricing_scenario_translation.csv"
    if pricing_scenario_publish.exists():
        try:
            pricing_scenarios = pd.read_csv(pricing_scenario_publish)
            if "scenario_role" not in pricing_scenarios.columns:
                errors.append("publish_artifact_missing_columns:pricing_scenario_translation.csv:scenario_role")
            else:
                stock_mask = pricing_scenarios.get("scenario_id", pd.Series(dtype=str)).astype(str).isin(
                    ["plus_500bn_term_out", "plus_1000bn_term_out"]
                )
                invalid_stock = pricing_scenarios.loc[stock_mask & (pricing_scenarios["scenario_role"].astype(str) != "illustrative_only")]
                if not invalid_stock.empty:
                    errors.append("pricing_stock_scenarios_not_illustrative_only")
        except Exception as exc:
            errors.append(f"publish_artifact_read_error:pricing_scenario_translation.csv:{exc}")

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
    causal_input_errors, causal_input_warnings = validate_manual_causal_review_inputs(MANUAL_DIR)
    errors.extend(causal_input_errors)
    warnings.extend(causal_input_warnings)

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
