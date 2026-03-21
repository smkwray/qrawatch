from __future__ import annotations

import pandas as pd


CURRENT_QUARTER_ACTION_VALUES = ("tightening", "easing", "hold", "mixed", "pending")
FORWARD_GUIDANCE_BIAS_VALUES = ("hawkish", "neutral", "dovish", "pending")
HEADLINE_BUCKET_VALUES = ("tightening", "easing", "control_hold", "exclude", "pending")
CLASSIFICATION_CONFIDENCE_VALUES = ("exact_statement", "table_diff", "hybrid", "heuristic", "pending")
CLASSIFICATION_REVIEW_STATUS_VALUES = ("reviewed", "provisional", "pending")
SHOCK_REVIEW_STATUS_VALUES = ("reviewed", "provisional", "pending")
SUMMARY_HEADLINE_BUCKETS = {"tightening", "easing", "control_hold"}

EVENT_CLASSIFICATION_COLUMNS = (
    "current_quarter_action",
    "forward_guidance_bias",
    "headline_bucket",
    "shock_sign_curated",
    "classification_confidence",
    "classification_review_status",
)
EVENT_CALENDAR_CONTEXT_COLUMNS = (
    "quarter",
    "financing_estimates_release_date",
    "policy_statement_release_date",
    "financing_estimates_url",
    "policy_statement_url",
    "timing_quality",
)


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return " ".join(str(value).split()).strip()


def normalize_lower(value: object) -> str:
    return normalize_text(value).lower()


def is_summary_headline_bucket(series: pd.Series) -> pd.Series:
    normalized = series.fillna("").astype(str).str.strip().str.lower()
    return normalized.isin(SUMMARY_HEADLINE_BUCKETS)


def derive_legacy_expected_direction(row: dict[str, object] | pd.Series) -> str:
    action = normalize_lower(row.get("current_quarter_action"))
    guidance = normalize_lower(row.get("forward_guidance_bias"))
    bucket = normalize_lower(row.get("headline_bucket"))

    if bucket in {"tightening", "easing"}:
        return bucket
    if bucket == "control_hold":
        return "hold"
    if bucket == "exclude" and action == "hold" and guidance == "hawkish":
        return "hold_hawkish_guidance"
    if action == "mixed":
        return "mixed"
    return "classification_pending"


def coerce_shock_sign(value: object) -> float | None:
    normalized = normalize_text(value)
    if not normalized:
        return None
    try:
        numeric = float(normalized)
    except ValueError:
        return None
    if numeric > 0:
        return 1.0
    if numeric < 0:
        return -1.0
    return 0.0
