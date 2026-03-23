from __future__ import annotations

from collections.abc import Iterable
import math
import re

import numpy as np
import pandas as pd

from ati_shadow_policy.research.qra_elasticity import (
    build_event_usability_table as build_event_usability_table_from_ledger,
    build_qra_review_ledger,
    build_qra_shock_crosswalk_v1 as build_qra_shock_crosswalk_from_ledger,
)
from ati_shadow_policy.specs import (
    SPEC_DURATION_TREATMENT_V1,
    SPEC_QRA_EVENT_V2,
    TREATMENT_VARIANTS,
)


_RELEASE_DATE_COLUMNS = ("official_release_date", "policy_statement_release_date", "event_date_requested")
_QUALITY_TIER_ORDER = {"Tier A": 0, "Tier B": 1, "Tier C": 2, "Tier D": 3}
_COMPONENT_REGISTRY_COLUMNS = [
    "release_component_id",
    "event_id",
    "quarter",
    "component_type",
    "release_timestamp_et",
    "timestamp_precision",
    "release_timestamp_source_method",
    "timestamp_evidence_url",
    "timestamp_evidence_note",
    "release_timezone_asserted",
    "bundle_decomposition_evidence",
    "source_url",
    "bundle_id",
    "release_sequence_label",
    "separable_component_flag",
    "review_status",
    "review_notes",
    "benchmark_timestamp_et",
    "benchmark_source",
    "benchmark_source_family",
    "benchmark_document_url",
    "benchmark_document_local",
    "benchmark_release_timestamp_et",
    "benchmark_release_timestamp_precision",
    "benchmark_timestamp_source_method",
    "benchmark_pre_release_verified_flag",
    "benchmark_observed_before_component_flag",
    "benchmark_timing_status",
    "external_benchmark_ready",
    "expected_composition_bn",
    "realized_composition_bn",
    "composition_surprise_bn",
    "surprise_construction_method",
    "surprise_units",
    "benchmark_stale_flag",
    "expectation_review_status",
    "expectation_notes",
    "expectation_status",
    "contamination_flag",
    "contamination_status",
    "contamination_review_status",
    "contamination_label",
    "contamination_window_start_et",
    "contamination_window_end_et",
    "confound_release_type",
    "confound_release_timestamp_et",
    "decision_rule",
    "exclude_from_causal_pool",
    "decision_confidence",
    "contamination_notes",
    "separability_status",
    "eligibility_blockers",
    "quality_tier",
    "causal_eligible",
]


def _first_non_empty(row: pd.Series, columns: Iterable[str]) -> object:
    for column in columns:
        if column not in row.index:
            continue
        value = row[column]
        if pd.isna(value):
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return pd.NA


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return " ".join(str(value).split()).strip()


def _normalize_lower(value: object) -> str:
    return _normalize_text(value).lower()


def _normalize_overlap_severity(value: object) -> str:
    return _normalize_lower(value)


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except TypeError:
        pass
    return _normalize_text(value) == ""


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _coerce_float(value: object) -> float | None:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None
    return float(numeric)


def _coerce_timestamp_et(value: object) -> object:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NA
    if ts.tzinfo is None:
        ts = ts.tz_localize("America/New_York")
    else:
        ts = ts.tz_convert("America/New_York")
    return ts.isoformat()


def _overlap_severity(flag: object, severity: object, label: object, note: object) -> str:
    manual = _normalize_overlap_severity(severity)
    if manual:
        return manual

    if not _coerce_bool(flag):
        return "none"
    combined = " ".join(
        part for part in (_normalize_lower(label), _normalize_lower(note)) if part
    )
    if "fomc" in combined or "cpi" in combined or "jobs" in combined:
        return "high"
    return "flagged"


def _usable_for_headline_reason(row: pd.Series) -> str:
    blockers: list[str] = []
    if _normalize_lower(row.get("event_date_type")) != "official_release_date":
        blockers.append("non_official_date_type")
    if bool(row.get("shock_missing_flag", False)):
        blockers.append("missing_shock")
    if bool(row.get("small_denominator_flag", False)):
        blockers.append("small_denominator")
    if _normalize_lower(row.get("headline_bucket")) not in {"tightening", "easing", "control_hold"}:
        blockers.append("non_headline_bucket")
    if _normalize_lower(row.get("classification_review_status")) != "reviewed":
        blockers.append("classification_not_reviewed")
    if _normalize_lower(row.get("shock_review_status")) != "reviewed":
        blockers.append("shock_not_reviewed")
    if blockers:
        return "|".join(blockers)
    return "eligible_headline_reviewed_official_release"


def _reviewer_from_status(value: object) -> str:
    status = _normalize_lower(value)
    if status == "reviewed":
        return "manual_review"
    if status == "provisional":
        return "provisional_review"
    return ""


def _review_date_from_text(value: object) -> object:
    text = _normalize_text(value)
    match = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", text)
    if not match:
        return pd.NA
    return match.group(1)


def _alternative_treatment_status(row: pd.Series) -> tuple[bool, str, str]:
    fields = (
        "schedule_diff_10y_eq_bn",
        "schedule_diff_dynamic_10y_eq_bn",
        "schedule_diff_dv01_usd",
        "gross_notional_delta_bn",
    )
    missing_fields = [
        field
        for field in fields
        if pd.isna(pd.to_numeric(pd.Series([row.get(field)]), errors="coerce").iloc[0])
    ]
    if not missing_fields:
        return True, "", ""

    review_status = _normalize_lower(row.get("shock_review_status"))
    shock_source = _normalize_lower(row.get("shock_source"))
    construction = _normalize_lower(row.get("shock_construction"))
    if "manual_statement_review" in shock_source:
        reason = "manual_statement_primary_only_pending_alt_treatments"
    elif "manual_schedule_diff" in shock_source:
        reason = "manual_schedule_diff_missing_alternative_fields"
    elif construction.startswith("manual_override"):
        reason = "manual_override_missing_alternative_fields"
    elif review_status == "reviewed":
        reason = "reviewed_event_missing_alternative_treatment_fields"
    else:
        reason = "alternative_treatment_fields_not_populated"
    return False, "|".join(missing_fields), reason


def _timestamp_has_clock(value: object) -> bool:
    return bool(re.search(r"\b\d{1,2}:\d{2}(?::\d{2})?\b", _normalize_text(value)))


def _timestamp_is_midnight_only(value: object) -> bool:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return False
    return bool(ts.hour == 0 and ts.minute == 0 and ts.second == 0)


def _coerce_timestamp_kind(value: object) -> str:
    text = _normalize_text(value)
    if not text:
        return "missing"
    ts = pd.to_datetime(text, errors="coerce")
    if pd.isna(ts):
        return "missing"
    if _timestamp_has_clock(text):
        if _timestamp_is_midnight_only(text):
            return "date_only"
        return "timestamp_with_time"
    return "date_only"


def _coerce_timestamp_et_ts(value: object) -> pd.Timestamp | pd.NaT:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tzinfo is None:
        return ts.tz_localize("America/New_York")
    return ts.tz_convert("America/New_York")


def _coerce_timestamp_components(value: object) -> tuple[object, str, object, object, object]:
    ts = _coerce_timestamp_et_ts(value)
    if pd.isna(ts):
        return pd.NA, "missing", pd.NA, pd.NA, pd.NA
    text = _normalize_text(value)
    kind = _coerce_timestamp_kind(text)
    date_value = ts.strftime("%Y-%m-%d")
    time_value: object = pd.NA
    if kind == "timestamp_with_time":
        time_value = ts.strftime("%H:%M:%S")
    return ts.isoformat(), kind, date_value, time_value, "America/New_York"


def _release_sequence_label(
    financing_ts: pd.Timestamp | pd.NaT,
    policy_ts: pd.Timestamp | pd.NaT,
) -> str:
    has_financing = pd.notna(financing_ts)
    has_policy = pd.notna(policy_ts)
    if has_financing and has_policy:
        if financing_ts < policy_ts:
            return "financing_then_policy"
        if financing_ts > policy_ts:
            return "policy_then_financing"
        return "simultaneous_release"
    if has_financing:
        return "financing_only"
    if has_policy:
        return "policy_only"
    return "unknown"


def _headline_eligibility_blockers(row: pd.Series) -> str:
    blockers: list[str] = []
    if not _coerce_bool(row.get("headline_check_official_release")):
        blockers.append("non_official_date_type")
    if not _coerce_bool(row.get("headline_check_bucket")):
        blockers.append("non_headline_bucket")
    if not _coerce_bool(row.get("headline_check_classification_reviewed")):
        blockers.append("classification_not_reviewed")
    if not _coerce_bool(row.get("headline_check_shock_reviewed")):
        blockers.append("shock_not_reviewed")
    if not _coerce_bool(row.get("headline_check_non_missing_shock")):
        blockers.append("missing_shock")
    if not _coerce_bool(row.get("headline_check_non_small_denominator")):
        blockers.append("small_denominator")
    return "|".join(blockers)


def _timestamp_precision_from_kind(value: object) -> str:
    text = _normalize_lower(value)
    if "timestamp_with_time" in text:
        return "exact_time"
    if "date_only" in text:
        return "date_only"
    if text in {"", "missing"}:
        return "missing"
    return "date_only"


def _quarter_sort_key(value: object) -> tuple[int, int]:
    text = _normalize_text(value)
    match = re.match(r"^(\d{4})Q([1-4])$", text)
    if not match:
        return (0, 0)
    return (int(match.group(1)), int(match.group(2)))


def _is_current_sample_financing_row(row: pd.Series) -> bool:
    return _normalize_lower(row.get("component_type")) == "financing_estimates" and _quarter_sort_key(
        row.get("quarter")
    ) >= (2022, 3)


def _current_sample_financing_components(component_registry: pd.DataFrame) -> pd.DataFrame:
    if component_registry.empty:
        return component_registry.copy()
    return component_registry.loc[
        component_registry.apply(_is_current_sample_financing_row, axis=1)
    ].copy()


def _has_timestamp_evidence(row: pd.Series) -> bool:
    source_method = _normalize_text(row.get("release_timestamp_source_method"))
    evidence_url = _normalize_text(row.get("timestamp_evidence_url"))
    evidence_note = _normalize_text(row.get("timestamp_evidence_note"))
    return bool(source_method and (evidence_url or evidence_note))


def _is_external_benchmark_family(value: object) -> bool:
    family = _normalize_lower(value)
    return bool(family and family not in {"treasury_release", "same_release"})


def _benchmark_release_timestamp(row: pd.Series) -> pd.Timestamp | pd.NaT:
    explicit = _coerce_timestamp_et_ts(row.get("benchmark_release_timestamp_et"))
    if pd.notna(explicit):
        return explicit
    return _coerce_timestamp_et_ts(row.get("benchmark_timestamp_et"))


def _benchmark_pre_release_verified(row: pd.Series) -> bool:
    return _coerce_bool(row.get("benchmark_pre_release_verified_flag"))


def _benchmark_observed_before_component(row: pd.Series) -> bool:
    return _coerce_bool(row.get("benchmark_observed_before_component_flag"))


def _component_release_component_id(event_id: object, component_type: object) -> str:
    return f"{_normalize_text(event_id)}__{_normalize_text(component_type)}"


def _component_default_rows(event_registry: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for _, row in event_registry.iterrows():
        event_id = _normalize_text(row.get("event_id"))
        quarter = row.get("quarter", pd.NA)
        release_sequence_label = row.get("release_sequence_label", pd.NA)
        bundle_id = event_id
        review_status = _normalize_lower(row.get("review_status") or row.get("classification_review_status"))
        review_notes = row.get("review_notes", row.get("notes", pd.NA))
        same_day_bundle = _coerce_bool(row.get("same_day_release_bundle_flag"))
        multi_stage = _coerce_bool(row.get("multi_stage_release_flag"))

        component_specs = [
            (
                "financing_estimates",
                row.get("financing_estimates_release_timestamp_et", pd.NA),
                row.get("financing_estimates_release_timestamp_kind", pd.NA),
                row.get("financing_estimates_url", pd.NA),
            ),
            (
                "policy_statement",
                row.get("policy_statement_release_timestamp_et", pd.NA),
                row.get("policy_statement_release_timestamp_kind", pd.NA),
                row.get("policy_statement_url", pd.NA),
            ),
        ]
        emitted = False
        for component_type, timestamp_et, timestamp_kind, source_url in component_specs:
            if _is_missing(timestamp_et) and _is_missing(source_url):
                continue
            emitted = True
            separable_default = bool(multi_stage and not same_day_bundle)
            rows.append(
                {
                    "release_component_id": _component_release_component_id(event_id, component_type),
                    "event_id": event_id,
                    "quarter": quarter,
                    "component_type": component_type,
                    "release_timestamp_et": timestamp_et,
                    "timestamp_precision": _timestamp_precision_from_kind(timestamp_kind),
                    "release_timestamp_source_method": pd.NA,
                    "timestamp_evidence_url": pd.NA,
                    "timestamp_evidence_note": pd.NA,
                    "release_timezone_asserted": pd.NA,
                    "bundle_decomposition_evidence": pd.NA,
                    "source_url": source_url,
                    "bundle_id": bundle_id,
                    "release_sequence_label": release_sequence_label,
                    "separable_component_flag": separable_default,
                    "review_status": review_status or "pending",
                    "review_notes": review_notes,
                }
            )
        if emitted:
            continue
        rows.append(
            {
                "release_component_id": _component_release_component_id(event_id, "event_composite"),
                "event_id": event_id,
                "quarter": quarter,
                "component_type": "event_composite",
                "release_timestamp_et": row.get("release_timestamp_et", pd.NA),
                "timestamp_precision": _timestamp_precision_from_kind(row.get("release_timestamp_kind", pd.NA)),
                "release_timestamp_source_method": pd.NA,
                "timestamp_evidence_url": pd.NA,
                "timestamp_evidence_note": pd.NA,
                "release_timezone_asserted": pd.NA,
                "bundle_decomposition_evidence": pd.NA,
                "source_url": _first_non_empty(
                    row,
                    ("policy_statement_url", "financing_estimates_url"),
                ),
                "bundle_id": bundle_id,
                "release_sequence_label": release_sequence_label,
                "separable_component_flag": False,
                "review_status": review_status or "pending",
                "review_notes": review_notes,
            }
        )
    return pd.DataFrame(rows)


def _prepare_component_overrides(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    out = frame.copy()
    if "release_component_id" not in out.columns:
        if {"event_id", "component_type"}.issubset(out.columns):
            out["release_component_id"] = out.apply(
                lambda row: _component_release_component_id(row.get("event_id"), row.get("component_type")),
                axis=1,
            )
        else:
            return pd.DataFrame()
    out["release_component_id"] = out["release_component_id"].map(_normalize_text)
    return out.drop_duplicates(subset=["release_component_id"], keep="last").reset_index(drop=True)


def _merge_component_overrides(
    derived: pd.DataFrame,
    overrides: pd.DataFrame | None,
) -> pd.DataFrame:
    if overrides is None or overrides.empty:
        return derived
    prepared = _prepare_component_overrides(overrides)
    if prepared.empty:
        return derived
    merged = derived.merge(prepared, on="release_component_id", how="left", suffixes=("", "_override"))
    for column in prepared.columns:
        if column == "release_component_id":
            continue
        override_column = f"{column}_override"
        if override_column not in merged.columns:
            continue
        if column not in merged.columns:
            merged[column] = merged[override_column]
        else:
            merged[column] = merged[override_column].where(~merged[override_column].isna(), merged[column])
        merged = merged.drop(columns=[override_column])
    return merged


def _merge_component_expectations(
    components: pd.DataFrame,
    expectation_template: pd.DataFrame | None,
) -> pd.DataFrame:
    defaults = {
        "benchmark_timestamp_et": pd.NA,
        "benchmark_source": pd.NA,
        "benchmark_source_family": pd.NA,
        "benchmark_document_url": pd.NA,
        "benchmark_document_local": pd.NA,
        "benchmark_release_timestamp_et": pd.NA,
        "benchmark_release_timestamp_precision": pd.NA,
        "benchmark_timestamp_source_method": pd.NA,
        "benchmark_pre_release_verified_flag": False,
        "benchmark_observed_before_component_flag": False,
        "benchmark_timing_status": "same_release_placeholder",
        "external_benchmark_ready": False,
        "expected_composition_bn": pd.NA,
        "realized_composition_bn": pd.NA,
        "composition_surprise_bn": pd.NA,
        "surprise_construction_method": pd.NA,
        "surprise_units": pd.NA,
        "benchmark_stale_flag": False,
        "expectation_review_status": "pending",
        "expectation_notes": pd.NA,
    }
    out = components.copy()
    for column, default in defaults.items():
        if column not in out.columns:
            out[column] = default
    if expectation_template is None or expectation_template.empty:
        return out
    prepared = _prepare_component_overrides(expectation_template)
    if prepared.empty:
        return out
    keep = [
        column
        for column in (
            "release_component_id",
            "benchmark_timestamp_et",
            "benchmark_source",
            "benchmark_source_family",
            "benchmark_document_url",
            "benchmark_document_local",
            "benchmark_release_timestamp_et",
            "benchmark_release_timestamp_precision",
            "benchmark_timestamp_source_method",
            "benchmark_pre_release_verified_flag",
            "benchmark_observed_before_component_flag",
            "benchmark_timing_status",
            "external_benchmark_ready",
            "expected_composition_bn",
            "realized_composition_bn",
            "composition_surprise_bn",
            "surprise_construction_method",
            "surprise_units",
            "benchmark_stale_flag",
            "expectation_review_status",
            "expectation_notes",
        )
        if column in prepared.columns
    ]
    merged = out.merge(prepared[keep], on="release_component_id", how="left", suffixes=("", "_expectation"))
    for column in keep:
        if column == "release_component_id":
            continue
        override_column = f"{column}_expectation"
        if override_column not in merged.columns:
            continue
        merged[column] = merged[override_column].where(~merged[override_column].isna(), merged[column])
        merged = merged.drop(columns=[override_column])
    return merged


def _merge_component_contamination(
    components: pd.DataFrame,
    contamination_reviews: pd.DataFrame | None,
) -> pd.DataFrame:
    defaults = {
        "contamination_flag": False,
        "contamination_status": "pending_review",
        "contamination_review_status": "pending",
        "contamination_label": pd.NA,
        "contamination_window_start_et": pd.NA,
        "contamination_window_end_et": pd.NA,
        "confound_release_type": pd.NA,
        "confound_release_timestamp_et": pd.NA,
        "decision_rule": pd.NA,
        "exclude_from_causal_pool": pd.NA,
        "decision_confidence": pd.NA,
        "contamination_notes": pd.NA,
    }
    out = components.copy()
    for column, default in defaults.items():
        if column not in out.columns:
            out[column] = default
    if contamination_reviews is None or contamination_reviews.empty:
        return out
    prepared = _prepare_component_overrides(contamination_reviews)
    if prepared.empty:
        return out
    keep = [
        column
        for column in (
            "release_component_id",
            "contamination_flag",
            "contamination_status",
            "contamination_review_status",
            "contamination_label",
            "contamination_window_start_et",
            "contamination_window_end_et",
            "confound_release_type",
            "confound_release_timestamp_et",
            "decision_rule",
            "exclude_from_causal_pool",
            "decision_confidence",
            "contamination_notes",
        )
        if column in prepared.columns
    ]
    merged = out.merge(prepared[keep], on="release_component_id", how="left", suffixes=("", "_contamination"))
    for column in keep:
        if column == "release_component_id":
            continue
        override_column = f"{column}_contamination"
        if override_column not in merged.columns:
            continue
        merged[column] = merged[override_column].where(~merged[override_column].isna(), merged[column])
        merged = merged.drop(columns=[override_column])
    return merged


def _component_separability_status(row: pd.Series) -> str:
    component_type = _normalize_lower(row.get("component_type"))
    if component_type == "event_composite":
        return "event_composite"
    if _coerce_bool(row.get("separable_component_flag")):
        return "separable_component"
    return "same_day_inseparable_bundle"


def _benchmark_source_family_from_text(value: object) -> str:
    text = _normalize_lower(value)
    if not text:
        return ""
    if "dealer" in text and "survey" in text:
        return "primary_dealer_auction_size_survey"
    if "tbac" in text and ("recommended" in text or "financing" in text):
        return "tbac_recommended_financing_tables"
    if "sources" in text and "uses" in text:
        return "sources_and_uses"
    if "treasury.gov" in text or "home.treasury.gov" in text or "quarterly-refunding" in text:
        return "treasury_release"
    return "other_external"


def _benchmark_source_family(row: pd.Series) -> str:
    manual = _normalize_lower(row.get("benchmark_source_family"))
    if manual:
        return manual
    return _benchmark_source_family_from_text(row.get("benchmark_source"))


def _benchmark_timing_status(row: pd.Series) -> str:
    manual = _normalize_lower(row.get("benchmark_timing_status"))
    benchmark_source_family = _benchmark_source_family(row)
    benchmark_source = _normalize_lower(row.get("benchmark_source"))
    release_source = _normalize_lower(row.get("source_url"))
    benchmark_ts = _benchmark_release_timestamp(row)
    release_ts = _coerce_timestamp_et_ts(row.get("release_timestamp_et"))
    benchmark_same_source = bool(benchmark_source and release_source and benchmark_source == release_source)
    external_family = _is_external_benchmark_family(benchmark_source_family)
    pre_release_verified = _benchmark_pre_release_verified(row)
    observed_before = _benchmark_observed_before_component(row)

    if manual and manual != "pre_release_external":
        return manual
    if manual == "pre_release_external":
        if external_family and pre_release_verified and observed_before:
            if pd.notna(benchmark_ts) and pd.notna(release_ts):
                if benchmark_ts >= release_ts:
                    return "post_release_invalid"
                return "pre_release_external"
        return "external_timing_unverified" if external_family else "same_release_placeholder"

    if (
        benchmark_source_family == "treasury_release"
        or benchmark_same_source
        or (pd.isna(benchmark_ts) and not benchmark_source and benchmark_source_family == "")
    ):
        return "same_release_placeholder"

    if external_family and pd.notna(benchmark_ts) and pd.notna(release_ts) and benchmark_ts >= release_ts:
        if benchmark_ts == release_ts:
            return "same_release_placeholder"
        return "post_release_invalid"

    if external_family and pre_release_verified and observed_before and pd.notna(benchmark_ts) and pd.notna(release_ts):
        return "pre_release_external"

    if external_family:
        return "external_timing_unverified"
    return "same_release_placeholder"


def _external_benchmark_ready(row: pd.Series) -> bool:
    if _coerce_bool(row.get("benchmark_stale_flag")):
        return False
    if _normalize_lower(row.get("expectation_review_status")) != "reviewed":
        return False
    if _coerce_float(row.get("composition_surprise_bn")) is None:
        return False
    if not _is_external_benchmark_family(_benchmark_source_family(row)):
        return False
    if not _benchmark_pre_release_verified(row):
        return False
    if not _benchmark_observed_before_component(row):
        return False
    return _benchmark_timing_status(row) == "pre_release_external"


def _component_expectation_status(row: pd.Series) -> str:
    review_status = _normalize_lower(row.get("expectation_review_status"))
    surprise = _coerce_float(row.get("composition_surprise_bn"))
    benchmark_timing_status = _benchmark_timing_status(row)
    if _coerce_bool(row.get("benchmark_stale_flag")):
        return "benchmark_stale"
    if surprise is None:
        return "missing_benchmark"
    if benchmark_timing_status == "post_release_invalid":
        return "post_release_invalid"
    if benchmark_timing_status == "same_release_placeholder":
        return "same_release_placeholder"
    if benchmark_timing_status == "external_timing_unverified":
        return "benchmark_timing_unverified"
    if review_status != "reviewed":
        return "unreviewed_surprise"
    if not _external_benchmark_ready(row):
        return "benchmark_verification_incomplete"
    return "reviewed_surprise_ready"


def _component_contamination_status(row: pd.Series) -> str:
    review_status = _normalize_lower(row.get("contamination_review_status"))
    status = _normalize_lower(row.get("contamination_status"))
    flagged = _coerce_bool(row.get("contamination_flag"))
    exclude_flag = _coerce_bool(row.get("exclude_from_causal_pool"))
    if review_status != "reviewed":
        return "pending_review"
    if status in {"reviewed_clean", "reviewed_contaminated_exclude", "reviewed_contaminated_context_only"}:
        return status
    if status in {"reviewed_contaminated", "reviewed_contaminated_flagged", "contaminated", "exclude", "excluded"}:
        return "reviewed_contaminated_exclude"
    if status in {"context_only", "reviewed_context_only", "reviewed_contaminated_context"}:
        return "reviewed_contaminated_context_only"
    if exclude_flag or flagged:
        return "reviewed_contaminated_exclude"
    return "reviewed_clean"


def _component_eligibility_blockers(row: pd.Series) -> str:
    blockers: list[str] = []
    component_type = _normalize_lower(row.get("component_type"))
    if _normalize_lower(row.get("review_status")) != "reviewed":
        blockers.append("review_not_complete")
    if component_type == "policy_statement":
        blockers.append("policy_statement_descriptive_only")
    elif component_type != "financing_estimates":
        blockers.append("component_type_not_in_causal_scope")
    if _normalize_lower(row.get("timestamp_precision")) != "exact_time":
        blockers.append("missing_exact_timestamp")
    if (
        _normalize_lower(row.get("timestamp_precision")) == "exact_time"
        and _normalize_lower(row.get("review_status")) == "reviewed"
        and _is_current_sample_financing_row(row)
        and not _has_timestamp_evidence(row)
    ):
        blockers.append("missing_exact_timestamp_evidence")
    if _component_separability_status(row) != "separable_component":
        blockers.append("inseparable_bundle")
    expectation_status = _component_expectation_status(row)
    if expectation_status != "reviewed_surprise_ready":
        if expectation_status == "benchmark_stale":
            blockers.append("benchmark_stale")
        elif expectation_status == "unreviewed_surprise":
            blockers.append("surprise_not_reviewed")
        elif expectation_status == "same_release_placeholder":
            blockers.append("same_release_placeholder_benchmark")
        elif expectation_status == "post_release_invalid":
            blockers.append("post_release_benchmark_invalid")
        elif expectation_status == "benchmark_timing_unverified":
            blockers.append("missing_pre_release_external_benchmark")
        elif expectation_status == "benchmark_verification_incomplete":
            blockers.append("benchmark_verification_incomplete")
        else:
            blockers.append("missing_expectation_benchmark")
    contamination_status = _component_contamination_status(row)
    if contamination_status != "reviewed_clean":
        if contamination_status == "pending_review":
            blockers.append("contamination_not_reviewed")
        elif contamination_status == "reviewed_contaminated_context_only":
            blockers.append("contamination_context_only")
        else:
            blockers.append("contamination_flagged")
    if _is_missing(row.get("source_url")):
        blockers.append("missing_source_url")
    return "|".join(blockers)


def _component_quality_tier(row: pd.Series) -> str:
    blockers = _normalize_text(row.get("eligibility_blockers"))
    review_status = _normalize_lower(row.get("review_status"))
    if not blockers:
        return "Tier A"
    if review_status == "reviewed":
        return "Tier B"
    if not _is_missing(row.get("source_url")):
        return "Tier C"
    return "Tier D"


def build_qra_release_component_registry(
    event_registry: pd.DataFrame,
    release_components: pd.DataFrame | None = None,
    expectation_template: pd.DataFrame | None = None,
    contamination_reviews: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if event_registry.empty:
        return pd.DataFrame(columns=_COMPONENT_REGISTRY_COLUMNS)
    components = _component_default_rows(event_registry)
    components = _merge_component_overrides(components, release_components)
    components = _merge_component_expectations(components, expectation_template)
    components = _merge_component_contamination(components, contamination_reviews)

    components["timestamp_precision"] = components["timestamp_precision"].map(_normalize_text)
    components["separable_component_flag"] = components["separable_component_flag"].map(_coerce_bool)
    components["benchmark_stale_flag"] = components["benchmark_stale_flag"].map(_coerce_bool)
    components["external_benchmark_ready"] = components["external_benchmark_ready"].map(_coerce_bool)
    components["benchmark_pre_release_verified_flag"] = components["benchmark_pre_release_verified_flag"].map(_coerce_bool)
    components["benchmark_observed_before_component_flag"] = components["benchmark_observed_before_component_flag"].map(_coerce_bool)
    components["contamination_flag"] = components["contamination_flag"].map(_coerce_bool)
    components["benchmark_source_family"] = components.apply(_benchmark_source_family, axis=1)
    components["benchmark_timing_status"] = components.apply(_benchmark_timing_status, axis=1)
    components["external_benchmark_ready"] = components.apply(_external_benchmark_ready, axis=1)
    components["separability_status"] = components.apply(_component_separability_status, axis=1)
    components["expectation_status"] = components.apply(_component_expectation_status, axis=1)
    components["contamination_status"] = components.apply(_component_contamination_status, axis=1)
    components["eligibility_blockers"] = components.apply(_component_eligibility_blockers, axis=1)
    components["quality_tier"] = components.apply(_component_quality_tier, axis=1)
    components["causal_eligible"] = components["quality_tier"].eq("Tier A")

    for column in _COMPONENT_REGISTRY_COLUMNS:
        if column not in components.columns:
            components[column] = pd.NA
    return components[_COMPONENT_REGISTRY_COLUMNS].sort_values(
        ["quarter", "event_id", "release_component_id"],
        kind="stable",
    ).reset_index(drop=True)


def summarize_qra_causal_qa(component_registry: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "event_id",
        "quality_tier",
        "eligibility_blockers",
        "timestamp_precision",
        "separability_status",
        "expectation_status",
        "contamination_status",
        "release_component_count",
        "causal_eligible_component_count",
        "event_release_component_id",
        "event_release_component_type",
        "event_release_timestamp_et",
        "event_release_timestamp_kind",
        "event_release_timestamp_source",
    ]
    if component_registry.empty:
        return pd.DataFrame(columns=columns)
    rows: list[dict[str, object]] = []
    for event_id, group in component_registry.groupby("event_id", sort=False, dropna=False):
        ordered = group.copy()
        ordered["_quality_order"] = ordered["quality_tier"].map(
            lambda value: _QUALITY_TIER_ORDER.get(str(value), 99)
        )
        ordered["_release_sort_ts"] = pd.to_datetime(
            ordered.get("release_timestamp_et", pd.Series(dtype=object)),
            errors="coerce",
        )
        ordered = ordered.sort_values(
            by=["_quality_order", "_release_sort_ts", "release_component_id"],
            kind="stable",
        ).reset_index(drop=True)
        best = ordered.iloc[0]
        blockers: list[str] = []
        for value in group.get("eligibility_blockers", pd.Series(dtype=object)).fillna(""):
            for item in str(value).split("|"):
                cleaned = item.strip()
                if cleaned and cleaned not in blockers:
                    blockers.append(cleaned)
        best_precision = _normalize_text(best.get("timestamp_precision"))
        if best_precision == "exact_time":
            best_kind = "release_component_registry_timestamp_with_time"
        elif best_precision == "date_only":
            best_kind = "release_component_registry_date_only"
        else:
            best_kind = "missing"
        rows.append(
            {
                "event_id": event_id,
                "quality_tier": best.get("quality_tier", "Tier D"),
                "eligibility_blockers": "|".join(blockers),
                "timestamp_precision": best.get("timestamp_precision", pd.NA),
                "separability_status": best.get("separability_status", pd.NA),
                "expectation_status": best.get("expectation_status", pd.NA),
                "contamination_status": best.get("contamination_status", pd.NA),
                "release_component_count": int(group["release_component_id"].nunique()),
                "causal_eligible_component_count": int(group["causal_eligible"].fillna(False).astype(bool).sum()),
                "event_release_component_id": best.get("release_component_id", pd.NA),
                "event_release_component_type": best.get("component_type", pd.NA),
                "event_release_timestamp_et": best.get("release_timestamp_et", pd.NA),
                "event_release_timestamp_kind": best_kind,
                "event_release_timestamp_source": (
                    "release_component_registry"
                    if _normalize_text(best.get("release_timestamp_et"))
                    else pd.NA
                ),
            }
        )
    return pd.DataFrame(rows)


def build_event_design_status(component_registry: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "metric",
        "value",
        "notes",
    ]
    if component_registry.empty:
        return pd.DataFrame(columns=columns)
    counts = component_registry["quality_tier"].value_counts(dropna=False).to_dict()
    reviewed_clean = int(component_registry["contamination_status"].astype(str).eq("reviewed_clean").sum())
    exact_time = int(component_registry["timestamp_precision"].astype(str).eq("exact_time").sum())
    surprise_ready = int(component_registry["expectation_status"].astype(str).eq("reviewed_surprise_ready").sum())
    current_sample_financing = _current_sample_financing_components(component_registry)
    rows = [
        {"metric": "release_component_count", "value": int(len(component_registry)), "notes": "Total release components in the current registry."},
        {"metric": "tier_a_count", "value": int(counts.get("Tier A", 0)), "notes": "Causal-eligible components."},
        {"metric": "tier_b_count", "value": int(counts.get("Tier B", 0)), "notes": "Reviewed descriptive-only components."},
        {"metric": "tier_c_count", "value": int(counts.get("Tier C", 0)), "notes": "Official components missing reviewed causal gates."},
        {"metric": "tier_d_count", "value": int(counts.get("Tier D", 0)), "notes": "Provisional or scaffold components."},
        {"metric": "exact_time_component_count", "value": exact_time, "notes": "Components with exact release timestamps."},
        {"metric": "reviewed_surprise_ready_count", "value": surprise_ready, "notes": "Components with reviewed expectation/surprise inputs."},
        {"metric": "reviewed_clean_component_count", "value": reviewed_clean, "notes": "Components marked reviewed_clean for contamination."},
        {
            "metric": "current_sample_financing_component_count",
            "value": int(len(current_sample_financing)),
            "notes": "Current-sample financing-estimates release components (`2022Q3+`).",
        },
        {
            "metric": "current_sample_financing_exact_time_count",
            "value": int(current_sample_financing.get("timestamp_precision", pd.Series(dtype=object)).astype(str).eq("exact_time").sum()),
            "notes": "Current-sample financing components with exact-time timestamps.",
        },
        {
            "metric": "current_sample_financing_reviewed_clean_count",
            "value": int(current_sample_financing.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("reviewed_clean").sum()),
            "notes": "Current-sample financing components with reviewed clean contamination decisions.",
        },
        {
            "metric": "current_sample_financing_pre_release_external_count",
            "value": int(current_sample_financing.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("pre_release_external").sum()),
            "notes": "Current-sample financing components with verified pre-release external benchmarks.",
        },
        {
            "metric": "current_sample_financing_reviewed_surprise_ready_count",
            "value": int(current_sample_financing.get("expectation_status", pd.Series(dtype=object)).astype(str).eq("reviewed_surprise_ready").sum()),
            "notes": "Current-sample financing components with reviewed surprise inputs ready for causal use.",
        },
        {
            "metric": "current_sample_financing_post_release_invalid_count",
            "value": int(current_sample_financing.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("post_release_invalid").sum()),
            "notes": "Current-sample financing components blocked by post-release benchmark timing.",
        },
        {
            "metric": "current_sample_financing_external_timing_unverified_count",
            "value": int(current_sample_financing.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("external_timing_unverified").sum()),
            "notes": "Current-sample financing components with external benchmarks that still lack timing verification.",
        },
        {
            "metric": "current_sample_financing_same_release_placeholder_count",
            "value": int(current_sample_financing.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("same_release_placeholder").sum()),
            "notes": "Current-sample financing components still relying on same-release placeholder benchmark semantics.",
        },
        {
            "metric": "current_sample_financing_pending_contamination_review_count",
            "value": int(current_sample_financing.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("pending_review").sum()),
            "notes": "Current-sample financing components still awaiting contamination review.",
        },
        {
            "metric": "current_sample_financing_reviewed_contaminated_context_only_count",
            "value": int(current_sample_financing.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("reviewed_contaminated_context_only").sum()),
            "notes": "Current-sample financing components retained as context-only because contamination was reviewed but not clean.",
        },
        {
            "metric": "current_sample_financing_reviewed_contaminated_exclude_count",
            "value": int(current_sample_financing.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("reviewed_contaminated_exclude").sum()),
            "notes": "Current-sample financing components excluded from the causal pool after reviewed contamination.",
        },
        {
            "metric": "current_sample_financing_tier_a_count",
            "value": int(current_sample_financing.get("quality_tier", pd.Series(dtype=object)).astype(str).eq("Tier A").sum()),
            "notes": "Current-sample financing components currently passing all causal gates (Tier A).",
        },
    ]
    return pd.DataFrame(rows, columns=columns)


def build_qra_benchmark_blockers_by_event(component_registry: pd.DataFrame) -> pd.DataFrame:
    columns = [
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
    ]
    if component_registry.empty:
        return pd.DataFrame(columns=columns)
    financing = component_registry.loc[
        component_registry.apply(_is_current_sample_financing_row, axis=1)
    ].copy()
    if financing.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, object]] = []
    for event_id, group in financing.groupby("event_id", sort=False, dropna=False):
        blockers: list[str] = []
        for value in group.get("eligibility_blockers", pd.Series(dtype=object)).fillna(""):
            for blocker in str(value).split("|"):
                cleaned = blocker.strip()
                if cleaned and "benchmark" in cleaned and cleaned not in blockers:
                    blockers.append(cleaned)
        rows.append(
            {
                "event_id": event_id,
                "quarter": _normalize_text(group.get("quarter", pd.Series(dtype=object)).iloc[0]),
                "release_component_count": int(len(group)),
                "pre_release_external_count": int(group.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("pre_release_external").sum()),
                "external_timing_unverified_count": int(group.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("external_timing_unverified").sum()),
                "same_release_placeholder_count": int(group.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("same_release_placeholder").sum()),
                "post_release_invalid_count": int(group.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("post_release_invalid").sum()),
                "benchmark_verification_incomplete_count": int(group.get("expectation_status", pd.Series(dtype=object)).astype(str).eq("benchmark_verification_incomplete").sum()),
                "reviewed_surprise_ready_count": int(group.get("expectation_status", pd.Series(dtype=object)).astype(str).eq("reviewed_surprise_ready").sum()),
                "tier_a_count": int(group.get("quality_tier", pd.Series(dtype=object)).astype(str).eq("Tier A").sum()),
                "benchmark_blockers": "|".join(blockers),
            }
        )
    out = pd.DataFrame(rows, columns=columns)
    out["_quarter_sort_key"] = out["quarter"].map(_quarter_sort_key)
    out = out.sort_values(["_quarter_sort_key", "event_id"], kind="stable").drop(columns=["_quarter_sort_key"])
    return out.reset_index(drop=True)


def build_qra_event_registry_v2(
    panel: pd.DataFrame,
    release_calendar: pd.DataFrame | None = None,
    overlap_annotations: pd.DataFrame | None = None,
    shock_summary: pd.DataFrame | None = None,
    release_components: pd.DataFrame | None = None,
    expectation_template: pd.DataFrame | None = None,
    contamination_reviews: pd.DataFrame | None = None,
    release_calendar_source: str | None = None,
    overlap_annotations_source: str | None = None,
    shock_summary_source: str | None = None,
) -> pd.DataFrame:
    if panel.empty:
        return pd.DataFrame(
            columns=[
                "event_id",
                "quarter",
                "release_timestamp_et",
                "release_timestamp_source",
                "release_timestamp_kind",
                "release_timestamp_precision",
                "release_date_et",
                "release_time_et",
                "release_timezone",
                "policy_statement_release_timestamp_et",
                "policy_statement_release_timestamp_kind",
                "policy_statement_release_date_et",
                "policy_statement_release_time_et",
                "financing_estimates_release_timestamp_et",
                "financing_estimates_release_timestamp_kind",
                "financing_estimates_release_date_et",
                "financing_estimates_release_time_et",
                "release_sequence_label",
                "release_bundle_type",
                "has_policy_statement_release",
                "has_financing_estimates_release",
                "same_day_release_bundle_flag",
                "multi_stage_release_flag",
                "bundle_decomposition_ready",
                "policy_statement_url",
                "financing_estimates_url",
                "timing_quality",
                "release_calendar_source",
                "overlap_severity",
                "overlap_flag",
                "overlap_label",
                "overlap_note",
                "overlap_annotation_present",
                "overlap_annotation_source",
                "financing_need_news_flag",
                "composition_news_flag",
                "forward_guidance_flag",
                "reviewer",
                "review_date",
                "review_status",
                "review_notes",
                "review_notes_source",
                "shock_summary_source",
                "headline_check_official_release",
                "headline_check_bucket",
                "headline_check_classification_reviewed",
                "headline_check_shock_reviewed",
                "headline_check_non_missing_shock",
                "headline_check_non_small_denominator",
                "headline_eligibility_blockers",
                "headline_eligible",
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
                "spec_id",
            ]
        )

    event_rows = panel.copy()
    if "event_date_type" in event_rows.columns:
        event_rows = event_rows.loc[
            event_rows["event_date_type"].astype(str) == "official_release_date"
        ].copy()
    event_rows = event_rows.sort_values(["event_id"]).drop_duplicates(subset=["event_id"], keep="first")

    if release_calendar is not None and not release_calendar.empty:
        calendar = release_calendar.copy()
        keep = [
            column
            for column in (
                "event_id",
                "quarter",
                "financing_estimates_release_date",
                "policy_statement_release_date",
                "timing_quality",
                "financing_estimates_url",
                "policy_statement_url",
            )
            if column in calendar.columns
        ]
        event_rows = event_rows.merge(calendar[keep], on="event_id", how="left", suffixes=("", "_calendar"))
        for left, right in (
            ("quarter", "quarter_calendar"),
            ("financing_estimates_release_date", "financing_estimates_release_date_calendar"),
            ("policy_statement_release_date", "policy_statement_release_date_calendar"),
            ("timing_quality", "timing_quality_calendar"),
            ("financing_estimates_url", "financing_estimates_url_calendar"),
            ("policy_statement_url", "policy_statement_url_calendar"),
        ):
            if right in event_rows.columns:
                event_rows[left] = event_rows[right].where(
                    event_rows[right].notna() & event_rows[right].astype(str).str.strip().ne(""),
                    event_rows[left],
                )

    if overlap_annotations is not None and not overlap_annotations.empty:
        overlap = overlap_annotations.copy()
        keep = [
            column
            for column in ("event_id", "overlap_flag", "overlap_label", "overlap_note", "overlap_severity")
            if column in overlap.columns
        ]
        event_rows = event_rows.merge(overlap[keep], on="event_id", how="left")

    for column, default in (
        ("timing_quality", ""),
        ("policy_statement_url", ""),
        ("financing_estimates_url", ""),
        ("forward_guidance_bias", ""),
        ("classification_review_status", ""),
        ("overlap_flag", False),
        ("overlap_label", ""),
        ("overlap_note", ""),
        ("overlap_severity", ""),
    ):
        if column not in event_rows.columns:
            event_rows[column] = default

    event_rows["release_timestamp_source"] = event_rows.apply(
        lambda row: (
            "official_release_date"
            if _normalize_text(row.get("official_release_date"))
            else (
                "policy_statement_release_date"
                if _normalize_text(_first_non_empty(row, ("policy_statement_release_date", "policy_statement_release_date_calendar")))
                else (
                    "financing_estimates_release_date"
                    if _normalize_text(_first_non_empty(row, ("financing_estimates_release_date", "financing_estimates_release_date_calendar")))
                    else (
                        "event_date_requested"
                        if _normalize_text(row.get("event_date_requested"))
                        else ""
                    )
                )
            )
        ),
        axis=1,
    )
    event_rows["release_date_seed"] = event_rows.apply(
        lambda row: _first_non_empty(
            row,
            (
                "official_release_date",
                "policy_statement_release_date",
                "policy_statement_release_date_calendar",
                "financing_estimates_release_date",
                "financing_estimates_release_date_calendar",
                "event_date_requested",
            ),
        ),
        axis=1,
    )
    release_timestamp_components = event_rows["release_date_seed"].map(_coerce_timestamp_components)
    event_rows["release_timestamp_et"] = release_timestamp_components.map(lambda value: value[0])
    event_rows["release_timestamp_kind"] = release_timestamp_components.map(lambda value: value[1])
    event_rows["release_date_et"] = release_timestamp_components.map(lambda value: value[2])
    event_rows["release_time_et"] = release_timestamp_components.map(lambda value: value[3])
    event_rows["release_timezone"] = release_timestamp_components.map(lambda value: value[4])
    event_rows["release_timestamp_precision"] = event_rows["release_timestamp_kind"].map(
        lambda value: (
            "date_only_seeded"
            if value == "date_only"
            else ("timestamp_seeded" if value == "timestamp_with_time" else "missing")
        )
    )
    event_rows["release_timestamp_kind"] = event_rows.apply(
        lambda row: (
            f"{_normalize_text(row.get('release_timestamp_source'))}_{_normalize_text(row.get('release_timestamp_kind'))}"
            if _normalize_text(row.get("release_timestamp_source")) and _normalize_text(row.get("release_timestamp_kind")) != "missing"
            else _normalize_text(row.get("release_timestamp_kind")) or "missing"
        ),
        axis=1,
    )
    event_rows["policy_release_date_seed"] = event_rows.apply(
        lambda row: _first_non_empty(row, ("policy_statement_release_date", "policy_statement_release_date_calendar")),
        axis=1,
    )
    policy_components = event_rows["policy_release_date_seed"].map(_coerce_timestamp_components)
    event_rows["policy_statement_release_timestamp_et"] = policy_components.map(lambda value: value[0])
    event_rows["policy_statement_release_timestamp_kind"] = policy_components.map(lambda value: value[1])
    event_rows["policy_statement_release_date_et"] = policy_components.map(lambda value: value[2])
    event_rows["policy_statement_release_time_et"] = policy_components.map(lambda value: value[3])
    event_rows["financing_release_date_seed"] = event_rows.apply(
        lambda row: _first_non_empty(row, ("financing_estimates_release_date", "financing_estimates_release_date_calendar")),
        axis=1,
    )
    financing_components = event_rows["financing_release_date_seed"].map(_coerce_timestamp_components)
    event_rows["financing_estimates_release_timestamp_et"] = financing_components.map(lambda value: value[0])
    event_rows["financing_estimates_release_timestamp_kind"] = financing_components.map(lambda value: value[1])
    event_rows["financing_estimates_release_date_et"] = financing_components.map(lambda value: value[2])
    event_rows["financing_estimates_release_time_et"] = financing_components.map(lambda value: value[3])
    policy_ts = event_rows["policy_release_date_seed"].map(_coerce_timestamp_et_ts)
    financing_ts = event_rows["financing_release_date_seed"].map(_coerce_timestamp_et_ts)
    event_rows["release_sequence_label"] = [
        _release_sequence_label(financing_value, policy_value)
        for financing_value, policy_value in zip(financing_ts, policy_ts)
    ]
    event_rows["release_bundle_type"] = event_rows["timing_quality"].fillna("")
    event_rows["financing_need_news_flag"] = event_rows["financing_estimates_url"].map(
        lambda value: bool(_normalize_text(value))
    )
    event_rows["composition_news_flag"] = event_rows["policy_statement_url"].map(
        lambda value: bool(_normalize_text(value))
    )
    event_rows["forward_guidance_flag"] = event_rows["forward_guidance_bias"].map(
        lambda value: _normalize_lower(value) in {"hawkish", "dovish"}
    )
    event_rows["has_policy_statement_release"] = event_rows["policy_statement_release_timestamp_et"].map(pd.notna)
    event_rows["has_financing_estimates_release"] = event_rows["financing_estimates_release_timestamp_et"].map(pd.notna)
    event_rows["same_day_release_bundle_flag"] = event_rows.apply(
        lambda row: (
            pd.notna(row.get("policy_statement_release_date_et"))
            and pd.notna(row.get("financing_estimates_release_date_et"))
            and str(row.get("policy_statement_release_date_et")) == str(row.get("financing_estimates_release_date_et"))
        ),
        axis=1,
    )
    event_rows["multi_stage_release_flag"] = event_rows["release_sequence_label"].isin(
        ["financing_then_policy", "policy_then_financing"]
    )
    event_rows["bundle_decomposition_ready"] = (
        event_rows["has_policy_statement_release"].astype(bool)
        & event_rows["has_financing_estimates_release"].astype(bool)
        & event_rows["financing_need_news_flag"].fillna(False).astype(bool)
        & event_rows["composition_news_flag"].fillna(False).astype(bool)
    )
    event_rows["overlap_severity"] = event_rows.apply(
        lambda row: _overlap_severity(
            row.get("overlap_flag", False),
            row.get("overlap_severity", ""),
            row.get("overlap_label", ""),
            row.get("overlap_note", ""),
        ),
        axis=1,
    )
    event_rows["overlap_flag"] = event_rows["overlap_flag"].map(_coerce_bool)
    event_rows["overlap_annotation_present"] = event_rows.apply(
        lambda row: (
            _coerce_bool(row.get("overlap_flag"))
            or bool(_normalize_text(row.get("overlap_label")))
            or bool(_normalize_text(row.get("overlap_note")))
            or bool(_normalize_text(row.get("overlap_severity")))
        ),
        axis=1,
    )
    event_rows["overlap_annotation_source"] = (
        overlap_annotations_source
        if overlap_annotations_source
        else ("manual_qra_event_overlap_annotations" if overlap_annotations is not None and not overlap_annotations.empty else pd.NA)
    )
    event_rows["reviewer"] = event_rows["classification_review_status"].map(_reviewer_from_status)
    event_rows["review_date"] = event_rows.get("notes", pd.Series(dtype=object)).map(_review_date_from_text)
    event_rows["review_status"] = event_rows.get("classification_review_status", pd.Series(dtype=object)).fillna("")
    event_rows["review_notes"] = event_rows.get("notes", pd.Series(dtype=object)).fillna("")
    event_rows["review_notes_source"] = "panel.notes"
    event_rows["release_calendar_source"] = (
        release_calendar_source
        if release_calendar_source
        else ("manual_qra_release_calendar_seed" if release_calendar is not None and not release_calendar.empty else pd.NA)
    )
    event_rows["shock_summary_source"] = (
        shock_summary_source
        if shock_summary_source
        else ("qra_event_elasticity" if shock_summary is not None and not shock_summary.empty else pd.NA)
    )
    event_rows["treatment_version_id"] = SPEC_DURATION_TREATMENT_V1
    event_rows["spec_id"] = SPEC_QRA_EVENT_V2

    if shock_summary is not None and not shock_summary.empty:
        usable = build_qra_review_ledger(shock_summary, overlap_annotations=overlap_annotations)
        usable = usable.loc[usable["event_date_type"].astype(str) == "official_release_date"].copy()
        keep = [
            "event_id",
            "event_date_type",
            "headline_bucket",
            "classification_review_status",
            "shock_review_status",
            "shock_missing_flag",
            "small_denominator_flag",
            "usable_for_headline_reason",
            "usable_for_headline",
            "reviewer",
            "review_date",
            "treatment_version_id",
            "spec_id",
        ]
        event_rows = event_rows.merge(
            usable[keep].drop_duplicates(subset=["event_id"], keep="first"),
            on="event_id",
            how="left",
            suffixes=("", "_headline"),
        )
    for source_column in ("usable_for_headline_reason_headline", "usable_for_headline_reason"):
        if source_column in event_rows.columns:
            event_rows["headline_eligibility_reason"] = event_rows[source_column]
            break
    if "headline_eligibility_reason" not in event_rows.columns:
        event_rows["headline_eligibility_reason"] = "missing_shock_summary"
    event_rows["headline_eligibility_reason"] = event_rows["headline_eligibility_reason"].fillna("missing_shock_summary")
    reviewer_source = "reviewer_headline" if "reviewer_headline" in event_rows.columns else ("reviewer" if shock_summary is not None and not shock_summary.empty else None)
    if reviewer_source is not None:
        event_rows["reviewer"] = event_rows[reviewer_source].where(
            event_rows[reviewer_source].notna() & event_rows[reviewer_source].astype(str).str.strip().ne(""),
            event_rows["reviewer"],
        )
    review_date_source = "review_date_headline" if "review_date_headline" in event_rows.columns else ("review_date" if shock_summary is not None and not shock_summary.empty else None)
    if review_date_source is not None:
        event_rows["review_date"] = event_rows[review_date_source].where(
            event_rows[review_date_source].notna() & event_rows[review_date_source].astype(str).str.strip().ne(""),
            event_rows["review_date"],
        )
    treatment_version_source = "treatment_version_id_headline" if "treatment_version_id_headline" in event_rows.columns else ("treatment_version_id" if shock_summary is not None and not shock_summary.empty else None)
    if treatment_version_source is not None:
        event_rows["treatment_version_id"] = event_rows[treatment_version_source].where(
            event_rows[treatment_version_source].notna() & event_rows[treatment_version_source].astype(str).str.strip().ne(""),
            event_rows["treatment_version_id"],
        )
    spec_source = "spec_id_headline" if "spec_id_headline" in event_rows.columns else ("spec_id" if shock_summary is not None and not shock_summary.empty else None)
    if spec_source is not None:
        event_rows["spec_id"] = event_rows[spec_source].where(
            event_rows[spec_source].notna() & event_rows[spec_source].astype(str).str.strip().ne(""),
            event_rows["spec_id"],
        )
    event_rows["headline_check_official_release"] = event_rows.get(
        "event_date_type_headline",
        event_rows.get("event_date_type", pd.Series(dtype=object)),
    ).map(lambda value: _normalize_lower(value) == "official_release_date")
    event_rows["headline_check_bucket"] = event_rows.get(
        "headline_bucket_headline",
        event_rows.get("headline_bucket", pd.Series(dtype=object)),
    ).map(lambda value: _normalize_lower(value) in {"tightening", "easing", "control_hold"})
    event_rows["headline_check_classification_reviewed"] = event_rows.get(
        "classification_review_status_headline",
        event_rows.get("classification_review_status", pd.Series(dtype=object)),
    ).map(lambda value: _normalize_lower(value) == "reviewed")
    event_rows["headline_check_shock_reviewed"] = event_rows.get(
        "shock_review_status_headline",
        event_rows.get("shock_review_status", pd.Series(dtype=object)),
    ).map(lambda value: _normalize_lower(value) == "reviewed")
    event_rows["headline_check_non_missing_shock"] = ~event_rows.get(
        "shock_missing_flag_headline",
        event_rows.get("shock_missing_flag", pd.Series(False, index=event_rows.index)),
    ).map(_coerce_bool)
    event_rows["headline_check_non_small_denominator"] = ~event_rows.get(
        "small_denominator_flag_headline",
        event_rows.get("small_denominator_flag", pd.Series(False, index=event_rows.index)),
    ).map(_coerce_bool)
    event_rows["headline_eligibility_blockers"] = event_rows.apply(_headline_eligibility_blockers, axis=1)
    headline_eligible_source = None
    for candidate in ("usable_for_headline_headline", "usable_for_headline"):
        if candidate in event_rows.columns:
            headline_eligible_source = candidate
            break
    if headline_eligible_source is not None:
        event_rows["headline_eligible"] = event_rows[headline_eligible_source].map(_coerce_bool)
    else:
        event_rows["headline_eligible"] = event_rows["headline_eligibility_blockers"].map(lambda value: _normalize_text(value) == "")

    component_registry = build_qra_release_component_registry(
        event_rows,
        release_components=release_components,
        expectation_template=expectation_template,
        contamination_reviews=contamination_reviews,
    )
    causal_summary = summarize_qra_causal_qa(component_registry)
    if not causal_summary.empty:
        event_rows = event_rows.merge(causal_summary, on="event_id", how="left")
        component_timestamp_mask = (
            event_rows["event_release_timestamp_et"].notna()
            & event_rows["event_release_timestamp_et"].astype(str).str.strip().ne("")
        )
        if component_timestamp_mask.any():
            component_timestamp_components = event_rows["event_release_timestamp_et"].map(
                _coerce_timestamp_components
            )
            event_rows["release_timestamp_et"] = event_rows["event_release_timestamp_et"].where(
                component_timestamp_mask,
                event_rows["release_timestamp_et"],
            )
            event_rows["release_timestamp_source"] = event_rows["event_release_timestamp_source"].where(
                component_timestamp_mask,
                event_rows["release_timestamp_source"],
            )
            event_rows["release_timestamp_kind"] = event_rows["event_release_timestamp_kind"].where(
                component_timestamp_mask,
                event_rows["release_timestamp_kind"],
            )
            event_rows["release_date_et"] = component_timestamp_components.map(lambda value: value[2]).where(
                component_timestamp_mask,
                event_rows["release_date_et"],
            )
            event_rows["release_time_et"] = component_timestamp_components.map(lambda value: value[3]).where(
                component_timestamp_mask,
                event_rows["release_time_et"],
            )
            event_rows["release_timezone"] = component_timestamp_components.map(lambda value: value[4]).where(
                component_timestamp_mask,
                event_rows["release_timezone"],
            )
            event_rows["release_timestamp_precision"] = event_rows["timestamp_precision"].where(
                component_timestamp_mask,
                event_rows["release_timestamp_precision"],
            )

    keep = [
        "event_id",
        "quarter",
        "release_timestamp_et",
        "release_timestamp_source",
        "release_timestamp_kind",
        "release_timestamp_precision",
        "release_date_et",
        "release_time_et",
        "release_timezone",
        "policy_statement_release_timestamp_et",
        "policy_statement_release_timestamp_kind",
        "policy_statement_release_date_et",
        "policy_statement_release_time_et",
        "financing_estimates_release_timestamp_et",
        "financing_estimates_release_timestamp_kind",
        "financing_estimates_release_date_et",
        "financing_estimates_release_time_et",
        "release_sequence_label",
        "release_bundle_type",
        "has_policy_statement_release",
        "has_financing_estimates_release",
        "same_day_release_bundle_flag",
        "multi_stage_release_flag",
        "bundle_decomposition_ready",
        "policy_statement_url",
        "financing_estimates_url",
        "timing_quality",
        "release_calendar_source",
        "overlap_severity",
        "overlap_flag",
        "overlap_label",
        "overlap_note",
        "overlap_annotation_present",
        "overlap_annotation_source",
        "financing_need_news_flag",
        "composition_news_flag",
        "forward_guidance_flag",
        "reviewer",
        "review_date",
        "review_status",
        "review_notes",
        "review_notes_source",
        "shock_summary_source",
        "headline_check_official_release",
        "headline_check_bucket",
        "headline_check_classification_reviewed",
        "headline_check_shock_reviewed",
        "headline_check_non_missing_shock",
        "headline_check_non_small_denominator",
        "headline_eligibility_blockers",
        "headline_eligible",
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
        "spec_id",
    ]
    for column in keep:
        if column not in event_rows.columns:
            event_rows[column] = pd.NA
    return event_rows[keep].sort_values(["quarter", "event_id"], kind="stable").reset_index(drop=True)


def build_qra_shock_crosswalk_v1(elasticity: pd.DataFrame) -> pd.DataFrame:
    return build_qra_shock_crosswalk_from_ledger(elasticity)


def expand_treatment_variants(elasticity: pd.DataFrame) -> pd.DataFrame:
    if elasticity.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    for _, row in elasticity.iterrows():
        for variant in TREATMENT_VARIANTS:
            denominator_column = variant["denominator_column"]
            denominator = pd.to_numeric(pd.Series([row.get(denominator_column)]), errors="coerce").iloc[0]
            delta_bp = pd.to_numeric(pd.Series([row.get("delta_bp")]), errors="coerce").iloc[0]
            elasticity_value = np.nan
            if pd.notna(delta_bp) and pd.notna(denominator) and float(denominator) != 0.0:
                scale = float(denominator) / 100.0
                if variant["treatment_variant"] == "dv01_usd":
                    scale = float(denominator) / 1_000_000.0
                if scale != 0.0:
                    elasticity_value = float(delta_bp) / scale
            record = row.to_dict()
            record["spec_id"] = SPEC_DURATION_TREATMENT_V1
            record["treatment_variant"] = variant["treatment_variant"]
            record["treatment_denominator_column"] = denominator_column
            record["treatment_denominator_units"] = variant["denominator_units"]
            record["treatment_role"] = variant["role"]
            record["elasticity_value"] = elasticity_value
            rows.append(record)
    return pd.DataFrame(rows)


def build_event_usability_table(
    elasticity: pd.DataFrame,
    overlap_annotations: pd.DataFrame | None = None,
) -> pd.DataFrame:
    return build_event_usability_table_from_ledger(
        elasticity,
        overlap_annotations=overlap_annotations,
    )


def build_leave_one_event_out_table(elasticity: pd.DataFrame) -> pd.DataFrame:
    expanded = expand_treatment_variants(elasticity)
    if expanded.empty:
        return pd.DataFrame(
            columns=[
                "leave_one_out_event_id",
                "left_out_event_id",
                "event_id",
                "event_date_type",
                "headline_bucket",
                "treatment_variant",
                "series",
                "window",
                "n_remaining_events",
                "mean_elasticity_value",
                "estimate",
                "leave_one_out_coefficient",
                "leave_one_out_std_err",
                "leave_one_out_delta",
                "spec_id",
            ]
        )
    if "usable_for_headline_reason" not in expanded.columns:
        expanded["usable_for_headline_reason"] = expanded.apply(_usable_for_headline_reason, axis=1)
    eligible = expanded.loc[
        expanded["event_date_type"].astype(str).eq("official_release_date")
        & expanded["usable_for_headline"].astype(bool)
    ].copy()
    if eligible.empty:
        return pd.DataFrame(
            columns=[
                "leave_one_out_event_id",
                "left_out_event_id",
                "event_id",
                "event_date_type",
                "headline_bucket",
                "treatment_variant",
                "series",
                "window",
                "n_remaining_events",
                "mean_elasticity_value",
                "estimate",
                "leave_one_out_coefficient",
                "leave_one_out_std_err",
                "leave_one_out_delta",
                "spec_id",
            ]
        )
    leave_out_ids = sorted(eligible["event_id"].dropna().astype(str).unique().tolist())
    rows: list[dict[str, object]] = []
    for leave_out_id in leave_out_ids:
        subset = eligible.loc[eligible["event_id"].astype(str) != leave_out_id].copy()
        grouped = subset.groupby(["treatment_variant", "series", "window"], dropna=False)
        summary = grouped["elasticity_value"].mean().reset_index(name="mean_elasticity_value")
        counts = grouped["event_id"].nunique().reset_index(name="n_remaining_events")
        merged = summary.merge(counts, on=["treatment_variant", "series", "window"], how="left")
        merged["leave_one_out_event_id"] = leave_out_id
        merged["left_out_event_id"] = leave_out_id
        merged["event_id"] = leave_out_id
        merged["event_date_type"] = "official_release_date"
        merged["headline_bucket"] = "headline_usable_pool"
        merged["leave_one_out_coefficient"] = merged["mean_elasticity_value"]
        merged["estimate"] = merged["mean_elasticity_value"]
        merged["leave_one_out_std_err"] = np.nan
        merged["leave_one_out_delta"] = np.nan
        merged["spec_id"] = SPEC_DURATION_TREATMENT_V1
        rows.extend(merged.to_dict(orient="records"))
    output = pd.DataFrame(rows)
    if output.empty:
        return output
    return output[
        [
            "leave_one_out_event_id",
            "left_out_event_id",
            "event_id",
            "event_date_type",
            "headline_bucket",
            "treatment_variant",
            "series",
            "window",
            "n_remaining_events",
            "mean_elasticity_value",
            "estimate",
            "leave_one_out_coefficient",
            "leave_one_out_std_err",
            "leave_one_out_delta",
            "spec_id",
        ]
    ].sort_values(
        ["leave_one_out_event_id", "treatment_variant", "series", "window"],
        kind="stable",
    ).reset_index(drop=True)
