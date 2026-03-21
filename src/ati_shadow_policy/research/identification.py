from __future__ import annotations

from collections.abc import Iterable
import math
import re

import numpy as np
import pandas as pd

from ati_shadow_policy.specs import (
    SPEC_DURATION_TREATMENT_V1,
    SPEC_QRA_EVENT_V2,
    TREATMENT_VARIANTS,
)


_RELEASE_DATE_COLUMNS = ("official_release_date", "policy_statement_release_date", "event_date_requested")


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
    if isinstance(value, float) and pd.isna(value):
        return ""
    return " ".join(str(value).split()).strip()


def _normalize_lower(value: object) -> str:
    return _normalize_text(value).lower()


def _normalize_overlap_severity(value: object) -> str:
    return _normalize_lower(value)


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


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


def _coerce_timestamp_kind(value: object) -> str:
    text = _normalize_text(value)
    if not text:
        return "missing"
    ts = pd.to_datetime(text, errors="coerce")
    if pd.isna(ts):
        return "missing"
    if _timestamp_has_clock(text):
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


def build_qra_event_registry_v2(
    panel: pd.DataFrame,
    release_calendar: pd.DataFrame | None = None,
    overlap_annotations: pd.DataFrame | None = None,
    shock_summary: pd.DataFrame | None = None,
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
            ("timing_quality", "timing_quality_calendar"),
            ("financing_estimates_url", "financing_estimates_url_calendar"),
            ("policy_statement_url", "policy_statement_url_calendar"),
        ):
            if right in event_rows.columns:
                event_rows[left] = event_rows[left].where(
                    event_rows[left].notna() & event_rows[left].astype(str).str.strip().ne(""),
                    event_rows[right],
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
    release_components = event_rows["release_date_seed"].map(_coerce_timestamp_components)
    event_rows["release_timestamp_et"] = release_components.map(lambda value: value[0])
    event_rows["release_timestamp_kind"] = release_components.map(lambda value: value[1])
    event_rows["release_date_et"] = release_components.map(lambda value: value[2])
    event_rows["release_time_et"] = release_components.map(lambda value: value[3])
    event_rows["release_timezone"] = release_components.map(lambda value: value[4])
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
        usable = shock_summary.copy()
        if "event_date_type" in usable.columns:
            usable = usable.loc[usable["event_date_type"].astype(str) == "official_release_date"].copy()
        usable["headline_eligibility_reason"] = usable.apply(_usable_for_headline_reason, axis=1)
        keep = [
            "event_id",
            "headline_eligibility_reason",
            "event_date_type",
            "headline_bucket",
            "classification_review_status",
            "shock_review_status",
            "shock_missing_flag",
            "small_denominator_flag",
        ]
        event_rows = event_rows.merge(
            usable[keep].drop_duplicates(subset=["event_id"], keep="first"),
            on="event_id",
            how="left",
            suffixes=("", "_headline"),
        )
    if "headline_eligibility_reason" not in event_rows.columns:
        event_rows["headline_eligibility_reason"] = "missing_shock_summary"
    event_rows["headline_eligibility_reason"] = event_rows["headline_eligibility_reason"].fillna("missing_shock_summary")
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
    event_rows["headline_eligible"] = event_rows["headline_eligibility_blockers"].map(lambda value: _normalize_text(value) == "")

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
        "treatment_version_id",
        "headline_eligibility_reason",
        "spec_id",
    ]
    for column in keep:
        if column not in event_rows.columns:
            event_rows[column] = pd.NA
    return event_rows[keep].sort_values(["quarter", "event_id"], kind="stable").reset_index(drop=True)


def build_qra_shock_crosswalk_v1(elasticity: pd.DataFrame) -> pd.DataFrame:
    if elasticity.empty:
        return pd.DataFrame(
            columns=[
                "event_id",
                "event_date_type",
                "canonical_shock_id",
                "shock_bn",
                "schedule_diff_10y_eq_bn",
                "schedule_diff_dynamic_10y_eq_bn",
                "schedule_diff_dv01_usd",
                "gross_notional_delta_bn",
                "shock_source",
                "manual_override_reason",
                "alternative_treatment_complete",
                "alternative_treatment_missing_fields",
                "alternative_treatment_missing_reason",
                "shock_review_status",
                "spec_id",
            ]
        )
    keep = [
        "event_id",
        "event_date_type",
        "shock_bn",
        "schedule_diff_10y_eq_bn",
        "schedule_diff_dynamic_10y_eq_bn",
        "schedule_diff_dv01_usd",
        "gross_notional_delta_bn",
        "shock_source",
        "shock_notes",
        "shock_review_status",
        "shock_construction",
    ]
    frame = elasticity[[column for column in keep if column in elasticity.columns]].copy()
    frame = frame.drop_duplicates(subset=["event_id", "event_date_type"], keep="first").reset_index(drop=True)
    frame["canonical_shock_id"] = "canonical_shock_bn"
    frame["manual_override_reason"] = frame.apply(
        lambda row: (
            _normalize_text(row.get("shock_notes"))
            if "manual" in _normalize_lower(row.get("shock_source"))
            or "manual_override" in _normalize_lower(row.get("shock_construction"))
            else ""
        ),
        axis=1,
    )
    alt_status = frame.apply(_alternative_treatment_status, axis=1, result_type="expand")
    alt_status.columns = [
        "alternative_treatment_complete",
        "alternative_treatment_missing_fields",
        "alternative_treatment_missing_reason",
    ]
    frame = pd.concat([frame, alt_status], axis=1)
    frame["spec_id"] = SPEC_DURATION_TREATMENT_V1
    keep_out = [
        "event_id",
        "event_date_type",
        "canonical_shock_id",
        "shock_bn",
        "schedule_diff_10y_eq_bn",
        "schedule_diff_dynamic_10y_eq_bn",
        "schedule_diff_dv01_usd",
        "gross_notional_delta_bn",
        "shock_source",
        "manual_override_reason",
        "alternative_treatment_complete",
        "alternative_treatment_missing_fields",
        "alternative_treatment_missing_reason",
        "shock_review_status",
        "spec_id",
    ]
    for column in keep_out:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame[keep_out].sort_values(["event_id", "event_date_type"], kind="stable").reset_index(drop=True)


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
    if elasticity.empty:
        return pd.DataFrame(
            columns=[
                "event_date_type",
                "headline_bucket",
                "classification_review_status",
                "shock_review_status",
                "overlap_severity",
                "usable_for_headline",
                "usable_for_headline_reason",
                "event_count",
                "n_rows",
                "n_events",
                "spec_id",
            ]
        )

    frame = elasticity.copy()
    if "usable_for_headline_reason" not in frame.columns:
        frame["usable_for_headline_reason"] = frame.apply(_usable_for_headline_reason, axis=1)
    frame = frame.drop_duplicates(subset=["event_id", "event_date_type"], keep="first").reset_index(drop=True)
    if overlap_annotations is not None and not overlap_annotations.empty:
        overlap = overlap_annotations.copy()
        frame = frame.merge(overlap, on="event_id", how="left")
    frame["overlap_severity"] = frame.apply(
        lambda row: _overlap_severity(
            row.get("overlap_flag", False),
            row.get("overlap_severity", ""),
            row.get("overlap_label", ""),
            row.get("overlap_note", ""),
        ),
        axis=1,
    )
    grouped = (
        frame.groupby(
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
        )
        .size()
        .reset_index(name="event_count")
    )
    grouped["n_rows"] = grouped["event_count"]
    grouped["n_events"] = grouped["event_count"]
    grouped["spec_id"] = SPEC_QRA_EVENT_V2
    return grouped.sort_values(
        [
            "event_date_type",
            "headline_bucket",
            "classification_review_status",
            "shock_review_status",
            "overlap_severity",
            "usable_for_headline",
        ],
        kind="stable",
    ).reset_index(drop=True)


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
