from __future__ import annotations

from typing import Sequence

import numpy as np
import pandas as pd

from .qra_classification import is_summary_headline_bucket

SPEC_QRA_EVENT_V2 = "spec_qra_event_v2"
DEFAULT_EVENT_TREATMENT_VARIANT = "event_window_deltas_v1"
DEFAULT_TREATMENT_VERSION_ID = "spec_duration_treatment_v1"


def _value_bases_from_panel(panel: pd.DataFrame) -> list[str]:
    value_bases: list[str] = []
    seen: set[str] = set()
    for column in panel.columns:
        if not (column.endswith("_d1") or column.endswith("_d3")):
            continue
        base = column[:-3]
        if base not in seen:
            seen.add(base)
            value_bases.append(base)
    value_bases.sort()
    return value_bases


def _resolve_date(series: pd.Series, event_date: pd.Timestamp) -> pd.Timestamp | None:
    dates = pd.Series(pd.to_datetime(series.dropna().unique())).sort_values().reset_index(drop=True)
    if event_date in set(dates):
        return event_date
    prior = dates[dates <= event_date]
    if len(prior) == 0:
        return None
    return prior.iloc[-1]


def _event_window_deltas(series: pd.Series, aligned: pd.Timestamp) -> tuple[float, float, float]:
    observed = series.dropna()
    if aligned not in observed.index:
        return np.nan, np.nan, np.nan

    location = observed.index.get_loc(aligned)
    if not isinstance(location, (int, np.integer)):
        raise ValueError(f"Duplicate market dates detected around {aligned.date()}")

    level_t = observed.iloc[location]
    d1 = np.nan
    d3 = np.nan
    if location >= 1:
        t_minus_1 = observed.iloc[location - 1]
        d1 = level_t - t_minus_1
    if location >= 1 and location + 1 < len(observed):
        t_minus_1 = observed.iloc[location - 1]
        t_plus_1 = observed.iloc[location + 1]
        d3 = t_plus_1 - t_minus_1
    return level_t, d1, d3


def _normalize_overlap_annotations(overlap_annotations: pd.DataFrame | None) -> pd.DataFrame | None:
    if overlap_annotations is None:
        return None
    if overlap_annotations.empty:
        return overlap_annotations.copy()

    annotations = overlap_annotations.copy()
    if "event_id" not in annotations.columns:
        raise KeyError("Overlap annotations must include an event_id column")
    if "overlap_flag" not in annotations.columns:
        raise KeyError("Overlap annotations must include an overlap_flag column")

    annotations["event_id"] = annotations["event_id"].astype(str)
    annotations["overlap_flag"] = annotations["overlap_flag"].map(
        lambda value: str(value).strip().lower() in {"1", "true", "t", "yes", "y"}
    )

    for column in ("overlap_label", "overlap_note", "overlap_severity"):
        if column not in annotations.columns:
            annotations[column] = ""
        else:
            annotations[column] = annotations[column].fillna("").astype(str)

    annotations = annotations[
        ["event_id", "overlap_flag", "overlap_label", "overlap_note", "overlap_severity"]
    ]
    annotations = annotations.sort_values(["event_id", "overlap_flag"], kind="stable")
    annotations = annotations.drop_duplicates(subset=["event_id"], keep="last").reset_index(drop=True)
    return annotations


def _normalize_text(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def _normalize_scalar_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def _first_non_empty_series_value(series: pd.Series | None, default: str) -> str:
    if series is None:
        return default
    cleaned = series.dropna().astype(str).str.strip()
    cleaned = cleaned.loc[cleaned.ne("")]
    if cleaned.empty:
        return default
    return cleaned.iloc[0]


def _overlap_severity(row: pd.Series) -> str:
    manual = _normalize_scalar_text(row.get("overlap_severity")).lower()
    if manual:
        return manual

    overlap_flag = str(row.get("overlap_flag", "")).strip().lower() in {"1", "true", "t", "yes", "y"}
    if not overlap_flag:
        return "none"
    label = _normalize_scalar_text(row.get("overlap_label")).lower()
    if any(token in label for token in ("fomc", "cpi", "payroll", "auction", "treasury")):
        return "high"
    if label:
        return "medium"
    return "low"


def _headline_registry_reason(row: pd.Series) -> str:
    classification_review_status = _normalize_scalar_text(row.get("classification_review_status")).lower()
    headline_bucket = _normalize_scalar_text(row.get("headline_bucket")).lower()
    if classification_review_status != "reviewed":
        return "classification_not_reviewed"
    if headline_bucket not in {"tightening", "easing", "control_hold"}:
        return "non_headline_bucket"
    return "eligible_pending_shock_checks"


def _bool_from_non_empty(value: object) -> bool:
    text = _normalize_scalar_text(value)
    return bool(text)


def build_qra_event_registry_v2(
    panel: pd.DataFrame,
    treatment_version_id: str = DEFAULT_TREATMENT_VERSION_ID,
) -> pd.DataFrame:
    required = {"event_id", "event_date_type"}
    missing = sorted(required - set(panel.columns))
    if missing:
        raise KeyError(f"Event panel missing required registry column(s): {missing}")

    if panel.empty:
        return pd.DataFrame(
            columns=[
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
                "treatment_version_id",
                "headline_eligibility_reason",
            ]
        )

    working = panel.copy()
    event_order = pd.CategoricalDtype(
        ["official_release_date", "market_pricing_marker_minus_1d"],
        ordered=True,
    )
    working["event_date_type"] = working["event_date_type"].astype(event_order)
    working = working.sort_values(["event_id", "event_date_type"], kind="stable")
    deduped = working.drop_duplicates(subset=["event_id"], keep="first").reset_index(drop=True)

    rows: list[dict[str, object]] = []
    for _, row in deduped.iterrows():
        reviewer = row.get("reviewer", row.get("classification_reviewer", pd.NA))
        review_date = row.get("review_date", row.get("classification_review_date", pd.NA))
        rows.append(
            {
                "event_id": row.get("event_id", pd.NA),
                "quarter": row.get("quarter", pd.NA),
                "release_timestamp_et": row.get("release_timestamp_et", row.get("official_release_timestamp_et", pd.NA)),
                "release_bundle_type": row.get("timing_quality", pd.NA),
                "policy_statement_url": row.get("policy_statement_url", pd.NA),
                "financing_estimates_url": row.get("financing_estimates_url", pd.NA),
                "timing_quality": row.get("timing_quality", pd.NA),
                "overlap_severity": _overlap_severity(row),
                "overlap_label": row.get("overlap_label", pd.NA),
                "financing_need_news_flag": _bool_from_non_empty(row.get("financing_estimates_url")),
                "composition_news_flag": _normalize_scalar_text(row.get("current_quarter_action")).lower() in {
                    "tightening",
                    "easing",
                    "hold",
                    "mixed",
                },
                "forward_guidance_flag": _normalize_scalar_text(row.get("forward_guidance_bias")).lower() in {"hawkish", "dovish"},
                "reviewer": reviewer if _normalize_scalar_text(reviewer) else pd.NA,
                "review_date": review_date if _normalize_scalar_text(review_date) else pd.NA,
                "treatment_version_id": treatment_version_id,
                "headline_eligibility_reason": _headline_registry_reason(row),
            }
        )
    out = pd.DataFrame(rows)
    return out.sort_values(["event_id"], kind="stable").reset_index(drop=True)


def _summary_group_column(panel: pd.DataFrame) -> str:
    if "headline_bucket" in panel.columns:
        return "headline_bucket"
    if "expected_direction" in panel.columns:
        return "expected_direction"
    raise KeyError("Event panel must include headline_bucket or expected_direction")


def _summary_mask(panel: pd.DataFrame, group_column: str) -> pd.Series:
    if group_column == "headline_bucket":
        return is_summary_headline_bucket(panel[group_column])
    normalized = _normalize_text(panel[group_column]).str.lower()
    return normalized.ne("") & ~normalized.isin({"classification_pending", "pending", "unclassified", "tbd", "todo"})


def build_event_panel(
    series_df: pd.DataFrame,
    events_df: pd.DataFrame,
    value_columns: Sequence[str],
    event_date_column: str = "official_release_date",
    overlap_annotations: pd.DataFrame | None = None,
    spec_id: str = SPEC_QRA_EVENT_V2,
    treatment_variant: str = DEFAULT_EVENT_TREATMENT_VARIANT,
) -> pd.DataFrame:
    outcomes = series_df.copy()
    outcomes["date"] = pd.to_datetime(outcomes["date"])
    outcomes = outcomes.sort_values("date").set_index("date")
    events = events_df.copy()
    events[event_date_column] = pd.to_datetime(events[event_date_column])
    overlap_annotations = _normalize_overlap_annotations(overlap_annotations)

    missing_columns = [col for col in value_columns if col not in outcomes.columns]
    if missing_columns:
        raise KeyError(f"Missing outcome series column(s): {missing_columns}")

    if overlap_annotations is not None:
        events["event_id"] = events["event_id"].astype(str)
        events = events.merge(overlap_annotations, on="event_id", how="left")
        events["overlap_flag"] = events["overlap_flag"].map(
            lambda value: str(value).strip().lower() in {"1", "true", "t", "yes", "y"}
        )
        for column in ("overlap_label", "overlap_note"):
            events[column] = events[column].fillna("").astype(str)

    rows = []
    for _, event in events.iterrows():
        event_date = pd.Timestamp(event[event_date_column])
        aligned = _resolve_date(outcomes.index.to_series(), event_date)
        if aligned is None:
            event_id = event.get("event_id")
            raise ValueError(
                f"Could not align event {event_id!r} from {event_date_column}={event_date.date()} "
                "to an available market-data date"
            )

        record = event.to_dict()
        record["event_date_requested"] = event_date
        record["event_date_aligned"] = aligned
        record["event_date_type"] = event_date_column
        record["spec_id"] = spec_id
        record["treatment_variant"] = treatment_variant
        if overlap_annotations is not None:
            record["overlap_flag"] = bool(event.get("overlap_flag", False))
            record["overlap_label"] = event.get("overlap_label", "")
            record["overlap_note"] = event.get("overlap_note", "")
            record["overlap_severity"] = event.get("overlap_severity", "")

        for col in value_columns:
            level_t, d1, d3 = _event_window_deltas(outcomes[col], aligned)
            record[f"{col}_level_t"] = level_t
            record[f"{col}_d1"] = d1
            record[f"{col}_d3"] = d3

        rows.append(record)

    return pd.DataFrame(rows)


def summarize_event_panel(panel: pd.DataFrame) -> pd.DataFrame:
    value_bases = _value_bases_from_panel(panel)
    value_cols = [f"{base}_{suffix}" for base in value_bases for suffix in ("d1", "d3")]
    if not value_cols:
        return pd.DataFrame()
    working = panel.copy()
    group_column = _summary_group_column(working)
    working[group_column] = _normalize_text(working[group_column])
    working = working.loc[_summary_mask(working, group_column)].copy()
    if working.empty:
        return pd.DataFrame(columns=["spec_id", "treatment_variant", group_column, *value_cols])
    summary = working.groupby(group_column, sort=True)[value_cols].mean(numeric_only=True)
    summary = summary.reset_index()
    summary.insert(
        0,
        "treatment_variant",
        _first_non_empty_series_value(working.get("treatment_variant"), DEFAULT_EVENT_TREATMENT_VARIANT),
    )
    summary.insert(
        0,
        "spec_id",
        _first_non_empty_series_value(working.get("spec_id"), SPEC_QRA_EVENT_V2),
    )
    return summary[["spec_id", "treatment_variant", group_column, *value_cols]]


def build_overlap_exclusion_audit_note(panel: pd.DataFrame, robustness: pd.DataFrame) -> str:
    if panel.empty:
        return "No event rows available for overlap exclusion checks."
    overlap_count = int(panel.get("overlap_flag", pd.Series(dtype=bool)).fillna(False).astype(bool).sum())
    if overlap_count == 0:
        return "overlap_excluded is identical to all_events because no overlap-annotated events were flagged."
    if robustness.empty:
        return f"overlap_excluded removed {overlap_count} overlap-annotated event(s), but no robustness rows were produced."

    value_cols = [col for col in robustness.columns if col.endswith("_d1") or col.endswith("_d3")]
    key_cols = [col for col in ("event_date_type", "headline_bucket") if col in robustness.columns]
    if not value_cols or not key_cols:
        return f"overlap_excluded removed {overlap_count} overlap-annotated event(s)."

    all_events = robustness.loc[robustness["sample_variant"] == "all_events", [*key_cols, *value_cols]].copy()
    overlap_excluded = robustness.loc[robustness["sample_variant"] == "overlap_excluded", [*key_cols, *value_cols]].copy()
    if all_events.empty or overlap_excluded.empty:
        return f"overlap_excluded removed {overlap_count} overlap-annotated event(s)."

    merged = all_events.merge(
        overlap_excluded,
        on=key_cols,
        how="outer",
        suffixes=("_all", "_overlap"),
        indicator=True,
    )
    identical = True
    for col in value_cols:
        left = pd.to_numeric(merged[f"{col}_all"], errors="coerce")
        right = pd.to_numeric(merged[f"{col}_overlap"], errors="coerce")
        both_missing = left.isna() & right.isna()
        same = both_missing | np.isclose(left.fillna(0.0), right.fillna(0.0))
        if not bool(same.all()):
            identical = False
            break
    if identical and bool((merged["_merge"] == "both").all()):
        return (
            f"overlap_excluded is numerically identical to all_events even though {overlap_count} "
            "overlap-annotated event(s) were flagged."
        )
    return f"overlap_excluded removed {overlap_count} overlap-annotated event(s)."


def summarize_event_panel_robustness(panel: pd.DataFrame) -> pd.DataFrame:
    value_bases = _value_bases_from_panel(panel)
    value_cols = [f"{base}_{suffix}" for base in value_bases for suffix in ("d1", "d3")]
    if not value_cols:
        return pd.DataFrame()

    working = panel.copy()
    if "event_date_type" not in working.columns:
        raise KeyError("Event panel must include event_date_type")
    if "overlap_flag" not in working.columns:
        working["overlap_flag"] = False
    else:
        working["overlap_flag"] = working["overlap_flag"].fillna(False).astype(bool)
    group_column = _summary_group_column(working)
    working[group_column] = _normalize_text(working[group_column])
    working = working.loc[_summary_mask(working, group_column)].copy()
    if working.empty:
        return pd.DataFrame(
            columns=[
                "spec_id",
                "treatment_variant",
                "sample_variant",
                "event_date_type",
                group_column,
                "n_events",
                "overlap_exclusion_note",
                *value_cols,
            ]
        )

    date_type_order = pd.CategoricalDtype(
        ["official_release_date", "market_pricing_marker_minus_1d"],
        ordered=True,
    )
    working["event_date_type"] = working["event_date_type"].astype(date_type_order)

    summaries = []
    for sample_variant, subset in (
        ("all_events", working),
        ("overlap_excluded", working.loc[~working["overlap_flag"]].copy()),
    ):
        if subset.empty:
            continue
        grouped = subset.groupby(["event_date_type", group_column], sort=True, observed=True)
        summary = grouped[value_cols].mean(numeric_only=True).reset_index()
        counts = grouped.size().reset_index(name="n_events")
        summary = summary.merge(counts, on=["event_date_type", group_column], how="left")
        summary.insert(0, "sample_variant", sample_variant)
        summary.insert(
            0,
            "treatment_variant",
            _first_non_empty_series_value(subset.get("treatment_variant"), DEFAULT_EVENT_TREATMENT_VARIANT),
        )
        summary.insert(
            0,
            "spec_id",
            _first_non_empty_series_value(subset.get("spec_id"), SPEC_QRA_EVENT_V2),
        )
        summaries.append(
            summary[["spec_id", "treatment_variant", "sample_variant", "event_date_type", group_column, "n_events", *value_cols]]
        )

    if not summaries:
        return pd.DataFrame(
            columns=[
                "spec_id",
                "treatment_variant",
                "sample_variant",
                "event_date_type",
                group_column,
                "n_events",
                "overlap_exclusion_note",
                *value_cols,
            ]
        )

    output = pd.concat(summaries, ignore_index=True)
    output["sample_variant"] = pd.Categorical(
        output["sample_variant"],
        categories=["all_events", "overlap_excluded"],
        ordered=True,
    )
    output = output.sort_values(["sample_variant", "event_date_type", group_column], kind="stable")
    output["sample_variant"] = output["sample_variant"].astype(str)
    output["event_date_type"] = output["event_date_type"].astype(str)
    audit_note = build_overlap_exclusion_audit_note(working, output)
    output["overlap_exclusion_note"] = audit_note
    return output.reset_index(drop=True)
