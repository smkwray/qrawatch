from __future__ import annotations

from typing import Sequence

import numpy as np
import pandas as pd

_PLACEHOLDER_EXPECTED_DIRECTIONS = {"classification_pending", "pending", "unclassified", "tbd", "todo"}


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

    for column in ("overlap_label", "overlap_note"):
        if column not in annotations.columns:
            annotations[column] = ""
        else:
            annotations[column] = annotations[column].fillna("").astype(str)

    annotations = annotations[["event_id", "overlap_flag", "overlap_label", "overlap_note"]]
    annotations = annotations.sort_values(["event_id", "overlap_flag"], kind="stable")
    annotations = annotations.drop_duplicates(subset=["event_id"], keep="last").reset_index(drop=True)
    return annotations


def _normalize_expected_direction(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def _is_headline_expected_direction(series: pd.Series) -> pd.Series:
    normalized = _normalize_expected_direction(series).str.lower()
    return (normalized != "") & ~normalized.isin(_PLACEHOLDER_EXPECTED_DIRECTIONS)


def build_event_panel(
    series_df: pd.DataFrame,
    events_df: pd.DataFrame,
    value_columns: Sequence[str],
    event_date_column: str = "official_release_date",
    overlap_annotations: pd.DataFrame | None = None,
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

        record = {
            "event_id": event.get("event_id"),
            "event_label": event.get("event_label"),
            "event_date_requested": event_date,
            "event_date_aligned": aligned,
            "event_date_type": event_date_column,
            "expected_direction": event.get("expected_direction"),
        }
        if overlap_annotations is not None:
            record["overlap_flag"] = bool(event.get("overlap_flag", False))
            record["overlap_label"] = event.get("overlap_label", "")
            record["overlap_note"] = event.get("overlap_note", "")

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
    working["expected_direction"] = _normalize_expected_direction(working["expected_direction"])
    working = working.loc[_is_headline_expected_direction(working["expected_direction"])].copy()
    if working.empty:
        return pd.DataFrame(columns=["expected_direction", *value_cols])
    summary = working.groupby("expected_direction", sort=True)[value_cols].mean(numeric_only=True)
    summary = summary.reset_index()
    return summary[["expected_direction", *value_cols]]


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
    working["expected_direction"] = _normalize_expected_direction(working["expected_direction"])
    working = working.loc[_is_headline_expected_direction(working["expected_direction"])].copy()
    if working.empty:
        return pd.DataFrame(columns=["sample_variant", "event_date_type", "expected_direction", "n_events", *value_cols])

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
        grouped = subset.groupby(["event_date_type", "expected_direction"], sort=True)
        summary = grouped[value_cols].mean(numeric_only=True).reset_index()
        counts = grouped.size().reset_index(name="n_events")
        summary = summary.merge(counts, on=["event_date_type", "expected_direction"], how="left")
        summary.insert(0, "sample_variant", sample_variant)
        summaries.append(summary[["sample_variant", "event_date_type", "expected_direction", "n_events", *value_cols]])

    if not summaries:
        return pd.DataFrame(columns=["sample_variant", "event_date_type", "expected_direction", "n_events", *value_cols])

    output = pd.concat(summaries, ignore_index=True)
    output["sample_variant"] = pd.Categorical(
        output["sample_variant"],
        categories=["all_events", "overlap_excluded"],
        ordered=True,
    )
    output = output.sort_values(["sample_variant", "event_date_type", "expected_direction"], kind="stable")
    output["sample_variant"] = output["sample_variant"].astype(str)
    output["event_date_type"] = output["event_date_type"].astype(str)
    return output.reset_index(drop=True)
