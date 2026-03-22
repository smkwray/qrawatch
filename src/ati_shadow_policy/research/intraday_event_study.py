from __future__ import annotations

from collections.abc import Sequence

import pandas as pd


def _normalize_timestamp(value: object) -> pd.Timestamp | pd.NaT:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tzinfo is None:
        return ts.tz_localize("America/New_York")
    return ts.tz_convert("America/New_York")


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def build_intraday_event_panel(
    series_df: pd.DataFrame,
    component_registry: pd.DataFrame,
    value_columns: Sequence[str],
    *,
    timestamp_column: str = "timestamp_et",
    pre_minutes: int = 30,
    post_minutes: int = 30,
    causal_only: bool = True,
) -> pd.DataFrame:
    required_series = [timestamp_column, *value_columns]
    missing_series = [column for column in required_series if column not in series_df.columns]
    if missing_series:
        raise KeyError(f"Intraday series frame missing required column(s): {missing_series}")

    if "release_component_id" not in component_registry.columns:
        raise KeyError("Component registry must include release_component_id")
    if "release_timestamp_et" not in component_registry.columns:
        raise KeyError("Component registry must include release_timestamp_et")

    market = series_df.copy()
    market[timestamp_column] = market[timestamp_column].map(_normalize_timestamp)
    market = market.dropna(subset=[timestamp_column]).sort_values(timestamp_column).reset_index(drop=True)
    if market.empty:
        return pd.DataFrame()

    components = component_registry.copy()
    components["release_timestamp_et"] = components["release_timestamp_et"].map(_normalize_timestamp)
    components = components.dropna(subset=["release_timestamp_et"]).copy()
    if "timestamp_precision" in components.columns:
        components = components.loc[components["timestamp_precision"].astype(str) == "exact_time"].copy()
    if causal_only and "causal_eligible" in components.columns:
        components = components.loc[components["causal_eligible"].map(_coerce_bool)].copy()
    if components.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    for _, component in components.iterrows():
        event_ts = component["release_timestamp_et"]
        pre_cutoff = event_ts - pd.Timedelta(minutes=int(pre_minutes))
        post_cutoff = event_ts + pd.Timedelta(minutes=int(post_minutes))

        pre_market = market.loc[market[timestamp_column] <= pre_cutoff].tail(1)
        post_market = market.loc[market[timestamp_column] >= post_cutoff].head(1)
        if pre_market.empty or post_market.empty:
            continue
        pre_row = pre_market.iloc[0]
        post_row = post_market.iloc[0]

        for series in value_columns:
            pre_value = pd.to_numeric(pd.Series([pre_row.get(series)]), errors="coerce").iloc[0]
            post_value = pd.to_numeric(pd.Series([post_row.get(series)]), errors="coerce").iloc[0]
            if pd.isna(pre_value) or pd.isna(post_value):
                continue
            rows.append(
                {
                    "release_component_id": component.get("release_component_id"),
                    "event_id": component.get("event_id"),
                    "quarter": component.get("quarter"),
                    "component_type": component.get("component_type"),
                    "event_timestamp_et": event_ts.isoformat(),
                    "pre_timestamp_et": pre_row[timestamp_column].isoformat(),
                    "post_timestamp_et": post_row[timestamp_column].isoformat(),
                    "series": series,
                    "window_label": f"m{pre_minutes}_to_p{post_minutes}",
                    "pre_value": float(pre_value),
                    "post_value": float(post_value),
                    "delta_value": float(post_value) - float(pre_value),
                    "quality_tier": component.get("quality_tier", pd.NA),
                    "eligibility_blockers": component.get("eligibility_blockers", pd.NA),
                }
            )
    return pd.DataFrame(rows)
