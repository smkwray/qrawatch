from __future__ import annotations

import re

import numpy as np
import pandas as pd


_MONTH_NUMBERS = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}

TENOR_10Y_EQ_WEIGHTS = {
    "2Y": 0.2,
    "3Y": 0.3,
    "5Y": 0.5,
    "7Y": 0.7,
    "10Y": 1.0,
    "20Y": 2.0,
    "30Y": 3.0,
    "2Y_FRN": 0.05,
}

_TENOR_MATURITY_YEARS = {
    "2Y": 2.0,
    "3Y": 3.0,
    "5Y": 5.0,
    "7Y": 7.0,
    "10Y": 10.0,
    "20Y": 20.0,
    "30Y": 30.0,
    "2Y_FRN": 2.0,
}
_FRED_SERIES_BY_TENOR = {
    "2Y": "DGS2",
    "3Y": "DGS3",
    "5Y": "DGS5",
    "7Y": "DGS7",
    "10Y": "DGS10",
    "20Y": "DGS20",
    "30Y": "DGS30",
}
_FRED_MATURITY_BY_SERIES = {
    "DGS2": 2.0,
    "DGS3": 3.0,
    "DGS5": 5.0,
    "DGS7": 7.0,
    "DGS10": 10.0,
    "DGS20": 20.0,
    "DGS30": 30.0,
}
_FRN_EFFECTIVE_DURATION_YEARS = 0.25
_DV01_PER_1BN_SCALE = 100000.0

_NOMINAL_ROW_RE = re.compile(
    r"(?P<month>[A-Z][a-z]{2}-\d{2}):\s*"
    r"2Y=(?P<two>\d+(?:\.\d+)?),\s*"
    r"3Y=(?P<three>\d+(?:\.\d+)?),\s*"
    r"5Y=(?P<five>\d+(?:\.\d+)?),\s*"
    r"7Y=(?P<seven>\d+(?:\.\d+)?),\s*"
    r"10Y=(?P<ten>\d+(?:\.\d+)?),\s*"
    r"20Y=(?P<twenty>\d+(?:\.\d+)?),\s*"
    r"30Y=(?P<thirty>\d+(?:\.\d+)?)"
)
_FRN_ROW_RE = re.compile(
    r"(?P<month>[A-Z][a-z]{2}-\d{2}):\s*2Y FRN=(?P<frn>\d+(?:\.\d+)?)"
)


def _month_label_to_timestamp(label: str) -> pd.Timestamp:
    month_text, year_suffix = label.split("-")
    year = 2000 + int(year_suffix)
    month = _MONTH_NUMBERS[month_text]
    return pd.Timestamp(year=year, month=month, day=1)


def _empty_schedule_components_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "event_id",
            "quarter",
            "qra_release_date",
            "previous_event_id",
            "previous_quarter",
            "tenor",
            "issue_type",
            "current_total_bn",
            "previous_total_bn",
            "delta_bn",
            "tenor_weight_10y_eq",
            "contribution_10y_eq_bn",
            "yield_date",
            "yield_curve_source",
            "tenor_yield_pct",
            "tenor_modified_duration",
            "duration_factor_source",
            "dynamic_10y_eq_weight",
            "contribution_dynamic_10y_eq_bn",
            "dv01_per_1bn_usd",
            "dv01_contribution_usd",
        ]
    )


def build_qra_schedule_table(
    capture_template: pd.DataFrame,
    release_calendar: pd.DataFrame,
) -> pd.DataFrame:
    if capture_template.empty or release_calendar.empty:
        return pd.DataFrame(
            columns=[
                "event_id",
                "quarter",
                "qra_release_date",
                "auction_month",
                "auction_month_position",
                "tenor",
                "issue_type",
                "announced_size_bn",
                "source_field",
            ]
        )

    calendar = release_calendar[["event_id", "quarter", "policy_statement_release_date"]].copy()
    calendar["policy_statement_release_date"] = pd.to_datetime(calendar["policy_statement_release_date"], errors="coerce")

    capture = capture_template.copy()
    capture["qra_release_date"] = pd.to_datetime(capture["qra_release_date"], errors="coerce")
    merged = capture.merge(
        calendar,
        left_on=["quarter", "qra_release_date"],
        right_on=["quarter", "policy_statement_release_date"],
        how="left",
    )

    rows: list[dict[str, object]] = []
    for _, row in merged.iterrows():
        event_id = row.get("event_id")
        quarter = row.get("quarter")
        qra_release_date = row.get("qra_release_date")
        if pd.isna(qra_release_date) or not str(event_id or "").strip():
            continue

        nominal_rows = _extract_nominal_schedule_rows(row.get("guidance_nominal_coupons"))
        frn_rows = _extract_frn_schedule_rows(row.get("guidance_frns"))
        position = 0
        for month_label, tenor_map in nominal_rows:
            position += 1
            auction_month = _month_label_to_timestamp(month_label)
            for tenor, value in tenor_map.items():
                rows.append(
                    {
                        "event_id": event_id,
                        "quarter": quarter,
                        "qra_release_date": qra_release_date,
                        "auction_month": auction_month,
                        "auction_month_position": position,
                        "tenor": tenor,
                        "issue_type": "nominal_coupon",
                        "announced_size_bn": float(value),
                        "source_field": "guidance_nominal_coupons",
                    }
                )
        for frn_index, (month_label, frn_value) in enumerate(frn_rows, start=1):
            auction_month = _month_label_to_timestamp(month_label)
            matching_position = next(
                (
                    int(existing["auction_month_position"])
                    for existing in rows
                    if existing["event_id"] == event_id and existing["auction_month"] == auction_month
                ),
                None,
            )
            rows.append(
                {
                    "event_id": event_id,
                    "quarter": quarter,
                    "qra_release_date": qra_release_date,
                    "auction_month": auction_month,
                    "auction_month_position": matching_position if matching_position is not None else frn_index,
                    "tenor": "2Y_FRN",
                    "issue_type": "frn",
                    "announced_size_bn": float(frn_value),
                    "source_field": "guidance_frns",
                }
            )

    output = pd.DataFrame(rows)
    if output.empty:
        return output
    output = output.sort_values(
        ["qra_release_date", "auction_month", "issue_type", "tenor"],
        kind="stable",
    ).reset_index(drop=True)
    return output


def _extract_nominal_schedule_rows(value: object) -> list[tuple[str, dict[str, float]]]:
    text = str(value or "")
    rows: list[tuple[str, dict[str, float]]] = []
    for match in _NOMINAL_ROW_RE.finditer(text):
        rows.append(
            (
                match.group("month"),
                {
                    "2Y": float(match.group("two")),
                    "3Y": float(match.group("three")),
                    "5Y": float(match.group("five")),
                    "7Y": float(match.group("seven")),
                    "10Y": float(match.group("ten")),
                    "20Y": float(match.group("twenty")),
                    "30Y": float(match.group("thirty")),
                },
            )
        )
    return rows


def _extract_frn_schedule_rows(value: object) -> list[tuple[str, float]]:
    text = str(value or "")
    return [(match.group("month"), float(match.group("frn"))) for match in _FRN_ROW_RE.finditer(text)]


def _prepare_yield_curve(yield_curve: pd.DataFrame | None) -> pd.DataFrame:
    if yield_curve is None or yield_curve.empty:
        return pd.DataFrame(columns=["date", *_FRED_MATURITY_BY_SERIES.keys()])
    output = yield_curve.copy()
    if "date" not in output.columns:
        raise KeyError("Yield curve frame must include a date column")
    output["date"] = pd.to_datetime(output["date"], errors="coerce")
    output = output.dropna(subset=["date"]).copy()
    for column in _FRED_MATURITY_BY_SERIES:
        if column in output.columns:
            output[column] = pd.to_numeric(output[column], errors="coerce")
    output = output.sort_values("date", kind="stable").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    return output


def _lookup_yield_snapshot(yield_curve: pd.DataFrame, anchor_date: object) -> tuple[pd.Timestamp | pd.NaT, object, pd.Series | None]:
    if yield_curve.empty:
        return pd.NaT, pd.NA, None
    anchor = pd.to_datetime(anchor_date, errors="coerce")
    if pd.isna(anchor):
        return pd.NaT, pd.NA, None
    dates = yield_curve["date"]
    idx = dates.searchsorted(anchor, side="right") - 1
    if idx < 0:
        return pd.NaT, pd.NA, None
    row = yield_curve.iloc[int(idx)]
    yield_date = pd.Timestamp(row["date"])
    if yield_date.normalize() == anchor.normalize():
        source = "fred_constant_maturity_exact_date"
    else:
        source = "fred_constant_maturity_prior_business_day"
    return yield_date, source, row


def _curve_points(snapshot: pd.Series | None) -> list[tuple[float, float]]:
    if snapshot is None:
        return []
    points: list[tuple[float, float]] = []
    for series_id, maturity in sorted(_FRED_MATURITY_BY_SERIES.items(), key=lambda item: item[1]):
        if series_id not in snapshot.index:
            continue
        value = pd.to_numeric(pd.Series([snapshot[series_id]]), errors="coerce").iloc[0]
        if pd.isna(value):
            continue
        points.append((maturity, float(value)))
    return points


def _yield_for_tenor(snapshot: pd.Series | None, tenor: str) -> tuple[float, str]:
    if tenor == "2Y_FRN":
        return np.nan, "frn_convention"
    maturity = _TENOR_MATURITY_YEARS.get(str(tenor))
    if maturity is None or snapshot is None:
        return np.nan, "missing_yield_curve"
    series_id = _FRED_SERIES_BY_TENOR.get(str(tenor))
    if series_id and series_id in snapshot.index:
        exact_value = pd.to_numeric(pd.Series([snapshot[series_id]]), errors="coerce").iloc[0]
        if not pd.isna(exact_value):
            return float(exact_value), "fred_exact"
    points = _curve_points(snapshot)
    if not points:
        return np.nan, "missing_yield_curve"
    maturities = [point[0] for point in points]
    values = [point[1] for point in points]
    if maturity <= maturities[0]:
        return float(values[0]), "fred_extrapolated"
    if maturity >= maturities[-1]:
        return float(values[-1]), "fred_extrapolated"
    for index in range(1, len(points)):
        lower_maturity, lower_value = points[index - 1]
        upper_maturity, upper_value = points[index]
        if lower_maturity <= maturity <= upper_maturity:
            if maturity == lower_maturity:
                return float(lower_value), "fred_exact"
            if maturity == upper_maturity:
                return float(upper_value), "fred_exact"
            slope = (upper_value - lower_value) / (upper_maturity - lower_maturity)
            interpolated = lower_value + slope * (maturity - lower_maturity)
            return float(interpolated), "fred_interpolated"
    return np.nan, "missing_yield_curve"


def _par_bond_modified_duration(maturity_years: float, yield_pct: float) -> float:
    if pd.isna(maturity_years) or pd.isna(yield_pct) or float(maturity_years) <= 0.0:
        return np.nan
    periods = int(round(float(maturity_years) * 2))
    if periods <= 0:
        return np.nan
    periodic_yield = float(yield_pct) / 100.0 / 2.0
    cashflows = np.full(periods, periodic_yield, dtype=float)
    cashflows[-1] += 1.0
    times = np.arange(1, periods + 1, dtype=float)
    discount_factors = np.power(1.0 + periodic_yield, times)
    present_values = cashflows / discount_factors
    price = float(present_values.sum())
    if price == 0.0:
        return np.nan
    macaulay_periods = float((times * present_values).sum() / price)
    return macaulay_periods / (2.0 * (1.0 + periodic_yield))


def build_qra_schedule_diff_components(
    schedule_table: pd.DataFrame,
    yield_curve: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if schedule_table.empty:
        return _empty_schedule_components_frame()

    grouped = (
        schedule_table.groupby(["event_id", "quarter", "qra_release_date", "tenor", "issue_type"], dropna=False)["announced_size_bn"]
        .sum()
        .reset_index(name="current_total_bn")
    )
    event_order = (
        grouped[["event_id", "quarter", "qra_release_date"]]
        .drop_duplicates()
        .sort_values(["qra_release_date", "quarter"], kind="stable")
        .reset_index(drop=True)
    )
    previous_by_event: dict[str, tuple[str, str]] = {}
    previous_event_id: str | None = None
    previous_quarter: str | None = None
    for _, row in event_order.iterrows():
        previous_by_event[str(row["event_id"])] = (
            previous_event_id or "",
            previous_quarter or "",
        )
        previous_event_id = str(row["event_id"])
        previous_quarter = str(row["quarter"])

    prepared_yield_curve = _prepare_yield_curve(yield_curve)
    totals_lookup = grouped.set_index(["event_id", "tenor", "issue_type"])["current_total_bn"].to_dict()
    records: list[dict[str, object]] = []
    for _, row in grouped.iterrows():
        event_id = str(row["event_id"])
        prior_event_id, prior_quarter = previous_by_event.get(event_id, ("", ""))
        previous_total: float | None = None
        if prior_event_id:
            previous_total = float(totals_lookup.get((prior_event_id, row["tenor"], row["issue_type"]), 0.0))
        delta_bn = np.nan if previous_total is None else float(row["current_total_bn"]) - previous_total
        weight = float(TENOR_10Y_EQ_WEIGHTS.get(str(row["tenor"]), 0.0))

        yield_date, yield_curve_source, snapshot = _lookup_yield_snapshot(prepared_yield_curve, row["qra_release_date"])
        tenor_yield_pct, duration_factor_source = _yield_for_tenor(snapshot, str(row["tenor"]))
        if str(row["tenor"]) == "2Y_FRN":
            tenor_modified_duration = _FRN_EFFECTIVE_DURATION_YEARS
        else:
            tenor_modified_duration = _par_bond_modified_duration(_TENOR_MATURITY_YEARS[str(row["tenor"])], tenor_yield_pct)
        ten_year_yield_pct, _ = _yield_for_tenor(snapshot, "10Y")
        ten_year_modified_duration = _par_bond_modified_duration(10.0, ten_year_yield_pct)
        dynamic_10y_eq_weight = np.nan
        if not pd.isna(tenor_modified_duration) and not pd.isna(ten_year_modified_duration) and float(ten_year_modified_duration) != 0.0:
            dynamic_10y_eq_weight = float(tenor_modified_duration) / float(ten_year_modified_duration)
        contribution_dynamic_10y_eq_bn = np.nan
        if not pd.isna(delta_bn) and not pd.isna(dynamic_10y_eq_weight):
            contribution_dynamic_10y_eq_bn = float(delta_bn) * float(dynamic_10y_eq_weight)
        dv01_per_1bn_usd = np.nan if pd.isna(tenor_modified_duration) else float(tenor_modified_duration) * _DV01_PER_1BN_SCALE
        dv01_contribution_usd = np.nan
        if not pd.isna(delta_bn) and not pd.isna(dv01_per_1bn_usd):
            dv01_contribution_usd = float(delta_bn) * float(dv01_per_1bn_usd)

        records.append(
            {
                "event_id": event_id,
                "quarter": row["quarter"],
                "qra_release_date": row["qra_release_date"],
                "previous_event_id": prior_event_id or pd.NA,
                "previous_quarter": prior_quarter or pd.NA,
                "tenor": row["tenor"],
                "issue_type": row["issue_type"],
                "current_total_bn": float(row["current_total_bn"]),
                "previous_total_bn": previous_total if previous_total is not None else pd.NA,
                "delta_bn": delta_bn,
                "tenor_weight_10y_eq": weight,
                "contribution_10y_eq_bn": np.nan if pd.isna(delta_bn) else delta_bn * weight,
                "yield_date": yield_date if not pd.isna(yield_date) else pd.NaT,
                "yield_curve_source": yield_curve_source,
                "tenor_yield_pct": tenor_yield_pct,
                "tenor_modified_duration": tenor_modified_duration,
                "duration_factor_source": duration_factor_source,
                "dynamic_10y_eq_weight": dynamic_10y_eq_weight,
                "contribution_dynamic_10y_eq_bn": contribution_dynamic_10y_eq_bn,
                "dv01_per_1bn_usd": dv01_per_1bn_usd,
                "dv01_contribution_usd": dv01_contribution_usd,
            }
        )

    output = pd.DataFrame(records)
    if output.empty:
        return _empty_schedule_components_frame()
    output = output.sort_values(["qra_release_date", "issue_type", "tenor"], kind="stable").reset_index(drop=True)
    return output


def build_qra_schedule_shock_summary(components: pd.DataFrame) -> pd.DataFrame:
    if components.empty:
        return pd.DataFrame(
            columns=[
                "event_id",
                "quarter",
                "qra_release_date",
                "previous_event_id",
                "previous_quarter",
                "gross_notional_delta_bn",
                "schedule_diff_10y_eq_bn",
                "schedule_diff_dynamic_10y_eq_bn",
                "schedule_diff_dv01_usd",
            ]
        )
    grouped = (
        components.groupby(["event_id", "quarter", "qra_release_date", "previous_event_id", "previous_quarter"], dropna=False)
        .agg(
            gross_notional_delta_bn=("delta_bn", lambda s: s.sum(min_count=1)),
            schedule_diff_10y_eq_bn=("contribution_10y_eq_bn", lambda s: s.sum(min_count=1)),
            schedule_diff_dynamic_10y_eq_bn=("contribution_dynamic_10y_eq_bn", lambda s: s.sum(min_count=1)),
            schedule_diff_dv01_usd=("dv01_contribution_usd", lambda s: s.sum(min_count=1)),
        )
        .reset_index()
    )
    return grouped.sort_values(["qra_release_date", "quarter"], kind="stable").reset_index(drop=True)
