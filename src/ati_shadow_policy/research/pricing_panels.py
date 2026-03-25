from __future__ import annotations

import pandas as pd
from pandas.tseries.offsets import BDay

from ..io_utils import coerce_numeric
from .ati_index import build_ati_index

TARGET_TAU = 0.18
MSPD_MONEY_UNITS_TO_BILLION = 1_000.0
RAW_DOLLARS_TO_BILLION = 1_000_000_000.0
MILLIONS_TO_BILLION = 1_000.0
RATE_PERCENT_TO_BPS = 100.0
RELEASE_FLOW_HORIZONS_BD = (1, 5, 10, 21, 42, 63)
RELEASE_FLOW_PLACEBO_WINDOWS_BD = ((-21, -1), (-5, -1))

MSPD_REQUIRED_COLUMNS = (
    "record_date",
    "security_class1_desc",
    "security_class2_desc",
    "outstanding_amt",
)
MSPD_CLASS1_MARKETABLE = "total marketable"
MSPD_CLASS1_BILLS = "bills maturity value"
MSPD_CLASS2_TOTAL_BILLS = "total treasury bills"
MSPD_CLASS2_TOTAL_MATURED_BILLS = "total matured treasury bills"
MSPD_CLASS2_TOTAL_UNMATURED_BILLS = "total unmatured treasury bills"

OFFICIAL_PANEL_REQUIRED_COLUMNS = (
    "qra_release_date",
    "total_financing_need_bn",
    "net_bill_issuance_bn",
    "quarter",
)
DEBT_LIMIT_KEYWORDS = (
    "debt limit",
    "debt ceiling",
    "extraordinary measures",
    "extraordinary-measures",
)
ATI_RELEASE_PANEL_COLUMNS = (
    "qra_release_date",
    "quarter",
    "bill_share",
    "missing_coupons_15_bn",
    "missing_coupons_15_bn_posonly",
    "missing_coupons_18_bn",
    "missing_coupons_18_bn_posonly",
    "missing_coupons_20_bn",
    "missing_coupons_20_bn_posonly",
    "ati_baseline_bn",
    "ati_baseline_bn_posonly",
    "cumulative_ati_baseline_bn",
)

def release_flow_window_label(horizon_bd: int) -> str:
    return f"release_plus_{int(horizon_bd)}bd"


def release_flow_placebo_label(start_bd: int, end_bd: int) -> str:
    return f"release_minus_{abs(int(start_bd))}bd_to_minus_{abs(int(end_bd))}bd"


def release_flow_end_date_column(horizon_bd: int) -> str:
    return f"{release_flow_window_label(horizon_bd)}_end_date"


def release_flow_placebo_start_date_column(start_bd: int, end_bd: int) -> str:
    return f"{release_flow_placebo_label(start_bd, end_bd)}_start_date"


def release_flow_delta_column(series_name: str, window_label: str) -> str:
    return f"delta_{series_name.lower()}_{window_label}"


def release_flow_control_column(window_label: str) -> str:
    return f"delta_dff_{window_label}"


def _release_flow_panel_columns() -> tuple[str, ...]:
    columns = [
        "release_id",
        "quarter",
        "source_quarters",
        "release_row_count",
        "qra_release_date",
        "market_pricing_marker_minus_1d",
        "total_financing_need_bn",
        "net_bill_issuance_bn",
        "bill_share",
        "ati_baseline_bn",
        "ati_baseline_bn_posonly",
        "debt_limit_dummy",
        "target_tau",
        "DGS10",
        "THREEFYTP10",
        "DGS30",
    ]
    for horizon_bd in RELEASE_FLOW_HORIZONS_BD:
        window_label = release_flow_window_label(horizon_bd)
        columns.append(release_flow_end_date_column(horizon_bd))
        for series_name in ("DGS10", "THREEFYTP10", "DGS30"):
            columns.append(release_flow_delta_column(series_name, window_label))
        columns.append(release_flow_control_column(window_label))
    for start_bd, end_bd in RELEASE_FLOW_PLACEBO_WINDOWS_BD:
        window_label = release_flow_placebo_label(start_bd, end_bd)
        columns.append(release_flow_placebo_start_date_column(start_bd, end_bd))
        for series_name in ("DGS10", "THREEFYTP10", "DGS30"):
            columns.append(release_flow_delta_column(series_name, window_label))
        columns.append(release_flow_control_column(window_label))
    return tuple(columns)


RELEASE_FLOW_PANEL_COLUMNS = _release_flow_panel_columns()


def _coerce_date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def _normalize_text(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.lower()


def _require_columns(frame: pd.DataFrame, required: tuple[str, ...], where: str) -> None:
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise KeyError(f"{where} missing required columns: {missing}")


def _sum_by_date(frame: pd.DataFrame, condition: pd.Series, out_col: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["record_date", out_col])
    selected = frame.loc[condition, ["record_date", "outstanding_bn"]].copy()
    if selected.empty:
        return pd.DataFrame(columns=["record_date", out_col])
    grouped = selected.groupby("record_date", as_index=False)["outstanding_bn"].sum()
    return grouped.rename(columns={"outstanding_bn": out_col})


def _select_bill_total_rows(frame: pd.DataFrame) -> pd.DataFrame:
    class1 = _normalize_text(frame["security_class1_desc"])
    class2 = _normalize_text(frame["security_class2_desc"])

    bills = class1 == MSPD_CLASS1_BILLS

    total_bills_rows = _sum_by_date(
        frame,
        (bills) & (class2 == MSPD_CLASS2_TOTAL_BILLS),
        "bill_outstanding_bn",
    )
    matured_bills = _sum_by_date(
        frame,
        (bills) & (class2 == MSPD_CLASS2_TOTAL_MATURED_BILLS),
        "bill_outstanding_bn",
    )
    unmatured_bills = _sum_by_date(
        frame,
        (bills) & (class2 == MSPD_CLASS2_TOTAL_UNMATURED_BILLS),
        "bill_outstanding_bn",
    )
    all_maturity = pd.merge(
        matured_bills.rename(columns={"bill_outstanding_bn": "matured_bn"}),
        unmatured_bills.rename(columns={"bill_outstanding_bn": "unmatured_bn"}),
        on="record_date",
        how="outer",
    )
    if not all_maturity.empty:
        all_maturity["bill_outstanding_bn"] = (
            all_maturity["matured_bn"].fillna(0.0) + all_maturity["unmatured_bn"].fillna(0.0)
        )
        all_maturity = all_maturity[["record_date", "bill_outstanding_bn"]]

    fallback_all_bills = _sum_by_date(
        frame,
        bills,
        "bill_outstanding_bn",
    )
    if total_bills_rows.empty and all_maturity.empty and fallback_all_bills.empty:
        return pd.DataFrame(columns=["record_date", "bill_outstanding_bn"])

    merged = total_bills_rows.merge(
        all_maturity, on="record_date", how="outer", suffixes=("_total", "_maturity")
    ).merge(
        fallback_all_bills.rename(columns={"bill_outstanding_bn": "bill_outstanding_bn_all"}),
        on="record_date",
        how="outer",
    )

    merged["bill_outstanding_bn"] = merged["bill_outstanding_bn_total"].combine_first(merged["bill_outstanding_bn_maturity"])
    merged["bill_outstanding_bn"] = merged["bill_outstanding_bn"].fillna(merged["bill_outstanding_bn_all"])
    merged = merged[["record_date", "bill_outstanding_bn"]].sort_values("record_date").reset_index(drop=True)
    return merged


def build_mspd_stock_excess_bills_panel(mspd_frame: pd.DataFrame) -> pd.DataFrame:
    """Build MSPD monthly stock panel with excess-bill construction.

    The bill outstanding construction resolves in this priority:
      1) bills maturity "total treasury bills"
      2) fallback to total matured + total unmatured bill rows
      3) fallback to all bills maturity value rows
    """
    _require_columns(mspd_frame, MSPD_REQUIRED_COLUMNS, "MSPD frame")

    raw = mspd_frame.copy()
    raw["record_date"] = _coerce_date_series(raw["record_date"])
    raw["outstanding_bn"] = coerce_numeric(raw["outstanding_amt"]) / MSPD_MONEY_UNITS_TO_BILLION
    raw = raw.dropna(subset=["record_date"]).sort_values("record_date").reset_index(drop=True)
    if raw.empty:
        return pd.DataFrame(columns=[
            "date",
            "marketable_outstanding_bn",
            "marketable_bill_share",
            "stock_excess_bills_share",
            "stock_excess_bills_bn",
        ])

    marketable_out = _sum_by_date(raw, _normalize_text(raw["security_class1_desc"]) == MSPD_CLASS1_MARKETABLE, "marketable_outstanding_bn")
    bill_out = _select_bill_total_rows(raw)

    out = marketable_out.merge(bill_out, on="record_date", how="outer").sort_values("record_date").reset_index(drop=True)
    out["marketable_bill_share"] = out["bill_outstanding_bn"] / out["marketable_outstanding_bn"]
    out["stock_excess_bills_share"] = out["marketable_bill_share"] - TARGET_TAU
    out["stock_excess_bills_bn"] = out["stock_excess_bills_share"] * out["marketable_outstanding_bn"]
    out = out.rename(columns={"record_date": "date"})
    return out[
        [
            "date",
            "marketable_outstanding_bn",
            "marketable_bill_share",
            "stock_excess_bills_share",
            "stock_excess_bills_bn",
        ]
    ]


def _build_ati_release_panel(official_capture: pd.DataFrame) -> pd.DataFrame:
    _require_columns(official_capture, OFFICIAL_PANEL_REQUIRED_COLUMNS, "official capture")
    capture = official_capture.copy()
    capture["qra_release_date"] = _coerce_date_series(capture["qra_release_date"])
    capture = capture.dropna(subset=["qra_release_date"]).sort_values("qra_release_date").reset_index(drop=True)

    index_input = pd.DataFrame(
        {
            "quarter": capture["quarter"],
            "qra_release_date": capture["qra_release_date"],
            "financing_need_bn": coerce_numeric(capture["total_financing_need_bn"]),
            "net_bills_bn": coerce_numeric(capture["net_bill_issuance_bn"]),
        }
    )
    if index_input.empty:
        return pd.DataFrame(columns=ATI_RELEASE_PANEL_COLUMNS)

    index_input = index_input.dropna(subset=["financing_need_bn", "net_bills_bn"])
    if index_input.empty:
        return pd.DataFrame(columns=ATI_RELEASE_PANEL_COLUMNS)

    with_ati = build_ati_index(index_input).sort_values("qra_release_date").reset_index(drop=True)
    with_ati["cumulative_ati_baseline_bn"] = with_ati["ati_baseline_bn"].fillna(0.0).cumsum()
    return with_ati.reindex(columns=ATI_RELEASE_PANEL_COLUMNS)


def _last_completed_month_end(dates: pd.Series) -> pd.Timestamp | None:
    cleaned = _coerce_date_series(dates).dropna().sort_values()
    if cleaned.empty:
        return None
    latest = cleaned.iloc[-1].normalize()
    current_month_end = latest + pd.offsets.MonthEnd(0)
    if latest == current_month_end:
        return current_month_end
    prior_month_end = latest + pd.offsets.MonthEnd(-1)
    return prior_month_end if prior_month_end >= cleaned.iloc[0].normalize() else None


def _prepare_monthly_fred_panel(fred: pd.DataFrame, outcome_cols: tuple[str, ...]) -> pd.DataFrame:
    _require_columns(fred, ("date",), "FRED core panel")
    panel = fred.copy()
    panel["date"] = _coerce_date_series(panel["date"])
    panel = panel.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    for col in panel.columns:
        if col == "date":
            continue
        panel[col] = coerce_numeric(panel[col])

    monthly = panel.set_index("date").resample("ME").last().reset_index()
    last_completed_month_end = _last_completed_month_end(panel["date"])
    if last_completed_month_end is None:
        return pd.DataFrame(columns=["date", *outcome_cols, "slope_10y_2y", "slope_30y_2y"])
    monthly = monthly.loc[monthly["date"] <= last_completed_month_end].copy()
    keep = [c for c in outcome_cols if c in monthly.columns]
    monthly = monthly[["date", *keep]].copy()
    rate_columns = [column for column in ("THREEFYTP10", "DGS2", "DGS10", "DGS30") if column in monthly.columns]
    for column in rate_columns:
        monthly[column] = monthly[column] * RATE_PERCENT_TO_BPS
    if "DGS2" in monthly.columns and "DGS10" in monthly.columns:
        monthly["slope_10y_2y"] = monthly["DGS10"] - monthly["DGS2"]
    else:
        monthly["slope_10y_2y"] = pd.NA
    if "DGS30" in monthly.columns and "DGS2" in monthly.columns:
        monthly["slope_30y_2y"] = monthly["DGS30"] - monthly["DGS2"]
    else:
        monthly["slope_30y_2y"] = pd.NA
    return monthly


def _prepare_daily_fred_panel(fred: pd.DataFrame, outcome_cols: tuple[str, ...]) -> pd.DataFrame:
    _require_columns(fred, ("date",), "FRED core panel")
    panel = fred.copy()
    panel["date"] = _coerce_date_series(panel["date"]).dt.normalize()
    panel = panel.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    for col in panel.columns:
        if col == "date":
            continue
        panel[col] = coerce_numeric(panel[col])

    keep = [column for column in outcome_cols if column in panel.columns]
    daily = panel[["date", *keep]].copy()
    for column in ("THREEFYTP10", "DGS10", "DGS30", "DGS2"):
        if column in daily.columns:
            daily[column] = daily[column] * RATE_PERCENT_TO_BPS
    if "DGS10" in daily.columns and "DGS2" in daily.columns:
        daily["slope_10y_2y"] = daily["DGS10"] - daily["DGS2"]
    if "DGS30" in daily.columns and "DGS2" in daily.columns:
        daily["slope_30y_2y"] = daily["DGS30"] - daily["DGS2"]
    return daily


def _first_available_on_or_after(
    panel: pd.DataFrame,
    target_date: pd.Timestamp,
    *,
    date_col: str = "date",
) -> pd.Series | None:
    if panel.empty:
        return None
    match = panel.loc[pd.to_datetime(panel[date_col], errors="coerce") >= target_date].head(1)
    if match.empty:
        return None
    return match.iloc[0]


def _join_unique_strings(values: pd.Series) -> str:
    cleaned = sorted({str(value).strip() for value in values if str(value).strip() and str(value).strip().lower() != "nan"})
    return "|".join(cleaned)


def _debt_limit_dummy_index(panel_dates: pd.Series, debt_limit_dates: pd.DataFrame | None) -> pd.Series:
    if debt_limit_dates is None:
        return pd.Series(0, index=panel_dates.index)
    if debt_limit_dates.empty:
        return pd.Series(0, index=panel_dates.index)

    if {"start_date", "end_date"}.issubset(debt_limit_dates.columns):
        starts = _coerce_date_series(debt_limit_dates["start_date"])
        ends = _coerce_date_series(debt_limit_dates["end_date"])
        normalized = panel_dates.dt.normalize()
        fallback_end = normalized.max()
        indicator = pd.Series(0, index=panel_dates.index, dtype=int)
        for start, end in zip(starts, ends, strict=False):
            if pd.isna(start):
                continue
            bound_end = fallback_end if pd.isna(end) else end.normalize()
            indicator = indicator | (
                (normalized >= start.normalize()) & (normalized <= bound_end)
            ).astype(int)
        return indicator.astype(int)

    if "date" in debt_limit_dates.columns:
        debt_col = debt_limit_dates["date"]
    else:
        debt_col = debt_limit_dates.iloc[:, 0]
    debt_dates = set(_coerce_date_series(debt_col).dropna().dt.normalize().astype("datetime64[ns]"))
    if not debt_dates:
        return pd.Series(0, index=panel_dates.index)
    return panel_dates.dt.normalize().isin(debt_dates).astype(int)


def build_debt_limit_intervals(official_capture: pd.DataFrame) -> pd.DataFrame:
    _require_columns(official_capture, ("qra_release_date", "quarter"), "official capture")
    capture = official_capture.copy()
    capture["qra_release_date"] = _coerce_date_series(capture["qra_release_date"])
    capture = capture.dropna(subset=["qra_release_date"]).sort_values("qra_release_date").reset_index(drop=True)
    if capture.empty:
        return pd.DataFrame(columns=["quarter", "start_date", "end_date", "trigger_text"])

    text_columns = [column for column in ("notes", "source_doc_type", "source_url") if column in capture.columns]
    if text_columns:
        combined_text = capture[text_columns].fillna("").astype(str).agg(" | ".join, axis=1).str.lower()
    else:
        combined_text = pd.Series("", index=capture.index)

    pattern = "|".join(DEBT_LIMIT_KEYWORDS)
    capture["debt_limit_flag"] = combined_text.str.contains(pattern, regex=True)
    capture["next_release_date"] = capture["qra_release_date"].shift(-1)
    capture["end_date"] = capture["next_release_date"] - pd.Timedelta(days=1)
    out = capture.loc[capture["debt_limit_flag"], ["quarter", "qra_release_date", "end_date"]].copy()
    if out.empty:
        return pd.DataFrame(columns=["quarter", "start_date", "end_date", "trigger_text"])
    out = out.rename(columns={"qra_release_date": "start_date"})
    out["trigger_text"] = combined_text.loc[out.index].values
    return out[["quarter", "start_date", "end_date", "trigger_text"]].reset_index(drop=True)


def build_official_ati_price_panel(
    official_capture: pd.DataFrame,
    fred: pd.DataFrame,
    *,
    mspd_stock_panel: pd.DataFrame | None = None,
    debt_limit_dates: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build ATI+yield panel with monthly outcomes and ATI carried forward.

    The ATI shock remains constant from each QRA release date forward until the next release.
    """
    release_panel = _build_ati_release_panel(official_capture)
    derived_debt_limit_dates = build_debt_limit_intervals(official_capture)
    if debt_limit_dates is None and not derived_debt_limit_dates.empty:
        debt_limit_dates = derived_debt_limit_dates
    fred_panel = _prepare_monthly_fred_panel(
        fred,
        ("THREEFYTP10", "DGS10", "DGS30", "DGS2", "DFF"),
    )

    outcome_cols = [c for c in ("THREEFYTP10", "DGS10", "DGS30", "slope_10y_2y", "slope_30y_2y", "DFF") if c in fred_panel.columns]
    outcome_frame = fred_panel[["date", *outcome_cols]].copy()

    if release_panel.empty:
        for column in ATI_RELEASE_PANEL_COLUMNS:
            outcome_frame[column] = pd.NaT if column == "qra_release_date" else pd.NA
    else:
        outcome_frame = pd.merge_asof(
            outcome_frame.sort_values("date"),
            release_panel.sort_values("qra_release_date"),
            left_on="date",
            right_on="qra_release_date",
            direction="backward",
            allow_exact_matches=True,
        )

    if mspd_stock_panel is not None and not mspd_stock_panel.empty:
        stock_panel = mspd_stock_panel.copy()
        if "date" not in stock_panel.columns:
            stock_panel = stock_panel.rename(columns={"record_date": "date"})
        stock_panel["date"] = _coerce_date_series(stock_panel["date"])
        stock_panel = stock_panel.dropna(subset=["date"]).sort_values("date")
        stock_panel = stock_panel[
            [
                c
                for c in [
                    "date",
                    "marketable_bill_share",
                    "marketable_outstanding_bn",
                    "stock_excess_bills_share",
                    "stock_excess_bills_bn",
                ]
                if c in stock_panel.columns
            ]
        ]
        outcome_frame = pd.merge_asof(
            outcome_frame.sort_values("date"),
            stock_panel,
            on="date",
            direction="backward",
            allow_exact_matches=True,
        )

    for column in [
        "bill_share",
        "missing_coupons_15_bn",
        "missing_coupons_15_bn_posonly",
        "missing_coupons_18_bn",
        "missing_coupons_18_bn_posonly",
        "missing_coupons_20_bn",
        "missing_coupons_20_bn_posonly",
        "ati_baseline_bn",
        "ati_baseline_bn_posonly",
        "marketable_bill_share",
        "marketable_outstanding_bn",
        "stock_excess_bills_share",
        "stock_excess_bills_bn",
        "cumulative_ati_baseline_bn",
    ]:
        if column not in outcome_frame.columns:
            outcome_frame[column] = pd.NA

    outcome_frame["debt_limit_dummy"] = _debt_limit_dummy_index(
        outcome_frame["date"],
        debt_limit_dates,
    )

    return outcome_frame[
        [
            "date",
            "quarter",
            "qra_release_date",
            "bill_share",
            "missing_coupons_15_bn",
            "missing_coupons_15_bn_posonly",
            "missing_coupons_18_bn",
            "missing_coupons_18_bn_posonly",
            "missing_coupons_20_bn",
            "missing_coupons_20_bn_posonly",
            "ati_baseline_bn",
            "ati_baseline_bn_posonly",
            "cumulative_ati_baseline_bn",
            "marketable_bill_share",
            "marketable_outstanding_bn",
            "stock_excess_bills_share",
            "stock_excess_bills_bn",
            "THREEFYTP10",
            "DGS10",
            "DGS30",
            "slope_10y_2y",
            "slope_30y_2y",
            "DFF",
            "debt_limit_dummy",
        ]
    ].sort_values("date").reset_index(drop=True)


def build_weekly_supply_price_panel(
    public_duration_supply: pd.DataFrame,
    fred: pd.DataFrame,
    *,
    plumbing_weekly_panel: pd.DataFrame | None = None,
    official_capture: pd.DataFrame | None = None,
    debt_limit_dates: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build weekly supply panel with weekly rate outcomes and slopes."""
    _require_columns(
        public_duration_supply,
        ("date", "headline_public_duration_supply", "buybacks_accepted"),
        "public duration supply",
    )
    _require_columns(fred, ("date",), "FRED core")
    if official_capture is not None and debt_limit_dates is None:
        debt_limit_dates = build_debt_limit_intervals(official_capture)

    duration = public_duration_supply.copy()
    duration["date"] = _coerce_date_series(duration["date"]).dt.normalize()
    duration["headline_public_duration_supply"] = (
        coerce_numeric(duration["headline_public_duration_supply"]) / RAW_DOLLARS_TO_BILLION
    )
    duration["buybacks_accepted"] = (
        coerce_numeric(duration["buybacks_accepted"]) / RAW_DOLLARS_TO_BILLION
    )
    if "qt_proxy" in duration.columns:
        duration["qt_proxy"] = coerce_numeric(duration["qt_proxy"]) / RAW_DOLLARS_TO_BILLION
    else:
        duration["qt_proxy"] = pd.NA
    duration = duration.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if plumbing_weekly_panel is not None and not plumbing_weekly_panel.empty:
        plumbing = plumbing_weekly_panel.copy()
        plumbing["date"] = _coerce_date_series(plumbing["date"]).dt.normalize()
        plumbing = plumbing.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        for column in ("qt_proxy", "delta_wdtgal"):
            if column in plumbing.columns:
                plumbing[column] = coerce_numeric(plumbing[column]) / MILLIONS_TO_BILLION
        merge_columns = [column for column in ("date", "qt_proxy", "delta_wdtgal") if column in plumbing.columns]
        if len(merge_columns) > 1:
            duration = duration.merge(
                plumbing[merge_columns],
                on="date",
                how="left",
                suffixes=("", "_plumbing"),
            )
            if "qt_proxy_plumbing" in duration.columns:
                duration["qt_proxy"] = duration["qt_proxy"].combine_first(duration["qt_proxy_plumbing"])
                duration = duration.drop(columns=["qt_proxy_plumbing"])
            if "delta_wdtgal" not in duration.columns:
                duration["delta_wdtgal"] = pd.NA
    duration["qt_proxy"] = duration["qt_proxy"].fillna(0.0)

    fred_panel = fred.copy()
    fred_panel["date"] = _coerce_date_series(fred_panel["date"])
    for col in fred_panel.columns:
        if col == "date":
            continue
        fred_panel[col] = coerce_numeric(fred_panel[col])
    fred_panel = fred_panel.dropna(subset=["date"]).sort_values("date")

    keep = [c for c in ["THREEFYTP10", "DGS10", "DGS30", "DGS2", "DFF", "WDTGAL"] if c in fred_panel.columns]
    if not keep:
        raise ValueError("FRED core panel does not include any supported weekly outcomes.")
    if "date" not in keep:
        keep.insert(0, "date")

    outcomes = fred_panel[["date", *keep[1:]]].copy()
    merged = pd.merge_asof(
        duration.sort_values("date"),
        outcomes.sort_values("date"),
        on="date",
        direction="backward",
        allow_exact_matches=True,
    )
    for column in ("THREEFYTP10", "DGS10", "DGS30", "DGS2"):
        if column in merged.columns:
            merged[column] = merged[column] * RATE_PERCENT_TO_BPS

    if "delta_wdtgal" in merged.columns:
        merged["delta_wdtgal"] = coerce_numeric(merged["delta_wdtgal"])
    elif "WDTGAL" in merged.columns:
        merged["delta_wdtgal"] = merged["WDTGAL"].diff() / MILLIONS_TO_BILLION
    else:
        merged["delta_wdtgal"] = pd.NA

    if {"DGS10", "DGS2"}.issubset(set(merged.columns)):
        merged["slope_10y_2y"] = merged["DGS10"] - merged["DGS2"]
    else:
        merged["slope_10y_2y"] = pd.NA

    if {"DGS30", "DGS2"}.issubset(set(merged.columns)):
        merged["slope_30y_2y"] = merged["DGS30"] - merged["DGS2"]
    else:
        merged["slope_30y_2y"] = pd.NA

    merged["debt_limit_dummy"] = _debt_limit_dummy_index(
        merged["date"],
        debt_limit_dates,
    )

    return merged[
        [
            "date",
            "headline_public_duration_supply",
            "qt_proxy",
            "buybacks_accepted",
            "delta_wdtgal",
            "THREEFYTP10",
            "DGS10",
            "DGS30",
            "slope_10y_2y",
            "slope_30y_2y",
            "DFF",
            "debt_limit_dummy",
        ]
    ].sort_values("date").reset_index(drop=True)


def build_pricing_release_flow_panel(
    official_capture: pd.DataFrame,
    fred: pd.DataFrame,
    *,
    debt_limit_dates: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build one-row-per-release pricing panel using daily outcomes.

    The start of each pricing window is the market-pricing marker one business day
    before the official QRA release when available, falling back to `qra_release_date - 1BD`.
    """
    release_panel = _build_ati_release_panel(official_capture)
    if release_panel.empty:
        return pd.DataFrame(columns=RELEASE_FLOW_PANEL_COLUMNS)

    capture = official_capture.copy()
    capture["qra_release_date"] = _coerce_date_series(capture["qra_release_date"]).dt.normalize()
    capture = capture.dropna(subset=["qra_release_date"]).sort_values("qra_release_date").reset_index(drop=True)
    if "market_pricing_marker_minus_1d" in capture.columns:
        capture["market_pricing_marker_minus_1d"] = _coerce_date_series(capture["market_pricing_marker_minus_1d"]).dt.normalize()
    else:
        capture["market_pricing_marker_minus_1d"] = capture["qra_release_date"] - BDay(1)
    capture["market_pricing_marker_minus_1d"] = capture["market_pricing_marker_minus_1d"].fillna(
        capture["qra_release_date"] - BDay(1)
    )
    keep_columns = [
        column
        for column in (
            "quarter",
            "qra_release_date",
            "market_pricing_marker_minus_1d",
            "total_financing_need_bn",
            "net_bill_issuance_bn",
        )
        if column in capture.columns
    ]
    release_panel = release_panel.merge(
        capture[keep_columns],
        on=["quarter", "qra_release_date"],
        how="left",
    )
    release_panel["market_pricing_marker_minus_1d"] = _coerce_date_series(
        release_panel["market_pricing_marker_minus_1d"]
    ).dt.normalize()
    release_panel["market_pricing_marker_minus_1d"] = release_panel["market_pricing_marker_minus_1d"].fillna(
        pd.to_datetime(release_panel["qra_release_date"], errors="coerce").dt.normalize() - BDay(1)
    )
    release_panel["qra_release_date"] = _coerce_date_series(release_panel["qra_release_date"]).dt.normalize()
    release_panel["total_financing_need_bn"] = coerce_numeric(release_panel.get("total_financing_need_bn"))
    release_panel["net_bill_issuance_bn"] = coerce_numeric(release_panel.get("net_bill_issuance_bn"))
    release_panel["source_quarters"] = release_panel["quarter"].astype(str).str.strip()
    release_panel["release_row_count"] = 1

    grouped = (
        release_panel.groupby(["qra_release_date", "market_pricing_marker_minus_1d"], as_index=False, dropna=False)
        .agg(
            quarter=("quarter", _join_unique_strings),
            source_quarters=("source_quarters", _join_unique_strings),
            release_row_count=("release_row_count", "sum"),
            total_financing_need_bn=("total_financing_need_bn", "sum"),
            net_bill_issuance_bn=("net_bill_issuance_bn", "sum"),
            ati_baseline_bn=("ati_baseline_bn", "sum"),
            ati_baseline_bn_posonly=("ati_baseline_bn_posonly", "sum"),
        )
    )
    grouped["bill_share"] = grouped["net_bill_issuance_bn"] / grouped["total_financing_need_bn"]
    grouped["target_tau"] = TARGET_TAU
    grouped["release_id"] = grouped.apply(
        lambda row: f"{pd.to_datetime(row['qra_release_date'], errors='coerce').strftime('%Y-%m-%d')}__{str(row['quarter']).replace('|', '+')}",
        axis=1,
    )
    if grouped["market_pricing_marker_minus_1d"].duplicated().any():
        duplicates = grouped.loc[grouped["market_pricing_marker_minus_1d"].duplicated(keep=False), "market_pricing_marker_minus_1d"]
        raise ValueError(f"Release-flow panel requires unique market_pricing_marker_minus_1d values after aggregation; duplicates={duplicates.astype(str).tolist()}")
    release_panel = grouped.sort_values(["qra_release_date", "market_pricing_marker_minus_1d"]).reset_index(drop=True)

    derived_debt_limit_dates = build_debt_limit_intervals(official_capture)
    if debt_limit_dates is None and not derived_debt_limit_dates.empty:
        debt_limit_dates = derived_debt_limit_dates
    release_panel["debt_limit_dummy"] = _debt_limit_dummy_index(
        release_panel["qra_release_date"],
        debt_limit_dates,
    )

    daily_fred = _prepare_daily_fred_panel(
        fred,
        ("THREEFYTP10", "DGS10", "DGS30", "DFF"),
    ).sort_values("date")
    if daily_fred.empty:
        return pd.DataFrame(columns=RELEASE_FLOW_PANEL_COLUMNS)

    rows: list[dict[str, object]] = []
    for _, row in release_panel.iterrows():
        start_date = pd.to_datetime(row["market_pricing_marker_minus_1d"], errors="coerce")
        if pd.isna(start_date):
            continue
        start_obs = _first_available_on_or_after(daily_fred, start_date)
        if start_obs is None:
            continue
        if pd.to_datetime(start_obs["date"], errors="coerce") <= start_date - BDay(1):
            raise ValueError("Release-flow start observation must align to the market-pricing marker or the next available business-day observation.")

        record: dict[str, object] = {
            "release_id": row["release_id"],
            "quarter": row["quarter"],
            "source_quarters": row.get("source_quarters"),
            "release_row_count": row.get("release_row_count"),
            "qra_release_date": row["qra_release_date"],
            "market_pricing_marker_minus_1d": start_obs["date"],
            "total_financing_need_bn": row.get("total_financing_need_bn"),
            "net_bill_issuance_bn": row.get("net_bill_issuance_bn"),
            "bill_share": row.get("bill_share"),
            "ati_baseline_bn": row.get("ati_baseline_bn"),
            "ati_baseline_bn_posonly": row.get("ati_baseline_bn_posonly"),
            "debt_limit_dummy": row.get("debt_limit_dummy", 0),
            "target_tau": row.get("target_tau", TARGET_TAU),
            "DGS10": start_obs.get("DGS10"),
            "THREEFYTP10": start_obs.get("THREEFYTP10"),
            "DGS30": start_obs.get("DGS30"),
        }
        for horizon_bd in RELEASE_FLOW_HORIZONS_BD:
            window_label = release_flow_window_label(horizon_bd)
            horizon_target = pd.to_datetime(row["qra_release_date"], errors="coerce") + BDay(horizon_bd)
            horizon_obs = _first_available_on_or_after(daily_fred, horizon_target)
            record[release_flow_end_date_column(horizon_bd)] = (
                horizon_obs["date"] if horizon_obs is not None else pd.NaT
            )
            if horizon_obs is None:
                for series_name in ("DGS10", "THREEFYTP10", "DGS30"):
                    record[release_flow_delta_column(series_name, window_label)] = pd.NA
                record[release_flow_control_column(window_label)] = pd.NA
                continue
            if pd.to_datetime(horizon_obs["date"], errors="coerce") <= pd.to_datetime(start_obs["date"], errors="coerce"):
                raise ValueError(f"Release-flow horizon {window_label} produced a non-positive pricing window for release_id={row['release_id']}")
            for series_name in ("DGS10", "THREEFYTP10", "DGS30"):
                record[release_flow_delta_column(series_name, window_label)] = horizon_obs.get(series_name) - start_obs.get(series_name)
            record[release_flow_control_column(window_label)] = horizon_obs.get("DFF") - start_obs.get("DFF")

        for start_bd, end_bd in RELEASE_FLOW_PLACEBO_WINDOWS_BD:
            window_label = release_flow_placebo_label(start_bd, end_bd)
            placebo_start_target = pd.to_datetime(row["qra_release_date"], errors="coerce") + BDay(start_bd)
            placebo_start_obs = _first_available_on_or_after(daily_fred, placebo_start_target)
            record[release_flow_placebo_start_date_column(start_bd, end_bd)] = (
                placebo_start_obs["date"] if placebo_start_obs is not None else pd.NaT
            )
            if placebo_start_obs is None:
                for series_name in ("DGS10", "THREEFYTP10", "DGS30"):
                    record[release_flow_delta_column(series_name, window_label)] = pd.NA
                record[release_flow_control_column(window_label)] = pd.NA
                continue
            if pd.to_datetime(placebo_start_obs["date"], errors="coerce") >= pd.to_datetime(start_obs["date"], errors="coerce"):
                raise ValueError(f"Release-flow placebo {window_label} produced a non-positive pricing window for release_id={row['release_id']}")
            for series_name in ("DGS10", "THREEFYTP10", "DGS30"):
                record[release_flow_delta_column(series_name, window_label)] = start_obs.get(series_name) - placebo_start_obs.get(series_name)
            record[release_flow_control_column(window_label)] = start_obs.get("DFF") - placebo_start_obs.get("DFF")
        rows.append(record)

    if not rows:
        return pd.DataFrame(columns=RELEASE_FLOW_PANEL_COLUMNS)

    return pd.DataFrame(rows, columns=RELEASE_FLOW_PANEL_COLUMNS).sort_values(
        ["qra_release_date", "release_id"]
    ).reset_index(drop=True)
