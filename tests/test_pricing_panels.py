from __future__ import annotations

import pandas as pd
import pytest

from ati_shadow_policy.research import pricing_panels


def _monthly_fred_frame() -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=270, freq="D")
    t = pd.Series(range(len(dates)), dtype=float)
    return pd.DataFrame(
        {
            "date": dates,
            "THREEFYTP10": 1.0 + 0.001 * t,
            "DGS10": 4.0 + 0.001 * t,
            "DGS30": 4.2 + 0.0012 * t,
            "DGS2": 4.5 + 0.0008 * t,
            "DFF": 2.0 + 0.001 * t,
            "WDTGAL": 500_000.0 + 100.0 * t,
        }
    )


def test_build_official_ati_price_panel_carries_forward_from_each_release_date() -> None:
    official_capture = pd.DataFrame(
        {
            "qra_release_date": ["2024-01-15", "2024-04-12", "2024-08-20"],
            "total_financing_need_bn": [100.0, 200.0, 300.0],
            "net_bill_issuance_bn": [20.0, 30.0, 40.0],
            "quarter": ["2024Q1", "2024Q2", "2024Q3"],
        }
    )

    panel = pricing_panels.build_official_ati_price_panel(official_capture, _monthly_fred_frame())

    jan = panel.loc[panel["date"] == pd.Timestamp("2024-01-31"), "ati_baseline_bn"].iloc[0]
    mar = panel.loc[panel["date"] == pd.Timestamp("2024-03-31"), "ati_baseline_bn"].iloc[0]
    may = panel.loc[panel["date"] == pd.Timestamp("2024-05-31"), "ati_baseline_bn"].iloc[0]
    sep = panel.loc[panel["date"] == pd.Timestamp("2024-09-30"), "ati_baseline_bn"].iloc[0]

    assert jan == 2.0
    assert mar == 2.0
    assert may == -6.0
    assert sep == -14.0
    assert (
        panel.loc[panel["date"] == pd.Timestamp("2024-01-31"), "qra_release_date"].iloc[0]
        == pd.Timestamp("2024-01-15")
    )
    assert (
        panel.loc[panel["date"] == pd.Timestamp("2024-05-31"), "qra_release_date"].iloc[0]
        == pd.Timestamp("2024-04-12")
    )


def test_build_mspd_stock_excess_bills_panel_builds_bill_share_prioritized_by_data_family() -> None:
    mspd = pd.DataFrame(
        {
            "record_date": [
                "2024-01-31",
                "2024-01-31",
                "2024-02-29",
                "2024-02-29",
                "2024-02-29",
            ],
            "security_class1_desc": [
                "Total Marketable",
                "Bills Maturity Value",
                "Total Marketable",
                "Bills Maturity Value",
                "Bills Maturity Value",
            ],
            "security_class2_desc": [
                "",
                "Total Treasury Bills",
                "",
                "Total Matured Treasury Bills",
                "Total Unmatured Treasury Bills",
            ],
            "outstanding_amt": [
                1_000_000.0,
                400_000.0,
                1_200_000.0,
                300_000.0,
                500_000.0,
            ],
        }
    )

    panel = pricing_panels.build_mspd_stock_excess_bills_panel(mspd)

    jan = panel.loc[panel["date"] == pd.Timestamp("2024-01-31")].iloc[0]
    feb = panel.loc[panel["date"] == pd.Timestamp("2024-02-29")].iloc[0]

    assert jan["marketable_outstanding_bn"] == 1000.0
    assert jan["marketable_bill_share"] == 0.4
    assert jan["stock_excess_bills_share"] == pytest.approx(0.22)
    assert jan["stock_excess_bills_bn"] == pytest.approx(220.0)

    assert feb["marketable_bill_share"] == (800_000.0 / 1_200_000.0)
    assert feb["stock_excess_bills_share"] == (800_000.0 / 1_200_000.0) - 0.18
    assert feb["stock_excess_bills_bn"] == 800_000.0 / 1_000.0 - 0.18 * 1_200.0


def test_build_pricing_panels_monthly_and_weekly_outcomes_are_aligned() -> None:
    official_capture = pd.DataFrame(
        {
            "quarter": ["2024Q1", "2024Q2"],
            "qra_release_date": ["2024-01-01", "2024-04-01"],
            "total_financing_need_bn": [100.0, 110.0],
            "net_bill_issuance_bn": [20.0, 25.0],
            "notes": [
                "Routine financing quarter.",
                "Debt limit and extraordinary measures remain binding through the quarter.",
            ],
        }
    )
    mspd = pd.DataFrame(
        {
            "date": ["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30"],
            "marketable_bill_share": [0.2, 0.21, 0.22, 0.24],
            "marketable_outstanding_bn": [1000.0, 1020.0, 1040.0, 1080.0],
            "stock_excess_bills_share": [0.02, 0.03, 0.04, 0.06],
            "stock_excess_bills_bn": [2.0, 3.0, 4.0, 6.0],
        }
    )
    public_duration = pd.DataFrame(
        {
            "date": pd.date_range("2024-03-20", periods=8, freq="W-WED"),
            "headline_public_duration_supply": 10_000_000_000.0,
            "buybacks_accepted": 1_000_000_000.0,
        }
    )
    public_duration["date"] = pd.to_datetime(public_duration["date"]).dt.strftime("%Y-%m-%d")
    plumbing_weekly = pd.DataFrame(
        {
            "date": pd.to_datetime(public_duration["date"]),
            "qt_proxy": [-2_000.0] * len(public_duration),
            "delta_wdtgal": [pd.NA, 50.0, -25.0, 10.0, 0.0, -5.0, 8.0, 12.0],
        }
    )

    fred = _monthly_fred_frame()
    fred["date"] = pd.to_datetime(fred["date"])

    official_panel = pricing_panels.build_official_ati_price_panel(
        official_capture,
        fred,
        mspd_stock_panel=mspd,
    )
    weekly_panel = pricing_panels.build_weekly_supply_price_panel(
        public_duration,
        fred,
        plumbing_weekly_panel=plumbing_weekly,
        official_capture=official_capture,
    )

    assert official_panel["date"].dtype.kind == "M"
    assert official_panel["date"].dt.is_month_end.all()
    assert {"THREEFYTP10", "DGS10", "DGS30", "slope_10y_2y", "slope_30y_2y"}.issubset(set(official_panel.columns))
    assert {"bill_share", "ati_baseline_bn_posonly", "missing_coupons_18_bn"}.issubset(set(official_panel.columns))
    assert official_panel["DGS10"].iloc[0] > 100.0
    assert weekly_panel["date"].dt.day_name().eq("Wednesday").all()
    assert len(weekly_panel) == len(public_duration)
    assert {"THREEFYTP10", "DGS10", "DGS30", "slope_10y_2y", "slope_30y_2y", "delta_wdtgal"}.issubset(set(weekly_panel.columns))
    assert weekly_panel["headline_public_duration_supply"].iloc[0] == 10.0
    assert weekly_panel["qt_proxy"].iloc[0] == -2.0
    assert pd.isna(weekly_panel["delta_wdtgal"].iloc[0])
    assert weekly_panel["delta_wdtgal"].iloc[1:].notna().all()
    assert official_panel.loc[official_panel["date"] == pd.Timestamp("2024-04-30"), "debt_limit_dummy"].iloc[0] == 1
    assert weekly_panel["debt_limit_dummy"].sum() > 0
