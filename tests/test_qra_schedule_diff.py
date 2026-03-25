from __future__ import annotations

import math

import pandas as pd

from ati_shadow_policy.research.qra_schedule_diff import (
    build_qra_schedule_diff_components,
    build_qra_schedule_shock_summary,
    build_qra_schedule_table,
)


def test_build_qra_schedule_table_normalizes_nominal_and_frn_rows() -> None:
    capture = pd.DataFrame(
        [
            {
                "quarter": "2023Q3",
                "qra_release_date": "2023-05-03",
                "guidance_nominal_coupons": (
                    "Monthly nominal schedule: Aug-23: 2Y=50, 3Y=40, 5Y=45, 7Y=30, 10Y=20, 20Y=10, 30Y=15; "
                    "Sep-23: 2Y=51, 3Y=41, 5Y=46, 7Y=31, 10Y=21, 20Y=11, 30Y=16."
                ),
                "guidance_frns": "Monthly FRN schedule: Aug-23: 2Y FRN=18; Sep-23: 2Y FRN=19.",
            },
            {
                "quarter": "2020Q3",
                "qra_release_date": "2020-08-05",
                "guidance_nominal_coupons": "Monthly nominal schedule: Jul-20: 2Y=40, 3Y=35, 5Y=38, 7Y=24, 10Y=20, 20Y=12, 30Y=14.",
                "guidance_frns": "Monthly FRN schedule: Jul-20: 2Y FRN=16.",
            }
        ]
    )
    calendar = pd.DataFrame(
        [
            {
                "event_id": "qra_2023_05",
                "quarter": "2023Q3",
                "policy_statement_release_date": "2023-05-03",
            }
        ]
    )

    table = build_qra_schedule_table(capture, calendar)

    assert set(table["issue_type"]) == {"nominal_coupon", "frn"}
    assert set(table["tenor"]) >= {"2Y", "10Y", "2Y_FRN"}
    assert len(table) == 16
    assert table["event_id"].eq("qra_2023_05").all()
    assert table.loc[table["tenor"] == "2Y_FRN", "announced_size_bn"].tolist() == [18.0, 19.0]


def test_build_qra_schedule_diff_components_computes_quarter_over_quarter_changes() -> None:
    schedule_table = pd.DataFrame(
        [
            {"event_id": "e0", "quarter": "2023Q2", "qra_release_date": "2023-02-01", "auction_month": "2023-05-01", "auction_month_position": 1, "tenor": "2Y", "issue_type": "nominal_coupon", "announced_size_bn": 40.0, "source_field": "guidance_nominal_coupons"},
            {"event_id": "e0", "quarter": "2023Q2", "qra_release_date": "2023-02-01", "auction_month": "2023-05-01", "auction_month_position": 1, "tenor": "5Y", "issue_type": "nominal_coupon", "announced_size_bn": 30.0, "source_field": "guidance_nominal_coupons"},
            {"event_id": "e0", "quarter": "2023Q2", "qra_release_date": "2023-02-01", "auction_month": "2023-05-01", "auction_month_position": 1, "tenor": "2Y_FRN", "issue_type": "frn", "announced_size_bn": 18.0, "source_field": "guidance_frns"},
            {"event_id": "e1", "quarter": "2023Q3", "qra_release_date": "2023-05-03", "auction_month": "2023-08-01", "auction_month_position": 1, "tenor": "2Y", "issue_type": "nominal_coupon", "announced_size_bn": 50.0, "source_field": "guidance_nominal_coupons"},
            {"event_id": "e1", "quarter": "2023Q3", "qra_release_date": "2023-05-03", "auction_month": "2023-08-01", "auction_month_position": 1, "tenor": "5Y", "issue_type": "nominal_coupon", "announced_size_bn": 36.0, "source_field": "guidance_nominal_coupons"},
            {"event_id": "e1", "quarter": "2023Q3", "qra_release_date": "2023-05-03", "auction_month": "2023-08-01", "auction_month_position": 1, "tenor": "2Y_FRN", "issue_type": "frn", "announced_size_bn": 20.0, "source_field": "guidance_frns"},
        ]
    )
    schedule_table["qra_release_date"] = pd.to_datetime(schedule_table["qra_release_date"])
    schedule_table["auction_month"] = pd.to_datetime(schedule_table["auction_month"])
    yield_curve = pd.DataFrame(
        [
            {
                "date": "2023-02-01",
                "DGS2": 4.00,
                "DGS3": 4.10,
                "DGS5": 4.30,
                "DGS7": 4.50,
                "DGS10": 4.70,
                "DGS20": 5.00,
                "DGS30": 5.10,
            },
            {
                "date": "2023-05-02",
                "DGS2": 3.90,
                "DGS3": 4.00,
                "DGS5": math.nan,
                "DGS7": 4.40,
                "DGS10": 4.60,
                "DGS20": 4.90,
                "DGS30": 5.00,
            },
        ]
    )
    yield_curve["date"] = pd.to_datetime(yield_curve["date"])

    components = build_qra_schedule_diff_components(schedule_table, yield_curve=yield_curve)
    shock_summary = build_qra_schedule_shock_summary(components)

    e0_2y = components.loc[(components["event_id"] == "e0") & (components["tenor"] == "2Y")].iloc[0]
    assert pd.isna(e0_2y["delta_bn"])
    assert pd.isna(e0_2y["contribution_dynamic_10y_eq_bn"])
    assert pd.isna(e0_2y["dv01_contribution_usd"])

    e1_2y = components.loc[(components["event_id"] == "e1") & (components["tenor"] == "2Y")].iloc[0]
    assert e1_2y["previous_event_id"] == "e0"
    assert e1_2y["delta_bn"] == 10.0
    assert e1_2y["contribution_10y_eq_bn"] == 2.0
    assert str(pd.Timestamp(e1_2y["yield_date"]).date()) == "2023-05-02"
    assert e1_2y["yield_curve_source"] == "fred_constant_maturity_prior_business_day"
    assert e1_2y["duration_factor_source"] == "fred_exact"
    assert e1_2y["tenor_modified_duration"] > 0
    assert e1_2y["contribution_dynamic_10y_eq_bn"] == e1_2y["delta_bn"] * e1_2y["dynamic_10y_eq_weight"]
    assert e1_2y["dv01_contribution_usd"] == e1_2y["delta_bn"] * e1_2y["dv01_per_1bn_usd"]

    e1_5y = components.loc[(components["event_id"] == "e1") & (components["tenor"] == "5Y")].iloc[0]
    assert e1_5y["delta_bn"] == 6.0
    assert e1_5y["duration_factor_source"] == "fred_interpolated"
    assert e1_5y["tenor_yield_pct"] == 4.2

    e1_frn = components.loc[(components["event_id"] == "e1") & (components["tenor"] == "2Y_FRN")].iloc[0]
    assert e1_frn["delta_bn"] == 2.0
    assert e1_frn["contribution_10y_eq_bn"] == 0.1
    assert e1_frn["duration_factor_source"] == "frn_convention"
    assert e1_frn["tenor_modified_duration"] == 0.25

    summary_e0 = shock_summary.loc[shock_summary["event_id"] == "e0"].iloc[0]
    assert pd.isna(summary_e0["gross_notional_delta_bn"])
    assert pd.isna(summary_e0["schedule_diff_dynamic_10y_eq_bn"])
    assert pd.isna(summary_e0["schedule_diff_dv01_usd"])
    summary_row = shock_summary.loc[shock_summary["event_id"] == "e1"].iloc[0]
    assert summary_row["gross_notional_delta_bn"] == 18.0
    assert summary_row["schedule_diff_10y_eq_bn"] == 5.1
    assert summary_row["schedule_diff_dynamic_10y_eq_bn"] == components.loc[components["event_id"] == "e1", "contribution_dynamic_10y_eq_bn"].sum()
    assert summary_row["schedule_diff_dv01_usd"] == components.loc[components["event_id"] == "e1", "dv01_contribution_usd"].sum()
