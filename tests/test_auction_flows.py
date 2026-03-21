import pandas as pd
import pytest

from ati_shadow_policy.research.ati_index import (
    aggregate_auction_flows,
    aggregate_auction_net_flows,
)


def test_aggregate_auction_flows_weekly_baseline_contract():
    auctions = pd.DataFrame(
        {
            "auction_date": [
                "2026-01-05",
                "2026-01-06",
                "2026-01-07",
                "2026-01-07",
                "2026-01-07",
                "2026-01-08",
                "2026-01-13",
                "2026-01-14",
            ],
            "security_type": [
                "Bill",
                "Floating Rate Note",
                "Treasury Inflation-Protected Security",
                "Treasury Note",
                "Mystery Security",
                "Treasury Bond",
                "Cash Management Bill",
                "FRN",
            ],
            "offering_amount": [100, 30, 20, 50, 7, 60, 40, 10],
        }
    )

    out = aggregate_auction_flows(auctions)
    assert list(out.columns) == [
        "date",
        "bill_like",
        "frn",
        "nominal_coupon",
        "tips",
        "coupon_like_total",
        "coupon_plus_frn_total",
        "gross_total",
        "unknown",
    ]

    week1 = out.loc[out["date"] == pd.Timestamp("2026-01-07")].iloc[0]
    assert week1["bill_like"] == 100
    assert week1["frn"] == 30
    assert week1["nominal_coupon"] == 50
    assert week1["tips"] == 20
    assert week1["coupon_like_total"] == 70
    assert week1["coupon_plus_frn_total"] == 100
    assert week1["unknown"] == 7
    assert week1["gross_total"] == 207

    week2 = out.loc[out["date"] == pd.Timestamp("2026-01-14")].iloc[0]
    assert week2["bill_like"] == 40
    assert week2["frn"] == 10
    assert week2["nominal_coupon"] == 60
    assert week2["tips"] == 0
    assert week2["coupon_like_total"] == 60
    assert week2["coupon_plus_frn_total"] == 70
    assert week2["unknown"] == 0
    assert week2["gross_total"] == 110


def test_aggregate_auction_flows_missing_required_schema_fails_loudly():
    auctions = pd.DataFrame(
        {
            "auction_date": ["2026-01-07"],
            "security_type": ["Bill"],
        }
    )
    with pytest.raises(ValueError, match="missing required amount column") as excinfo:
        aggregate_auction_flows(auctions)
    message = str(excinfo.value)
    assert "Expected one of" in message
    assert "offering_amt" in message
    assert "available columns" in message


def test_aggregate_auction_net_flows_reconstructs_bill_and_nonbill_without_double_counting():
    auctions = pd.DataFrame(
        {
            "issue_date": [
                "2026-01-05",
                "2026-01-05",
                "2026-01-06",
                "2026-01-06",
                "2026-01-13",
            ],
            "security_type": [
                "Bill",
                "Cash Management Bill",
                "Treasury Note",
                "Floating Rate Note",
                "Treasury Bond",
            ],
            "offering_amt": [100.0, 20.0, 50.0, 30.0, 40.0],
            "est_pub_held_mat_by_type_amt": [70.0, 70.0, 60.0, 60.0, 10.0],
            "cash_management_bill_cmb": ["No", "Yes", "No", "No", "No"],
            "floating_rate": ["No", "No", "No", "Yes", "No"],
            "inflation_index_security": ["No", "No", "No", "No", "No"],
        }
    )

    out = aggregate_auction_net_flows(auctions)

    assert list(out.columns) == [
        "date",
        "bill_net_exact",
        "nonbill_net_exact",
        "headline_treasury_net_exact",
        "bill_net_gross",
        "nonbill_net_gross",
        "bill_net_maturing",
        "nonbill_net_maturing",
        "bill_net_issue_dates",
        "nonbill_net_issue_dates",
        "bill_net_issue_dates_missing_maturity",
        "nonbill_net_issue_dates_missing_maturity",
        "bill_net_reconstruction_status",
        "nonbill_net_reconstruction_status",
    ]

    week1 = out.loc[out["date"] == pd.Timestamp("2026-01-07")].iloc[0]
    assert week1["bill_net_gross"] == 120.0
    assert week1["bill_net_maturing"] == 70.0
    assert week1["bill_net_exact"] == 50.0
    assert week1["nonbill_net_gross"] == 80.0
    assert week1["nonbill_net_maturing"] == 60.0
    assert week1["nonbill_net_exact"] == 20.0
    assert week1["headline_treasury_net_exact"] == 70.0
    assert week1["bill_net_reconstruction_status"] == "complete"
    assert week1["nonbill_net_reconstruction_status"] == "complete"

    week2 = out.loc[out["date"] == pd.Timestamp("2026-01-14")].iloc[0]
    assert week2["bill_net_exact"] == 0.0
    assert week2["nonbill_net_exact"] == 30.0
