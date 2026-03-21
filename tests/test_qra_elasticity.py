import math

import pandas as pd

from ati_shadow_policy.research.qra_elasticity import (
    autofill_qra_shock_template_from_capture,
    build_qra_event_elasticity,
    build_qra_shock_template,
)


def test_build_qra_shock_template_preserves_manual_fills_and_context() -> None:
    panel = pd.DataFrame(
        {
            "event_id": ["e1", "e1", "e2"],
            "quarter": ["2024Q1", "2024Q1", "2024Q2"],
            "policy_statement_url": ["https://example.com/a", "https://example.com/a", "https://example.com/b"],
            "event_date_type": [
                "official_release_date",
                "market_pricing_marker_minus_1d",
                "official_release_date",
            ],
            "event_label": ["New Label", "New Label", "Event 2"],
            "event_date_requested": ["2024-01-02", "2024-01-01", "2024-02-01"],
            "event_date_aligned": ["2024-01-02", "2024-01-01", "2024-02-01"],
            "current_quarter_action": ["tightening", "tightening", "easing"],
            "forward_guidance_bias": ["neutral", "neutral", "neutral"],
            "headline_bucket": ["tightening", "tightening", "easing"],
            "shock_sign_curated": [1, 1, -1],
            "classification_confidence": ["exact_statement", "exact_statement", "hybrid"],
            "classification_review_status": ["reviewed", "reviewed", "reviewed"],
            "expected_direction": ["tightening", "tightening", "easing"],
        }
    )
    existing = pd.DataFrame(
        {
            "event_id": ["e1", "e1"],
            "event_date_type": ["official_release_date", "official_release_date"],
            "event_label": ["Old Label", "Old Label"],
            "shock_bn": [25.0, 50.0],
            "shock_source": ["old", "manual_fill_latest"],
            "shock_notes": ["", "keep me"],
            "shock_review_status": ["provisional", "reviewed"],
        }
    )

    seeded = build_qra_shock_template(panel=panel, existing_template=existing)
    row = seeded.loc[
        (seeded["event_id"] == "e1") & (seeded["event_date_type"] == "official_release_date")
    ].iloc[0]
    assert row["quarter"] == "2024Q1"
    assert row["policy_statement_url"] == "https://example.com/a"
    assert row["event_label"] == "New Label"
    assert row["shock_bn"] == 50.0
    assert row["shock_source"] == "manual_fill_latest"
    assert row["shock_notes"] == "keep me"
    assert row["shock_review_status"] == "reviewed"


def test_autofill_qra_shock_template_from_capture_parses_explicit_increase_and_sign() -> None:
    template = pd.DataFrame(
        {
            "event_id": ["qra_2023_05"],
            "event_date_type": ["official_release_date"],
            "event_date_requested": ["2023-05-03"],
            "current_quarter_action": ["tightening"],
            "headline_bucket": ["tightening"],
            "shock_sign_curated": [1],
            "classification_review_status": ["reviewed"],
            "shock_bn": [math.nan],
            "shock_source": [math.nan],
            "shock_notes": [math.nan],
            "shock_review_status": [math.nan],
        }
    )
    capture = pd.DataFrame(
        {
            "qra_release_date": ["2023-05-03"],
            "guidance_nominal_coupons": [
                (
                    "Treasury plans to increase the auction sizes of the 2- and 5-year by $3 billion per month, "
                    "the 3-year by $2 billion per month, and the 7-year by $1 billion per month. "
                    "Treasury plans to increase both the new issue and the reopening auction size of the 10-year note "
                    "by $3 billion, the 30-year bond by $2 billion, and the $20-year bond by $1 billion."
                )
            ],
            "guidance_frns": [
                "Treasury plans to increase the August and September reopening auction size of the 2-year FRN by $2 billion and the October new issue auction size by $2 billion."
            ],
        }
    )

    filled = autofill_qra_shock_template_from_capture(template, capture)
    row = filled.iloc[0]
    assert row["shock_bn"] == 43.5
    assert row["shock_source"] == "auto_tenor_parser_v1"
    assert row["shock_review_status"] == "provisional"
    assert "Signed positive from shock_sign_curated=1." in row["shock_notes"]


def test_autofill_qra_shock_template_from_capture_parses_reduce_language() -> None:
    template = pd.DataFrame(
        {
            "event_id": ["qra_2022_05"],
            "event_date_type": ["official_release_date"],
            "event_date_requested": ["2022-05-04"],
            "current_quarter_action": ["easing"],
            "headline_bucket": ["easing"],
            "shock_sign_curated": [-1],
            "classification_review_status": ["reviewed"],
            "shock_bn": [math.nan],
            "shock_source": [math.nan],
            "shock_notes": [math.nan],
            "shock_review_status": [math.nan],
        }
    )
    capture = pd.DataFrame(
        {
            "qra_release_date": ["2022-05-04"],
            "guidance_nominal_coupons": [
                (
                    "Treasury plans to reduce the auction sizes of the 2- and 5-year by $1 billion per month, "
                    "the 3-year by $1 billion per month, and the 7-year by $2 billion per month. "
                    "Treasury plans to reduce the 10-year note by $1 billion and the 30-year bond by $1 billion."
                )
            ],
            "guidance_frns": [""],
        }
    )

    filled = autofill_qra_shock_template_from_capture(template, capture)
    row = filled.iloc[0]
    assert row["shock_bn"] < 0
    assert "2Y=-3" in row["shock_notes"]


def test_autofill_qra_shock_template_from_capture_fills_keep_unchanged_as_provisional_zero() -> None:
    template = pd.DataFrame(
        {
            "event_id": ["qra_2023_02"],
            "event_date_type": ["official_release_date"],
            "event_date_requested": ["2023-02-01"],
            "current_quarter_action": ["hold"],
            "headline_bucket": ["exclude"],
            "shock_sign_curated": [0],
            "classification_review_status": ["reviewed"],
            "shock_bn": [math.nan],
            "shock_source": [math.nan],
            "shock_notes": [math.nan],
            "shock_review_status": [math.nan],
        }
    )
    capture = pd.DataFrame(
        {
            "qra_release_date": ["2023-02-01"],
            "guidance_nominal_coupons": ["Treasury expects to keep nominal coupon auction sizes unchanged."],
            "guidance_frns": ["Treasury expects to keep FRN auction sizes unchanged."],
        }
    )

    filled = autofill_qra_shock_template_from_capture(template, capture)
    row = filled.iloc[0]
    assert row["shock_bn"] == 0.0
    assert row["shock_review_status"] == "provisional"
    assert "inferred no coupon/FRN size change" in row["shock_notes"]


def test_build_qra_event_elasticity_uses_curated_schema_for_headline_flags() -> None:
    panel = pd.DataFrame(
        {
            "quarter": ["2024Q3", "2024Q3", "2024Q4"],
            "event_id": ["e1", "e1", "e2"],
            "event_date_type": [
                "official_release_date",
                "market_pricing_marker_minus_1d",
                "official_release_date",
            ],
            "current_quarter_action": ["tightening", "tightening", "mixed"],
            "forward_guidance_bias": ["neutral", "neutral", "neutral"],
            "headline_bucket": ["tightening", "tightening", "exclude"],
            "shock_sign_curated": [1, 1, math.nan],
            "classification_confidence": ["exact_statement", "exact_statement", "heuristic"],
            "classification_review_status": ["reviewed", "reviewed", "provisional"],
            "expected_direction": ["tightening", "tightening", "mixed"],
            "DGS10_d1": [0.05, -0.02, 0.02],
            "DGS10_d3": [-0.10, -0.03, 0.01],
        }
    )
    shocks = pd.DataFrame(
        {
            "event_id": ["e1", "e1", "e2"],
            "event_date_type": [
                "official_release_date",
                "market_pricing_marker_minus_1d",
                "official_release_date",
            ],
            "shock_bn": [50.0, 50.0, 0.0],
            "shock_source": ["manual", "manual", "auto"],
            "shock_notes": ["", "", ""],
            "shock_review_status": ["reviewed", "reviewed", "provisional"],
            "previous_event_id": ["e0", "e0", "e1"],
            "previous_quarter": ["2024Q2", "2024Q2", "2024Q3"],
            "gross_notional_delta_bn": [60.0, 60.0, 0.0],
            "schedule_diff_10y_eq_bn": [50.0, 50.0, 0.0],
            "schedule_diff_dynamic_10y_eq_bn": [47.5, 47.5, 0.0],
            "schedule_diff_dv01_usd": [4750000.0, 4750000.0, 0.0],
            "shock_construction": ["schedule_diff_primary", "schedule_diff_primary", "schedule_diff_primary"],
        }
    )

    output = build_qra_event_elasticity(panel=panel, shock_template=shocks, small_denominator_threshold_bn=10.0)

    e1_official = output.loc[
        (output["event_id"] == "e1")
        & (output["event_date_type"] == "official_release_date")
        & (output["series"] == "DGS10")
        & (output["window"] == "d1")
    ].iloc[0]
    assert e1_official["quarter"] == "2024Q3"
    assert e1_official["elasticity_bp_per_100bn"] == 10.0
    assert e1_official["schedule_diff_10y_eq_bn"] == 50.0
    assert e1_official["schedule_diff_dynamic_10y_eq_bn"] == 47.5
    assert e1_official["schedule_diff_dv01_usd"] == 4750000.0
    assert e1_official["shock_construction"] == "schedule_diff_primary"
    assert bool(e1_official["usable_for_headline"]) is True

    e1_tminus1 = output.loc[
        (output["event_id"] == "e1")
        & (output["event_date_type"] == "market_pricing_marker_minus_1d")
        & (output["series"] == "DGS10")
        & (output["window"] == "d1")
    ].iloc[0]
    assert bool(e1_tminus1["usable_for_headline"]) is False

    e2_official = output.loc[
        (output["event_id"] == "e2")
        & (output["event_date_type"] == "official_release_date")
        & (output["series"] == "DGS10")
        & (output["window"] == "d1")
    ].iloc[0]
    assert bool(e2_official["usable_for_headline"]) is False
    assert e2_official["shock_review_status"] == "provisional"


def test_build_qra_shock_template_prefers_schedule_diff_autofill_before_parser() -> None:
    panel = pd.DataFrame(
        {
            "event_id": ["e1"],
            "event_date_type": ["official_release_date"],
            "quarter": ["2023Q3"],
            "event_label": ["Event 1"],
            "event_date_requested": ["2023-05-03"],
            "event_date_aligned": ["2023-05-03"],
            "headline_bucket": ["tightening"],
            "shock_sign_curated": [1],
            "classification_review_status": ["reviewed"],
        }
    )
    schedule_components = pd.DataFrame(
        [
            {
                "event_id": "e1",
                "quarter": "2023Q3",
                "qra_release_date": "2023-05-03",
                "previous_event_id": "e0",
                "previous_quarter": "2023Q2",
                "tenor": "10Y",
                "issue_type": "nominal_coupon",
                "current_total_bn": 12.0,
                "previous_total_bn": 9.0,
                "delta_bn": 3.0,
                "tenor_weight_10y_eq": 1.0,
                "contribution_10y_eq_bn": 3.0,
                "yield_date": "2023-05-03",
                "yield_curve_source": "fred_constant_maturity_exact_date",
                "tenor_yield_pct": 4.0,
                "tenor_modified_duration": 8.0,
                "duration_factor_source": "fred_exact",
                "dynamic_10y_eq_weight": 1.0,
                "contribution_dynamic_10y_eq_bn": 3.0,
                "dv01_per_1bn_usd": 800000.0,
                "dv01_contribution_usd": 2400000.0,
            }
        ]
    )
    capture = pd.DataFrame(
        {
            "qra_release_date": ["2023-05-03"],
            "guidance_nominal_coupons": [
                "Treasury plans to increase the auction sizes of the 2- and 5-year by $3 billion per month, the 3-year by $2 billion per month, and the 7-year by $1 billion per month."
            ],
            "guidance_frns": [""],
        }
    )

    seeded = build_qra_shock_template(
        panel=panel,
        existing_template=None,
        capture_template=capture,
        schedule_components=schedule_components,
    )

    row = seeded.iloc[0]
    assert row["shock_bn"] == 3.0
    assert row["shock_source"] == "schedule_diff_10y_eq_v1"
    assert row["schedule_diff_10y_eq_bn"] == 3.0
    assert row["schedule_diff_dynamic_10y_eq_bn"] == 3.0
    assert row["schedule_diff_dv01_usd"] == 2400000.0
    assert row["shock_construction"] == "schedule_diff_primary"
    assert "Schedule diff quarterly totals" in row["shock_notes"]
