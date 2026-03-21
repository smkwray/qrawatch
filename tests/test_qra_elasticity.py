import math

import pandas as pd

from ati_shadow_policy.research.qra_elasticity import (
    autofill_qra_shock_template_from_capture,
    build_qra_event_elasticity,
    build_qra_shock_template,
)


def test_build_qra_shock_template_preserves_manual_fills_on_reseed() -> None:
    panel = pd.DataFrame(
        {
            "event_id": ["e1", "e1", "e2"],
            "quarter": ["2024Q1", "2024Q1", "2024Q2"],
            "event_date_type": [
                "official_release_date",
                "market_pricing_marker_minus_1d",
                "official_release_date",
            ],
            "event_label": ["New Label", "New Label", "Event 2"],
            "event_date_requested": ["2024-01-02", "2024-01-01", "2024-02-01"],
            "event_date_aligned": ["2024-01-02", "2024-01-01", "2024-02-01"],
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
        }
    )

    seeded = build_qra_shock_template(panel=panel, existing_template=existing)
    assert len(seeded) == 3

    row = seeded.loc[
        (seeded["event_id"] == "e1") & (seeded["event_date_type"] == "official_release_date")
    ].iloc[0]
    assert row["event_label"] == "New Label"
    assert row["shock_bn"] == 50.0
    assert row["shock_source"] == "manual_fill_latest"
    assert row["shock_notes"] == "keep me"

    new_row = seeded.loc[
        (seeded["event_id"] == "e2") & (seeded["event_date_type"] == "official_release_date")
    ].iloc[0]
    assert math.isnan(new_row["shock_bn"])


def test_build_qra_shock_template_dedupes_duplicate_keys_safely() -> None:
    panel = pd.DataFrame(
        {
            "event_id": ["dup", "dup"],
            "event_date_type": ["official_release_date", "official_release_date"],
            "quarter": ["2024Q1", "2024Q1"],
            "event_label": ["first", "second"],
        }
    )
    seeded = build_qra_shock_template(panel=panel)
    assert len(seeded) == 1
    assert seeded.loc[0, "event_label"] == "second"


def test_autofill_qra_shock_template_from_capture_parses_signed_tenor_proxy() -> None:
    template = pd.DataFrame(
        {
            "event_id": ["qra_2023_08", "qra_2023_08", "qra_2023_11", "qra_2023_11"],
            "event_date_type": [
                "official_release_date",
                "market_pricing_marker_minus_1d",
                "official_release_date",
                "market_pricing_marker_minus_1d",
            ],
            "event_date_requested": ["2023-08-02", "2023-08-01", "2023-11-01", "2023-10-31"],
            "event_date_aligned": ["2023-08-02", "2023-08-01", "2023-11-01", "2023-10-31"],
            "expected_direction": [
                "tightening_duration_supply",
                "tightening_duration_supply",
                "easing_coupon_restraint",
                "easing_coupon_restraint",
            ],
            "shock_bn": [math.nan, math.nan, math.nan, math.nan],
            "shock_source": [math.nan, math.nan, math.nan, math.nan],
            "shock_notes": [math.nan, math.nan, math.nan, math.nan],
        }
    )
    capture = pd.DataFrame(
        {
            "qra_release_date": ["2023-08-02", "2023-11-01"],
            "guidance_nominal_coupons": [
                (
                    "Treasury plans to increase the auction sizes of the 2- and 5-year by $3 billion per month, "
                    "the 3-year by $2 billion per month, and the 7-year by $1 billion per month. "
                    "Treasury plans to increase both the new issue and the reopening auction size of the 10-year note "
                    "by $2 billion, and the 30-year bond by $1 billion. Treasury plans to maintain the 20-year bond "
                    "new issue and reopening auction size."
                ),
                (
                    "Treasury plans to increase the auction sizes of the 2- and 5-year by $3 billion per month, "
                    "the 3-year by $2 billion per month, and the 7-year by $1 billion per month. "
                    "Treasury plans to increase both the new issue and the reopening auction size of the 10-year note "
                    "by $2 billion, and the 30-year bond by $1 billion. Treasury plans to maintain the 20-year bond "
                    "new issue and reopening auction size."
                ),
            ],
            "guidance_frns": [
                "Treasury plans to increase the August and September reopening auction size of the 2-year FRN by $2 billion and the October new issue auction size by $2 billion.",
                "Treasury plans to increase the November and December reopening auction size of the 2-year FRN by $2 billion and the January new issue auction size by $2 billion.",
            ],
        }
    )

    filled = autofill_qra_shock_template_from_capture(template, capture)

    aug = filled.loc[
        (filled["event_id"] == "qra_2023_08") & (filled["event_date_type"] == "official_release_date")
    ].iloc[0]
    nov = filled.loc[
        (filled["event_id"] == "qra_2023_11") & (filled["event_date_type"] == "official_release_date")
    ].iloc[0]
    assert aug["shock_bn"] == 25.5
    assert nov["shock_bn"] == -25.5
    assert aug["shock_source"] == "auto_tenor_parser_v1"
    assert "Unsigned 10y-equivalent shock=25.5bn." in aug["shock_notes"]
    assert "Signed negative" in nov["shock_notes"]


def test_autofill_qra_shock_template_from_capture_fills_hold_language_as_zero_for_pending() -> None:
    template = pd.DataFrame(
        {
            "event_id": ["qra_2025_07"],
            "event_date_type": ["official_release_date"],
            "event_date_requested": ["2025-07-30"],
            "event_date_aligned": ["2025-07-30"],
            "expected_direction": ["classification_pending"],
            "shock_bn": [math.nan],
            "shock_source": [math.nan],
            "shock_notes": [math.nan],
        }
    )
    capture = pd.DataFrame(
        {
            "qra_release_date": ["2025-07-30"],
            "guidance_nominal_coupons": [
                "Treasury believes its current auction sizes leave it well positioned to address potential changes to the size and composition of the SOMA portfolio."
            ],
            "guidance_frns": [
                "Based on current projected borrowing needs, Treasury anticipates maintaining nominal coupon and FRN auction sizes for at least the next several quarters."
            ],
        }
    )

    filled = autofill_qra_shock_template_from_capture(template, capture)
    row = filled.iloc[0]
    assert row["shock_bn"] == 0.0
    assert row["shock_source"] == "auto_tenor_parser_v1"
    assert "inferred no coupon/FRN size change" in row["shock_notes"]


def test_build_qra_event_elasticity_uses_bp_per_100bn_and_flags() -> None:
    panel = pd.DataFrame(
        {
            "quarter": ["2024Q3", "2024Q3", "2024Q4", "2025Q1"],
            "event_id": ["e1", "e1", "e2", "e3"],
            "event_date_type": [
                "official_release_date",
                "market_pricing_marker_minus_1d",
                "official_release_date",
                "official_release_date",
            ],
            "expected_direction": [
                "tightening_duration_supply",
                "tightening_duration_supply",
                "classification_pending",
                "tightening_duration_supply",
            ],
            "DGS10_d1": [0.05, -0.02, 0.02, 0.02],
            "DGS10_d3": [-0.10, -0.03, 0.01, 0.01],
        }
    )
    shocks = pd.DataFrame(
        {
            "event_id": ["e1", "e1", "e1", "e3"],
            "event_date_type": [
                "official_release_date",
                "official_release_date",
                "market_pricing_marker_minus_1d",
                "official_release_date",
            ],
            "shock_bn": [20.0, 50.0, 50.0, 5.0],
        }
    )

    output = build_qra_event_elasticity(panel=panel, shock_template=shocks, small_denominator_threshold_bn=10.0)

    e1_d1 = output.loc[
        (output["event_id"] == "e1")
        & (output["event_date_type"] == "official_release_date")
        & (output["series"] == "DGS10")
        & (output["window"] == "d1")
    ].iloc[0]
    assert e1_d1["delta_pp"] == 0.05
    assert e1_d1["delta_bp"] == 5.0
    assert e1_d1["elasticity_bp_per_100bn"] == 10.0
    assert bool(e1_d1["sign_flip_flag"]) is True
    assert bool(e1_d1["small_denominator_flag"]) is False
    assert bool(e1_d1["usable_for_headline"]) is True
    assert e1_d1["quarter"] == "2024Q3"

    e1_d3 = output.loc[
        (output["event_id"] == "e1")
        & (output["event_date_type"] == "official_release_date")
        & (output["series"] == "DGS10")
        & (output["window"] == "d3")
    ].iloc[0]
    assert e1_d3["delta_bp"] == -10.0
    assert e1_d3["elasticity_bp_per_100bn"] == -20.0

    e2_d1 = output.loc[
        (output["event_id"] == "e2") & (output["series"] == "DGS10") & (output["window"] == "d1")
    ].iloc[0]
    assert bool(e2_d1["shock_missing_flag"]) is True
    assert math.isnan(e2_d1["elasticity_bp_per_100bn"])
    assert bool(e2_d1["usable_for_headline"]) is False

    e3_d1 = output.loc[
        (output["event_id"] == "e3") & (output["series"] == "DGS10") & (output["window"] == "d1")
    ].iloc[0]
    assert bool(e3_d1["small_denominator_flag"]) is True
