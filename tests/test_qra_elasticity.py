import math

import pandas as pd

from ati_shadow_policy.research.qra_elasticity import (
    autofill_qra_shock_template_from_capture,
    build_event_usability_table,
    build_leave_one_event_out_table,
    build_qra_event_elasticity,
    build_qra_shock_crosswalk_v1,
    build_qra_shock_template,
    build_treatment_comparison_table,
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


def test_build_qra_shock_template_preserves_expectation_fields() -> None:
    panel = pd.DataFrame(
        {
            "event_id": ["e1"],
            "event_date_type": ["official_release_date"],
            "event_label": ["Event 1"],
            "event_date_requested": ["2024-01-31"],
            "event_date_aligned": ["2024-01-31"],
        }
    )
    existing = pd.DataFrame(
        {
            "event_id": ["e1"],
            "event_date_type": ["official_release_date"],
            "release_component_id": ["e1__policy_statement"],
            "benchmark_timestamp_et": ["2024-01-31T08:00:00-05:00"],
            "benchmark_source": ["dealer_survey"],
            "expected_composition_bn": [10.0],
            "realized_composition_bn": [20.0],
            "composition_surprise_bn": [10.0],
            "benchmark_stale_flag": [False],
            "expectation_review_status": ["reviewed"],
            "expectation_notes": ["Reviewed benchmark."],
        }
    )

    seeded = build_qra_shock_template(panel=panel, existing_template=existing)
    row = seeded.iloc[0]
    assert row["release_component_id"] == "e1__policy_statement"
    assert row["composition_surprise_bn"] == 10.0
    assert row["expectation_review_status"] == "reviewed"


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
        & (output["treatment_variant"] == "canonical_shock_bn")
    ].iloc[0]
    assert e1_official["quarter"] == "2024Q3"
    assert e1_official["spec_id"] == "spec_duration_treatment_v1"
    assert e1_official["elasticity_value"] == 10.0
    assert e1_official["elasticity_bp_per_100bn"] == 10.0
    assert e1_official["schedule_diff_10y_eq_bn"] == 50.0
    assert e1_official["schedule_diff_dynamic_10y_eq_bn"] == 47.5
    assert e1_official["schedule_diff_dv01_usd"] == 4750000.0
    assert e1_official["shock_construction"] == "schedule_diff_primary"
    assert e1_official["usable_for_headline_reason"] == "usable"
    assert bool(e1_official["usable_for_headline"]) is True

    e1_tminus1 = output.loc[
        (output["event_id"] == "e1")
        & (output["event_date_type"] == "market_pricing_marker_minus_1d")
        & (output["series"] == "DGS10")
        & (output["window"] == "d1")
        & (output["treatment_variant"] == "canonical_shock_bn")
    ].iloc[0]
    assert bool(e1_tminus1["usable_for_headline"]) is False
    assert e1_tminus1["usable_for_headline_reason"] == "non_official_event_date_type"

    e2_official = output.loc[
        (output["event_id"] == "e2")
        & (output["event_date_type"] == "official_release_date")
        & (output["series"] == "DGS10")
        & (output["window"] == "d1")
        & (output["treatment_variant"] == "canonical_shock_bn")
    ].iloc[0]
    assert bool(e2_official["usable_for_headline"]) is False
    assert e2_official["usable_for_headline_reason"] == "small_denominator"
    assert e2_official["shock_review_status"] == "provisional"

    fixed_variant = output.loc[
        (output["event_id"] == "e1")
        & (output["event_date_type"] == "official_release_date")
        & (output["series"] == "DGS10")
        & (output["window"] == "d1")
        & (output["treatment_variant"] == "fixed_10y_eq_bn")
    ].iloc[0]
    assert fixed_variant["elasticity_value"] == 10.0
    assert fixed_variant["usable_for_headline_reason"] == "non_canonical_treatment_variant"

    dv01_variant = output.loc[
        (output["event_id"] == "e1")
        & (output["event_date_type"] == "official_release_date")
        & (output["series"] == "DGS10")
        & (output["window"] == "d1")
        & (output["treatment_variant"] == "dv01_usd")
    ].iloc[0]
    assert dv01_variant["elasticity_units"] == "bp_per_1mm_dv01"
    assert dv01_variant["elasticity_value"] > 0


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


def test_build_qra_shock_crosswalk_v1_emits_required_columns() -> None:
    template = pd.DataFrame(
        {
            "event_id": ["e1"],
            "event_date_type": ["official_release_date"],
            "shock_bn": [25.0],
            "schedule_diff_10y_eq_bn": [24.0],
            "schedule_diff_dynamic_10y_eq_bn": [23.0],
            "schedule_diff_dv01_usd": [2100000.0],
            "gross_notional_delta_bn": [30.0],
            "shock_source": ["manual_override"],
            "shock_review_status": ["reviewed"],
            "shock_construction": ["manual_override_with_schedule_context"],
        }
    )
    crosswalk = build_qra_shock_crosswalk_v1(template)
    assert list(crosswalk.columns) == [
        "spec_id",
        "treatment_version_id",
        "treatment_variant",
        "event_id",
        "event_date_type",
        "canonical_shock_id",
        "shock_bn",
        "schedule_diff_10y_eq_bn",
        "schedule_diff_dynamic_10y_eq_bn",
        "schedule_diff_dv01_usd",
        "gross_notional_delta_bn",
        "shock_source",
        "manual_override_reason",
        "alternative_treatment_complete",
        "alternative_treatment_missing_fields",
        "alternative_treatment_missing_reason",
        "shock_review_status",
        "usable_for_headline_reason",
    ]
    assert crosswalk.loc[0, "manual_override_reason"] == "manual_override_with_schedule_context"
    assert bool(crosswalk.loc[0, "alternative_treatment_complete"]) is True


def test_build_qra_shock_crosswalk_v1_marks_reviewed_missing_alternative_treatments() -> None:
    template = pd.DataFrame(
        {
            "event_id": ["e1"],
            "event_date_type": ["official_release_date"],
            "shock_bn": [25.0],
            "schedule_diff_10y_eq_bn": [pd.NA],
            "schedule_diff_dynamic_10y_eq_bn": [pd.NA],
            "schedule_diff_dv01_usd": [pd.NA],
            "gross_notional_delta_bn": [pd.NA],
            "shock_source": ["manual_statement_review_v1"],
            "shock_review_status": ["reviewed"],
            "shock_construction": ["manual_override_with_schedule_context"],
        }
    )

    crosswalk = build_qra_shock_crosswalk_v1(template)

    assert bool(crosswalk.loc[0, "alternative_treatment_complete"]) is False
    assert crosswalk.loc[0, "alternative_treatment_missing_fields"] == (
        "schedule_diff_10y_eq_bn|schedule_diff_dynamic_10y_eq_bn|"
        "schedule_diff_dv01_usd|gross_notional_delta_bn"
    )
    assert (
        crosswalk.loc[0, "alternative_treatment_missing_reason"]
        == "manual_statement_primary_only_pending_alt_treatments"
    )


def test_build_event_usability_table_uses_canonical_variant_only() -> None:
    elasticity = pd.DataFrame(
        {
            "spec_id": ["spec_duration_treatment_v1", "spec_duration_treatment_v1"],
            "event_id": ["e1", "e1"],
            "event_date_type": ["official_release_date", "official_release_date"],
            "treatment_variant": ["canonical_shock_bn", "fixed_10y_eq_bn"],
            "headline_bucket": ["tightening", "tightening"],
            "classification_review_status": ["reviewed", "reviewed"],
            "shock_review_status": ["reviewed", "reviewed"],
            "overlap_severity": ["none", "none"],
            "usable_for_headline": [True, False],
            "usable_for_headline_reason": ["usable", "non_canonical_treatment_variant"],
        }
    )
    usability = build_event_usability_table(elasticity)
    assert len(usability) == 1
    assert usability.loc[0, "spec_id"] == "spec_qra_event_v2"
    assert usability.loc[0, "treatment_version_id"] == "spec_duration_treatment_v1"
    assert usability.loc[0, "treatment_variant"] == "canonical_shock_bn"
    assert bool(usability.loc[0, "usable_for_headline"]) is True
    assert usability.loc[0, "n_events"] == 1


def test_build_leave_one_event_out_table_recomputes_by_treatment_variant() -> None:
    elasticity = pd.DataFrame(
        {
            "spec_id": ["spec_duration_treatment_v1"] * 3,
            "event_id": ["e1", "e2", "e3"],
            "series": ["DGS10", "DGS10", "DGS10"],
            "window": ["d1", "d1", "d1"],
            "treatment_variant": ["canonical_shock_bn", "canonical_shock_bn", "canonical_shock_bn"],
            "elasticity_units": ["bp_per_100bn"] * 3,
            "elasticity_value": [10.0, 20.0, 30.0],
            "usable_for_headline": [True, True, True],
        }
    )
    leave_one_out = build_leave_one_event_out_table(elasticity)
    baseline = leave_one_out.loc[leave_one_out["dropped_event_id"] == "__none__"].iloc[0]
    assert baseline["mean_elasticity"] == 20.0
    drop_e1 = leave_one_out.loc[leave_one_out["dropped_event_id"] == "e1"].iloc[0]
    assert drop_e1["n_events"] == 2
    assert drop_e1["mean_elasticity"] == 25.0


def test_build_treatment_comparison_table_tracks_family_diagnostics() -> None:
    elasticity = pd.DataFrame(
        {
            "spec_id": ["spec_duration_treatment_v1"] * 8,
            "event_id": ["e1", "e1", "e1", "e1", "e2", "e2", "e2", "e2"],
            "event_date_type": ["official_release_date"] * 8,
            "series": ["DGS10"] * 8,
            "window": ["d3"] * 8,
            "treatment_variant": [
                "canonical_shock_bn",
                "fixed_10y_eq_bn",
                "dynamic_10y_eq_bn",
                "dv01_usd",
                "canonical_shock_bn",
                "fixed_10y_eq_bn",
                "dynamic_10y_eq_bn",
                "dv01_usd",
            ],
            "elasticity_units": [
                "bp_per_100bn",
                "bp_per_100bn",
                "bp_per_100bn",
                "bp_per_1mm_dv01",
                "bp_per_100bn",
                "bp_per_100bn",
                "bp_per_100bn",
                "bp_per_1mm_dv01",
            ],
            "elasticity_value": [10.0, 11.0, 12.0, 20.0, 14.0, 15.0, 16.0, 24.0],
            "usable_for_headline": [True, False, False, False, True, False, False, False],
        }
    )

    out = build_treatment_comparison_table(elasticity)
    assert list(out["treatment_variant"]) == [
        "canonical_shock_bn",
        "fixed_10y_eq_bn",
        "dynamic_10y_eq_bn",
        "dv01_usd",
    ]
    canonical = out.loc[out["treatment_variant"] == "canonical_shock_bn"].iloc[0]
    fixed = out.loc[out["treatment_variant"] == "fixed_10y_eq_bn"].iloc[0]
    dv01 = out.loc[out["treatment_variant"] == "dv01_usd"].iloc[0]

    assert canonical["headline_recommendation_status"] == "retain_canonical_contract"
    assert canonical["n_headline_eligible_events"] == 2
    assert canonical["headline_eligible_share"] == 1.0
    assert fixed["delta_vs_family_reference_mean_elasticity_value"] == 1.0
    assert fixed["bp_family_spread_elasticity_value"] == 2.0
    assert dv01["comparison_family"] == "dv01_equivalent"
    assert dv01["primary_treatment_variant"] == "canonical_shock_bn"
