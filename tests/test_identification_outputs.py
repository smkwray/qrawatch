from __future__ import annotations

import pandas as pd

from ati_shadow_policy.research.identification import (
    build_event_design_status,
    build_event_usability_table,
    build_leave_one_event_out_table,
    build_qra_event_registry_v2,
    build_qra_release_component_registry,
    build_qra_shock_crosswalk_v1,
    summarize_qra_causal_qa,
)


def test_build_qra_event_registry_v2_carries_review_and_eligibility_fields() -> None:
    panel = pd.DataFrame(
        {
            "event_id": ["e1"],
            "quarter": ["2024Q1"],
            "event_date_type": ["official_release_date"],
            "official_release_date": ["2024-01-31"],
            "financing_estimates_release_date": ["2024-01-29"],
            "policy_statement_release_date": ["2024-01-31"],
            "policy_statement_url": ["https://example.com/policy"],
            "financing_estimates_url": ["https://example.com/financing"],
            "timing_quality": ["same_day_release_bundle"],
            "forward_guidance_bias": ["dovish"],
            "classification_review_status": ["reviewed"],
            "overlap_flag": [False],
            "overlap_label": [""],
            "notes": ["Reviewed 2024-02-01 by hand."],
        }
    )
    shock_summary = pd.DataFrame(
        {
            "event_id": ["e1"],
            "event_date_type": ["official_release_date"],
            "headline_bucket": ["tightening"],
            "classification_review_status": ["reviewed"],
            "shock_review_status": ["reviewed"],
            "shock_missing_flag": [False],
            "small_denominator_flag": [False],
        }
    )

    out = build_qra_event_registry_v2(panel=panel, shock_summary=shock_summary)

    assert out.loc[0, "release_bundle_type"] == "same_day_release_bundle"
    assert out.loc[0, "release_timestamp_kind"] == "release_component_registry_date_only"
    assert out.loc[0, "release_sequence_label"] == "financing_then_policy"
    assert pd.notna(out.loc[0, "policy_statement_release_timestamp_et"])
    assert pd.notna(out.loc[0, "financing_estimates_release_timestamp_et"])
    assert bool(out.loc[0, "financing_need_news_flag"]) is True
    assert bool(out.loc[0, "composition_news_flag"]) is True
    assert bool(out.loc[0, "forward_guidance_flag"]) is True
    assert bool(out.loc[0, "bundle_decomposition_ready"]) is True
    assert bool(out.loc[0, "headline_check_official_release"]) is True
    assert bool(out.loc[0, "headline_eligible"]) is True
    assert out.loc[0, "headline_eligibility_blockers"] == ""
    assert out.loc[0, "headline_eligibility_reason"] == "usable"
    assert out.loc[0, "quality_tier"] == "Tier B"
    assert out.loc[0, "contamination_status"] == "pending_review"


def test_build_qra_event_registry_v2_adds_overlap_and_headline_decomposition_lineage() -> None:
    panel = pd.DataFrame(
        {
            "event_id": ["e2"],
            "quarter": ["2024Q2"],
            "event_date_type": ["official_release_date"],
            "official_release_date": ["2024-04-30 08:30:00-04:00"],
            "policy_statement_release_date": ["2024-04-30"],
            "financing_estimates_release_date": ["2024-04-29"],
            "policy_statement_url": ["https://example.com/policy2"],
            "financing_estimates_url": ["https://example.com/financing2"],
            "timing_quality": ["explicit_multi_stage_release"],
            "forward_guidance_bias": ["neutral"],
            "classification_review_status": ["provisional"],
            "notes": ["Manual review pending 2024-05-01."],
        }
    )
    overlap = pd.DataFrame(
        {
            "event_id": ["e2"],
            "overlap_flag": [True],
            "overlap_label": ["fomc_overlap"],
            "overlap_note": ["FOMC window overlap."],
        }
    )
    shock_summary = pd.DataFrame(
        {
            "event_id": ["e2"],
            "event_date_type": ["official_release_date"],
            "headline_bucket": ["tightening"],
            "classification_review_status": ["provisional"],
            "shock_review_status": ["provisional"],
            "shock_missing_flag": [True],
            "small_denominator_flag": [True],
        }
    )

    out = build_qra_event_registry_v2(
        panel=panel,
        overlap_annotations=overlap,
        shock_summary=shock_summary,
        overlap_annotations_source="data/manual/qra_event_overlap_annotations.csv",
    )

    assert out.loc[0, "release_timestamp_kind"] == "release_component_registry_date_only"
    assert pd.isna(out.loc[0, "release_time_et"])
    assert out.loc[0, "overlap_annotation_source"] == "data/manual/qra_event_overlap_annotations.csv"
    assert bool(out.loc[0, "overlap_annotation_present"]) is True
    assert bool(out.loc[0, "headline_eligible"]) is False
    assert out.loc[0, "headline_eligibility_blockers"] == "classification_not_reviewed|shock_not_reviewed|missing_shock|small_denominator"
    assert out.loc[0, "headline_eligibility_reason"] == "shock_missing"
    assert out.loc[0, "quality_tier"] == "Tier C"
    assert "contamination_not_reviewed" in out.loc[0, "eligibility_blockers"]


def test_build_qra_event_registry_v2_uses_best_component_timestamp_when_available() -> None:
    panel = pd.DataFrame(
        {
            "event_id": ["e4"],
            "quarter": ["2024Q2"],
            "event_date_type": ["official_release_date"],
            "official_release_date": ["2024-04-30"],
            "policy_statement_release_date": ["2024-04-30"],
            "financing_estimates_release_date": ["2024-04-29"],
            "policy_statement_url": ["https://example.com/policy4"],
            "financing_estimates_url": ["https://example.com/financing4"],
            "timing_quality": ["explicit_multi_stage_release"],
            "classification_review_status": ["reviewed"],
        }
    )
    release_components = pd.DataFrame(
        {
            "release_component_id": ["e4__policy_statement", "e4__financing_estimates"],
            "event_id": ["e4", "e4"],
            "component_type": ["policy_statement", "financing_estimates"],
            "release_timestamp_et": ["2024-04-30T08:30:00-04:00", "2024-04-29T15:00:00-04:00"],
            "timestamp_precision": ["exact_time", "exact_time"],
            "source_url": ["https://example.com/policy4", "https://example.com/financing4"],
            "review_status": ["reviewed", "reviewed"],
            "separable_component_flag": [True, True],
        }
    )
    expectation_template = pd.DataFrame(
        {
            "release_component_id": ["e4__financing_estimates"],
            "benchmark_timestamp_et": ["2024-04-29T08:00:00-04:00"],
            "benchmark_source": ["dealer_survey"],
            "benchmark_source_family": ["primary_dealer_auction_size_survey"],
            "benchmark_timing_status": ["pre_release_external"],
            "expected_composition_bn": [10.0],
            "realized_composition_bn": [20.0],
            "composition_surprise_bn": [10.0],
            "benchmark_stale_flag": [False],
            "expectation_review_status": ["reviewed"],
        }
    )
    contamination_reviews = pd.DataFrame(
        {
            "release_component_id": ["e4__financing_estimates"],
            "contamination_flag": [False],
            "contamination_status": ["reviewed_clean"],
            "contamination_review_status": ["reviewed"],
        }
    )

    out = build_qra_event_registry_v2(
        panel=panel,
        release_components=release_components,
        expectation_template=expectation_template,
        contamination_reviews=contamination_reviews,
    )

    assert out.loc[0, "release_timestamp_et"] == "2024-04-29T15:00:00-04:00"
    assert out.loc[0, "release_timestamp_kind"] == "release_component_registry_timestamp_with_time"
    assert out.loc[0, "release_time_et"] == "15:00:00"
    assert out.loc[0, "timestamp_precision"] == "exact_time"


def test_build_qra_event_registry_v2_uses_manual_overlap_severity_override() -> None:
    panel = pd.DataFrame(
        {
            "event_id": ["e3"],
            "quarter": ["2024Q3"],
            "event_date_type": ["official_release_date"],
            "official_release_date": ["2024-07-03"],
            "policy_statement_release_date": ["2024-07-03"],
            "financing_estimates_release_date": ["2024-07-02"],
            "policy_statement_url": ["https://example.com/policy3"],
            "financing_estimates_url": ["https://example.com/financing3"],
            "timing_quality": ["explicit_multi_stage_release"],
            "forward_guidance_bias": ["neutral"],
            "classification_review_status": ["reviewed"],
        }
    )
    overlap = pd.DataFrame(
        {
            "event_id": ["e3"],
            "overlap_flag": [True],
            "overlap_label": ["fomc_overlap"],
            "overlap_note": ["FOMC window overlap."],
            "overlap_severity": ["low"],
        }
    )

    out = build_qra_event_registry_v2(panel=panel, overlap_annotations=overlap)

    assert out.loc[0, "overlap_severity"] == "low"


def test_build_qra_shock_crosswalk_v1_extracts_manual_override_reason() -> None:
    elasticity = pd.DataFrame(
        {
            "event_id": ["e1"],
            "event_date_type": ["official_release_date"],
            "shock_bn": [25.0],
            "schedule_diff_10y_eq_bn": [30.0],
            "schedule_diff_dynamic_10y_eq_bn": [28.0],
            "schedule_diff_dv01_usd": [2_500_000.0],
            "gross_notional_delta_bn": [40.0],
            "shock_source": ["manual_schedule_diff_v1"],
            "shock_notes": ["Manual override after review."],
            "shock_review_status": ["reviewed"],
            "shock_construction": ["manual_override_with_schedule_context"],
        }
    )

    out = build_qra_shock_crosswalk_v1(elasticity)

    assert out.loc[0, "canonical_shock_id"] == "canonical_shock_bn"
    assert out.loc[0, "manual_override_reason"] == "Manual override after review."
    assert bool(out.loc[0, "alternative_treatment_complete"]) is True


def test_build_qra_release_component_registry_tracks_causal_gates() -> None:
    event_registry = pd.DataFrame(
        {
            "event_id": ["e1"],
            "quarter": ["2024Q1"],
            "release_sequence_label": ["financing_then_policy"],
            "same_day_release_bundle_flag": [False],
            "multi_stage_release_flag": [True],
            "review_status": ["reviewed"],
            "review_notes": ["Reviewed component split."],
            "policy_statement_release_timestamp_et": ["2024-01-31T08:30:00-05:00"],
            "policy_statement_release_timestamp_kind": ["timestamp_with_time"],
            "policy_statement_url": ["https://example.com/policy"],
            "financing_estimates_release_timestamp_et": ["2024-01-29T08:30:00-05:00"],
            "financing_estimates_release_timestamp_kind": ["timestamp_with_time"],
            "financing_estimates_url": ["https://example.com/financing"],
        }
    )
    expectations = pd.DataFrame(
        {
            "release_component_id": ["e1__financing_estimates"],
            "benchmark_timestamp_et": ["2024-01-29T08:00:00-05:00"],
            "benchmark_source": ["dealer_survey"],
            "benchmark_source_family": ["primary_dealer_auction_size_survey"],
            "benchmark_timing_status": ["pre_release_external"],
            "expected_composition_bn": [10.0],
            "realized_composition_bn": [25.0],
            "composition_surprise_bn": [15.0],
            "benchmark_stale_flag": [False],
            "expectation_review_status": ["reviewed"],
            "expectation_notes": ["Reviewed survey benchmark."],
        }
    )
    contamination = pd.DataFrame(
        {
            "release_component_id": ["e1__financing_estimates"],
            "contamination_flag": [False],
            "contamination_status": ["reviewed_clean"],
            "contamination_review_status": ["reviewed"],
            "contamination_label": [""],
            "contamination_notes": ["No conflicting macro release."],
        }
    )

    out = build_qra_release_component_registry(
        event_registry,
        expectation_template=expectations,
        contamination_reviews=contamination,
    )

    policy = out.loc[out["release_component_id"] == "e1__policy_statement"].iloc[0]
    financing = out.loc[out["release_component_id"] == "e1__financing_estimates"].iloc[0]
    assert financing["quality_tier"] == "Tier A"
    assert bool(financing["causal_eligible"]) is True
    assert financing["expectation_status"] == "reviewed_surprise_ready"
    assert financing["contamination_status"] == "reviewed_clean"
    assert policy["quality_tier"] == "Tier B"
    assert "policy_statement_descriptive_only" in policy["eligibility_blockers"]


def test_summarize_qra_causal_qa_and_event_design_status() -> None:
    component_registry = pd.DataFrame(
        {
            "release_component_id": ["e1__policy_statement", "e1__financing_estimates", "e2__policy_statement"],
            "event_id": ["e1", "e1", "e2"],
            "quality_tier": ["Tier A", "Tier B", "Tier D"],
            "eligibility_blockers": ["", "missing_expectation_benchmark", "review_not_complete|missing_exact_timestamp"],
            "timestamp_precision": ["exact_time", "exact_time", "date_only"],
            "separability_status": ["separable_component", "separable_component", "same_day_inseparable_bundle"],
            "expectation_status": ["reviewed_surprise_ready", "missing_benchmark", "missing_benchmark"],
            "contamination_status": ["reviewed_clean", "pending_review", "pending_review"],
            "causal_eligible": [True, False, False],
        }
    )

    summary = summarize_qra_causal_qa(component_registry)
    status = build_event_design_status(component_registry)

    e1 = summary.loc[summary["event_id"] == "e1"].iloc[0]
    assert e1["quality_tier"] == "Tier A"
    assert e1["release_component_count"] == 2
    assert e1["causal_eligible_component_count"] == 1
    assert "missing_expectation_benchmark" in e1["eligibility_blockers"]
    assert int(status.loc[status["metric"] == "tier_a_count", "value"].iloc[0]) == 1
    assert int(status.loc[status["metric"] == "release_component_count", "value"].iloc[0]) == 3


def test_build_qra_shock_crosswalk_v1_adds_missing_reason_for_reviewed_manual_statement_rows() -> None:
    elasticity = pd.DataFrame(
        {
            "event_id": ["e1"],
            "event_date_type": ["official_release_date"],
            "shock_bn": [25.0],
            "schedule_diff_10y_eq_bn": [pd.NA],
            "schedule_diff_dynamic_10y_eq_bn": [pd.NA],
            "schedule_diff_dv01_usd": [pd.NA],
            "gross_notional_delta_bn": [pd.NA],
            "shock_source": ["manual_statement_review_v1"],
            "shock_notes": [""],
            "shock_review_status": ["reviewed"],
            "shock_construction": ["manual_override_with_schedule_context"],
        }
    )

    out = build_qra_shock_crosswalk_v1(elasticity)

    assert bool(out.loc[0, "alternative_treatment_complete"]) is False
    assert (
        out.loc[0, "alternative_treatment_missing_reason"]
        == "manual_statement_primary_only_pending_alt_treatments"
    )


def test_build_event_usability_table_counts_unique_events() -> None:
    elasticity = pd.DataFrame(
        {
            "event_id": ["e1", "e1", "e2"],
            "event_date_type": ["official_release_date", "official_release_date", "market_pricing_marker_minus_1d"],
            "headline_bucket": ["tightening", "tightening", "exclude"],
            "classification_review_status": ["reviewed", "reviewed", "provisional"],
            "shock_review_status": ["reviewed", "reviewed", "provisional"],
            "usable_for_headline": [True, True, False],
            "shock_missing_flag": [False, False, False],
            "small_denominator_flag": [False, False, True],
        }
    )

    out = build_event_usability_table(elasticity)

    assert out["event_count"].sum() == 2
    assert "usable_for_headline_reason" in out.columns


def test_build_event_usability_table_uses_manual_overlap_severity_override() -> None:
    elasticity = pd.DataFrame(
        {
            "event_id": ["e3", "e3"],
            "event_date_type": ["official_release_date", "official_release_date"],
            "headline_bucket": ["tightening", "tightening"],
            "classification_review_status": ["reviewed", "reviewed"],
            "shock_review_status": ["reviewed", "reviewed"],
            "overlap_flag": [True, True],
            "overlap_label": ["", ""],
            "overlap_note": ["", ""],
            "usable_for_headline": [True, True],
        }
    )
    overlap_annotations = pd.DataFrame(
        {
            "event_id": ["e3"],
            "overlap_flag": [True],
            "overlap_label": ["fomc_overlap"],
            "overlap_note": [""],
            "overlap_severity": ["high"],
        }
    )

    out = build_event_usability_table(elasticity, overlap_annotations=overlap_annotations)

    assert out.loc[0, "overlap_severity"] == "high"
    assert out.loc[0, "event_count"] == 1


def test_build_leave_one_event_out_table_uses_headline_eligible_official_rows() -> None:
    elasticity = pd.DataFrame(
        {
            "event_id": ["e1", "e2", "e3"],
            "event_date_type": ["official_release_date", "official_release_date", "official_release_date"],
            "series": ["DGS10", "DGS10", "DGS10"],
            "window": ["d1", "d1", "d1"],
            "delta_bp": [10.0, 20.0, 30.0],
            "shock_bn": [50.0, 50.0, 50.0],
            "schedule_diff_10y_eq_bn": [45.0, 45.0, 45.0],
            "schedule_diff_dynamic_10y_eq_bn": [40.0, 40.0, 40.0],
            "schedule_diff_dv01_usd": [4_000_000.0, 4_000_000.0, 4_000_000.0],
            "usable_for_headline": [True, True, True],
        }
    )

    out = build_leave_one_event_out_table(elasticity)

    assert set(out["leave_one_out_event_id"]) == {"e1", "e2", "e3"}
    assert set(out["treatment_variant"]) >= {"canonical_shock_bn", "fixed_10y_eq_bn", "dynamic_10y_eq_bn", "dv01_usd"}
