from __future__ import annotations

import pandas as pd

from ati_shadow_policy.research.identification import (
    build_event_usability_table,
    build_leave_one_event_out_table,
    build_qra_event_registry_v2,
    build_qra_shock_crosswalk_v1,
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
    assert out.loc[0, "release_timestamp_kind"] == "official_release_date_date_only"
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
    assert out.loc[0, "headline_eligibility_reason"] == "eligible_headline_reviewed_official_release"


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

    assert out.loc[0, "release_timestamp_kind"] == "official_release_date_timestamp_with_time"
    assert out.loc[0, "release_time_et"] == "08:30:00"
    assert out.loc[0, "overlap_annotation_source"] == "data/manual/qra_event_overlap_annotations.csv"
    assert bool(out.loc[0, "overlap_annotation_present"]) is True
    assert bool(out.loc[0, "headline_eligible"]) is False
    assert out.loc[0, "headline_eligibility_blockers"] == "classification_not_reviewed|shock_not_reviewed|missing_shock|small_denominator"
    assert out.loc[0, "headline_eligibility_reason"] == "missing_shock|small_denominator|classification_not_reviewed|shock_not_reviewed"


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
