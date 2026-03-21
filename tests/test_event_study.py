import pandas as pd
import pytest

from ati_shadow_policy.research.event_study import (
    build_overlap_exclusion_audit_note,
    build_event_panel,
    build_qra_event_registry_v2,
    summarize_event_panel,
    summarize_event_panel_robustness,
)


def test_build_event_panel_preserves_context_columns() -> None:
    series = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=5, freq="D"),
            "x": [1, 2, 4, 7, 11],
        }
    )
    events = pd.DataFrame(
        {
            "event_id": ["e1"],
            "event_label": ["Event 1"],
            "quarter": ["2024Q1"],
            "official_release_date": ["2024-01-03"],
            "market_pricing_marker_minus_1d": ["2024-01-02"],
            "policy_statement_url": ["https://example.com/policy"],
            "current_quarter_action": ["tightening"],
            "forward_guidance_bias": ["neutral"],
            "headline_bucket": ["tightening"],
            "classification_review_status": ["reviewed"],
            "expected_direction": ["tightening"],
        }
    )

    panel = build_event_panel(series, events, value_columns=["x"], event_date_column="official_release_date")

    assert len(panel) == 1
    assert panel.loc[0, "quarter"] == "2024Q1"
    assert panel.loc[0, "policy_statement_url"] == "https://example.com/policy"
    assert panel.loc[0, "headline_bucket"] == "tightening"
    assert panel.loc[0, "event_date_requested"] == pd.Timestamp("2024-01-03")
    assert panel.loc[0, "event_date_aligned"] == pd.Timestamp("2024-01-03")
    assert panel.loc[0, "spec_id"] == "spec_qra_event_v2"
    assert panel.loc[0, "treatment_variant"] == "event_window_deltas_v1"
    assert panel.loc[0, "x_level_t"] == 4
    assert panel.loc[0, "x_d1"] == 2
    assert panel.loc[0, "x_d3"] == 5


def test_build_event_panel_merges_overlap_annotations() -> None:
    series = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=6, freq="D"),
            "x": [1, 2, 4, 7, 11, 16],
        }
    )
    events = pd.DataFrame(
        {
            "event_id": ["e1", "e2"],
            "event_label": ["Event 1", "Event 2"],
            "official_release_date": ["2024-01-03", "2024-01-04"],
            "headline_bucket": ["tightening", "tightening"],
        }
    )
    overlap_annotations = pd.DataFrame(
        {
            "event_id": ["e1", "e2"],
            "overlap_flag": [True, False],
            "overlap_label": ["FOMC", ""],
            "overlap_note": ["same day", ""],
            "overlap_severity": ["high", ""],
        }
    )

    panel = build_event_panel(
        series,
        events,
        value_columns=["x"],
        event_date_column="official_release_date",
        overlap_annotations=overlap_annotations,
    )

    assert list(panel["overlap_flag"]) == [True, False]
    assert list(panel["overlap_label"]) == ["FOMC", ""]
    assert list(panel["overlap_note"]) == ["same day", ""]
    assert list(panel["overlap_severity"]) == ["high", ""]


def test_build_event_panel_missing_outcome_column_raises() -> None:
    series = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=5, freq="D"),
            "x": [1, 2, 4, 7, 11],
        }
    )
    events = pd.DataFrame(
        {
            "event_id": ["e1"],
            "event_label": ["Event 1"],
            "official_release_date": ["2024-01-03"],
            "headline_bucket": ["tightening"],
        }
    )

    with pytest.raises(KeyError, match="Missing outcome series column"):
        build_event_panel(series, events, value_columns=["x", "y"], event_date_column="official_release_date")


def test_build_event_panel_unalignable_event_raises() -> None:
    series = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=5, freq="D"),
            "x": [1, 2, 4, 7, 11],
        }
    )
    events = pd.DataFrame(
        {
            "event_id": ["e1"],
            "event_label": ["Event 1"],
            "official_release_date": ["2023-12-31"],
            "headline_bucket": ["tightening"],
        }
    )

    with pytest.raises(ValueError, match="Could not align event"):
        build_event_panel(series, events, value_columns=["x"], event_date_column="official_release_date")


def test_summarize_event_panel_groups_on_headline_bucket() -> None:
    panel = pd.DataFrame(
        {
            "headline_bucket": ["control_hold", "tightening", "exclude", "pending"],
            "y_d3": [6.0, 8.0, 99.0, 100.0],
            "x_d1": [1.0, 3.0, 50.0, 60.0],
            "x_d3": [2.0, 4.0, 60.0, 70.0],
            "y_d1": [5.0, 7.0, 70.0, 80.0],
        }
    )

    summary = summarize_event_panel(panel)

    assert list(summary.columns) == ["spec_id", "treatment_variant", "headline_bucket", "x_d1", "x_d3", "y_d1", "y_d3"]
    assert list(summary["headline_bucket"]) == ["control_hold", "tightening"]
    assert set(summary["spec_id"]) == {"spec_qra_event_v2"}
    assert summary.loc[summary["headline_bucket"] == "tightening", "x_d1"].iloc[0] == 3.0


def test_summarize_event_panel_robustness_separates_overlap_exclusion() -> None:
    series = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=6, freq="D"),
            "x": [1, 2, 4, 7, 11, 16],
        }
    )
    events = pd.DataFrame(
        {
            "event_id": ["e1", "e2"],
            "event_label": ["Event 1", "Event 2"],
            "official_release_date": ["2024-01-03", "2024-01-04"],
            "headline_bucket": ["tightening", "tightening"],
        }
    )
    overlap_annotations = pd.DataFrame(
        {
            "event_id": ["e1", "e2"],
            "overlap_flag": [False, True],
            "overlap_label": ["", "FOMC"],
            "overlap_note": ["", "same day"],
        }
    )

    panel = build_event_panel(
        series,
        events,
        value_columns=["x"],
        event_date_column="official_release_date",
        overlap_annotations=overlap_annotations,
    )
    summary = summarize_event_panel_robustness(panel)

    assert list(summary.columns) == [
        "spec_id",
        "treatment_variant",
        "sample_variant",
        "event_date_type",
        "headline_bucket",
        "n_events",
        "x_d1",
        "x_d3",
        "overlap_exclusion_note",
    ]
    assert list(summary["sample_variant"]) == ["all_events", "overlap_excluded"]
    assert list(summary["n_events"]) == [2, 1]
    assert summary.loc[summary["sample_variant"] == "all_events", "x_d1"].iloc[0] == 2.5
    assert summary.loc[summary["sample_variant"] == "overlap_excluded", "x_d1"].iloc[0] == 2.0
    assert "removed 1 overlap-annotated event" in summary.loc[0, "overlap_exclusion_note"]


def test_summarize_event_panel_robustness_does_not_emit_unused_date_type_categories() -> None:
    panel = pd.DataFrame(
        {
            "event_date_type": ["official_release_date", "official_release_date"],
            "headline_bucket": ["tightening", "tightening"],
            "overlap_flag": [False, False],
            "x_d1": [2.0, 3.0],
            "x_d3": [4.0, 5.0],
        }
    )

    summary = summarize_event_panel_robustness(panel)

    assert len(summary) == 2
    assert set(summary["event_date_type"]) == {"official_release_date"}
    assert list(summary["headline_bucket"]) == ["tightening", "tightening"]
    assert "identical to all_events" in summary.loc[0, "overlap_exclusion_note"]


def test_build_overlap_exclusion_audit_note_handles_no_overlap_flags() -> None:
    panel = pd.DataFrame(
        {
            "event_id": ["e1"],
            "overlap_flag": [False],
        }
    )
    robustness = pd.DataFrame(
        {
            "sample_variant": ["all_events", "overlap_excluded"],
            "event_date_type": ["official_release_date", "official_release_date"],
            "headline_bucket": ["tightening", "tightening"],
            "x_d1": [1.0, 1.0],
            "x_d3": [2.0, 2.0],
        }
    )
    note = build_overlap_exclusion_audit_note(panel, robustness)
    assert "no overlap-annotated events were flagged" in note


def test_build_qra_event_registry_v2_emits_required_fields() -> None:
    panel = pd.DataFrame(
        {
            "event_id": ["e1", "e1"],
            "quarter": ["2024Q1", "2024Q1"],
            "event_date_type": ["official_release_date", "market_pricing_marker_minus_1d"],
            "policy_statement_url": ["https://example.com/policy", "https://example.com/policy"],
            "financing_estimates_url": ["https://example.com/financing", "https://example.com/financing"],
            "timing_quality": ["same_day_release_bundle", "same_day_release_bundle"],
            "overlap_flag": [True, True],
            "overlap_label": ["FOMC", "FOMC"],
            "current_quarter_action": ["tightening", "tightening"],
            "forward_guidance_bias": ["neutral", "neutral"],
            "headline_bucket": ["tightening", "tightening"],
            "classification_review_status": ["reviewed", "reviewed"],
        }
    )
    registry = build_qra_event_registry_v2(panel)
    assert list(registry.columns) == [
        "event_id",
        "quarter",
        "release_timestamp_et",
        "release_bundle_type",
        "policy_statement_url",
        "financing_estimates_url",
        "timing_quality",
        "overlap_severity",
        "overlap_label",
        "financing_need_news_flag",
        "composition_news_flag",
        "forward_guidance_flag",
        "reviewer",
        "review_date",
        "treatment_version_id",
        "headline_eligibility_reason",
    ]
    assert len(registry) == 1
    assert registry.loc[0, "overlap_severity"] == "high"
    assert bool(registry.loc[0, "financing_need_news_flag"]) is True
    assert registry.loc[0, "headline_eligibility_reason"] == "eligible_pending_shock_checks"


def test_build_qra_event_registry_v2_respects_explicit_overlap_severity() -> None:
    panel = pd.DataFrame(
        {
            "event_id": ["e2"],
            "quarter": ["2024Q2"],
            "event_date_type": ["official_release_date"],
            "overlap_flag": [True],
            "overlap_label": ["FOMC"],
            "overlap_note": ["same day"],
            "overlap_severity": ["medium"],
            "current_quarter_action": ["tightening"],
            "forward_guidance_bias": ["neutral"],
            "headline_bucket": ["tightening"],
            "classification_review_status": ["reviewed"],
            "release_timestamp_et": ["2024-01-01T00:00:00-05:00"],
            "timing_quality": ["explicit_multi_stage_release"],
        }
    )

    registry = build_qra_event_registry_v2(panel)

    assert registry.loc[0, "overlap_severity"] == "medium"
