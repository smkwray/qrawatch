import pandas as pd
import pytest

from ati_shadow_policy.research.event_study import (
    build_event_panel,
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

    assert list(summary.columns) == ["headline_bucket", "x_d1", "x_d3", "y_d1", "y_d3"]
    assert list(summary["headline_bucket"]) == ["control_hold", "tightening"]
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

    assert list(summary.columns) == ["sample_variant", "event_date_type", "headline_bucket", "n_events", "x_d1", "x_d3"]
    assert list(summary["sample_variant"]) == ["all_events", "overlap_excluded"]
    assert list(summary["n_events"]) == [2, 1]
    assert summary.loc[summary["sample_variant"] == "all_events", "x_d1"].iloc[0] == 2.5
    assert summary.loc[summary["sample_variant"] == "overlap_excluded", "x_d1"].iloc[0] == 2.0


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
