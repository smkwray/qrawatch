import pytest
import pandas as pd

from ati_shadow_policy.research.event_study import build_event_panel, summarize_event_panel, summarize_event_panel_robustness

def test_build_event_panel_basic():
    series = pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=5, freq="D"),
        "x": [1, 2, 4, 7, 11],
    })
    events = pd.DataFrame({
        "event_id": ["e1"],
        "event_label": ["Event 1"],
        "official_release_date": ["2024-01-03"],
        "expected_direction": ["tightening"],
    })
    panel = build_event_panel(series, events, value_columns=["x"], event_date_column="official_release_date")
    assert len(panel) == 1
    assert panel.loc[0, "x_level_t"] == 4
    assert panel.loc[0, "x_d1"] == 2  # 4 - 2
    assert panel.loc[0, "x_d3"] == 5  # 7 - 2


def test_build_event_panel_merges_overlap_annotations():
    series = pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=6, freq="D"),
        "x": [1, 2, 4, 7, 11, 16],
    })
    events = pd.DataFrame({
        "event_id": ["e1", "e2"],
        "event_label": ["Event 1", "Event 2"],
        "official_release_date": ["2024-01-03", "2024-01-04"],
        "expected_direction": ["tightening", "tightening"],
    })
    overlap_annotations = pd.DataFrame({
        "event_id": ["e1", "e2"],
        "overlap_flag": [True, False],
        "overlap_label": ["FOMC", ""],
        "overlap_note": ["same day", ""],
    })

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


def test_build_event_panel_missing_outcome_column_raises():
    series = pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=5, freq="D"),
        "x": [1, 2, 4, 7, 11],
    })
    events = pd.DataFrame({
        "event_id": ["e1"],
        "event_label": ["Event 1"],
        "official_release_date": ["2024-01-03"],
        "expected_direction": ["tightening"],
    })

    with pytest.raises(KeyError, match="Missing outcome series column"):
        build_event_panel(series, events, value_columns=["x", "y"], event_date_column="official_release_date")


def test_build_event_panel_unalignable_event_raises():
    series = pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=5, freq="D"),
        "x": [1, 2, 4, 7, 11],
    })
    events = pd.DataFrame({
        "event_id": ["e1"],
        "event_label": ["Event 1"],
        "official_release_date": ["2023-12-31"],
        "expected_direction": ["tightening"],
    })

    with pytest.raises(ValueError, match="Could not align event"):
        build_event_panel(series, events, value_columns=["x"], event_date_column="official_release_date")


def test_summarize_event_panel_is_stable():
    panel = pd.DataFrame({
        "expected_direction": ["zeta", "alpha"],
        "y_d3": [6.0, 8.0],
        "x_d1": [1.0, 3.0],
        "x_d3": [2.0, 4.0],
        "y_d1": [5.0, 7.0],
    })

    summary = summarize_event_panel(panel)

    assert list(summary.columns) == ["expected_direction", "x_d1", "x_d3", "y_d1", "y_d3"]
    assert list(summary["expected_direction"]) == ["alpha", "zeta"]
    assert summary.loc[summary["expected_direction"] == "alpha", "x_d1"].iloc[0] == 3.0


def test_summarize_event_panel_ignores_unclassified_rows():
    panel = pd.DataFrame({
        "expected_direction": ["tightening", "", None],
        "x_d1": [1.0, 5.0, 9.0],
        "x_d3": [2.0, 6.0, 10.0],
    })

    summary = summarize_event_panel(panel)

    assert list(summary["expected_direction"]) == ["tightening"]
    assert summary.loc[0, "x_d1"] == 1.0


def test_summarize_event_panel_robustness_separates_overlap_exclusion():
    series = pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=6, freq="D"),
        "x": [1, 2, 4, 7, 11, 16],
    })
    events = pd.DataFrame({
        "event_id": ["e1", "e2"],
        "event_label": ["Event 1", "Event 2"],
        "official_release_date": ["2024-01-03", "2024-01-04"],
        "expected_direction": ["tightening", "tightening"],
    })
    overlap_annotations = pd.DataFrame({
        "event_id": ["e1", "e2"],
        "overlap_flag": [False, True],
        "overlap_label": ["", "FOMC"],
        "overlap_note": ["", "same day"],
    })

    panel = build_event_panel(
        series,
        events,
        value_columns=["x"],
        event_date_column="official_release_date",
        overlap_annotations=overlap_annotations,
    )
    summary = summarize_event_panel_robustness(panel)

    assert list(summary.columns) == ["sample_variant", "event_date_type", "expected_direction", "n_events", "x_d1", "x_d3"]
    assert list(summary["sample_variant"]) == ["all_events", "overlap_excluded"]
    assert list(summary["n_events"]) == [2, 1]
    assert summary.loc[summary["sample_variant"] == "all_events", "x_d1"].iloc[0] == 2.5
    assert summary.loc[summary["sample_variant"] == "overlap_excluded", "x_d1"].iloc[0] == 2.0


def test_summarize_event_panel_robustness_ignores_unclassified_rows():
    panel = pd.DataFrame({
        "event_date_type": ["official_release_date", "official_release_date"],
        "expected_direction": ["tightening", ""],
        "overlap_flag": [False, False],
        "x_d1": [2.0, 20.0],
        "x_d3": [3.0, 30.0],
    })

    summary = summarize_event_panel_robustness(panel)

    assert list(summary["expected_direction"]) == ["tightening", "tightening"]
    assert list(summary["n_events"]) == [1, 1]
