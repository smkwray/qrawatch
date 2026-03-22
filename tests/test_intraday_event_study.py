from __future__ import annotations

import math

import pandas as pd

from ati_shadow_policy.research.intraday_event_study import build_intraday_event_panel


def test_build_intraday_event_panel_uses_exact_time_components_only() -> None:
    series = pd.DataFrame(
        {
            "timestamp_et": [
                "2024-01-31T08:00:00-05:00",
                "2024-01-31T08:35:00-05:00",
                "2024-01-31T09:00:00-05:00",
            ],
            "DGS10": [4.00, 4.05, 4.08],
        }
    )
    components = pd.DataFrame(
        {
            "release_component_id": ["e1__policy_statement", "e2__policy_statement"],
            "event_id": ["e1", "e2"],
            "quarter": ["2024Q1", "2024Q1"],
            "component_type": ["policy_statement", "policy_statement"],
            "release_timestamp_et": ["2024-01-31T08:30:00-05:00", "2024-01-31"],
            "timestamp_precision": ["exact_time", "date_only"],
            "quality_tier": ["Tier A", "Tier B"],
            "causal_eligible": [True, True],
            "eligibility_blockers": ["", "missing_exact_timestamp"],
        }
    )

    out = build_intraday_event_panel(series, components, ["DGS10"], pre_minutes=30, post_minutes=30)

    assert len(out) == 1
    row = out.iloc[0]
    assert row["release_component_id"] == "e1__policy_statement"
    assert math.isclose(row["delta_value"], 0.08)
    assert row["window_label"] == "m30_to_p30"


def test_build_intraday_event_panel_can_include_noncausal_exact_time_rows() -> None:
    series = pd.DataFrame(
        {
            "timestamp_et": [
                "2024-01-31T08:00:00-05:00",
                "2024-01-31T09:00:00-05:00",
            ],
            "THREEFYTP10": [0.10, 0.12],
        }
    )
    components = pd.DataFrame(
        {
            "release_component_id": ["e1__policy_statement"],
            "event_id": ["e1"],
            "quarter": ["2024Q1"],
            "component_type": ["policy_statement"],
            "release_timestamp_et": ["2024-01-31T08:30:00-05:00"],
            "timestamp_precision": ["exact_time"],
            "quality_tier": ["Tier B"],
            "causal_eligible": [False],
            "eligibility_blockers": ["missing_expectation_benchmark"],
        }
    )

    out = build_intraday_event_panel(
        series,
        components,
        ["THREEFYTP10"],
        pre_minutes=30,
        post_minutes=30,
        causal_only=False,
    )

    assert len(out) == 1
    assert out.iloc[0]["quality_tier"] == "Tier B"
