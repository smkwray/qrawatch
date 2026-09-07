from __future__ import annotations

import pandas as pd

from ati_shadow_policy.research.qra_promotion_audit import build_qra_promotion_audit


def test_qra_promotion_audit_keeps_pricing_bounded(tmp_path):
    publish_dir = tmp_path / "publish"
    publish_dir.mkdir()
    pd.DataFrame(
        [
            {"quarter": "2025Q4", "is_headline_ready": True},
            {"quarter": "2025Q3", "is_headline_ready": True},
        ]
    ).to_csv(publish_dir / "official_capture_completion.csv", index=False)
    pd.DataFrame(
        [
            {
                "scope": "current_sample_financing_estimates",
                "metric": "external_benchmark_ready_count",
                "value": 6,
                "notes": "",
            },
            {
                "scope": "current_sample_financing_estimates",
                "metric": "causal_eligible_count",
                "value": 5,
                "notes": "",
            },
            {
                "scope": "current_sample_financing_estimates",
                "metric": "post_release_invalid_count",
                "value": 8,
                "notes": "",
            },
        ]
    ).to_csv(publish_dir / "qra_benchmark_coverage.csv", index=False)
    pd.DataFrame(
        [{"source_family_exhausted_count": 8}]
    ).to_csv(publish_dir / "causal_claims_status.csv", index=False)
    pd.DataFrame(
        [
            {
                "event_date_type": "official_release_date",
                "usable_for_headline": True,
            },
            {
                "event_date_type": "market_pricing_marker_minus_1d",
                "usable_for_headline": False,
            },
        ]
    ).to_csv(publish_dir / "event_usability_table.csv", index=False)
    pd.DataFrame(
        [
            {
                "usable_for_headline_reason": "non_official_event_date_type",
            }
        ]
    ).to_csv(publish_dir / "qra_event_elasticity_diagnostic.csv", index=False)
    pd.DataFrame(
        [
            {
                "model_id": "release_flow_baseline_63bd",
                "term_role": "primary_predictor",
                "dependent_variable": "DGS10",
                "coef": -0.7,
                "p_value": 0.48,
            },
            {
                "model_id": "monthly_flow_baseline",
                "term_role": "primary_predictor",
                "dependent_variable": "DGS10",
                "coef": -5.2,
                "p_value": 0.03,
            },
        ]
    ).to_csv(publish_dir / "pricing_regression_summary.csv", index=False)
    pd.DataFrame(
        [
            {
                "model_id": "release_flow_placebo_5bd_to_1bd",
                "variant_family": "release_flow_placebo",
                "dependent_variable": "DGS10",
                "coef": 0.2,
                "p_value": 0.49,
            }
        ]
    ).to_csv(publish_dir / "pricing_regression_robustness.csv", index=False)

    audit = build_qra_promotion_audit(publish_dir)

    assert set(audit["lane"]) >= {"official_qra_measurement", "release_flow_pricing_anchor"}
    release = audit.loc[audit["lane"] == "release_flow_pricing_anchor"].iloc[0]
    assert release["status"] == "weak_imprecise"
    assert "Do not use the release-flow coefficient" in release["forbidden_upgrade"]


def test_missing_evidence_cannot_promote(tmp_path):
    import pytest

    with pytest.raises(ValueError, match="Missing required audit evidence"):
        build_qra_promotion_audit(tmp_path)


def test_duplicate_metric_is_rejected():
    import pytest
    from ati_shadow_policy.research.qra_promotion_audit import _metric_value

    frame = pd.DataFrame({"scope": ["s", "s"], "metric": ["n", "n"], "value": [1, 2]})
    with pytest.raises(ValueError, match="Expected one"):
        _metric_value(frame, "s", "n")
