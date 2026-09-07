from __future__ import annotations

import pandas as pd
import pytest

from ati_shadow_policy.research.qra_promotion_audit import build_qra_promotion_audit


@pytest.fixture
def publish_dir(tmp_path):
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
        [{"claim_id": "current_sample_financing_pilot", "claim_scope": "causal_pilot_only",
          "headline_ready": False, "source_family_exhausted_count": 8}]
    ).to_csv(publish_dir / "causal_claims_status.csv", index=False)
    pd.DataFrame(
        [
            {
                "event_date_type": "official_release_date",
                "claim_scope": "descriptive_only",
                "usable_for_headline": True,
            },
            {
                "event_date_type": "market_pricing_marker_minus_1d",
                "claim_scope": "descriptive_only",
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
                "public_claim_role": "supporting",
                "public_readiness": "supporting_provisional",
                "term_units": "USD 100bn",
                "outcome_units": "basis points",
                "coef": -0.7,
                "p_value": 0.48,
            },
            {
                "model_id": "monthly_flow_baseline",
                "term_role": "primary_predictor",
                "dependent_variable": "DGS10",
                "public_claim_role": "supporting",
                "public_readiness": "supporting_provisional",
                "term_units": "USD 100bn",
                "outcome_units": "basis points",
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
                "public_claim_role": "supporting",
                "public_readiness": "supporting_provisional",
                "term_units": "USD 100bn",
                "outcome_units": "basis points",
                "coef": 0.2,
                "p_value": 0.49,
            }
        ]
    ).to_csv(publish_dir / "pricing_regression_robustness.csv", index=False)

    return publish_dir


def test_qra_promotion_audit_keeps_pricing_bounded(publish_dir):
    audit = build_qra_promotion_audit(publish_dir)

    assert set(audit["lane"]) >= {"official_qra_measurement", "release_flow_pricing_anchor"}
    release = audit.loc[audit["lane"] == "release_flow_pricing_anchor"].iloc[0]
    assert release["status"] == "supporting_provisional"
    assert "Do not use the release-flow coefficient" in release["forbidden_upgrade"]


def test_missing_evidence_cannot_promote(tmp_path):

    with pytest.raises(ValueError, match="Missing required audit evidence"):
        build_qra_promotion_audit(tmp_path)


def test_duplicate_metric_is_rejected():
    from ati_shadow_policy.research.qra_promotion_audit import _metric_value

    frame = pd.DataFrame({"scope": ["s", "s"], "metric": ["n", "n"], "value": [1, 2]})
    with pytest.raises(ValueError, match="Expected one"):
        _metric_value(frame, "s", "n")


@pytest.mark.parametrize("values", [[1, "bad"], ["bad"], [float("nan")], [float("inf")], [-1.5], [1.5], []])
def test_malformed_counts_are_rejected(values):
    from ati_shadow_policy.research.qra_promotion_audit import _metric_value

    frame = pd.DataFrame({"scope": ["s"] * len(values), "metric": ["n"] * len(values), "value": values})
    with pytest.raises(ValueError, match="Expected one nonnegative integer"):
        _metric_value(frame, "s", "n")


@pytest.mark.parametrize("mutation", ["duplicate", "missing", "negative", "malformed"])
def test_causal_count_requires_exact_valid_pilot(publish_dir, mutation):
    path = publish_dir / "causal_claims_status.csv"
    frame = pd.read_csv(path)
    if mutation == "duplicate":
        frame = pd.concat([frame, frame], ignore_index=True)
    elif mutation == "missing":
        frame["claim_id"] = "unrelated_claim"
    else:
        frame["source_family_exhausted_count"] = -1.5 if mutation == "negative" else "bad"
    frame.to_csv(path, index=False)
    with pytest.raises(ValueError, match="Expected one nonnegative integer"):
        build_qra_promotion_audit(publish_dir)


@pytest.mark.parametrize("mutation", ["zero_financing", "zero_official", "absent_official"])
def test_empty_lane_cannot_make_empirical_claim(publish_dir, mutation):
    if mutation == "zero_financing":
        path = publish_dir / "qra_benchmark_coverage.csv"
        frame = pd.read_csv(path)
        frame["value"] = 0
        lane = "current_sample_financing_pilot"
    else:
        path = publish_dir / "event_usability_table.csv"
        frame = pd.read_csv(path)
        if mutation == "zero_official":
            frame["usable_for_headline"] = False
        else:
            frame = frame[frame["event_date_type"] != "official_release_date"]
        lane = "official_release_event_windows"
    frame.to_csv(path, index=False)
    result = build_qra_promotion_audit(publish_dir).set_index("lane").loc[lane]
    assert result["status"] == "evidence_unavailable"
    assert result["allowed_claim"].startswith("No empirical claim:")


@pytest.mark.parametrize("stem,column,value", [
    ("causal_claims_status", "claim_scope", "headline"),
    ("causal_claims_status", "headline_ready", True),
    ("event_usability_table", "claim_scope", "headline"),
    ("pricing_regression_summary", "public_readiness", "headline"),
    ("pricing_regression_summary", "public_claim_role", "headline"),
    ("pricing_regression_summary", "term_units", "USD bn"),
    ("pricing_regression_summary", "outcome_units", "percentage points"),
    ("pricing_regression_robustness", "public_readiness", "headline"),
])
def test_contradictory_scope_or_units_fail(publish_dir, stem, column, value):
    path = publish_dir / f"{stem}.csv"
    frame = pd.read_csv(path)
    frame[column] = value
    frame.to_csv(path, index=False)
    with pytest.raises(ValueError, match="contradictory"):
        build_qra_promotion_audit(publish_dir)


def test_pricing_roles_remain_neutral_when_estimates_change(publish_dir, tmp_path):
    from ati_shadow_policy.research.qra_promotion_audit import write_qra_promotion_audit

    for stem in ["pricing_regression_summary", "pricing_regression_robustness"]:
        path = publish_dir / f"{stem}.csv"
        frame = pd.read_csv(path)
        frame["coef"] = 0
        frame["p_value"] = 1
        frame.to_csv(path, index=False)
    csv_path, md_path = write_qra_promotion_audit(
        tmp_path / "audit.csv", tmp_path / "audit.md", publish_dir=publish_dir
    )
    audit = pd.read_csv(csv_path).set_index("lane")
    assert audit.loc["monthly_maturity_tilt_flow", "status"] == "supporting_provisional"
    assert audit.loc["pre_release_placebo", "status"] == "boundary_check"
    assert "+0.000 bp per $100bn, p=1.000" in audit.loc["monthly_maturity_tilt_flow", "evidence"]
    for path in [csv_path, md_path]:
        text = path.read_text()
        assert not any(word in text for word in ["strongest", "strong_but", "weak_imprecise", "mixed_but"])


@pytest.mark.parametrize("stem,column", [
    ("causal_claims_status", "claim_scope"),
    ("event_usability_table", "claim_scope"),
    ("pricing_regression_summary", "term_units"),
])
def test_scope_columns_are_required(publish_dir, stem, column):
    path = publish_dir / f"{stem}.csv"
    pd.read_csv(path).drop(columns=[column]).to_csv(path, index=False)
    with pytest.raises(ValueError, match="missing columns"):
        build_qra_promotion_audit(publish_dir)


@pytest.mark.parametrize("column,value", [("coef", float("nan")), ("coef", float("inf")), ("p_value", "bad"), ("p_value", 1.5)])
def test_invalid_pricing_values_fail(publish_dir, column, value):
    path = publish_dir / "pricing_regression_summary.csv"
    frame = pd.read_csv(path)
    frame[column] = value
    frame.to_csv(path, index=False)
    with pytest.raises(ValueError, match="invalid"):
        build_qra_promotion_audit(publish_dir)
