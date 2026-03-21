from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]


def _load_module(script_name: str, module_name: str):
    script_path = ROOT / "scripts" / script_name
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_plumbing_regression_variants_are_named_and_labeled():
    mod = _load_module("11_run_plumbing_regressions.py", "plumbing_script")
    n = 40
    x = np.arange(n, dtype=float)
    panel = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=n, freq="W-WED"),
            "bill_like": 100 + 2 * x,
            "bill_net_exact": 95 + 1.7 * x,
            "frn": 20 + 0.5 * x,
            "coupon_like_total": 80 + 1.5 * x,
            "coupon_plus_frn_total": 100 + 2.0 * x,
            "nonbill_net_exact": 75 + 1.2 * x,
            "nominal_coupon": 70 + 1.1 * x,
            "delta_wdtgal": 5 + 0.1 * x,
            "qt_proxy": 3 + 0.2 * x,
            "delta_wlrral": 10 + 0.3 * x,
            "delta_wresbal": 50 - 0.4 * x,
        }
    )
    panel = mod.add_plumbing_proxy_columns(panel)
    out = mod.run_regression_variants(panel)
    assert not out.empty
    assert set(out["variant_id"].unique()) == {
        "baseline_frn_separate_tips_included",
        "robustness_frn_with_bills_tips_included",
        "robustness_frn_with_coupons_tips_included",
        "robustness_frn_separate_tips_excluded",
    }
    assert set(out["dependent_variable"].unique()) == {"delta_wlrral", "delta_wresbal"}
    assert set(out["proxy_units"].unique()) == {"USD notional (weekly sum)"}

    baseline = out.loc[out["variant_id"] == mod.BASELINE_VARIANT_ID].copy()
    assert set(baseline["bill_proxy_col"].unique()) == {"bill_net_exact"}
    assert set(baseline["duration_proxy_col"].unique()) == {"nonbill_net_exact"}
    assert set(baseline["series_role"].unique()) == {"headline"}
    assert set(baseline["bill_proxy_source_quality"].unique()) == {"exact_official_net"}

    robustness_summary = mod.build_robustness_summary(out)
    assert set(robustness_summary["proxy_role"].unique()) == {"bill_proxy", "duration_proxy"}
    assert "expected_sign_bill_proxy" in robustness_summary.columns
    assert "expected_sign_duration_proxy" in robustness_summary.columns


def test_duration_supply_comparison_includes_treasury_vs_combined_constructions():
    mod = _load_module("12_build_public_duration_supply.py", "duration_script")
    panel = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-01-07", "2026-01-14", "2026-01-21"]),
            "nonbill_net_exact": [92.0, 118.0, 108.0],
            "coupon_like_total": [100.0, 120.0, 110.0],
            "coupon_plus_frn_total": [130.0, 155.0, 145.0],
            "nominal_coupon": [80.0, 95.0, 90.0],
            "qt_proxy": [10.0, -2.0, 5.0],
            "buybacks_accepted": [3.0, 4.0, 2.0],
        }
    )
    with_variants = mod.add_duration_supply_variants(panel)
    assert np.allclose(
        with_variants["headline_public_duration_supply"],
        with_variants["nonbill_net_exact"] + with_variants["qt_proxy"] - with_variants["buybacks_accepted"],
    )
    assert np.allclose(
        with_variants["provisional_public_duration_supply"],
        with_variants["combined_duration_nominal_plus_tips"],
    )
    assert np.allclose(with_variants["treasury_only_duration_nominal_plus_tips"], with_variants["coupon_like_total"])

    comparison = mod.build_duration_supply_comparison(with_variants, latest_rows=2)
    assert set(comparison["construction_family"].unique()) == {"headline", "treasury_only", "combined_duration"}
    assert "headline_nonbill_net_exact_qt_minus_buybacks" in set(comparison["construction_id"])
    assert "treasury_only_nominal_plus_tips" in set(comparison["construction_id"])
    assert "combined_nominal_plus_tips_qt_minus_buybacks" in set(comparison["construction_id"])
    assert set(comparison["value_units"].unique()) == {"USD notional (weekly sum)"}
    assert set(comparison["sign_convention"].unique()) == {"Higher values imply more duration supply to the public."}
