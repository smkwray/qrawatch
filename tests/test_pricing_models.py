from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ati_shadow_policy.research import pricing_models as pricing_models


def _load_script(script_name: str, module_name: str):
    root = Path(__file__).resolve().parents[1]
    script_path = root / "scripts" / script_name
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _make_synthetic_panels() -> dict[str, pd.DataFrame]:
    months = pd.date_range("2008-01-31", periods=216, freq="ME")
    t = np.arange(len(months), dtype=float)
    ati = 20.0 + 0.25 * t + 4.0 * np.sin(t / 9.0)
    stock = -80.0 + 0.9 * t + 8.0 * np.cos(t / 10.0)
    dff = 0.8 + 0.03 * np.sin(t / 5.0)
    debt_limit = (((months >= "2013-05-31") & (months <= "2013-10-31")) | ((months >= "2023-02-28") & (months <= "2023-06-30"))).astype(float)

    official = pd.DataFrame(
        {
            "date": months,
            "ati_baseline_bn": ati,
            "stock_excess_bills_bn": stock,
            "stock_excess_bills_share": stock / 20000.0,
            "cumulative_ati_baseline_bn": np.cumsum(ati) / 12.0,
            "DFF": dff,
            "debt_limit_dummy": debt_limit,
            "THREEFYTP10": 5.0 + 0.08 * ati + 0.03 * stock + 2.0 * dff + 0.7 * debt_limit,
            "DGS10": 45.0 + 0.11 * ati + 0.02 * stock + 2.5 * dff + 1.1 * debt_limit,
            "DGS30": 65.0 + 0.09 * ati + 0.025 * stock + 2.2 * dff + 0.9 * debt_limit,
            "slope_10y_2y": 10.0 + 0.03 * ati - 0.01 * stock + 0.2 * dff,
            "slope_30y_2y": 20.0 + 0.02 * ati - 0.01 * stock + 0.2 * dff,
        }
    )

    mspd = pd.DataFrame(
        {
            "date": months,
            "marketable_outstanding_bn": 15000.0 + 50.0 * t,
            "marketable_bill_share": 0.19 + 0.0002 * np.sin(t / 7.0),
            "stock_excess_bills_share": stock / (15000.0 + 50.0 * t),
            "stock_excess_bills_bn": stock,
        }
    )

    weekly = pd.date_range("2008-01-02", periods=700, freq="W-WED")
    wt = np.arange(len(weekly), dtype=float)
    duration = 40.0 + 0.15 * wt + 4.0 * np.sin(wt / 13.0)
    qt = 12.0 + 0.08 * np.cos(wt / 6.0)
    buybacks = 2.0 + 0.03 * np.sin(wt / 4.0)
    delta_wdtgal = -0.8 + 0.05 * np.cos(wt / 8.0)
    dff_weekly = 1.5 + 0.02 * np.sin(wt / 11.0)
    debt_limit_weekly = (((weekly >= "2013-05-01") & (weekly <= "2013-10-30")) | ((weekly >= "2023-02-01") & (weekly <= "2023-06-28"))).astype(float)

    weekly_panel = pd.DataFrame(
        {
            "date": weekly,
            "headline_public_duration_supply": duration,
            "qt_proxy": qt,
            "buybacks_accepted": buybacks,
            "delta_wdtgal": delta_wdtgal,
            "DFF": dff_weekly,
            "debt_limit_dummy": debt_limit_weekly,
            "THREEFYTP10": 8.0 + 0.12 * duration + 0.05 * qt - 0.08 * buybacks + 0.4 * delta_wdtgal + 1.5 * dff_weekly,
            "DGS10": 55.0 + 0.15 * duration + 0.03 * qt - 0.05 * buybacks + 0.5 * delta_wdtgal + 1.8 * dff_weekly,
            "DGS30": 72.0 + 0.13 * duration + 0.04 * qt - 0.03 * buybacks + 0.4 * delta_wdtgal + 1.6 * dff_weekly,
            "slope_10y_2y": 15.0 + 0.04 * duration - 0.02 * qt + 0.01 * buybacks + 0.1 * dff_weekly,
            "slope_30y_2y": 22.0 + 0.03 * duration - 0.02 * qt + 0.01 * buybacks + 0.1 * dff_weekly,
        }
    )

    return {
        pricing_models.OFFICIAL_ATI_PRICE_PANEL: official,
        pricing_models.MSPD_STOCK_PANEL: mspd,
        pricing_models.WEEKLY_SUPPLY_PANEL: weekly_panel,
    }


def _load_regression_outputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    panels = _make_synthetic_panels()
    registry = pricing_models.build_pricing_spec_registry(panels)
    summary = pricing_models.build_pricing_regression_summary(panels)
    subsample = pricing_models.build_pricing_subsample_grid(panels)
    robustness = pricing_models.build_pricing_regression_robustness(panels)
    scenarios = pricing_models.build_pricing_scenario_translation(summary)
    return registry, summary, subsample, robustness, scenarios


def test_pricing_spec_registry_is_deterministic_and_covers_headline_specs() -> None:
    registry, _, _, _, _ = _load_regression_outputs()

    required_columns = set(pricing_models.PRICING_SPEC_REGISTRY_COLUMNS)
    assert required_columns <= set(registry.columns)
    assert set(registry["spec_id"]) == {
        "monthly_flow_baseline",
        "monthly_stock_baseline",
        "weekly_duration_baseline",
    }
    assert set(registry["outcome"]) == {"THREEFYTP10", "DGS10"}
    assert registry["headline_flag"].eq(True).all()


def test_build_pricing_regression_summary_has_locked_headline_specs() -> None:
    _, summary, _, _, _ = _load_regression_outputs()

    required_columns = set(pricing_models.PRICING_REGRESSION_SUMMARY_COLUMNS)
    assert required_columns <= set(summary.columns)
    assert set(summary["model_id"]) == {
        "monthly_flow_baseline",
        "monthly_stock_baseline",
        "weekly_duration_baseline",
    }
    assert summary["cov_type"].eq("HAC").all()
    assert summary["cov_maxlags"].eq(pricing_models.NEWEY_WEST_MAXLAGS).all()
    assert set(summary["outcome_role"]) == {"headline"}
    assert set(summary["dependent_variable"]) == {"THREEFYTP10", "DGS10"}


def test_build_pricing_subsample_grid_covers_required_families_and_outcomes() -> None:
    _, _, subsample, _, _ = _load_regression_outputs()

    required_columns = set(pricing_models.PRICING_SUBSAMPLE_GRID_COLUMNS)
    assert required_columns <= set(subsample.columns)
    assert set(subsample["variant_family"]) == {
        "post_2009",
        "post_2014",
        "post_2020",
        "exclude_debt_limit",
    }
    assert {"THREEFYTP10", "DGS10"}.issubset(set(subsample["dependent_variable"]))
    assert "DGS30" in set(subsample["dependent_variable"])


def test_build_pricing_regression_robustness_covers_required_robustness_families() -> None:
    _, _, _, robustness, _ = _load_regression_outputs()

    required_columns = set(pricing_models.PRICING_REGRESSION_ROBUSTNESS_COLUMNS)
    assert required_columns <= set(robustness.columns)
    assert set(robustness["variant_family"]) == {
        "supporting_outcome_dgs30",
        "flow_vs_stock_horse_race",
        "standardized_predictors",
    }
    assert "const" not in set(robustness["term"])
    assert "DFF" not in set(robustness["term"])
    assert robustness["term_mode"].isin(
        {"supporting_outcome_dgs30", "flow_vs_stock_horse_race", "standardized_predictors"}
    ).all()


def test_build_pricing_scenario_translation_applies_linear_shock_arithmetic() -> None:
    _, summary, _, _, scenarios = _load_regression_outputs()

    assert set(scenarios["scenario_id"]) == {
        "plus_100bn_duration_supply",
        "plus_500bn_term_out",
        "plus_1000bn_term_out",
    }
    duration_rows = scenarios.loc[scenarios["scenario_id"] == "plus_100bn_duration_supply"]
    assert len(duration_rows) == 2
    flow_row = duration_rows.iloc[0]
    source = summary.loc[
        (summary["model_id"] == "weekly_duration_baseline")
        & (summary["dependent_variable"] == flow_row["dependent_variable"])
        & (summary["term"] == "headline_public_duration_supply")
    ]
    assert not source.empty
    expected = pricing_models.implied_bp_change(float(source.iloc[0]["coef"]), 100.0, pricing_models.SCENARIO_SCALE_BN)
    assert pytest.approx(expected, rel=1e-12) == float(flow_row["implied_bp_change"])

    stock_rows = scenarios.loc[scenarios["scenario_id"] == "plus_500bn_term_out"]
    assert len(stock_rows) == 2
    stock_row = stock_rows.iloc[0]
    source_stock = summary.loc[
        (summary["model_id"] == "monthly_stock_baseline")
        & (summary["dependent_variable"] == stock_row["dependent_variable"])
        & (summary["term"] == "stock_excess_bills_bn")
    ]
    assert not source_stock.empty
    expected_stock = pricing_models.implied_bp_change(
        float(source_stock.iloc[0]["coef"]),
        500.0,
        pricing_models.SCENARIO_SCALE_BN,
    )
    assert pytest.approx(expected_stock, rel=1e-12) == float(stock_row["implied_bp_change"])


def test_pricing_regression_script_writes_expected_artifacts(tmp_path) -> None:
    script = _load_script("30_run_pricing_regressions.py", "pricing_script")
    panels = _make_synthetic_panels()
    processed_dir = tmp_path / "processed"
    tables_dir = tmp_path / "tables"
    processed_dir.mkdir()
    tables_dir.mkdir()

    official = processed_dir / "official_ati_price_panel.csv"
    mspd = processed_dir / "mspd_stock_excess_bills_panel.csv"
    weekly = processed_dir / "weekly_supply_price_panel.csv"
    panels[pricing_models.OFFICIAL_ATI_PRICE_PANEL].to_csv(official, index=False)
    panels[pricing_models.MSPD_STOCK_PANEL].to_csv(mspd, index=False)
    panels[pricing_models.WEEKLY_SUPPLY_PANEL].to_csv(weekly, index=False)

    spec_registry = tables_dir / "pricing_spec_registry.csv"
    summary = tables_dir / "pricing_regression_summary.csv"
    subsample = tables_dir / "pricing_subsample_grid.csv"
    robustness = tables_dir / "pricing_regression_robustness.csv"
    scenario = tables_dir / "pricing_scenario_translation.csv"

    script.main(
        [
            "--official-ati-panel",
            str(official),
            "--mspd-stock-panel",
            str(mspd),
            "--weekly-supply-panel",
            str(weekly),
            "--spec-registry-output",
            str(spec_registry),
            "--summary-output",
            str(summary),
            "--subsample-output",
            str(subsample),
            "--robustness-output",
            str(robustness),
            "--scenario-output",
            str(scenario),
        ]
    )

    assert spec_registry.exists()
    assert summary.exists()
    assert subsample.exists()
    assert robustness.exists()
    assert scenario.exists()

    spec_df = pd.read_csv(spec_registry)
    summary_df = pd.read_csv(summary)
    subsample_df = pd.read_csv(subsample)
    robustness_df = pd.read_csv(robustness)
    scenario_df = pd.read_csv(scenario)

    assert not spec_df.empty
    assert not summary_df.empty
    assert not subsample_df.empty
    assert not robustness_df.empty
    assert not scenario_df.empty
    assert {"spec_id", "spec_family", "headline_flag"} <= set(spec_df.columns)
    assert {"dependent_variable", "coef", "std_err", "t_stat", "p_value", "cov_type"} <= set(summary_df.columns)
    assert {"variant_id", "variant_family", "dependent_variable", "coef"} <= set(subsample_df.columns)
