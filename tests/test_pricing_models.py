from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ati_shadow_policy.research import pricing_panels as pricing_panels
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
            "marketable_bill_share": 0.19 + 0.0002 * np.sin(t / 7.0),
            "marketable_outstanding_bn": 15000.0 + 50.0 * t,
            "DFF": dff,
            "debt_limit_dummy": debt_limit,
            "THREEFYTP10": 5.0 - 0.08 * ati + 0.03 * stock + 2.0 * dff + 0.7 * debt_limit,
            "DGS10": 45.0 - 0.11 * ati + 0.02 * stock + 2.5 * dff + 1.1 * debt_limit,
            "DGS30": 65.0 - 0.09 * ati + 0.025 * stock + 2.2 * dff + 0.9 * debt_limit,
            "slope_10y_2y": 10.0 - 0.03 * ati - 0.01 * stock + 0.2 * dff,
            "slope_30y_2y": 20.0 - 0.02 * ati - 0.01 * stock + 0.2 * dff,
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
            "THREEFYTP10": 8.0 - 0.12 * duration + 0.05 * qt - 0.08 * buybacks + 0.4 * delta_wdtgal + 1.5 * dff_weekly,
            "DGS10": 55.0 - 0.15 * duration + 0.03 * qt - 0.05 * buybacks + 0.5 * delta_wdtgal + 1.8 * dff_weekly,
            "DGS30": 72.0 - 0.13 * duration + 0.04 * qt - 0.03 * buybacks + 0.4 * delta_wdtgal + 1.6 * dff_weekly,
            "slope_10y_2y": 15.0 - 0.04 * duration - 0.02 * qt + 0.01 * buybacks + 0.1 * dff_weekly,
            "slope_30y_2y": 22.0 - 0.03 * duration - 0.02 * qt + 0.01 * buybacks + 0.1 * dff_weekly,
        }
    )

    releases = pd.date_range("2008-02-15", periods=56, freq="QS")
    rt = np.arange(len(releases), dtype=float)
    release_ati = 30.0 + 2.0 * np.sin(rt / 3.0) + 0.8 * rt
    release_dff = 0.02 * np.sin(rt / 5.0)
    release_debt_limit = (((releases >= "2013-04-01") & (releases <= "2013-10-01")) | ((releases >= "2023-01-01") & (releases <= "2023-07-01"))).astype(float)
    release_panel = pd.DataFrame(
        {
            "release_id": [f"{date.strftime('%Y-%m-%d')}__{date.year}Q{((date.month - 1) // 3) + 1}" for date in releases],
            "quarter": [f"{date.year}Q{((date.month - 1) // 3) + 1}" for date in releases],
            "source_quarters": [f"{date.year}Q{((date.month - 1) // 3) + 1}" for date in releases],
            "release_row_count": 1,
            "qra_release_date": releases,
            "market_pricing_marker_minus_1d": releases - pd.offsets.BDay(1),
            "total_financing_need_bn": 150.0 + 2.0 * rt,
            "net_bill_issuance_bn": 30.0 + 0.8 * rt,
            "bill_share": 0.20 + 0.001 * np.sin(rt / 4.0),
            "ati_baseline_bn": release_ati,
            "ati_baseline_bn_posonly": np.maximum(release_ati, 0.0),
            "debt_limit_dummy": release_debt_limit,
            "target_tau": 0.18,
            "DGS10": 400.0 - 0.4 * release_ati,
            "THREEFYTP10": 100.0 - 0.2 * release_ati,
            "DGS30": 450.0 - 0.35 * release_ati,
        }
    )
    horizon_specs = {
        1: (-0.015, -0.010, -0.012, 0.45),
        5: (-0.025, -0.016, -0.020, 0.65),
        10: (-0.035, -0.022, -0.028, 0.80),
        21: (-0.050, -0.032, -0.040, 1.00),
        42: (-0.070, -0.045, -0.055, 1.20),
        63: (-0.095, -0.060, -0.075, 1.40),
    }
    for horizon_bd, (coef_dgs10, coef_tp, coef_dgs30, dff_scale) in horizon_specs.items():
        window_label = pricing_panels.release_flow_window_label(horizon_bd)
        release_panel[pricing_panels.release_flow_end_date_column(horizon_bd)] = releases + pd.offsets.BDay(horizon_bd)
        release_panel[pricing_panels.release_flow_delta_column("DGS10", window_label)] = (
            coef_dgs10 * release_ati + dff_scale * release_dff + 0.35 * release_debt_limit
        )
        release_panel[pricing_panels.release_flow_delta_column("THREEFYTP10", window_label)] = (
            coef_tp * release_ati + 0.6 * dff_scale * release_dff + 0.25 * release_debt_limit
        )
        release_panel[pricing_panels.release_flow_delta_column("DGS30", window_label)] = (
            coef_dgs30 * release_ati + 0.8 * dff_scale * release_dff + 0.30 * release_debt_limit
        )
        release_panel[pricing_panels.release_flow_control_column(window_label)] = release_dff * dff_scale

    placebo_specs = {
        (-21, -1): (0.004, 0.003, 0.0035, 0.5),
        (-5, -1): (0.002, 0.0015, 0.0018, 0.3),
    }
    for (start_bd, end_bd), (coef_dgs10, coef_tp, coef_dgs30, dff_scale) in placebo_specs.items():
        window_label = pricing_panels.release_flow_placebo_label(start_bd, end_bd)
        release_panel[pricing_panels.release_flow_placebo_start_date_column(start_bd, end_bd)] = (
            releases + pd.offsets.BDay(start_bd)
        )
        release_panel[pricing_panels.release_flow_delta_column("DGS10", window_label)] = (
            coef_dgs10 * release_ati + dff_scale * release_dff + 0.10 * release_debt_limit
        )
        release_panel[pricing_panels.release_flow_delta_column("THREEFYTP10", window_label)] = (
            coef_tp * release_ati + 0.5 * dff_scale * release_dff + 0.08 * release_debt_limit
        )
        release_panel[pricing_panels.release_flow_delta_column("DGS30", window_label)] = (
            coef_dgs30 * release_ati + 0.6 * dff_scale * release_dff + 0.09 * release_debt_limit
        )
        release_panel[pricing_panels.release_flow_control_column(window_label)] = release_dff * dff_scale

    return {
        pricing_models.OFFICIAL_ATI_PRICE_PANEL: official,
        pricing_models.MSPD_STOCK_PANEL: mspd,
        pricing_models.WEEKLY_SUPPLY_PANEL: weekly_panel,
        pricing_models.RELEASE_FLOW_PANEL: release_panel,
    }


def _load_regression_outputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    panels = _make_synthetic_panels()
    registry = pricing_models.build_pricing_spec_registry(panels)
    summary = pricing_models.build_pricing_regression_summary(panels)
    subsample = pricing_models.build_pricing_subsample_grid(panels)
    robustness = pricing_models.build_pricing_regression_robustness(panels)
    scenarios = pricing_models.build_pricing_scenario_translation(summary)
    leave_one_out = pricing_models.build_pricing_release_flow_leave_one_out(panels)
    tau_grid = pricing_models.build_pricing_tau_sensitivity_grid(panels)
    return registry, summary, subsample, robustness, scenarios, leave_one_out, tau_grid


def test_pricing_spec_registry_is_deterministic_and_covers_release_anchor_specs() -> None:
    registry, _, _, _, _, _, _ = _load_regression_outputs()

    required_columns = set(pricing_models.PRICING_SPEC_REGISTRY_COLUMNS)
    assert required_columns <= set(registry.columns)
    assert {
        "release_flow_baseline_63bd",
        "release_flow_horizon_1bd",
        "release_flow_horizon_5bd",
        "release_flow_horizon_10bd",
        "release_flow_horizon_21bd",
        "release_flow_horizon_42bd",
        "monthly_flow_baseline",
        "monthly_stock_baseline",
        "weekly_duration_baseline",
    } == set(registry["spec_id"])
    assert set(registry["anchor_role"]) == {"credibility_anchor", "headline_context", "supporting"}
    assert "release_plus_63bd" in set(registry["window_definition"])
    assert registry.loc[registry["spec_id"] == "release_flow_baseline_63bd", "anchor_role"].eq("credibility_anchor").all()
    assert registry.loc[registry["spec_id"] == "monthly_flow_baseline", "anchor_role"].eq("headline_context").all()


def test_build_pricing_regression_summary_has_release_level_anchor_and_effective_shocks() -> None:
    panels = _make_synthetic_panels()
    _, summary, _, _, _, _, _ = _load_regression_outputs()

    required_columns = set(pricing_models.PRICING_REGRESSION_SUMMARY_COLUMNS)
    assert required_columns <= set(summary.columns)
    assert {"release_flow_baseline_63bd", "release_flow_horizon_21bd"}.issubset(set(summary["model_id"]))
    assert summary["cov_type"].eq("HAC").all()
    assert summary["cov_maxlags"].eq(pricing_models.NEWEY_WEST_MAXLAGS).all()
    assert set(summary["dependent_variable"]) == {"THREEFYTP10", "DGS10"}
    assert summary["effective_shock_count"].gt(0).all()
    anchor = summary.loc[(summary["model_id"] == "release_flow_baseline_63bd") & (summary["term"] == "ati_baseline_bn")]
    assert not anchor.empty
    assert anchor["anchor_role"].eq("credibility_anchor").all()
    assert anchor["window_definition"].eq("release_plus_63bd").all()
    assert anchor["coef"].lt(0).all()
    release_spec = next(spec for spec in pricing_models.BASELINE_SPECS if spec["spec_id"] == "release_flow_baseline_63bd")
    prepared, _ = pricing_models._prepare_regression_panel(  # noqa: SLF001
        panels[pricing_models.RELEASE_FLOW_PANEL],
        release_spec,
        "DGS10",
    )
    expected_shocks = prepared["market_pricing_marker_minus_1d"].nunique()
    assert anchor["effective_shock_count"].eq(expected_shocks).all()


def test_build_pricing_subsample_grid_covers_required_families_outcomes_and_release_windows() -> None:
    _, _, subsample, _, _, _, _ = _load_regression_outputs()

    required_columns = set(pricing_models.PRICING_SUBSAMPLE_GRID_COLUMNS)
    assert required_columns <= set(subsample.columns)
    assert set(subsample["variant_family"]) == {
        "post_2009",
        "post_2014",
        "post_2020",
        "exclude_debt_limit",
    }
    assert {"THREEFYTP10", "DGS10", "DGS30"}.issubset(set(subsample["dependent_variable"]))
    assert {"release_plus_63bd", "release_plus_21bd", "carry_forward_monthly"}.issubset(set(subsample["window_definition"]))


def test_build_pricing_regression_robustness_covers_required_families() -> None:
    _, _, _, robustness, _, _, _ = _load_regression_outputs()

    required_columns = set(pricing_models.PRICING_REGRESSION_ROBUSTNESS_COLUMNS)
    assert required_columns <= set(robustness.columns)
    assert set(robustness["variant_family"]) == {
        "supporting_outcome_dgs30",
        "flow_vs_stock_horse_race",
        "release_flow_placebo",
        "standardized_predictors",
    }
    assert "const" not in set(robustness["term"])
    assert "DFF" not in set(robustness["term"])


def test_build_pricing_scenario_translation_marks_stock_scenarios_illustrative_only() -> None:
    _, summary, _, _, scenarios, _, _ = _load_regression_outputs()

    assert set(scenarios["scenario_id"]) == {
        "plus_100bn_duration_supply",
        "plus_500bn_term_out",
        "plus_1000bn_term_out",
    }
    stock_rows = scenarios.loc[scenarios["scenario_id"].isin(["plus_500bn_term_out", "plus_1000bn_term_out"])]
    assert stock_rows["scenario_role"].eq("illustrative_only").all()
    duration_rows = scenarios.loc[scenarios["scenario_id"] == "plus_100bn_duration_supply"]
    assert duration_rows["scenario_role"].eq("supporting").all()

    stock_row = stock_rows.iloc[0]
    source_stock = summary.loc[
        (summary["model_id"] == "monthly_stock_baseline")
        & (summary["dependent_variable"] == stock_row["dependent_variable"])
        & (summary["term"] == "stock_excess_bills_bn")
    ]
    assert not source_stock.empty
    expected_stock = pricing_models.implied_bp_change(
        float(source_stock.iloc[0]["coef"]),
        float(stock_row["scenario_shock_bn"]),
        pricing_models.SCENARIO_SCALE_BN,
    )
    assert pytest.approx(expected_stock, rel=1e-12) == float(stock_row["implied_bp_change"])


def test_release_flow_leave_one_out_and_tau_grid_publish_expected_shapes() -> None:
    _, _, _, _, _, leave_one_out, tau_grid = _load_regression_outputs()

    assert set(leave_one_out.columns) == set(pricing_models.PRICING_RELEASE_FLOW_LEAVE_ONE_OUT_COLUMNS)
    release_count = leave_one_out["omitted_release_id"].nunique()
    assert len(leave_one_out) == release_count * len(pricing_models.HEADLINE_OUTCOMES)
    assert leave_one_out["coef"].lt(0).all()
    assert leave_one_out["spec_id"].eq("release_flow_baseline_63bd").all()

    assert set(tau_grid.columns) == set(pricing_models.PRICING_TAU_SENSITIVITY_GRID_COLUMNS)
    assert set(tau_grid["tau"]) == {0.15, 0.18, 0.20}
    assert {"THREEFYTP10", "DGS10", "DGS30"}.issubset(set(tau_grid["dependent_variable"]))


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
    release_flow = processed_dir / "pricing_release_flow_panel.csv"
    panels[pricing_models.OFFICIAL_ATI_PRICE_PANEL].to_csv(official, index=False)
    panels[pricing_models.MSPD_STOCK_PANEL].to_csv(mspd, index=False)
    panels[pricing_models.WEEKLY_SUPPLY_PANEL].to_csv(weekly, index=False)
    panels[pricing_models.RELEASE_FLOW_PANEL].to_csv(release_flow, index=False)

    spec_registry = tables_dir / "pricing_spec_registry.csv"
    summary = tables_dir / "pricing_regression_summary.csv"
    subsample = tables_dir / "pricing_subsample_grid.csv"
    robustness = tables_dir / "pricing_regression_robustness.csv"
    scenario = tables_dir / "pricing_scenario_translation.csv"
    leave_one_out = tables_dir / "pricing_release_flow_leave_one_out.csv"
    tau_grid = tables_dir / "pricing_tau_sensitivity_grid.csv"

    script.main(
        [
            "--official-ati-panel",
            str(official),
            "--mspd-stock-panel",
            str(mspd),
            "--weekly-supply-panel",
            str(weekly),
            "--release-flow-panel",
            str(release_flow),
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
            "--leave-one-out-output",
            str(leave_one_out),
            "--tau-sensitivity-output",
            str(tau_grid),
        ]
    )

    for path in (spec_registry, summary, subsample, robustness, scenario, leave_one_out, tau_grid):
        assert path.exists()
        assert path.with_suffix(".md").exists()

    spec_df = pd.read_csv(spec_registry)
    summary_df = pd.read_csv(summary)
    leave_one_out_df = pd.read_csv(leave_one_out)
    tau_df = pd.read_csv(tau_grid)

    assert "anchor_role" in spec_df.columns
    assert {"window_definition", "effective_shock_count"}.issubset(set(summary_df.columns))
    assert "scenario_role" in pd.read_csv(scenario).columns
    assert not leave_one_out_df.empty
    assert set(tau_df["tau"]) == {0.15, 0.18, 0.20}
