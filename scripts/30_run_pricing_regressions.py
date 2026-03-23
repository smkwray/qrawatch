from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import PROCESSED_DIR, TABLES_DIR, ensure_project_dirs
from ati_shadow_policy.research.pricing_models import (
    OFFICIAL_ATI_PRICE_PANEL,
    MSPD_STOCK_PANEL,
    WEEKLY_SUPPLY_PANEL,
    build_pricing_spec_registry,
    build_pricing_regression_robustness,
    build_pricing_regression_summary,
    build_pricing_scenario_translation,
    build_pricing_subsample_grid,
)

DEFAULT_PANEL_OFFICIAL = PROCESSED_DIR / "official_ati_price_panel.csv"
DEFAULT_PANEL_MSPD_STOCK = PROCESSED_DIR / "mspd_stock_excess_bills_panel.csv"
DEFAULT_PANEL_WEEKLY = PROCESSED_DIR / "weekly_supply_price_panel.csv"
DEFAULT_SPEC_REGISTRY = TABLES_DIR / "pricing_spec_registry.csv"
DEFAULT_SUMMARY = TABLES_DIR / "pricing_regression_summary.csv"
DEFAULT_SUBSAMPLE_GRID = TABLES_DIR / "pricing_subsample_grid.csv"
DEFAULT_ROBUSTNESS = TABLES_DIR / "pricing_regression_robustness.csv"
DEFAULT_SCENARIO = TABLES_DIR / "pricing_scenario_translation.csv"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build HAC/Newey-West pricing regressions for ATI and duration-supply outcomes."
    )
    parser.add_argument(
        "--official-ati-panel",
        default=str(DEFAULT_PANEL_OFFICIAL),
        help="Path to official ATI monthly pricing panel.",
    )
    parser.add_argument(
        "--mspd-stock-panel",
        default=str(DEFAULT_PANEL_MSPD_STOCK),
        help="Path to monthly MSPD stock panel.",
    )
    parser.add_argument(
        "--weekly-supply-panel",
        default=str(DEFAULT_PANEL_WEEKLY),
        help="Path to weekly duration-supply pricing panel.",
    )
    parser.add_argument(
        "--spec-registry-output",
        default=str(DEFAULT_SPEC_REGISTRY),
        help="Output CSV path for pricing spec registry.",
    )
    parser.add_argument(
        "--summary-output",
        default=str(DEFAULT_SUMMARY),
        help="Output CSV path for pricing regression summary.",
    )
    parser.add_argument(
        "--subsample-output",
        default=str(DEFAULT_SUBSAMPLE_GRID),
        help="Output CSV path for pricing subsample grid.",
    )
    parser.add_argument(
        "--robustness-output",
        default=str(DEFAULT_ROBUSTNESS),
        help="Output CSV path for pricing robustness summary.",
    )
    parser.add_argument(
        "--scenario-output",
        default=str(DEFAULT_SCENARIO),
        help="Output CSV path for scenario translation table.",
    )
    return parser.parse_args(argv)


def _read_panel(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} panel: {path}")
    return pd.read_csv(path, low_memory=False)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    ensure_project_dirs()

    panels = {
        OFFICIAL_ATI_PRICE_PANEL: _read_panel(Path(args.official_ati_panel), "official ATI"),
        MSPD_STOCK_PANEL: _read_panel(Path(args.mspd_stock_panel), "MSPD stock"),
        WEEKLY_SUPPLY_PANEL: _read_panel(Path(args.weekly_supply_panel), "weekly supply"),
    }

    spec_registry = build_pricing_spec_registry(panels)
    summary = build_pricing_regression_summary(panels)
    subsample_grid = build_pricing_subsample_grid(panels)
    robustness = build_pricing_regression_robustness(panels)
    scenarios = build_pricing_scenario_translation(summary)

    spec_registry_path = Path(args.spec_registry_output)
    summary_path = Path(args.summary_output)
    subsample_path = Path(args.subsample_output)
    robustness_path = Path(args.robustness_output)
    scenario_path = Path(args.scenario_output)

    write_df(spec_registry, spec_registry_path)
    write_df(summary, summary_path)
    write_df(subsample_grid, subsample_path)
    write_df(robustness, robustness_path)
    write_df(scenarios, scenario_path)

    (spec_registry_path.with_suffix(".md")).write_text(
        "# Pricing Spec Registry\n\n" + spec_registry.to_markdown(index=False) + "\n",
        encoding="utf-8",
    )
    (summary_path.with_suffix(".md")).write_text(
        "# Pricing Regression Summary\n\n" + summary.to_markdown(index=False) + "\n",
        encoding="utf-8",
    )
    (subsample_path.with_suffix(".md")).write_text(
        "# Pricing Subsample Grid\n\n" + subsample_grid.to_markdown(index=False) + "\n",
        encoding="utf-8",
    )
    (robustness_path.with_suffix(".md")).write_text(
        "# Pricing Regression Robustness\n\n" + robustness.to_markdown(index=False) + "\n",
        encoding="utf-8",
    )
    (scenario_path.with_suffix(".md")).write_text(
        "# Pricing Scenario Translation\n\n" + scenarios.to_markdown(index=False) + "\n",
        encoding="utf-8",
    )

    print(
        f"Saved pricing outputs with spec rows {len(spec_registry):,}, summary rows {len(summary):,}, "
        f"subsample rows {len(subsample_grid):,}, robustness rows {len(robustness):,}, "
        f"and scenario rows {len(scenarios):,}"
    )


if __name__ == "__main__":
    main()
