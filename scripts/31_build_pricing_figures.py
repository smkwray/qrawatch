from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from ati_shadow_policy.paths import FIGURES_DIR, PROCESSED_DIR, TABLES_DIR, ensure_project_dirs
from ati_shadow_policy.research.pricing_figures import build_horizontal_bar_svg, build_overlay_svg

DEFAULT_OFFICIAL_PANEL = PROCESSED_DIR / "official_ati_price_panel.csv"
DEFAULT_SUMMARY = TABLES_DIR / "pricing_regression_summary.csv"
DEFAULT_SUBSAMPLE = TABLES_DIR / "pricing_subsample_grid.csv"
DEFAULT_SCENARIO = TABLES_DIR / "pricing_scenario_translation.csv"

OVERLAY_FLOW = "maturity_tilt_flow_vs_dgs10.svg"
OVERLAY_STOCK = "excess_bills_stock_vs_threefytp10.svg"
COEFFICIENT_PLOT = "pricing_headline_coefficients.svg"
SCENARIO_PLOT = "pricing_scenario_translation.svg"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build paper-style SVG figures for pricing outputs.")
    parser.add_argument("--official-panel", default=str(DEFAULT_OFFICIAL_PANEL), help="Monthly official pricing panel CSV.")
    parser.add_argument("--summary", default=str(DEFAULT_SUMMARY), help="Pricing regression summary CSV.")
    parser.add_argument("--subsample", default=str(DEFAULT_SUBSAMPLE), help="Pricing subsample grid CSV.")
    parser.add_argument("--scenario", default=str(DEFAULT_SCENARIO), help="Pricing scenario translation CSV.")
    parser.add_argument("--figures-dir", default=str(FIGURES_DIR), help="Output directory for SVG figures.")
    return parser.parse_args(argv)


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_csv(path)


def _coefficient_labels(frame: pd.DataFrame) -> tuple[list[str], list[float]]:
    labels: list[str] = []
    values: list[float] = []
    for _, row in frame.iterrows():
        outcome = "10Y Yield" if str(row.get("dependent_variable")) == "DGS10" else "10Y Term Premium"
        spec = str(row.get("model_id") or row.get("spec_id"))
        variant = str(row.get("variant_family", "baseline"))
        spec_label = (
            spec
            .replace("release_flow_baseline_next_release", "Release flow (next release)")
            .replace("release_flow_baseline_21bd", "Release flow (+21bd)")
            .replace("monthly_flow_baseline", "Flow baseline")
            .replace("monthly_stock_baseline", "Stock baseline")
            .replace("weekly_duration_baseline", "Duration baseline")
        )
        variant_label = "" if variant in {"", "baseline", "nan"} else f" | {variant.replace('_', ' ')}"
        labels.append(f"{spec_label}{variant_label} | {outcome}")
        values.append(float(row.get("coef", 0.0)))
    return labels, values


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    ensure_project_dirs()

    official = _read_csv(Path(args.official_panel))
    summary = _read_csv(Path(args.summary))
    subsample = _read_csv(Path(args.subsample))
    scenario = _read_csv(Path(args.scenario))

    figures_dir = Path(args.figures_dir)
    figures_dir.mkdir(parents=True, exist_ok=True)

    official["date"] = pd.to_datetime(official["date"], errors="coerce")

    build_overlay_svg(
        official.loc[official["date"] >= pd.Timestamp("2009-01-01")].copy(),
        date_col="date",
        left_col="ati_baseline_bn",
        right_col="DGS10",
        left_label="Maturity-Tilt Flow (standardized)",
        right_label="10Y Treasury yield (standardized)",
        title="Maturity-Tilt Flow vs 10Y Yield",
        subtitle="Overlay shown in standardized units from 2009 onward to compare timing, not absolute levels.",
        output_path=figures_dir / OVERLAY_FLOW,
    )
    build_overlay_svg(
        official.loc[official["date"] >= pd.Timestamp("2009-01-01")].copy(),
        date_col="date",
        left_col="stock_excess_bills_bn",
        right_col="THREEFYTP10",
        left_label="Excess Bills Stock (standardized)",
        right_label="10Y term premium proxy (standardized)",
        title="Excess Bills Stock vs 10Y Term Premium Proxy",
        subtitle="Overlay shown in standardized units from 2009 onward to compare timing, not absolute levels.",
        output_path=figures_dir / OVERLAY_STOCK,
    )

    primary = summary.loc[
        summary.get("term_role", pd.Series(dtype=str)).astype(str).eq("primary_predictor")
        & summary.get("dependent_variable", pd.Series(dtype=str)).astype(str).isin(["DGS10", "THREEFYTP10"])
    ].copy()
    subsample_primary = subsample.loc[
        subsample.get("dependent_variable", pd.Series(dtype=str)).astype(str).isin(["DGS10", "THREEFYTP10"])
    ].copy()
    coeff_frame = pd.concat([primary, subsample_primary], ignore_index=True, sort=False)
    coeff_labels, coeff_values = _coefficient_labels(coeff_frame)
    build_horizontal_bar_svg(
        coeff_labels,
        coeff_values,
        title="Release-Level Flow Anchor and Context Specs",
        subtitle="Coefficients are reported in basis points per $100bn on the named input; release-level flow rows are the current anchor.",
        output_path=figures_dir / COEFFICIENT_PLOT,
    )

    scenario_labels = [
        f"{row['scenario_label']} | {row.get('scenario_role', 'supporting').replace('_', ' ')} | {'10Y Yield' if row['dependent_variable'] == 'DGS10' else '10Y Term Premium'}"
        for _, row in scenario.iterrows()
    ]
    scenario_values = [float(value) for value in scenario["implied_bp_change"]]
    build_horizontal_bar_svg(
        scenario_labels,
        scenario_values,
        title="Scenario Translation",
        subtitle="Implied basis-point change using the matching baseline coefficient.",
        output_path=figures_dir / SCENARIO_PLOT,
    )

    print(
        "Saved pricing figures: "
        + ", ".join(str(figures_dir / name) for name in (OVERLAY_FLOW, OVERLAY_STOCK, COEFFICIENT_PLOT, SCENARIO_PLOT))
    )


if __name__ == "__main__":
    main()
