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
from ati_shadow_policy.paths import MANUAL_DIR, PROCESSED_DIR, TABLES_DIR, ensure_project_dirs
from ati_shadow_policy.research.qra_elasticity import build_qra_event_elasticity

DEFAULT_PANEL = PROCESSED_DIR / "qra_event_panel.csv"
DEFAULT_TEMPLATE = MANUAL_DIR / "qra_event_shock_template.csv"
DEFAULT_OUTPUT = TABLES_DIR / "qra_event_elasticity.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build QRA event elasticities in basis points per $100bn from event deltas and manual shock inputs."
    )
    parser.add_argument(
        "--panel",
        default=str(DEFAULT_PANEL),
        help="Path to qra_event_panel.csv (default: %(default)s).",
    )
    parser.add_argument(
        "--shock-template",
        default=str(DEFAULT_TEMPLATE),
        help="Path to qra_event_shock_template.csv (default: %(default)s).",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Output CSV path (default: %(default)s).",
    )
    parser.add_argument(
        "--small-denominator-threshold-bn",
        default=10.0,
        type=float,
        help="Absolute shock threshold in $bn for small-denominator flagging (default: %(default)s).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_project_dirs()

    panel = pd.read_csv(Path(args.panel))
    shocks = pd.read_csv(Path(args.shock_template))
    output = build_qra_event_elasticity(
        panel=panel,
        shock_template=shocks,
        small_denominator_threshold_bn=float(args.small_denominator_threshold_bn),
    )

    output_path = Path(args.output)
    write_df(output, output_path)
    markdown_path = output_path.with_suffix(".md")
    markdown = "# QRA Event Elasticity\n\n" + output.to_markdown(index=False)
    markdown_path.write_text(markdown + "\n", encoding="utf-8")
    print(f"Saved QRA event elasticity with {len(output):,} rows to {output_path}")


if __name__ == "__main__":
    main()
