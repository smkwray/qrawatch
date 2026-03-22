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
from ati_shadow_policy.research.intraday_event_study import build_intraday_event_panel

DEFAULT_COMPONENT_REGISTRY = TABLES_DIR / "qra_release_component_registry.csv"
DEFAULT_MARKET_DATA = PROCESSED_DIR / "qra_intraday_market_data.csv"
DEFAULT_OUTPUT = TABLES_DIR / "qra_intraday_event_panel.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an intraday QRA event panel from exact-time release components and timestamped market data."
    )
    parser.add_argument("--component-registry", default=str(DEFAULT_COMPONENT_REGISTRY))
    parser.add_argument("--market-data", default=str(DEFAULT_MARKET_DATA))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--timestamp-column", default="timestamp_et")
    parser.add_argument("--pre-minutes", default=30, type=int)
    parser.add_argument("--post-minutes", default=30, type=int)
    parser.add_argument(
        "--value-columns",
        default="DGS10,DGS30,THREEFYTP10,slope_10y_2y",
        help="Comma-separated intraday value columns to evaluate.",
    )
    parser.add_argument(
        "--include-noncausal",
        action="store_true",
        help="Include exact-time components even when they are not causal-eligible.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_project_dirs()

    component_registry = pd.read_csv(Path(args.component_registry))
    market_data = pd.read_csv(Path(args.market_data))
    value_columns = [column.strip() for column in args.value_columns.split(",") if column.strip()]
    output = build_intraday_event_panel(
        market_data,
        component_registry,
        value_columns,
        timestamp_column=args.timestamp_column,
        pre_minutes=int(args.pre_minutes),
        post_minutes=int(args.post_minutes),
        causal_only=not args.include_noncausal,
    )
    output_path = Path(args.output)
    write_df(output, output_path)
    print(f"Saved intraday QRA event panel with {len(output):,} rows to {output_path}")


if __name__ == "__main__":
    main()
