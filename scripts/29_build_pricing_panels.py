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
from ati_shadow_policy.paths import PROCESSED_DIR, RAW_DIR, ensure_project_dirs
from ati_shadow_policy.research.pricing_panels import (
    build_mspd_stock_excess_bills_panel,
    build_official_ati_price_panel,
    build_pricing_release_flow_panel,
    build_weekly_supply_price_panel,
)

DEFAULT_OFFICIAL_CAPTURE = PROCESSED_DIR / "official_quarterly_refunding_capture.csv"
DEFAULT_MSPD = RAW_DIR / "fiscaldata" / "mspd_table_3_market.csv"
DEFAULT_FRED = RAW_DIR / "fred" / "core_wide.csv"
DEFAULT_PUBLIC_DURATION_SUPPLY = PROCESSED_DIR / "public_duration_supply.csv"
DEFAULT_PLUMBING_WEEKLY_PANEL = PROCESSED_DIR / "plumbing_weekly_panel.csv"
DEFAULT_MSPD_PANEL = PROCESSED_DIR / "mspd_stock_excess_bills_panel.csv"
DEFAULT_OFFICIAL_ATI_PANEL = PROCESSED_DIR / "official_ati_price_panel.csv"
DEFAULT_WEEKLY_PANEL = PROCESSED_DIR / "weekly_supply_price_panel.csv"
DEFAULT_RELEASE_FLOW_PANEL = PROCESSED_DIR / "pricing_release_flow_panel.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build pricing input panels for MSPD stock, official ATI monthly outcomes, "
            "release-level flow windows, and weekly public-duration supply outcomes."
        )
    )
    parser.add_argument("--official-capture", default=str(DEFAULT_OFFICIAL_CAPTURE), help="Official quarterly capture CSV.")
    parser.add_argument("--mspd", default=str(DEFAULT_MSPD), help="MSPD marketable security table CSV.")
    parser.add_argument("--fred", default=str(DEFAULT_FRED), help="Core FRED wide CSV.")
    parser.add_argument(
        "--public-duration-supply",
        default=str(DEFAULT_PUBLIC_DURATION_SUPPLY),
        help="Weekly public duration supply CSV.",
    )
    parser.add_argument(
        "--plumbing-weekly-panel",
        default=str(DEFAULT_PLUMBING_WEEKLY_PANEL),
        help="Weekly plumbing panel CSV used for QT and TGA controls.",
    )
    parser.add_argument(
        "--mspd-panel-output",
        default=str(DEFAULT_MSPD_PANEL),
        help="Output MSPD stock panel CSV.",
    )
    parser.add_argument(
        "--official-ati-price-panel-output",
        default=str(DEFAULT_OFFICIAL_ATI_PANEL),
        help="Output monthly official ATI pricing panel CSV.",
    )
    parser.add_argument(
        "--weekly-supply-panel-output",
        default=str(DEFAULT_WEEKLY_PANEL),
        help="Output weekly duration-price panel CSV.",
    )
    parser.add_argument(
        "--release-flow-panel-output",
        default=str(DEFAULT_RELEASE_FLOW_PANEL),
        help="Output one-row-per-release pricing flow panel CSV.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_project_dirs()

    official_capture = pd.read_csv(Path(args.official_capture))
    mspd_frame = pd.read_csv(Path(args.mspd), low_memory=False)
    fred = pd.read_csv(Path(args.fred))
    public_duration_supply = pd.read_csv(Path(args.public_duration_supply))
    plumbing_weekly_panel = pd.read_csv(Path(args.plumbing_weekly_panel))

    mspd_stock_panel = build_mspd_stock_excess_bills_panel(mspd_frame)
    official_ati_panel = build_official_ati_price_panel(official_capture, fred, mspd_stock_panel=mspd_stock_panel)
    release_flow_panel = build_pricing_release_flow_panel(official_capture, fred)
    weekly_supply_panel = build_weekly_supply_price_panel(
        public_duration_supply,
        fred,
        plumbing_weekly_panel=plumbing_weekly_panel,
        official_capture=official_capture,
    )

    mspd_output = Path(args.mspd_panel_output)
    official_ati_output = Path(args.official_ati_price_panel_output)
    weekly_output = Path(args.weekly_supply_panel_output)
    release_flow_output = Path(args.release_flow_panel_output)

    write_df(mspd_stock_panel, mspd_output)
    write_df(official_ati_panel, official_ati_output)
    write_df(release_flow_panel, release_flow_output)
    write_df(weekly_supply_panel, weekly_output)
    print(
        "Pricing panel artifacts written: "
        f"mspd_stock={len(mspd_stock_panel):,}, "
        f"official_ati_price={len(official_ati_panel):,}, "
        f"release_flow={len(release_flow_panel):,}, "
        f"weekly_supply={len(weekly_supply_panel):,}"
    )


if __name__ == "__main__":
    main()
