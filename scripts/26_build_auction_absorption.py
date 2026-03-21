from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import PROCESSED_DIR, RAW_DIR, TABLES_DIR, ensure_project_dirs
from ati_shadow_policy.research.auction_absorption import (
    build_auction_absorption_panel_v1,
    build_auction_absorption_table,
)


def _read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def main() -> None:
    ensure_project_dirs()

    investor_allotments = _read_csv_if_exists(PROCESSED_DIR / "investor_allotments_panel.csv")
    primary_dealer = _read_csv_if_exists(PROCESSED_DIR / "primary_dealer_panel.csv")
    event_registry = _read_csv_if_exists(PROCESSED_DIR / "qra_event_registry_v2.csv")
    auction_results = _read_csv_if_exists(RAW_DIR / "fiscaldata" / "auctions_query.csv")

    panel = build_auction_absorption_panel_v1(
        investor_allotments=investor_allotments,
        primary_dealer=primary_dealer,
        event_registry=event_registry,
        auction_results=auction_results,
    )
    table = build_auction_absorption_table(panel)

    write_df(panel, PROCESSED_DIR / "auction_absorption_panel_v1.csv")
    write_df(table, TABLES_DIR / "auction_absorption_table.csv")

    print(
        "Saved auction absorption artifacts: "
        f"panel={len(panel):,}, table={len(table):,}"
    )


if __name__ == "__main__":
    main()
