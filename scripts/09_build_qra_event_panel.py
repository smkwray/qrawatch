from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import MANUAL_DIR, PROCESSED_DIR, RAW_DIR, ensure_project_dirs
from ati_shadow_policy.research.event_study import build_event_panel

VALUE_COLUMNS = ["THREEFYTP10", "DGS10", "DGS2", "DGS30", "SP500", "VIXCLS"]

def main() -> None:
    ensure_project_dirs()
    events = pd.read_csv(MANUAL_DIR / "qra_event_seed.csv")
    events = events.loc[
        events["expected_direction"].fillna("").astype(str).str.strip().ne("")
    ].copy()
    fred = pd.read_csv(RAW_DIR / "fred" / "core_wide.csv")
    fred["date"] = pd.to_datetime(fred["date"])

    fred["slope_10y_2y"] = fred["DGS10"] - fred["DGS2"]
    fred["slope_30y_2y"] = fred["DGS30"] - fred["DGS2"]

    base_cols = [col for col in VALUE_COLUMNS if col in fred.columns]
    base_cols.extend([c for c in ["slope_10y_2y", "slope_30y_2y"] if c in fred.columns])

    panel_official = build_event_panel(
        fred,
        events,
        base_cols,
        event_date_column="official_release_date",
    )
    panel_tminus1 = build_event_panel(
        fred,
        events,
        base_cols,
        event_date_column="market_pricing_marker_minus_1d",
    )
    panel = pd.concat([panel_official, panel_tminus1], ignore_index=True)
    event_type_order = pd.CategoricalDtype(
        ["official_release_date", "market_pricing_marker_minus_1d"],
        ordered=True,
    )
    panel["event_date_type"] = panel["event_date_type"].astype(event_type_order)
    panel = panel.sort_values(["event_id", "event_date_type"]).reset_index(drop=True)
    panel["event_date_type"] = panel["event_date_type"].astype(str)
    write_df(panel, PROCESSED_DIR / "qra_event_panel.csv")
    print(f"Saved QRA event panel with {len(panel):,} rows")

if __name__ == "__main__":
    main()
