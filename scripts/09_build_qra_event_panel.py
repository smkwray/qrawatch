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
from ati_shadow_policy.research.qra_classification import derive_legacy_expected_direction

VALUE_COLUMNS = ["THREEFYTP10", "DGS10", "DGS2", "DGS30", "SP500", "VIXCLS"]


def _coalesce(left: pd.Series, right: pd.Series) -> pd.Series:
    return left.where(left.notna() & left.astype(str).str.strip().ne(""), right)


def _load_qra_events() -> pd.DataFrame:
    events = pd.read_csv(MANUAL_DIR / "qra_event_seed.csv")
    calendar_path = MANUAL_DIR / "qra_release_calendar_seed.csv"
    if calendar_path.exists():
        calendar = pd.read_csv(calendar_path).rename(
            columns={
                "notes": "calendar_notes",
                "seed_source": "calendar_seed_source",
                "event_label": "calendar_event_label",
                "market_pricing_marker_minus_1d": "calendar_market_pricing_marker_minus_1d",
            }
        )
        events = events.merge(calendar, on="event_id", how="left")
        for left_col, right_col in (
            ("event_label", "calendar_event_label"),
            ("market_pricing_marker_minus_1d", "calendar_market_pricing_marker_minus_1d"),
        ):
            if right_col in events.columns:
                events[left_col] = _coalesce(events[left_col], events[right_col])
        if "policy_statement_release_date" in events.columns:
            events["official_release_date"] = _coalesce(
                events["official_release_date"],
                events["policy_statement_release_date"],
            )

    derived = events.apply(derive_legacy_expected_direction, axis=1)
    if "expected_direction" not in events.columns:
        events["expected_direction"] = derived
    else:
        events["expected_direction"] = events["expected_direction"].where(
            events["expected_direction"].fillna("").astype(str).str.strip().ne(""),
            derived,
        )
    return events

def main() -> None:
    ensure_project_dirs()
    events = _load_qra_events()
    if "classification_review_status" in events.columns:
        events = events.loc[
            events["classification_review_status"].fillna("").astype(str).str.strip().ne("")
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
