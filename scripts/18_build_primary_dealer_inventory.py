from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import PROCESSED_DIR, RAW_DIR, ensure_project_dirs
from ati_shadow_policy.primary_dealer import (
    PROCESSED_FILENAME,
    PROCESSED_PANEL_FILENAME,
    build_inventory,
    build_panel,
    summarize_inventory,
    summarize_panel,
)


def main() -> None:
    ensure_project_dirs()
    raw_dir = RAW_DIR / "primary_dealer"
    manifest_path = raw_dir / "manifest.csv"
    if not manifest_path.exists():
        raise SystemExit(f"Missing manifest: {manifest_path}")

    manifest = pd.read_csv(manifest_path)
    downloads_path = raw_dir / "downloads.csv"
    downloads = pd.read_csv(downloads_path) if downloads_path.exists() else None
    inventory = build_inventory(manifest, downloads)
    panel = build_panel(manifest, downloads)
    write_df(inventory, PROCESSED_DIR / PROCESSED_FILENAME)
    write_df(panel, PROCESSED_DIR / PROCESSED_PANEL_FILENAME)
    summary = summarize_inventory(inventory)
    panel_summary = summarize_panel(panel)
    print(f"Saved primary dealer inventory with {len(inventory):,} rows")
    print(f"Saved primary dealer panel with {len(panel):,} rows")
    if not summary.empty:
        print(summary.to_string(index=False))
    if not panel_summary.empty:
        print(panel_summary.to_string(index=False))


if __name__ == "__main__":
    main()
