from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from ati_shadow_policy.investor_allotments import (
    PANEL_FILENAME,
    PROCESSED_FILENAME,
    build_inventory,
    build_normalized_panel,
)
from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import PROCESSED_DIR, RAW_DIR, ensure_project_dirs


def main() -> None:
    ensure_project_dirs()
    raw_dir = RAW_DIR / "investor_allotments"
    manifest_path = raw_dir / "manifest.csv"
    if not manifest_path.exists():
        raise SystemExit(f"Missing manifest: {manifest_path}")

    manifest = pd.read_csv(manifest_path)
    downloads_path = raw_dir / "downloads.csv"
    downloads = pd.read_csv(downloads_path) if downloads_path.exists() else None
    inventory = build_inventory(manifest, downloads)
    write_df(inventory, PROCESSED_DIR / PROCESSED_FILENAME)
    panel = build_normalized_panel(inventory)
    write_df(panel, PROCESSED_DIR / PANEL_FILENAME)
    print(
        f"Saved investor allotments inventory with {len(inventory):,} rows; "
        f"panel rows={len(panel):,} path={PROCESSED_DIR / PANEL_FILENAME}"
    )


if __name__ == "__main__":
    main()
