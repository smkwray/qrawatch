from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from ati_shadow_policy import sec_nmfp
from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import PROCESSED_DIR, RAW_DIR, ensure_project_dirs


def main() -> None:
    ensure_project_dirs()
    raw_dir = RAW_DIR / sec_nmfp.RAW_DIR_NAME
    manifest_path = raw_dir / "manifest.csv"
    if not manifest_path.exists():
        raise SystemExit(f"Missing manifest: {manifest_path}")

    manifest = pd.read_csv(manifest_path)
    downloads_path = raw_dir / "downloads.csv"
    downloads = pd.read_csv(downloads_path) if downloads_path.exists() else None
    inventory = sec_nmfp.build_inventory(manifest, downloads)
    write_df(inventory, PROCESSED_DIR / sec_nmfp.PROCESSED_FILENAME)
    summary_panel = sec_nmfp.build_summary_panel(inventory)
    write_df(summary_panel, PROCESSED_DIR / sec_nmfp.SUMMARY_PANEL_FILENAME)
    print(f"Saved SEC N-MFP inventory with {len(inventory):,} rows")
    print(f"Saved SEC N-MFP summary panel with {len(summary_panel):,} rows")


if __name__ == "__main__":
    main()
