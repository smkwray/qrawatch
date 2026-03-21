from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import argparse
import json
import pandas as pd

from ati_shadow_policy.fred import fetch_bundle, fetch_series, save_bundle
from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import CONFIG_DIR, RAW_DIR, ensure_project_dirs


def validate_fred_outputs(series_meta: list[dict], series_frames: dict[str, pd.DataFrame], bundle: pd.DataFrame) -> None:
    requested_ids = [item["series_id"] for item in series_meta]
    unique_requested_ids = list(dict.fromkeys(requested_ids))
    missing_series_ids = [sid for sid in unique_requested_ids if sid not in series_frames]
    missing_bundle_cols = [sid for sid in unique_requested_ids if sid not in bundle.columns]
    unexpected_cols = [col for col in bundle.columns if col != "date" and col not in unique_requested_ids]

    if missing_series_ids or missing_bundle_cols or unexpected_cols:
        problems = []
        if missing_series_ids:
            problems.append(f"missing downloaded series frames: {missing_series_ids}")
        if missing_bundle_cols:
            problems.append(f"missing bundle columns: {missing_bundle_cols}")
        if unexpected_cols:
            problems.append(f"unexpected bundle columns: {unexpected_cols}")
        raise RuntimeError("FRED validation failed: " + "; ".join(problems))

    if len(unique_requested_ids) != len(series_frames):
        print(
            "Validation warning: manifest contains duplicate series IDs; "
            f"fetched {len(series_frames)} unique series for {len(requested_ids)} requested."
        )

    print("Validation passed: bundle and per-series outputs align with manifest.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download FRED series defined in configs/fred_series_core.json")
    parser.add_argument("--preset", default="core", choices=["core"], help="Series preset to download")
    return parser.parse_args()


def main() -> None:
    ensure_project_dirs()
    args = parse_args()
    _ = args.preset
    manifest_path = CONFIG_DIR / "fred_series_core.json"
    series_meta = json.loads(manifest_path.read_text(encoding="utf-8"))
    out_dir = RAW_DIR / "fred"
    series_dir = out_dir / "series"
    series_dir.mkdir(parents=True, exist_ok=True)
    series_frames: dict[str, pd.DataFrame] = {}
    unique_series_ids = list(dict.fromkeys(item["series_id"] for item in series_meta))

    for series_id in unique_series_ids:
        df = fetch_series(series_id)
        series_frames[series_id] = df
        write_df(df, series_dir / f"{series_id}.csv")
        print(f"Saved {series_id}: {len(df):,} rows")

    bundle = fetch_bundle(series_frames)
    validate_fred_outputs(series_meta, series_frames, bundle)
    save_bundle(bundle, series_meta, out_dir)
    print(f"Saved wide bundle: {len(bundle):,} rows")

if __name__ == "__main__":
    main()
