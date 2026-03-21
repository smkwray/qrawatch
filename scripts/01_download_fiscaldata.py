from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import argparse
from pathlib import Path

from ati_shadow_policy.fiscaldata import fetch_dataset, load_manifest, save_dataset
from ati_shadow_policy.paths import CONFIG_DIR, RAW_DIR, ensure_project_dirs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download FiscalData datasets defined in the repo manifest.")
    parser.add_argument("--dataset", action="append", help="Dataset key from configs/fiscaldata_datasets.json")
    parser.add_argument("--all", action="store_true", help="Download all manifest datasets")
    parser.add_argument("--max-pages", type=int, default=None, help="Optional page limit for debugging")
    return parser.parse_args()


def main() -> None:
    ensure_project_dirs()
    args = parse_args()
    manifest = load_manifest(CONFIG_DIR / "fiscaldata_datasets.json")

    if args.all:
        keys = list(manifest.keys())
    elif args.dataset:
        keys = args.dataset
    else:
        raise SystemExit("Pass --all or at least one --dataset")

    out_dir = RAW_DIR / "fiscaldata"
    out_dir.mkdir(parents=True, exist_ok=True)

    for key in keys:
        if key not in manifest:
            raise KeyError(f"Unknown dataset key: {key}. Valid keys: {list(manifest)}")
        spec = manifest[key]
        df, meta = fetch_dataset(spec["endpoint"], params=spec.get("default_params", {}), max_pages=args.max_pages)
        save_dataset(df, meta, out_dir / f"{key}.csv", out_dir / f"{key}_meta.json")
        print(f"Saved {key}: {len(df):,} rows")

if __name__ == "__main__":
    main()
