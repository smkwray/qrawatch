from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import argparse

from ati_shadow_policy.investor_allotments import build_manifest, collect_links, download_manifest
from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import RAW_DIR, ensure_project_dirs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect Treasury investor class auction allotment links.")
    parser.add_argument("--download-files", action="store_true", help="Also download linked source files")
    parser.add_argument("--limit", type=_positive_int, default=None, help="Optional file download limit")
    return parser.parse_args()


def _positive_int(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("--limit must be a positive integer")
    return value


def main() -> None:
    args = parse_args()
    ensure_project_dirs()
    out_dir = RAW_DIR / "investor_allotments"
    out_dir.mkdir(parents=True, exist_ok=True)
    links = collect_links()
    manifest = build_manifest(links)
    write_df(links, out_dir / "all_links.csv")
    write_df(manifest, out_dir / "manifest.csv")
    print(f"Saved investor allotments manifest with {len(manifest):,} rows")
    if args.download_files:
        downloaded = download_manifest(manifest, out_dir / "files", limit=args.limit)
        write_df(downloaded, out_dir / "downloads.csv")
        print(f"Downloaded {len(downloaded):,} investor allotments files")


if __name__ == "__main__":
    main()
