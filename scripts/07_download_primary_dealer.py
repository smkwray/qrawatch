from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import argparse

from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import RAW_DIR, ensure_project_dirs
from ati_shadow_policy.primary_dealer import build_manifest, collect_links, download_manifest

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect New York Fed primary dealer export links.")
    parser.add_argument("--download-files", action="store_true", help="Download export files")
    parser.add_argument("--limit", type=int, default=20)
    return parser.parse_args()

def main() -> None:
    ensure_project_dirs()
    args = parse_args()
    out_dir = RAW_DIR / "primary_dealer"
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        links_df = collect_links()
    except Exception as exc:
        raise SystemExit(f"Failed to collect primary dealer links: {exc}") from exc

    if links_df.empty:
        raise SystemExit("No primary dealer pages fetched.")

    manifest = build_manifest(links_df)
    write_df(links_df, out_dir / "all_links.csv")
    write_df(manifest, out_dir / "manifest.csv")
    print(f"Saved primary dealer manifest with {len(manifest):,} rows")

    if args.download_files:
        downloaded = download_manifest(manifest, out_dir / "files", limit=args.limit)
        write_df(downloaded, out_dir / "downloads.csv")
        print(f"Downloaded {len(downloaded):,} files")

if __name__ == "__main__":
    main()
