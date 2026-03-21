from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import argparse
from ati_shadow_policy import sec_nmfp
from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import RAW_DIR, ensure_project_dirs

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect SEC N-MFP dataset links.")
    parser.add_argument("--download-files", action="store_true", help="Download linked SEC files")
    parser.add_argument("--limit", type=_positive_int, default=None, help="Optional file download limit")
    return parser.parse_args()


def _positive_int(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("--limit must be a positive integer")
    return value


def main() -> None:
    ensure_project_dirs()
    args = parse_args()
    out_dir = RAW_DIR / sec_nmfp.RAW_DIR_NAME
    out_dir.mkdir(parents=True, exist_ok=True)
    links = sec_nmfp.collect_links()
    filtered = sec_nmfp.build_manifest(links)
    write_df(links, out_dir / "all_links.csv")
    write_df(filtered, out_dir / "manifest.csv")
    print(f"Saved SEC N-MFP manifest with {len(filtered):,} rows")
    if args.download_files:
        downloaded = sec_nmfp.download_manifest(filtered, out_dir / "files", limit=args.limit)
        write_df(downloaded, out_dir / "downloads.csv")
        print(f"Downloaded {len(downloaded):,} SEC files")

if __name__ == "__main__":
    main()
