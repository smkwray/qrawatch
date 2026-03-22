from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import argparse
from pathlib import Path

import pandas as pd

from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import RAW_DIR, ensure_project_dirs
from ati_shadow_policy.webscrape import build_qra_manifest, download_link_manifest, extract_links

START_URLS = [
    "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives",
    "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/quarterly-refunding-financing-estimates-by-calendar-year",
    "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/office-of-economic-policy-statements-to-tbac",
    "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/tbac-recommended-financing-tables-by-calendar-year",
    "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/primary-dealer-auction-size-survey",
    "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/2010-and-before-quarterly-refunding-charts-data",
    "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding",
]

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect QRA / TBAC source links from Treasury pages.")
    parser.add_argument("--download-files", action="store_true", help="Also download linked documents")
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=None,
        help="Optional file download limit (must be a positive integer)",
    )
    return parser.parse_args()


def _positive_int(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("--limit must be a positive integer")
    return value

def main() -> None:
    args = parse_args()
    ensure_project_dirs()
    out_dir = RAW_DIR / "qra"
    out_dir.mkdir(parents=True, exist_ok=True)

    all_links = []
    for url in START_URLS:
        try:
            links = extract_links(url)
            links["start_url"] = url
            all_links.append(links)
            print(f"Collected links from {url}: {len(links):,}")
        except Exception as exc:
            print(f"Failed to collect links from {url}: {exc}")

    if not all_links:
        raise SystemExit("No links collected from QRA pages.")

    links_df = pd.concat(all_links, ignore_index=True).drop_duplicates()
    filtered = build_qra_manifest(links_df).drop_duplicates()

    write_df(links_df, out_dir / "all_links.csv")
    write_df(filtered, out_dir / "manifest.csv")
    print(f"Saved manifest with {len(filtered):,} candidate links")

    if args.download_files:
        files_dir = out_dir / "files"
        download_manifest = filtered.copy()
        if "preferred_for_download" in download_manifest.columns:
            preferred = download_manifest.loc[
                download_manifest["preferred_for_download"].fillna(False).astype(bool)
            ].copy()
            if not preferred.empty:
                download_manifest = preferred
        downloaded = download_link_manifest(download_manifest, files_dir, limit=args.limit)
        write_df(downloaded, out_dir / "downloads.csv")
        print(
            "Downloaded "
            f"{len(downloaded):,} preferred candidate files to {files_dir} "
            f"(manifest candidates={len(filtered):,})"
        )

if __name__ == "__main__":
    main()
