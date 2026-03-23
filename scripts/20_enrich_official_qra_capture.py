from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import INTERIM_DIR, MANUAL_DIR, PROCESSED_DIR, RAW_DIR, ensure_project_dirs
from ati_shadow_policy.qra_capture import (
    build_financing_release_source_map,
    build_official_capture,
    build_refunding_statement_manifest,
    build_refunding_statement_source_map,
    enrich_capture_with_financing_release_map,
    enrich_capture_with_refunding_statement_map,
    read_capture_template,
    read_qra_event_seed,
)
from ati_shadow_policy.webscrape import download_link_manifest


def main() -> None:
    ensure_project_dirs()
    template_path = MANUAL_DIR / "official_quarterly_refunding_capture_template.csv"
    events_path = MANUAL_DIR / "qra_event_seed.csv"
    downloads_path = RAW_DIR / "qra" / "downloads.csv"
    raw_qra_dir = RAW_DIR / "qra"
    raw_qra_files_dir = raw_qra_dir / "files"
    text_dir = INTERIM_DIR / "qra_text"

    template = read_capture_template(template_path)
    events = read_qra_event_seed(events_path)
    downloads = pd.read_csv(downloads_path)

    source_map = build_financing_release_source_map(events, downloads, text_dir)
    write_df(source_map, PROCESSED_DIR / "qra_financing_release_map.csv")

    enriched = enrich_capture_with_financing_release_map(template, source_map)
    archive_paths = sorted(raw_qra_files_dir.glob("official-remarks-on-quarterly-refunding-by-calendar-year_*.html"))
    statement_manifest = build_refunding_statement_manifest(enriched, archive_paths)
    write_df(statement_manifest, PROCESSED_DIR / "qra_refunding_statement_manifest.csv")

    statement_downloads = download_link_manifest(
        statement_manifest,
        raw_qra_files_dir,
        skip_existing=True,
    )
    write_df(statement_downloads, raw_qra_dir / "refunding_statement_downloads.csv")

    statement_map = build_refunding_statement_source_map(statement_downloads)
    write_df(statement_map, PROCESSED_DIR / "qra_refunding_statement_source_map.csv")
    enriched = enrich_capture_with_refunding_statement_map(enriched, statement_map)
    processed = build_official_capture(enriched).dataframe
    write_df(processed, template_path)
    write_df(processed, PROCESSED_DIR / "official_quarterly_refunding_capture.csv")

    matched = int((source_map["match_status"] == "matched").sum())
    statement_matched = int((statement_map["match_status"] == "matched").sum())
    print(
        "Enriched official QRA capture template with "
        f"{matched} matched financing-release sources; "
        f"{statement_matched} matched refunding statements; "
        f"saved source maps to {PROCESSED_DIR / 'qra_financing_release_map.csv'} "
        f"and {PROCESSED_DIR / 'qra_refunding_statement_source_map.csv'}"
    )


if __name__ == "__main__":
    main()
