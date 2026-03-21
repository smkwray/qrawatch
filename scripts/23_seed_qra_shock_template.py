from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import MANUAL_DIR, PROCESSED_DIR, RAW_DIR, TABLES_DIR, ensure_project_dirs
from ati_shadow_policy.research.qra_elasticity import build_qra_shock_template
from ati_shadow_policy.research.qra_schedule_diff import (
    build_qra_schedule_diff_components,
    build_qra_schedule_table,
)

DEFAULT_PANEL = PROCESSED_DIR / "qra_event_panel.csv"
DEFAULT_TEMPLATE = MANUAL_DIR / "qra_event_shock_template.csv"
DEFAULT_CAPTURE_TEMPLATE = MANUAL_DIR / "official_quarterly_refunding_capture_template.csv"
DEFAULT_RELEASE_CALENDAR = MANUAL_DIR / "qra_release_calendar_seed.csv"
DEFAULT_SCHEDULE_TABLE_OUTPUT = PROCESSED_DIR / "qra_schedule_table.csv"
DEFAULT_COMPONENTS_OUTPUT = TABLES_DIR / "qra_event_shock_components.csv"
DEFAULT_YIELD_CURVE = RAW_DIR / "fred" / "core_wide.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Seed or reseed the manual QRA shock template keyed by event_id and event_date_type. "
            "Existing manual shock values are preserved when reseeding."
        )
    )
    parser.add_argument(
        "--panel",
        default=str(DEFAULT_PANEL),
        help="Path to qra_event_panel.csv (default: %(default)s).",
    )
    parser.add_argument(
        "--template",
        default=str(DEFAULT_TEMPLATE),
        help="Path to qra_event_shock_template.csv (default: %(default)s).",
    )
    parser.add_argument(
        "--capture-template",
        default=str(DEFAULT_CAPTURE_TEMPLATE),
        help="Path to official_quarterly_refunding_capture_template.csv for automated tenor parsing (default: %(default)s).",
    )
    parser.add_argument(
        "--release-calendar",
        default=str(DEFAULT_RELEASE_CALENDAR),
        help="Path to qra_release_calendar_seed.csv for schedule diff event mapping (default: %(default)s).",
    )
    parser.add_argument(
        "--schedule-table-output",
        default=str(DEFAULT_SCHEDULE_TABLE_OUTPUT),
        help="Derived normalized schedule table output path (default: %(default)s).",
    )
    parser.add_argument(
        "--components-output",
        default=str(DEFAULT_COMPONENTS_OUTPUT),
        help="Derived schedule-diff component output path (default: %(default)s).",
    )
    parser.add_argument(
        "--yield-curve",
        default=str(DEFAULT_YIELD_CURVE),
        help="Path to the FRED wide curve bundle used for dynamic duration and DV01 factors (default: %(default)s).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_project_dirs()

    panel = pd.read_csv(Path(args.panel))
    template_path = Path(args.template)
    existing = pd.read_csv(template_path) if template_path.exists() else None
    capture_template_path = Path(args.capture_template)
    capture_template = pd.read_csv(capture_template_path) if capture_template_path.exists() else None
    release_calendar_path = Path(args.release_calendar)
    release_calendar = pd.read_csv(release_calendar_path) if release_calendar_path.exists() else None
    yield_curve_path = Path(args.yield_curve)
    yield_curve = pd.read_csv(yield_curve_path) if yield_curve_path.exists() else None

    schedule_table = pd.DataFrame()
    schedule_components = pd.DataFrame()
    if capture_template is not None and release_calendar is not None:
        schedule_table = build_qra_schedule_table(capture_template, release_calendar)
        schedule_components = build_qra_schedule_diff_components(schedule_table, yield_curve=yield_curve)
        write_df(schedule_table, Path(args.schedule_table_output))
        write_df(schedule_components, Path(args.components_output))

    seeded = build_qra_shock_template(
        panel=panel,
        existing_template=existing,
        capture_template=capture_template,
        schedule_components=schedule_components,
    )
    write_df(seeded, template_path)
    print(f"Saved QRA shock template with {len(seeded):,} rows to {template_path}")


if __name__ == "__main__":
    main()
