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
from ati_shadow_policy.paths import MANUAL_DIR, PROCESSED_DIR, ensure_project_dirs
from ati_shadow_policy.research.qra_elasticity import build_qra_shock_template

DEFAULT_PANEL = PROCESSED_DIR / "qra_event_panel.csv"
DEFAULT_TEMPLATE = MANUAL_DIR / "qra_event_shock_template.csv"
DEFAULT_CAPTURE_TEMPLATE = MANUAL_DIR / "official_quarterly_refunding_capture_template.csv"


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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_project_dirs()

    panel = pd.read_csv(Path(args.panel))
    template_path = Path(args.template)
    existing = pd.read_csv(template_path) if template_path.exists() else None
    capture_template_path = Path(args.capture_template)
    capture_template = pd.read_csv(capture_template_path) if capture_template_path.exists() else None

    seeded = build_qra_shock_template(
        panel=panel,
        existing_template=existing,
        capture_template=capture_template,
    )
    write_df(seeded, template_path)
    print(f"Saved QRA shock template with {len(seeded):,} rows to {template_path}")


if __name__ == "__main__":
    main()
