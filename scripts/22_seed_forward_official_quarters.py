from __future__ import annotations

from collections.abc import Sequence
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import MANUAL_DIR
from ati_shadow_policy.qra_capture import (
    read_capture_template,
    read_qra_event_seed,
    read_quarterly_refunding_seed,
)
from ati_shadow_policy.research.qra_seed_sync import build_seed_rows, sync_capture_template


DEFAULT_TEMPLATE = MANUAL_DIR / "official_quarterly_refunding_capture_template.csv"
DEFAULT_QRA_EVENT_SEED = MANUAL_DIR / "qra_event_seed.csv"
DEFAULT_QUARTERLY_REFUNDING_SEED = MANUAL_DIR / "quarterly_refunding_seed.csv"
DEFAULT_HISTORICAL_SEED = MANUAL_DIR / "official_quarterly_refunding_historical_seed.csv"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Synchronize seed-based quarter rows into the official-quarter capture template "
            "without overwriting richer existing values."
        )
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_TEMPLATE),
        help="Input capture template CSV path (default: %(default)s).",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_TEMPLATE),
        help="Output capture template CSV path (default: overwrite input).",
    )
    parser.add_argument(
        "--direction",
        choices=["forward", "backward", "both"],
        default="forward",
        help=(
            "Seed direction. forward uses qra_event_seed+quarterly_refunding_seed "
            "(backward-compatible default); backward uses historical seed file; "
            "both applies both sources."
        ),
    )
    parser.add_argument(
        "--qra-event-seed",
        default=str(DEFAULT_QRA_EVENT_SEED),
        help="Path to qra_event_seed.csv (used by forward/both).",
    )
    parser.add_argument(
        "--quarterly-seed",
        default=str(DEFAULT_QUARTERLY_REFUNDING_SEED),
        help="Path to quarterly_refunding_seed.csv (used by forward/both).",
    )
    parser.add_argument(
        "--historical-seed",
        default=str(DEFAULT_HISTORICAL_SEED),
        help="Path to historical quarter seed CSV (used by backward/both).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute sync and print summary without writing output.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    capture = read_capture_template(Path(args.input))

    historical_seed_df = None
    qra_event_seed_df = None
    quarterly_refunding_seed_df = None

    if args.direction in {"backward", "both"}:
        historical_seed_df = read_capture_template(Path(args.historical_seed))
    if args.direction in {"forward", "both"}:
        qra_event_seed_df = read_qra_event_seed(Path(args.qra_event_seed))
        quarterly_refunding_seed_df = read_quarterly_refunding_seed(Path(args.quarterly_seed))

    seed_rows = build_seed_rows(
        direction=args.direction,
        historical_seed_df=historical_seed_df,
        qra_event_seed_df=qra_event_seed_df,
        quarterly_refunding_seed_df=quarterly_refunding_seed_df,
    )
    result = sync_capture_template(capture, seed_rows)

    if not args.dry_run:
        write_df(result.dataframe, Path(args.output))

    print(
        "Seed sync complete "
        f"direction={args.direction} "
        f"rows_added={result.rows_added} "
        f"cells_enriched={result.cells_enriched} "
        f"conflicting_cells_skipped={result.conflicting_cells_skipped} "
        f"total_rows={len(result.dataframe)} "
        f"output={Path(args.output)} "
        f"dry_run={args.dry_run}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
