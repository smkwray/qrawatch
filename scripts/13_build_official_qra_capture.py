from __future__ import annotations

from collections.abc import Sequence
import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import MANUAL_DIR, PROCESSED_DIR, RAW_DIR, ensure_project_dirs
from ati_shadow_policy.qra_capture import (
    build_official_capture,
    build_capture_completion_status,
    build_quarter_net_issuance_from_auctions,
    enrich_capture_with_auction_reconstruction,
    read_capture_template,
    read_qra_event_seed,
    read_quarterly_refunding_seed,
)

DEFAULT_CAPTURE_TEMPLATE = MANUAL_DIR / "official_quarterly_refunding_capture_template.csv"
DEFAULT_QRA_EVENT_SEED = MANUAL_DIR / "qra_event_seed.csv"
DEFAULT_QUARTERLY_REFUNDING_SEED = MANUAL_DIR / "quarterly_refunding_seed.csv"
DEFAULT_OUTPUT = PROCESSED_DIR / "official_quarterly_refunding_capture.csv"
DEFAULT_AUCTIONS = RAW_DIR / "fiscaldata" / "auctions_query.csv"
DEFAULT_RECONSTRUCTION_OUTPUT = PROCESSED_DIR / "official_quarter_net_issuance_reconstruction.csv"
DEFAULT_COMPLETION_OUTPUT = PROCESSED_DIR / "official_capture_completion_status.csv"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build validated official-quarter QRA capture output from the manual-first capture template."
        )
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_CAPTURE_TEMPLATE),
        help="Path to official-quarter manual capture template CSV (default: %(default)s).",
    )
    parser.add_argument(
        "--qra-event-seed",
        default=str(DEFAULT_QRA_EVENT_SEED),
        help="Path to qra_event_seed.csv for optional bootstrap seeding (default: %(default)s).",
    )
    parser.add_argument(
        "--quarterly-seed",
        default=str(DEFAULT_QUARTERLY_REFUNDING_SEED),
        help="Path to quarterly_refunding_seed.csv for optional bootstrap seeding (default: %(default)s).",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Output path for processed official-quarter capture CSV (default: %(default)s).",
    )
    parser.add_argument(
        "--seed-missing-quarters",
        action="store_true",
        help=(
            "Add deterministic seed_only rows for known event quarters that are missing "
            "from the manual capture template."
        ),
    )
    parser.add_argument(
        "--auctions",
        default=str(DEFAULT_AUCTIONS),
        help="Path to fiscaldata auctions CSV for quarter reconstruction (default: %(default)s).",
    )
    parser.add_argument(
        "--reconstruction-output",
        default=str(DEFAULT_RECONSTRUCTION_OUTPUT),
        help="Output path for quarter reconstruction detail CSV (default: %(default)s).",
    )
    parser.add_argument(
        "--completion-status-output",
        default=str(DEFAULT_COMPLETION_OUTPUT),
        help="Output path for official-capture completion status CSV (default: %(default)s).",
    )
    parser.add_argument(
        "--disable-auction-reconstruction",
        action="store_true",
        help="Skip the official auctions-based quarter reconstruction step.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    ensure_project_dirs()

    capture = read_capture_template(Path(args.input))
    qra_event_seed = None
    quarterly_refunding_seed = None
    if args.seed_missing_quarters:
        qra_event_seed = read_qra_event_seed(Path(args.qra_event_seed))
        quarterly_refunding_seed = read_quarterly_refunding_seed(Path(args.quarterly_seed))

    result = build_official_capture(
        capture,
        qra_event_seed_df=qra_event_seed,
        quarterly_refunding_seed_df=quarterly_refunding_seed,
        seed_missing_quarters=args.seed_missing_quarters,
    )
    official_capture = result.dataframe

    if args.disable_auction_reconstruction:
        reconstruction = pd.DataFrame()
    else:
        auctions = pd.read_csv(Path(args.auctions), low_memory=False)
        reconstruction = build_quarter_net_issuance_from_auctions(
            auctions,
            quarters=official_capture["quarter"].dropna().astype(str).tolist(),
        )
        write_df(reconstruction, Path(args.reconstruction_output))
        official_capture = enrich_capture_with_auction_reconstruction(official_capture, reconstruction)
        official_capture = build_official_capture(official_capture).dataframe

    completion = build_capture_completion_status(official_capture, reconstruction)
    write_df(completion, Path(args.completion_status_output))
    write_df(official_capture, Path(args.output))

    print(
        "Saved official-quarter capture "
        f"rows={len(official_capture):,} seeded_rows_added={result.seeded_rows_added} "
        f"path={Path(args.output)} completion_status={Path(args.completion_status_output)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
