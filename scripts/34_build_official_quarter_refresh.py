"""Rebuild the official quarter table and its completion table under a new directory.

Runs the capture and auction-reconstruction step of script 13, the ATI step of script 17,
and the two matching publish tables from explicit inputs, then reconciles every quarter's net
bill issuance with the MSPD change in marketable bills outstanding. Nothing is written outside
--out-dir, so data/processed and output/publish keep their bytes. A receipt records the
producer commit and the input and output hashes. The build fails when a quarter's bill
reconstruction is incomplete, when a reconciliation term is missing, or when a residual exceeds
--max-mspd-residual-bn.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import MANUAL_DIR
from ati_shadow_policy.publish import (
    PUBLISH_DIR,
    build_ati_publish_table,
    build_official_capture_completion_publish_table,
)
from ati_shadow_policy.qra_capture import (
    bill_settlement_flows,
    build_ati_input_from_official_capture,
    build_capture_completion_status,
    build_official_capture,
    build_quarter_net_issuance_from_auctions,
    enrich_capture_with_auction_reconstruction,
    read_capture_template,
    reconcile_net_bills_with_mspd,
)
from ati_shadow_policy.research.ati_index import build_ati_index

OUTPUTS = {
    "reconstruction": "processed/official_quarter_net_issuance_reconstruction.csv",
    "completion_status": "processed/official_capture_completion_status.csv",
    "capture": "processed/official_quarterly_refunding_capture.csv",
    "ati_index": "processed/ati_index_official_capture.csv",
    "ati_quarter_table": "publish/ati_quarter_table.csv",
    "official_capture_completion": "publish/official_capture_completion.csv",
    "net_bills_mspd_reconciliation": "publish/net_bills_mspd_reconciliation.csv",
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--template",
        default=str(MANUAL_DIR / "official_quarterly_refunding_capture_template.csv"),
        help="Official-quarter capture template (default: %(default)s).",
    )
    parser.add_argument("--auctions", required=True, help="FiscalData auctions_query CSV extract.")
    parser.add_argument("--mspd", required=True, help="FiscalData mspd_table_1 CSV extract.")
    parser.add_argument(
        "--max-mspd-residual-bn",
        type=float,
        default=1.0,
        help="Largest admissible absolute net-bill residual against MSPD, USD billions (default: %(default)s).",
    )
    parser.add_argument("--out-dir", required=True, help="Directory for outputs; existing outputs are never replaced.")
    return parser.parse_args(argv)


def _rel(path: Path) -> str:
    path = Path(path).resolve()
    return path.relative_to(ROOT).as_posix() if path.is_relative_to(ROOT) else str(path)


def _file(path: Path) -> dict[str, object]:
    stat = Path(path).stat()
    return {
        "path": _rel(path),
        "bytes": stat.st_size,
        "sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest(),
        "modified_utc": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
    }


def _git(*args: str) -> str | None:
    try:
        return subprocess.run(["git", *args], cwd=ROOT, check=True, capture_output=True, text=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return None


def _source_documents(capture: pd.DataFrame) -> tuple[dict[str, str], list[str]]:
    found: dict[str, str] = {}
    missing: list[str] = []
    for column in ("financing_source_doc_local", "refunding_statement_source_doc_local"):
        for value in capture[column].dropna().astype(str):
            for local in filter(None, (part.strip() for part in value.split("|"))):
                path = ROOT / local
                if path.exists():
                    found[local] = hashlib.sha256(path.read_bytes()).hexdigest()
                elif local not in missing:
                    missing.append(local)
    return dict(sorted(found.items())), sorted(missing)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    out_dir = Path(args.out_dir)
    targets = {name: out_dir / rel for name, rel in OUTPUTS.items()}
    existing = [str(path) for path in [*targets.values(), out_dir / "receipt.json"] if path.exists()]
    if existing:
        raise SystemExit(f"Refusing to replace existing outputs: {existing}")

    template, auctions_path, mspd_path = Path(args.template), Path(args.auctions), Path(args.mspd)
    auctions = pd.read_csv(auctions_path, low_memory=False)
    capture = build_official_capture(read_capture_template(template)).dataframe
    quarters = capture["quarter"].dropna().astype(str).tolist()
    reconstruction = build_quarter_net_issuance_from_auctions(auctions, quarters=quarters)
    bill_status = reconstruction.loc[reconstruction["bucket"].eq("bill_like")].set_index("quarter")["reconstruction_status"]
    incomplete = [quarter for quarter in quarters if bill_status.get(quarter) != "complete"]
    if incomplete:
        raise SystemExit(f"Net bill reconstruction is incomplete or missing for: {', '.join(incomplete)}")
    capture = build_official_capture(enrich_capture_with_auction_reconstruction(capture, reconstruction)).dataframe
    write_df(reconstruction, targets["reconstruction"])
    write_df(build_capture_completion_status(capture, reconstruction), targets["completion_status"])
    write_df(capture, targets["capture"])

    # Script 17 and the publish step read their inputs back from CSV; do the same.
    ati = build_ati_index(build_ati_input_from_official_capture(pd.read_csv(targets["capture"])))
    write_df(ati, targets["ati_index"])
    table = build_ati_publish_table(targets["ati_index"])
    write_df(table, targets["ati_quarter_table"])
    completion = build_official_capture_completion_publish_table(targets["completion_status"])
    write_df(completion, targets["official_capture_completion"])
    reconciliation = reconcile_net_bills_with_mspd(
        table.set_index("quarter")["net_bills_bn"], bill_settlement_flows(auctions), pd.read_csv(mspd_path)
    )
    write_df(reconciliation, targets["net_bills_mspd_reconciliation"])
    residual = reconciliation.set_index("quarter")["residual_bn"]
    over = residual[residual.abs() > args.max_mspd_residual_bn]

    documents, missing_documents = _source_documents(capture)
    status = _git("status", "--porcelain", "--untracked-files=no")
    meta_path = auctions_path.with_name(f"{auctions_path.stem}_meta.json")
    mspd_meta = mspd_path.with_name(f"{mspd_path.stem}_meta.json")
    headline = completion["is_headline_ready"].astype(str).eq("True")
    receipt = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "producer_commit": (_git("rev-parse", "HEAD") or "").strip() or None,
        "tracked_tree_clean": status == "" if status is not None else None,
        "argv": [_rel(Path(sys.argv[0])), *(argv if argv is not None else sys.argv[1:])],
        "inputs": {
            "template": _file(template),
            "auctions": {
                **_file(auctions_path),
                "max_record_date": str(auctions["record_date"].max()) if "record_date" in auctions else None,
                "meta": _file(meta_path) if meta_path.exists() else None,
            },
            "mspd_table_1": {
                **_file(mspd_path),
                "max_record_date": str(pd.read_csv(mspd_path, usecols=["record_date"])["record_date"].max()),
                "meta": _file(mspd_meta) if mspd_meta.exists() else None,
            },
            "source_documents": documents,
            "missing_source_documents": missing_documents,
        },
        "outputs": {name: _file(path) for name, path in targets.items()},
        "default_publish_tables_at_run": {
            name: _file(PUBLISH_DIR / f"{name}.csv")
            for name in ("ati_quarter_table", "official_capture_completion")
            if (PUBLISH_DIR / f"{name}.csv").exists()
        },
        "validation": {
            "quarters": int(len(table)),
            "sample_start": str(table["quarter"].min()) if len(table) else None,
            "sample_end": str(table["quarter"].max()) if len(table) else None,
            "unit": "USD billions",
            "ati_identity": "net_bills_bn - 0.18 * financing_need_bn",
            "max_identity_residual_usd_billions": float(
                (table["ati_baseline_bn"] - (table["net_bills_bn"] - 0.18 * table["financing_need_bn"])).abs().max()
            ) if len(table) else None,
            "exact_official_numeric_rows": int(table["source_quality"].eq("exact_official_numeric").sum()),
            "headline_ready_rows": int(headline.sum()),
            "seed_rows": int(completion["uses_seed_source"].astype(str).eq("True").sum()),
            "max_net_bill_reconstruction_difference_usd_billions": float(
                (pd.to_numeric(completion["net_bill_issuance_bn"]) - pd.to_numeric(completion["reconstructed_net_bill_issuance_bn"])).abs().max()
            ) if len(completion) else None,
            "mspd_reconciliation": {
                "identity": "net_bills_bn = mspd_bills_change_bn + soma_bill_redemptions_bn - awards_above_offering_bn + residual_bn",
                "quarters": int(len(reconciliation)),
                "max_abs_residual_usd_billions": float(residual.abs().max()),
                "max_abs_residual_quarter": str(residual.abs().idxmax()),
                "quarters_abs_residual_over_0_1bn": int((residual.abs() > 0.1).sum()),
                "max_admissible_abs_residual_usd_billions": args.max_mspd_residual_bn,
                "quarters_over_admissible": {quarter: float(value) for quarter, value in over.items()},
                "max_abs_residual_after_estimate_gap_usd_billions": float(
                    reconciliation["residual_after_estimate_gap_bn"].abs().max()
                ),
            },
        },
    }
    (out_dir / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    if not over.empty:
        print(f"Net bills differ from MSPD by more than {args.max_mspd_residual_bn} bn in: {over.round(3).to_dict()}")
        return 1
    print(f"Saved {len(table)} official quarter rows, the MSPD reconciliation and receipt to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
