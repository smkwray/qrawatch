from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest

from ati_shadow_policy.qra_capture import CAPTURE_COLUMNS

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "34_build_official_quarter_refresh.py"
spec = importlib.util.spec_from_file_location("official_quarter_refresh", SCRIPT_PATH)
assert spec is not None and spec.loader is not None
refresh = importlib.util.module_from_spec(spec)
spec.loader.exec_module(refresh)

FD = "https://fiscaldata.treasury.gov/datasets/treasury-securities-auctions-data/auctions-query"


def _inputs(tmp_path: Path, mspd_shift_mil: float = 0.0) -> tuple[Path, Path, Path]:
    row = {column: "" for column in CAPTURE_COLUMNS}
    row.update(
        {
            "quarter": "2026Q2",
            "qra_release_date": "2026-02-04",
            "market_pricing_marker_minus_1d": "2026-02-03",
            "total_financing_need_bn": "109",
            "net_bill_issuance_bn": "1",
            "financing_source_url": "https://home.treasury.gov/news/press-releases/sb0377",
            "financing_source_doc_type": "quarterly_refunding_press_release",
            "refunding_statement_source_url": "https://home.treasury.gov/news/press-releases/sb0489",
            "refunding_statement_source_doc_type": "official_quarterly_refunding_statement",
            "auction_reconstruction_source_url": FD,
            "auction_reconstruction_source_doc_type": "official_auction_reconstruction",
            "source_url": f"https://home.treasury.gov/news/press-releases/sb0489|{FD}",
            "source_doc_local": "data/raw/fiscaldata/auctions_query.csv",
            "source_doc_type": "official_quarterly_refunding_statement|quarterly_refunding_press_release|official_auction_reconstruction",
            "qa_status": "manual_official_capture",
        }
    )
    template = tmp_path / "template.csv"
    pd.DataFrame([row], columns=CAPTURE_COLUMNS).to_csv(template, index=False)
    auctions = tmp_path / "auctions_query.csv"
    pd.DataFrame(
        {
            "record_date": ["2026-04-07", "2026-04-07", "2026-05-12"],
            "issue_date": ["2026-04-09", "2026-04-09", "2026-05-14"],
            "maturity_date": ["2026-07-09", "2026-10-08", "2026-08-13"],
            "announcemt_date": ["2026-04-02", "2026-04-02", "2026-05-07"],
            "security_type": ["Bill", "Bill", "Bill"],
            "offering_amt": [80e9, 70e9, 90e9],
            "total_accepted": [80e9, 70e9, 90e9],
            "soma_accepted": [0.0, 0.0, 0.0],
            "soma_holdings": [0.0, 0.0, 0.0],
            "est_pub_held_mat_by_type_amt": [160e9, 160e9, 120e9],
            "cash_management_bill_cmb": ["No", "No", "No"],
        }
    ).to_csv(auctions, index=False)
    # Marketable bills outstanding (USD millions) fall by the quarter's net issuance of -40.
    pd.DataFrame(
        {
            "record_date": ["2026-03-31", "2026-06-30"],
            "security_type_desc": ["Marketable", "Marketable"],
            "security_class_desc": ["Bills", "Bills"],
            "total_mil_amt": [1_000_000 + mspd_shift_mil, 960_000],
        }
    ).to_csv(tmp_path / "mspd_table_1.csv", index=False)
    return template, auctions, tmp_path / "mspd_table_1.csv"


def test_refresh_rebuilds_official_tables_only_under_out_dir(tmp_path: Path) -> None:
    # A refresh must leave admitted publish bytes alone and still apply the published
    # definitions: reconstructed net bills replace the template value, tilt = net - 0.18 * need.
    template, auctions, mspd = _inputs(tmp_path)
    out_dir = tmp_path / "refresh"
    argv = ["--template", str(template), "--auctions", str(auctions), "--mspd", str(mspd), "--out-dir", str(out_dir)]

    assert refresh.main(argv) == 0

    table = pd.read_csv(out_dir / "publish" / "ati_quarter_table.csv")
    row = table.iloc[0]
    net_bills = (80 + 70 - 160) + (90 - 120)  # same-day maturities are counted once
    assert row["net_bills_bn"] == pytest.approx(net_bills)
    assert row["ati_baseline_bn"] == pytest.approx(net_bills - 0.18 * 109)
    assert row["bill_share"] == pytest.approx(net_bills / 109)
    assert (row["source_quality"], row["public_role"]) == ("exact_official_numeric", "headline")

    completion = pd.read_csv(out_dir / "publish" / "official_capture_completion.csv").iloc[0]
    assert bool(completion["is_headline_ready"]) and completion["reconstruction_status_bill"] == "complete"

    receipt = json.loads((out_dir / "receipt.json").read_text())
    for entry in receipt["outputs"].values():
        path = Path(entry["path"])
        assert hashlib.sha256(path.read_bytes()).hexdigest() == entry["sha256"]
        assert path.resolve().is_relative_to(out_dir.resolve())
    assert receipt["validation"]["quarters"] == 1
    reconciliation = pd.read_csv(out_dir / "publish" / "net_bills_mspd_reconciliation.csv")
    assert reconciliation["quarter"].tolist() == ["2026Q2"]
    assert reconciliation.loc[0, "residual_bn"] == pytest.approx(0.0, abs=1e-9)
    assert receipt["validation"]["mspd_reconciliation"]["quarters_over_admissible"] == {}

    with pytest.raises(SystemExit, match="Refusing to replace"):
        refresh.main(argv)


def test_refresh_fails_when_a_quarter_cannot_be_reconciled_with_mspd(tmp_path: Path) -> None:
    # Net bills are only admitted with their MSPD check; a missing quarter-end stock must stop
    # the build rather than publish an unreconciled quarter.
    template, auctions, mspd = _inputs(tmp_path)
    frame = pd.read_csv(mspd)
    frame[frame["record_date"] != "2026-03-31"].to_csv(mspd, index=False)

    with pytest.raises(ValueError, match="2026Q2"):
        refresh.main(["--template", str(template), "--auctions", str(auctions), "--mspd", str(mspd), "--out-dir", str(tmp_path / "out")])
    assert not (tmp_path / "out" / "receipt.json").exists()


def test_refresh_fails_loudly_when_net_bills_and_mspd_disagree(tmp_path: Path) -> None:
    # A $3 billion gap means a bill flow is missing or misdated; the receipt names the quarter
    # and the build exits nonzero so the table is not consumed as reconciled.
    template, auctions, mspd = _inputs(tmp_path, mspd_shift_mil=-3_000)
    out_dir = tmp_path / "refresh"

    status = refresh.main(["--template", str(template), "--auctions", str(auctions), "--mspd", str(mspd), "--out-dir", str(out_dir)])

    assert status == 1
    receipt = json.loads((out_dir / "receipt.json").read_text())
    assert receipt["validation"]["mspd_reconciliation"]["quarters_over_admissible"] == {"2026Q2": pytest.approx(-3.0)}


def test_refresh_fails_when_a_quarter_has_no_complete_bill_reconstruction(tmp_path: Path) -> None:
    # Without a complete reconstruction the template's own net bills would pass through silently.
    template, auctions, mspd = _inputs(tmp_path)
    frame = pd.read_csv(auctions)
    frame["issue_date"] = ["2026-01-08", "2026-01-08", "2026-02-12"]
    frame["announcemt_date"] = ["2026-01-02", "2026-01-02", "2026-02-05"]
    frame["maturity_date"] = ["2026-03-05", "2026-03-05", "2026-03-12"]
    frame.to_csv(auctions, index=False)

    with pytest.raises(SystemExit, match="incomplete or missing for: 2026Q2"):
        refresh.main(["--template", str(template), "--auctions", str(auctions), "--mspd", str(mspd), "--out-dir", str(tmp_path / "out")])
