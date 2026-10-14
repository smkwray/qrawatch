from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest

from ati_shadow_policy.paths import MANUAL_DIR
from ati_shadow_policy.qra_capture import CAPTURE_COLUMNS, financing_sentences

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "35_build_realized_financing_refresh.py"
spec = importlib.util.spec_from_file_location("realized_financing_refresh", SCRIPT_PATH)
assert spec is not None and spec.loader is not None
realized_refresh = importlib.util.module_from_spec(spec)
spec.loader.exec_module(realized_refresh)

PR = "https://home.treasury.gov/news/press-releases/"
FD = "https://fiscaldata.treasury.gov/datasets/treasury-securities-auctions-data/auctions-query"
REALIZED = MANUAL_DIR / "realized_privately_held_borrowing.csv"


def test_release_sentences_separate_what_treasury_borrowed_from_what_it_expected():
    # A release estimates the current and next quarter and reports the quarter just closed.
    # Only the past-tense figure is realized borrowing; a footnote digit after "billion." must
    # not swallow it, and paydowns (including the 2016 "payed down" spelling) are negative.
    text = (
        "During the April – June 2018 quarter, Treasury expects to borrow $75 billion in "
        "privately-held net marketable debt. During the October – December 2018 quarter, Treasury "
        "expects to borrow $440 billion, assuming a cash balance of $390 billion.2 During the "
        "April – June 2018 quarter, Treasury borrowed $72 billion in privately-held net marketable "
        "debt and ended the quarter with a cash balance of $333 billion. During the April – June "
        "2016 quarter, Treasury payed down $25 billion in net marketable debt. During the "
        "April–June 2020 quarter, Treasury borrowed $2.753 trillion in privately-held net marketable debt."
    )
    rows = financing_sentences(text)

    realized = {(row["quarter"], row["amount_bn"]) for row in rows if row["kind"] == "realized"}
    estimates = {(row["quarter"], row["amount_bn"]) for row in rows if row["kind"] == "estimate"}
    assert realized == {("2018Q2", 72.0), ("2016Q2", -25.0), ("2020Q2", 2753.0)}
    assert estimates == {("2018Q2", 75.0), ("2018Q4", 440.0)}


def test_year_less_2009_sentences_resolve_to_the_quarter_next_to_the_release():
    # Some 2009 releases write "the October – December quarter"; the only such quarter within
    # one quarter of a November 2009 release is 2009Q4, and January – March is 2010Q1.
    text = (
        "During the October – December quarter, Treasury expects to issue $276 billion in net "
        "marketable debt. During the January - March quarter, Treasury expects to issue $478 billion "
        "in net marketable debt. During the July – September 2009 quarter, Treasury issued $393 "
        "billion in net marketable debt."
    )
    rows = financing_sentences(text, "2009-11-02")

    assert [(r["quarter"], r["kind"], r["amount_bn"]) for r in rows] == [
        ("2009Q4", "estimate", 276.0),
        ("2010Q1", "estimate", 478.0),
        ("2009Q3", "realized", 393.0),
    ]


def test_tracked_capture_is_realized_borrowing_from_the_following_quarter_for_every_quarter():
    # The issuance tilt divides bills by what Treasury actually borrowed in the quarter. That
    # figure exists only after the quarter closes, so it must come from the next quarter's
    # release, be quoted in the past tense, name the quarter's own months, and cover every
    # quarter in the sample.
    capture = pd.read_csv(REALIZED, dtype={"quarter": str})
    quarters = realized_refresh.quarter_range("2009Q1", "2026Q2")

    assert capture["quarter"].tolist() == quarters
    assert capture["status"].eq("captured").all()
    assert capture["realized_financing_bn"].notna().all()
    for row in capture.itertuples():
        quarter = pd.Period(row.quarter, freq="Q")
        released = pd.Timestamp(row.release_date)
        assert quarter.end_time < released <= (quarter + 1).end_time, row.quarter
        assert row.release_quarter == realized_refresh.qshift(row.quarter, 1)
        assert "expects" not in row.quoted_sentence
        parsed = financing_sentences(row.quoted_sentence)
        assert [(p["quarter"], p["kind"]) for p in parsed] == [(row.quarter, "realized")]
        assert parsed[0]["amount_bn"] == pytest.approx(row.realized_financing_bn)
        assert row.release_url.startswith("https://home.treasury.gov/")
        assert len(row.doc_sha256) == 64 and pd.notna(row.retrieved_utc)
    # Since the 2018Q1 release Treasury names the measure privately held net marketable borrowing.
    assert capture.loc[capture["quarter"] >= "2017Q4", "wording"].eq("privately_held_net_marketable").all()


def test_formerly_misread_quarters_carry_realized_borrowing_not_the_next_quarter_estimate():
    # The earlier table held the following quarter's estimate for these quarters (169, 155,
    # 677). 2020Q2 matters most: Treasury borrowed $2.753 trillion, not $677 billion.
    capture = pd.read_csv(REALIZED, dtype={"quarter": str}).set_index("quarter")["realized_financing_bn"]

    assert capture["2014Q2"] == -64.0
    assert capture["2016Q2"] == -25.0
    assert capture["2020Q2"] == 2753.0


def _release_html(date: str, sentences: str) -> str:
    return f'<html><body><time datetime="{date}T08:00:00-04:00">x</time><p>{sentences}</p></body></html>'


def _template_row(quarter: str, release_date: str, financing: str, source: str) -> dict[str, str]:
    row = {column: "" for column in CAPTURE_COLUMNS}
    row.update(
        {
            "quarter": quarter,
            "qra_release_date": release_date,
            "market_pricing_marker_minus_1d": str((pd.Timestamp(release_date) - pd.Timedelta(days=1)).date()),
            "total_financing_need_bn": financing,
            "net_bill_issuance_bn": "1",
            "financing_source_url": PR + source,
            "financing_source_doc_type": "quarterly_refunding_press_release",
            "refunding_statement_source_url": PR + "stmt" + quarter,
            "refunding_statement_source_doc_type": "official_quarterly_refunding_statement",
            "auction_reconstruction_source_url": FD,
            "auction_reconstruction_source_doc_local": "data/raw/fiscaldata/auctions_query.csv",
            "auction_reconstruction_source_doc_type": "official_auction_reconstruction",
            "source_url": f"{PR}stmt{quarter}|{PR}{source}|{FD}",
            "source_doc_local": "data/raw/fiscaldata/auctions_query.csv",
            "source_doc_type": "official_quarterly_refunding_statement|quarterly_refunding_press_release|official_auction_reconstruction",
            "qa_status": "manual_official_capture",
        }
    )
    return row


def _inputs(tmp_path: Path, *, drop_release: str | None = None) -> tuple[Path, Path, Path]:
    # Three quarters whose template values follow the three conventions found in the admitted
    # table: own-quarter estimate (2020Q1), following-quarter estimate misread as the quarter's
    # own (2020Q2), and the prior release's next-quarter estimate (2020Q3).
    releases = {
        "2019Q4": ("2019-10-28", "During the January – March 2020 quarter, Treasury expects to borrow $389 billion."),
        "2020Q1": ("2020-02-03", "During the January – March 2020 quarter, Treasury expects to borrow $367 billion. "
                   "During the April – June 2020 quarter, Treasury expects to pay down $56 billion."),
        "2020Q2": ("2020-05-04", "During the April – June 2020 quarter, Treasury expects to borrow $2.999 trillion. "
                   "During the July – September 2020 quarter, Treasury expects to borrow $677 billion. "
                   "During the January – March 2020 quarter, Treasury borrowed $477 billion in privately-held net marketable debt."),
        "2020Q3": ("2020-08-03", "During the July – September 2020 quarter, Treasury expects to borrow $947 billion. "
                   "During the April – June 2020 quarter, Treasury borrowed $2.753 trillion in privately-held net marketable debt."),
        "2020Q4": ("2020-11-02", "During the July – September 2020 quarter, Treasury borrowed $454 billion in privately-held net marketable debt."),
    }
    out_dir = tmp_path / "refresh"
    log = []
    for quarter, (date, text) in releases.items():
        if quarter == drop_release:
            continue
        path = out_dir / "sources" / "releases" / f"r{quarter}.html"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(_release_html(date, text), encoding="utf-8")
        log.append(
            {
                "release_quarter": quarter,
                "url": PR + f"r{quarter}",
                "local_path": str(path),
                "download_status": "ok",
                "downloaded_at_utc": "2026-10-14T00:00:00+00:00",
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    pd.DataFrame(log).to_csv(out_dir / "sources" / "release_downloads.csv", index=False)

    template = tmp_path / "template.csv"
    pd.DataFrame(
        [
            _template_row("2020Q1", "2020-02-03", "367", "r2020Q1"),
            _template_row("2020Q2", "2020-05-04", "677", "r2020Q2"),
            _template_row("2020Q3", "2020-05-04", "677", "r2020Q2"),
        ],
        columns=CAPTURE_COLUMNS,
    ).to_csv(template, index=False)
    auctions = tmp_path / "auctions_query.csv"
    pd.DataFrame(
        {
            "record_date": ["2020-03-01", "2020-05-01", "2020-08-01"],
            "issue_date": ["2020-02-06", "2020-05-07", "2020-08-06"],
            "maturity_date": ["2020-05-07", "2020-08-06", "2020-11-05"],
            "security_type": ["Bill", "Bill", "Bill"],
            "offering_amt": [100e9, 900e9, 50e9],
            "est_pub_held_mat_by_type_amt": [60e9, 100e9, 900e9],
            "cash_management_bill_cmb": ["No", "No", "No"],
        }
    ).to_csv(auctions, index=False)
    return out_dir, template, auctions


def test_build_puts_realized_borrowing_in_the_tilt_and_labels_what_it_replaced(tmp_path: Path):
    out_dir, template, auctions = _inputs(tmp_path)
    realized_csv = tmp_path / "realized.csv"
    argv = ["build", "--out-dir", str(out_dir), "--template", str(template), "--auctions", str(auctions), "--realized", str(realized_csv)]

    assert realized_refresh.main(argv) == 0

    table = pd.read_csv(out_dir / "publish" / "ati_quarter_table.csv").set_index("quarter")
    realized = {"2020Q1": 477.0, "2020Q2": 2753.0, "2020Q3": 454.0}
    for quarter, value in realized.items():
        assert table.loc[quarter, "financing_need_bn"] == value
        assert table.loc[quarter, "ati_baseline_bn"] == pytest.approx(table.loc[quarter, "net_bills_bn"] - 0.18 * value)

    audit = pd.read_csv(out_dir / "publish" / "financing_convention_audit.csv").set_index("quarter")
    assert audit["old_convention"].to_dict() == {
        "2020Q1": "own_quarter_estimate",
        "2020Q2": "misread_following_quarter_estimate",
        "2020Q3": "prior_release_estimate",
    }
    assert audit.loc["2020Q2", ["old_financing_bn", "own_quarter_estimate_bn"]].tolist() == [677.0, 2999.0]
    assert audit.loc["2020Q2", "old_ati_baseline_bn"] == pytest.approx(audit.loc["2020Q2", "net_bills_bn"] - 0.18 * 677)

    receipt = json.loads((out_dir / "receipt.json").read_text())
    block = receipt["realized_financing"]
    assert block["validation"]["captured"] == 3 and block["validation"]["max_financing_residual_usd_billions"] == 0.0
    assert block["realized_capture"]["sha256"] == hashlib.sha256(realized_csv.read_bytes()).hexdigest()
    assert receipt["argv"][1:] == argv


def test_build_refuses_to_publish_a_table_with_a_missing_realized_quarter(tmp_path: Path):
    # Without the next quarter's release the quarter has no realized figure; the build must say
    # so and write no tilt table rather than drop the quarter or fall back to an estimate.
    out_dir, template, auctions = _inputs(tmp_path, drop_release="2020Q4")
    realized_csv = tmp_path / "realized.csv"

    status = realized_refresh.main(
        ["build", "--out-dir", str(out_dir), "--template", str(template), "--auctions", str(auctions), "--realized", str(realized_csv)]
    )

    assert status == 1
    capture = pd.read_csv(realized_csv).set_index("quarter")
    assert capture.loc["2020Q3", "status"] == "release_not_downloaded"
    assert pd.isna(capture.loc["2020Q3", "realized_financing_bn"])
    assert not (out_dir / "publish" / "ati_quarter_table.csv").exists()


def test_coverage_diagnostic_counts_bill_flows_the_reconstruction_leaves_out():
    # A CMB issued on a day with no maturing bills has no maturity estimate, so the
    # reconstruction drops its offering; a CMB maturing on a day with no bill issue is never
    # netted. The diagnostic reports both so the numerator gap is visible.
    auctions = pd.DataFrame(
        {
            "issue_date": ["2021-10-07", "2021-10-13", "2021-10-28"],
            "maturity_date": ["2022-01-06", "2021-10-15", "2021-12-15"],
            "security_type": ["Bill", "Bill", "Bill"],
            "cash_management_bill_cmb": ["No", "Yes", "Yes"],
            "offering_amt": [100e9, 30e9, 40e9],
            "est_pub_held_mat_by_type_amt": [90e9, None, 20e9],
        }
    )
    gaps = realized_refresh.bill_coverage_gaps(auctions).set_index("quarter")

    assert gaps.loc["2021Q4", "cmb_only_issue_offering_bn"] == pytest.approx(30.0)
    assert gaps.loc["2021Q4", "unnetted_bill_maturities_bn"] == pytest.approx(70.0)
