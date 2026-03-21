from __future__ import annotations

import pandas as pd
import pytest

from ati_shadow_policy.paths import RAW_DIR
from ati_shadow_policy.qra_capture import (
    CAPTURE_COLUMNS,
    build_capture_completion_status,
    build_official_capture,
    build_financing_release_source_map,
    build_quarter_net_issuance_from_auctions,
    build_refunding_statement_manifest,
    build_refunding_statement_source_map,
    enrich_capture_with_auction_reconstruction,
    enrich_capture_with_financing_release_map,
    enrich_capture_with_refunding_statement_map,
    seed_capture_rows_from_local_sources,
)


def _empty_capture_df() -> pd.DataFrame:
    return pd.DataFrame(columns=CAPTURE_COLUMNS)


def test_build_official_capture_accepts_empty_template_contract() -> None:
    result = build_official_capture(_empty_capture_df())
    assert list(result.dataframe.columns) == list(CAPTURE_COLUMNS)
    assert result.dataframe.empty
    assert result.seeded_rows_added == 0


def test_build_official_capture_rejects_missing_column() -> None:
    bad = pd.DataFrame(columns=[col for col in CAPTURE_COLUMNS if col != "notes"])
    with pytest.raises(ValueError, match="columns do not match required contract"):
        build_official_capture(bad)


def test_build_official_capture_rejects_invalid_qa_label() -> None:
    frame = _empty_capture_df()
    frame.loc[0] = {
        "quarter": "2026Q1",
        "qra_release_date": "2026-01-29",
        "market_pricing_marker_minus_1d": "2026-01-28",
        "qa_status": "bad_status",
    }
    with pytest.raises(ValueError, match="qa_status='bad_status' is invalid"):
        build_official_capture(frame)


def test_build_official_capture_requires_official_fields_for_manual_capture() -> None:
    frame = _empty_capture_df()
    frame.loc[0] = {
        "quarter": "2026Q1",
        "qra_release_date": "2026-01-29",
        "market_pricing_marker_minus_1d": "2026-01-28",
        "total_financing_need_bn": "123",
        "net_bill_issuance_bn": "45",
        "qa_status": "manual_official_capture",
        "source_doc_local": "doc1.pdf",
        "source_doc_type": "pdf",
    }
    with pytest.raises(ValueError, match="requires non-empty 'source_url'"):
        build_official_capture(frame)


def test_seed_capture_rows_from_local_sources_maps_known_quarters() -> None:
    qra_events = pd.DataFrame(
        [
            {
                "official_release_date": "2023-08-02",
                "market_pricing_marker_minus_1d": "2023-08-01",
            },
            {
                "official_release_date": "2023-11-01",
                "market_pricing_marker_minus_1d": "2023-10-31",
            },
            {
                "official_release_date": "2024-01-31",
                "market_pricing_marker_minus_1d": "2024-01-30",
            },
            {
                "official_release_date": "2024-05-01",
                "market_pricing_marker_minus_1d": "2024-04-30",
            },
        ]
    )
    quarterly_seed = pd.DataFrame(
        [
            {"quarter": "2023Q4", "financing_need_bn": "852.0", "net_bills_bn": "513.0"},
            {"quarter": "2024Q1", "financing_need_bn": "816.0", "net_bills_bn": "468.0"},
            {"quarter": "2024Q3", "financing_need_bn": "847.0", "net_bills_bn": "285.0"},
        ]
    )
    seeded = seed_capture_rows_from_local_sources(qra_events, quarterly_seed)

    assert seeded["quarter"].tolist() == ["2023Q4", "2024Q1", "2024Q2", "2024Q3"]
    assert set(seeded["qa_status"].tolist()) == {"seed_only"}
    row_q2 = seeded.loc[seeded["quarter"] == "2024Q2"].iloc[0]
    assert row_q2["total_financing_need_bn"] == ""
    assert row_q2["source_doc_type"] == "seed_csv"


def test_build_official_capture_can_seed_missing_quarters() -> None:
    capture = _empty_capture_df()
    capture.loc[0] = {
        "quarter": "2023Q4",
        "qra_release_date": "2023-08-02",
        "market_pricing_marker_minus_1d": "2023-08-01",
        "qa_status": "seed_only",
        "source_doc_local": "data/manual/qra_event_seed.csv",
        "source_doc_type": "seed_csv",
    }

    qra_events = pd.DataFrame(
        [
            {
                "official_release_date": "2023-08-02",
                "market_pricing_marker_minus_1d": "2023-08-01",
            },
            {
                "official_release_date": "2023-11-01",
                "market_pricing_marker_minus_1d": "2023-10-31",
            },
        ]
    )
    quarterly_seed = pd.DataFrame(
        [{"quarter": "2024Q1", "financing_need_bn": "816.0", "net_bills_bn": "468.0"}]
    )

    result = build_official_capture(
        capture,
        qra_event_seed_df=qra_events,
        quarterly_refunding_seed_df=quarterly_seed,
        seed_missing_quarters=True,
    )
    assert result.seeded_rows_added == 1
    assert sorted(result.dataframe["quarter"].dropna().tolist()) == ["2023Q4", "2024Q1"]


def test_build_financing_release_source_map_extracts_official_borrowing(tmp_path) -> None:
    text_dir = tmp_path / "qra_text"
    text_dir.mkdir()
    local_html = tmp_path / "jy1662_demo.html"
    local_html.write_text("<html></html>", encoding="utf-8")
    (text_dir / "jy1662_demo.txt").write_text(
        "\n".join(
            [
                "Treasury Announces Marketable Borrowing Estimates",
                "July 31, 2023",
                "Additional financing details relating to Treasury’s Quarterly Refunding will be released at 8:30 a.m. on Wednesday, August 2, 2023.",
                "During the October – December 2023 quarter, Treasury expects to borrow $852 billion in privately-held net marketable debt, assuming an end-of-December cash balance of $750 billion.",
            ]
        ),
        encoding="utf-8",
    )
    events = pd.DataFrame(
        [
            {
                "official_release_date": "2023-08-02",
                "market_pricing_marker_minus_1d": "2023-08-01",
            }
        ]
    )
    downloads = pd.DataFrame(
        [
            {
                "href": "https://home.treasury.gov/news/press-releases/jy1662",
                "local_path": str(local_html),
                "doc_type": "quarterly_refunding_press_release",
            }
        ]
    )

    source_map = build_financing_release_source_map(events, downloads, text_dir)

    assert list(source_map["quarter"]) == ["2023Q4"]
    assert source_map.loc[0, "announced_borrowing_bn"] == 852.0
    assert source_map.loc[0, "match_status"] == "matched"


def test_enrich_capture_with_financing_release_map_promotes_seed_rows() -> None:
    capture = _empty_capture_df()
    capture.loc[0] = {
        "quarter": "2024Q2",
        "qra_release_date": "2024-01-31",
        "market_pricing_marker_minus_1d": "2024-01-30",
        "source_doc_local": "data/manual/qra_event_seed.csv|data/manual/quarterly_refunding_seed.csv",
        "source_doc_type": "seed_csv",
        "qa_status": "seed_only",
        "notes": "Seeded from qra_event_seed only.",
    }
    release_map = pd.DataFrame(
        [
            {
                "quarter": "2024Q2",
                "qra_release_date": "2024-01-31",
                "official_release_date_phrase": "January 31, 2024",
                "expected_period": "April - June 2024",
                "source_url": "https://home.treasury.gov/news/press-releases/jy2054",
                "source_doc_local": "/tmp/jy2054.html",
                "source_doc_type": "quarterly_refunding_press_release",
                "announced_borrowing_bn": 202.0,
                "match_status": "matched",
            }
        ]
    )

    enriched = enrich_capture_with_financing_release_map(capture, release_map)

    row = enriched.iloc[0]
    assert row["qa_status"] == "semi_automated_capture"
    assert row["total_financing_need_bn"] == "202"
    assert row["source_url"] == "https://home.treasury.gov/news/press-releases/jy2054"
    assert row["source_doc_local"].startswith("/tmp/jy2054.html|")
    assert "Official borrowing estimate matched to January 31, 2024" in row["notes"]


def test_build_refunding_statement_manifest_maps_archive_links(tmp_path) -> None:
    archive = tmp_path / "official-remarks-on-quarterly-refunding-by-calendar-year_demo.html"
    archive.write_text(
        """
        <html><body>
        <a href="/news/press-releases/jy2062" aria-label="2024 1st Quarter">1st Quarter</a>
        <a href="/news/press-releases/jy2315" aria-label="2024 2nd Quarter">2nd Quarter</a>
        </body></html>
        """,
        encoding="utf-8",
    )
    capture = _empty_capture_df()
    capture.loc[0] = {"quarter": "2024Q1"}
    capture.loc[1] = {"quarter": "2024Q2"}

    manifest = build_refunding_statement_manifest(capture, [archive])

    assert manifest["quarter"].tolist() == ["2024Q1", "2024Q2"]
    assert manifest.loc[0, "href"] == "https://home.treasury.gov/news/press-releases/jy2062"
    assert manifest.loc[0, "doc_type"] == "official_quarterly_refunding_statement"


def test_build_refunding_statement_source_map_extracts_guidance(tmp_path) -> None:
    html_path = tmp_path / "jy2315_demo.html"
    html_path.write_text(
        """
        <html><body>
        NOMINAL COUPON AND FRN FINANCING
        Based on current projected borrowing needs, Treasury does not anticipate needing to increase nominal coupon or FRN auction sizes for at least the next several quarters.
        The table below presents, in billions of dollars, the actual auction sizes for the February to April 2024 quarter and the anticipated auction sizes for the May to July 2024 quarter:
        Feb-24 63 54 64 42 42 16 25 28
        Mar-24 66 56 67 43 39 13 22 28
        Apr-24 69 58 70 44 39 13 22 30
        May-24 69 58 70 44 42 16 25 28
        Jun-24 69 58 70 44 39 13 22 28
        Jul-24 69 58 70 44 39 13 22 30
        Treasury plans to address any seasonal or unexpected variations in borrowing needs over the next quarter through changes in regular bill auction sizes and/or CMBs.
        TIPS FINANCING
        BILL ISSUANCE
        Treasury expects to increase the 4-, 6-, and 8-week bill auction sizes in the coming days.
        INTRODUCTION OF THE 6-WEEK BILL BENCHMARK
        Treasury will continue with weekly issuance of the 6-week CMB.
        BUYBACKS
        Today, Treasury is announcing the launch of its buyback program.
        </body></html>
        """,
        encoding="utf-8",
    )
    downloads = pd.DataFrame(
        [
            {
                "quarter": "2024Q2",
                "href": "https://home.treasury.gov/news/press-releases/jy2315",
                "local_path": str(html_path),
                "doc_type": "official_quarterly_refunding_statement",
            }
        ]
    )

    source_map = build_refunding_statement_source_map(downloads)

    row = source_map.iloc[0]
    assert row["quarter"] == "2024Q2"
    assert "May-24: 2Y=69" in row["guidance_nominal_coupons"]
    assert "Jul-24: 2Y=69" in row["guidance_nominal_coupons"]
    assert "Monthly FRN schedule: May-24: 2Y FRN=28" in row["guidance_frns"]
    assert "8-week bill auction sizes" in row["bill_guidance"]
    assert "6-week CMB" in row["bill_guidance"]
    assert "buyback program" in row["guidance_buybacks"]


def test_enrich_capture_with_refunding_statement_map_merges_sources_and_notes() -> None:
    capture = _empty_capture_df()
    capture.loc[0] = {
        "quarter": "2024Q2",
        "qra_release_date": "2024-01-31",
        "market_pricing_marker_minus_1d": "2024-01-30",
        "source_url": "https://home.treasury.gov/news/press-releases/jy2054",
        "source_doc_local": "/tmp/jy2054.html|data/manual/quarterly_refunding_seed.csv",
        "source_doc_type": "quarterly_refunding_press_release|seed_csv",
        "qa_status": "semi_automated_capture",
        "notes": "Seeded from qra_event_seed only.",
    }
    statement_map = pd.DataFrame(
        [
            {
                "quarter": "2024Q2",
                "source_url": "https://home.treasury.gov/news/press-releases/jy2315",
                "source_doc_local": "/tmp/jy2315.html",
                "source_doc_type": "official_quarterly_refunding_statement",
                "guidance_nominal_coupons": "No further coupon increases expected.",
                "guidance_frns": "FRN sizes unchanged.",
                "guidance_buybacks": "Buybacks launched.",
                "bill_guidance": "Bill auction sizes increase near month-end, then ease.",
                "match_status": "matched",
            }
        ]
    )

    enriched = enrich_capture_with_refunding_statement_map(capture, statement_map)

    row = enriched.iloc[0]
    assert "jy2315" in row["source_url"]
    assert row["source_doc_local"].startswith("/tmp/jy2315.html|")
    assert "official_quarterly_refunding_statement" in row["source_doc_type"]
    assert row["guidance_nominal_coupons"] == "No further coupon increases expected."
    assert row["guidance_frns"] == "FRN sizes unchanged."
    assert row["guidance_buybacks"] == "Buybacks launched."
    assert "Official quarterly refunding statement matched for issuance guidance." in row["notes"]
    assert "Bill guidance: Bill auction sizes increase near month-end, then ease." in row["notes"]


def test_build_quarter_net_issuance_from_auctions_reconstructs_2023q4_bill_net() -> None:
    auctions_path = RAW_DIR / "fiscaldata" / "auctions_query.csv"
    if not auctions_path.exists():
        pytest.skip("fiscaldata auctions_query.csv not available in this environment")
    auctions = pd.read_csv(auctions_path, low_memory=False)

    reconstruction = build_quarter_net_issuance_from_auctions(auctions, quarters=["2023Q4"])
    bill_row = reconstruction.loc[reconstruction["bucket"] == "bill_like"].iloc[0]

    assert bill_row["quarter"] == "2023Q4"
    assert bill_row["reconstruction_status"] == "complete"
    assert float(bill_row["net_issuance_bn"]) == pytest.approx(437.45, abs=0.05)


def test_enrich_capture_with_auction_reconstruction_promotes_rows() -> None:
    capture = _empty_capture_df()
    capture.loc[0] = {
        "quarter": "2024Q2",
        "qra_release_date": "2024-01-31",
        "market_pricing_marker_minus_1d": "2024-01-30",
        "total_financing_need_bn": "202",
        "source_url": "https://home.treasury.gov/news/press-releases/jy2054",
        "source_doc_local": "/tmp/jy2054.html",
        "source_doc_type": "quarterly_refunding_press_release",
        "qa_status": "semi_automated_capture",
        "notes": "Seeded placeholder.",
    }
    reconstruction = pd.DataFrame(
        [
            {
                "quarter": "2024Q2",
                "bucket": "bill_like",
                "gross_offering_bn": 5513.0,
                "maturing_estimate_bn": 5809.523,
                "net_issuance_bn": -296.523,
                "issue_dates": 26,
                "issue_dates_missing_maturing_estimate": 0,
                "reconstruction_status": "complete",
            },
            {
                "quarter": "2024Q2",
                "bucket": "nominal_coupon",
                "gross_offering_bn": 947.0,
                "maturing_estimate_bn": 580.65,
                "net_issuance_bn": 366.35,
                "issue_dates": 8,
                "issue_dates_missing_maturing_estimate": 0,
                "reconstruction_status": "complete",
            },
            {
                "quarter": "2024Q2",
                "bucket": "tips",
                "gross_offering_bn": 60.0,
                "maturing_estimate_bn": 263.136,
                "net_issuance_bn": -203.136,
                "issue_dates": 3,
                "issue_dates_missing_maturing_estimate": 0,
                "reconstruction_status": "complete",
            },
            {
                "quarter": "2024Q2",
                "bucket": "frn",
                "gross_offering_bn": 114.0,
                "maturing_estimate_bn": 263.136,
                "net_issuance_bn": -149.136,
                "issue_dates": 4,
                "issue_dates_missing_maturing_estimate": 0,
                "reconstruction_status": "complete",
            },
        ]
    )

    enriched = enrich_capture_with_auction_reconstruction(capture, reconstruction)
    row = enriched.iloc[0]
    assert row["qa_status"] == "manual_official_capture"
    assert row["net_bill_issuance_bn"] == "-296.523"
    assert row["source_doc_type"].startswith("official_auction_reconstruction|")
    assert "Exact net bill issuance reconstructed from official auctions feed" in row["notes"]
    assert pd.isna(row["net_coupon_issuance_bn"])
    assert pd.isna(row["frn_issuance_bn"])


def test_enrich_capture_with_auction_reconstruction_strips_seed_provenance_on_promotion() -> None:
    capture = _empty_capture_df()
    capture.loc[0] = {
        "quarter": "2024Q1",
        "qra_release_date": "2023-11-01",
        "market_pricing_marker_minus_1d": "2023-10-31",
        "total_financing_need_bn": "816",
        "source_url": "https://home.treasury.gov/news/press-releases/jy1662",
        "source_doc_local": "/tmp/jy1662.html|data/manual/qra_event_seed.csv|data/manual/quarterly_refunding_seed.csv",
        "source_doc_type": "quarterly_refunding_press_release|seed_csv",
        "qa_status": "semi_automated_capture",
    }
    reconstruction = pd.DataFrame(
        [
            {
                "quarter": "2024Q1",
                "bucket": "bill_like",
                "gross_offering_bn": 6097.0,
                "maturing_estimate_bn": 5687.883,
                "net_issuance_bn": 409.117,
                "issue_dates": 26,
                "issue_dates_missing_maturing_estimate": 0,
                "reconstruction_status": "complete",
            }
        ]
    )

    enriched = enrich_capture_with_auction_reconstruction(capture, reconstruction)
    row = enriched.iloc[0]

    assert row["qa_status"] == "manual_official_capture"
    assert "seed_csv" not in row["source_doc_type"]
    assert "qra_event_seed.csv" not in row["source_doc_local"]
    assert "quarterly_refunding_seed.csv" not in row["source_doc_local"]


def test_build_capture_completion_status_distinguishes_tiers() -> None:
    capture = _empty_capture_df()
    capture.loc[0] = {
        "quarter": "2024Q1",
        "qra_release_date": "2023-11-01",
        "market_pricing_marker_minus_1d": "2023-10-31",
        "total_financing_need_bn": "816",
        "net_bill_issuance_bn": "409.117",
        "source_doc_type": "official_auction_reconstruction|quarterly_refunding_press_release",
        "source_doc_local": "data/raw/fiscaldata/auctions_query.csv",
        "qa_status": "manual_official_capture",
    }
    capture.loc[1] = {
        "quarter": "2024Q2",
        "qra_release_date": "2024-01-31",
        "market_pricing_marker_minus_1d": "2024-01-30",
        "total_financing_need_bn": "202",
        "net_bill_issuance_bn": pd.NA,
        "source_doc_type": "official_quarterly_refunding_statement",
        "source_doc_local": "/tmp/jy2315.html",
        "qa_status": "semi_automated_capture",
    }
    capture.loc[2] = {
        "quarter": "2024Q3",
        "qra_release_date": "2024-05-01",
        "market_pricing_marker_minus_1d": "2024-04-30",
        "total_financing_need_bn": "847",
        "net_bill_issuance_bn": "285",
        "source_doc_type": "seed_csv",
        "source_doc_local": "data/manual/quarterly_refunding_seed.csv",
        "qa_status": "seed_only",
    }

    reconstruction = pd.DataFrame(
        [
            {
                "quarter": "2024Q1",
                "bucket": "bill_like",
                "gross_offering_bn": 6097.0,
                "maturing_estimate_bn": 5687.883,
                "net_issuance_bn": 409.117,
                "issue_dates": 26,
                "issue_dates_missing_maturing_estimate": 0,
                "reconstruction_status": "complete",
            }
        ]
    )
    status = build_capture_completion_status(capture, reconstruction)
    status = status.set_index("quarter")

    assert status.loc["2024Q1", "completion_tier"] == "exact_official_numeric"
    assert status.loc["2024Q1", "is_headline_ready"]
    assert status.loc["2024Q2", "completion_tier"] == "semi_automated_guidance_only"
    assert status.loc["2024Q3", "completion_tier"] == "seed_assisted"
