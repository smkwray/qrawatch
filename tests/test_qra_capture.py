from __future__ import annotations

import pandas as pd
import pytest

import ati_shadow_policy.qra_capture as qra_capture_mod
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
    quarter_from_event_seed_row,
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


def test_build_official_capture_normalizes_project_root_local_paths(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(qra_capture_mod, "PROJECT_ROOT", tmp_path)
    frame = _empty_capture_df()
    frame.loc[0] = {
        "quarter": "2026Q1",
        "qra_release_date": "2026-01-29",
        "market_pricing_marker_minus_1d": "2026-01-28",
        "total_financing_need_bn": "123",
        "net_bill_issuance_bn": "45",
        "financing_source_url": "https://example.com/financing",
        "financing_source_doc_local": str(tmp_path / "data/raw/qra/files/financing.html"),
        "financing_source_doc_type": "quarterly_refunding_press_release",
        "refunding_statement_source_url": "https://example.com/statement",
        "refunding_statement_source_doc_local": str(tmp_path / "data/raw/qra/files/statement.html"),
        "refunding_statement_source_doc_type": "official_quarterly_refunding_statement",
        "auction_reconstruction_source_url": "https://example.com/auction",
        "auction_reconstruction_source_doc_local": str(tmp_path / "data/raw/fiscaldata/auctions_query.csv"),
        "auction_reconstruction_source_doc_type": "official_auction_reconstruction",
        "source_url": "https://example.com/financing|https://example.com/statement|https://example.com/auction",
        "source_doc_local": (
            f"{tmp_path / 'data/raw/qra/files/financing.html'}|"
            f"{tmp_path / 'data/raw/qra/files/statement.html'}|"
            f"{tmp_path / 'data/raw/fiscaldata/auctions_query.csv'}"
        ),
        "source_doc_type": (
            "quarterly_refunding_press_release|"
            "official_quarterly_refunding_statement|"
            "official_auction_reconstruction"
        ),
        "qa_status": "manual_official_capture",
    }

    result = build_official_capture(frame).dataframe.iloc[0]

    assert result["financing_source_doc_local"] == "data/raw/qra/files/financing.html"
    assert result["refunding_statement_source_doc_local"] == "data/raw/qra/files/statement.html"
    assert result["auction_reconstruction_source_doc_local"] == "data/raw/fiscaldata/auctions_query.csv"
    assert result["source_doc_local"] == (
        "data/raw/qra/files/financing.html|"
        "data/raw/qra/files/statement.html|"
        "data/raw/fiscaldata/auctions_query.csv"
    )


def test_build_official_capture_promotes_complete_semi_automated_rows() -> None:
    frame = _empty_capture_df()
    frame.loc[0] = {
        "quarter": "2010Q1",
        "qra_release_date": "2010-02-03",
        "market_pricing_marker_minus_1d": "2010-02-02",
        "total_financing_need_bn": "392",
        "net_bill_issuance_bn": "49.943",
        "financing_source_url": "https://home.treasury.gov/news/press-releases/tg524",
        "financing_source_doc_local": "data/raw/qra/files/tg524.html",
        "financing_source_doc_type": "quarterly_refunding_press_release",
        "refunding_statement_source_url": "https://home.treasury.gov/news/press-releases/tg527",
        "refunding_statement_source_doc_local": "data/raw/qra/files/tg527.html",
        "refunding_statement_source_doc_type": "official_quarterly_refunding_statement",
        "auction_reconstruction_source_url": "https://fiscaldata.treasury.gov/datasets/treasury-securities-auctions-data/auctions-query",
        "auction_reconstruction_source_doc_local": "data/raw/fiscaldata/auctions_query.csv",
        "auction_reconstruction_source_doc_type": "official_auction_reconstruction",
        "source_url": (
            "https://home.treasury.gov/news/press-releases/tg524|"
            "https://home.treasury.gov/news/press-releases/tg527|"
            "https://fiscaldata.treasury.gov/datasets/treasury-securities-auctions-data/auctions-query"
        ),
        "source_doc_local": (
            "data/raw/qra/files/tg524.html|"
            "data/raw/qra/files/tg527.html|"
            "data/raw/fiscaldata/auctions_query.csv"
        ),
        "source_doc_type": (
            "quarterly_refunding_press_release|"
            "official_quarterly_refunding_statement|"
            "official_auction_reconstruction"
        ),
        "qa_status": "semi_automated_capture",
    }

    result = build_official_capture(frame).dataframe.iloc[0]

    assert result["qa_status"] == "manual_official_capture"


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
            {
                "official_release_date": "2024-07-31",
                "market_pricing_marker_minus_1d": "2024-07-30",
            },
            {
                "official_release_date": "2024-10-30",
                "market_pricing_marker_minus_1d": "2024-10-29",
            },
            {
                "official_release_date": "2025-02-05",
                "market_pricing_marker_minus_1d": "2025-02-04",
            },
            {
                "official_release_date": "2025-04-30",
                "market_pricing_marker_minus_1d": "2025-04-29",
            },
            {
                "official_release_date": "2025-07-30",
                "market_pricing_marker_minus_1d": "2025-07-29",
            },
        ]
    )
    quarterly_seed = pd.DataFrame(
        [
            {"quarter": "2023Q4", "financing_need_bn": "852.0", "net_bills_bn": "513.0"},
            {"quarter": "2024Q1", "financing_need_bn": "816.0", "net_bills_bn": "468.0"},
            {"quarter": "2024Q3", "financing_need_bn": "847.0", "net_bills_bn": "285.0"},
            {"quarter": "2024Q4", "financing_need_bn": "565.0", "net_bills_bn": "183.0"},
            {"quarter": "2025Q1", "financing_need_bn": "823.0", "net_bills_bn": "-31.0"},
        ]
    )
    seeded = seed_capture_rows_from_local_sources(qra_events, quarterly_seed)

    assert seeded["quarter"].tolist() == [
        "2023Q4",
        "2024Q1",
        "2024Q2",
        "2024Q3",
        "2024Q4",
        "2025Q1",
        "2025Q2",
        "2025Q3",
        "2025Q4",
    ]
    assert set(seeded["qa_status"].tolist()) == {"seed_only"}
    row_q2 = seeded.loc[seeded["quarter"] == "2024Q2"].iloc[0]
    assert row_q2["total_financing_need_bn"] == ""
    assert row_q2["source_doc_type"] == "seed_csv"
    row_2025q2 = seeded.loc[seeded["quarter"] == "2025Q2"].iloc[0]
    assert row_2025q2["total_financing_need_bn"] == ""


def test_quarter_from_event_seed_row_prefers_explicit_quarter() -> None:
    event = pd.Series({"quarter": "2010Q1", "official_release_date": "2010-02-03"})

    assert quarter_from_event_seed_row(event) == "2010Q1"


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


def test_build_financing_release_source_map_uses_event_quarter_and_html_fallback(tmp_path) -> None:
    text_dir = tmp_path / "qra_text"
    text_dir.mkdir()
    local_html = tmp_path / "tg524_demo.html"
    local_html.write_text(
        "\n".join(
            [
                "<html><body>",
                "Treasury Announces Marketable Borrowing Estimates",
                "Additional financing details relating to Treasury's Quarterly Refunding will be released on Wednesday, February 3, 2010.",
                "During the January - March 2010 quarter, Treasury expects to borrow $392 billion in privately-held net marketable debt.",
                "</body></html>",
            ]
        ),
        encoding="utf-8",
    )
    events = pd.DataFrame(
        [
            {
                "quarter": "2010Q1",
                "official_release_date": "2010-02-03",
                "market_pricing_marker_minus_1d": "2010-02-02",
            }
        ]
    )
    downloads = pd.DataFrame(
        [
            {
                "href": "https://home.treasury.gov/news/press-releases/tg524",
                "local_path": str(local_html),
                "doc_type": "quarterly_refunding_press_release",
            }
        ]
    )

    source_map = build_financing_release_source_map(events, downloads, text_dir)

    assert list(source_map["quarter"]) == ["2010Q1"]
    assert source_map.loc[0, "announced_borrowing_bn"] == 392.0
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


def test_enrich_capture_with_financing_release_map_strips_stale_seed_derived_note() -> None:
    capture = _empty_capture_df()
    capture.loc[0] = {
        "quarter": "2010Q1",
        "qra_release_date": "2010-02-03",
        "market_pricing_marker_minus_1d": "2010-02-02",
        "net_bill_issuance_bn": "49.943",
        "notes": (
            "Historical archive scaffold row. "
            "Official borrowing estimate matched to February 3, 2010 Treasury borrowing-estimate release. "
            "net_bill_issuance_bn remains seed-derived pending official quarter capture."
        ),
        "qa_status": "manual_official_capture",
    }
    release_map = pd.DataFrame(
        [
            {
                "quarter": "2010Q1",
                "qra_release_date": "2010-02-03",
                "official_release_date_phrase": "February 3, 2010",
                "source_url": "https://home.treasury.gov/news/press-releases/tg524",
                "source_doc_local": "/tmp/tg524.html",
                "source_doc_type": "quarterly_refunding_press_release",
                "announced_borrowing_bn": 392.0,
                "match_status": "matched",
            }
        ]
    )

    enriched = enrich_capture_with_financing_release_map(capture, release_map)

    row = enriched.iloc[0]
    assert row["notes"].count("Official borrowing estimate matched to February 3, 2010") == 1
    assert "seed-derived pending official quarter capture" not in row["notes"]


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


def test_build_refunding_statement_manifest_infers_year_from_table_headers(tmp_path) -> None:
    archive = tmp_path / "official-remarks-on-quarterly-refunding-by-calendar-year_demo.html"
    archive.write_text(
        """
        <html><body>
        <table aria-label="Official Remarks on Quarterly Refunding by Calendar Year">
          <tr><th id="2025">2025</th></tr>
          <tr>
            <th><a href="/news/press-releases/sb0305">4th Quarter</a></th>
            <th><a href="/news/press-releases/sb0212">3rd Quarter</a></th>
          </tr>
        </table>
        </body></html>
        """,
        encoding="utf-8",
    )
    capture = _empty_capture_df()
    capture.loc[0] = {"quarter": "2025Q3"}
    capture.loc[1] = {"quarter": "2025Q4"}

    manifest = build_refunding_statement_manifest(capture, [archive])

    assert manifest["quarter"].tolist() == ["2025Q3", "2025Q4"]
    assert manifest.loc[0, "href"] == "https://home.treasury.gov/news/press-releases/sb0212"
    assert manifest.loc[1, "href"] == "https://home.treasury.gov/news/press-releases/sb0305"


def test_build_refunding_statement_manifest_maps_repo_forward_quarters() -> None:
    archive_paths = sorted(
        (RAW_DIR / "qra" / "files").glob("official-remarks-on-quarterly-refunding-by-calendar-year_*.html")
    )
    if not archive_paths:
        pytest.skip("official remarks archive HTML files are not available in this environment")

    capture = _empty_capture_df()
    capture.loc[0] = {"quarter": "2024Q4"}
    capture.loc[1] = {"quarter": "2025Q1"}
    capture.loc[2] = {"quarter": "2025Q2"}
    capture.loc[3] = {"quarter": "2025Q3"}
    capture.loc[4] = {"quarter": "2025Q4"}

    manifest = build_refunding_statement_manifest(capture, archive_paths)
    by_quarter = manifest.set_index("quarter")

    assert by_quarter.loc["2024Q4", "href"] == "https://home.treasury.gov/news/press-releases/jy2697"
    assert by_quarter.loc["2025Q1", "href"] == "https://home.treasury.gov/news/press-releases/sb0010"
    assert by_quarter.loc["2025Q2", "href"] == "https://home.treasury.gov/news/press-releases/sb0120"
    assert by_quarter.loc["2025Q3", "href"] == "https://home.treasury.gov/news/press-releases/sb0212"
    assert by_quarter.loc["2025Q4", "href"] == "https://home.treasury.gov/news/press-releases/sb0305"


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


def test_build_refunding_statement_source_map_normalizes_project_root_local_paths(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.setattr(qra_capture_mod, "PROJECT_ROOT", tmp_path)
    html_dir = tmp_path / "data/raw/qra/files"
    html_dir.mkdir(parents=True)
    html_path = html_dir / "jy2315_demo.html"
    html_path.write_text(
        """
        <html><body>
        NOMINAL COUPON AND FRN FINANCING
        Treasury does not anticipate changing coupon sizes.
        BILL ISSUANCE
        Treasury plans to address borrowing needs through regular bill auction sizes.
        BUYBACKS
        Treasury continues to study a potential buyback program.
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

    assert source_map.loc[0, "refunding_statement_source_doc_local"] == "data/raw/qra/files/jy2315_demo.html"
    assert source_map.loc[0, "source_doc_local"] == "data/raw/qra/files/jy2315_demo.html"


def test_build_refunding_statement_source_map_falls_back_to_projected_financing_section(tmp_path) -> None:
    html_path = tmp_path / "jy1238_demo.html"
    html_path.write_text(
        """
        <html><body>
        PROJECTED FINANCING NEEDS AND ISSUANCE PLANS
        Treasury believes that current issuance sizes leave it well-positioned to address a range of potential borrowing needs, and as such, does not anticipate making any changes to nominal coupon and FRN new issue or reopening auction sizes over the upcoming February 2023 – April 2023 quarter.
        The table below presents the anticipated auction sizes in billions of dollars for the February 2023 – April 2023 quarter:
        Nov-22 42 40 43 35 35 15 21 22
        Dec-22 42 40 43 35 32 12 18 22
        Jan-23 42 40 43 35 32 12 18 24
        Feb-23 42 40 43 35 35 15 21 22
        Mar-23 42 40 43 35 32 12 18 22
        Apr-23 42 40 43 35 32 12 18 24
        Treasury plans to address any seasonal or unexpected variations in borrowing needs over the next quarter through changes in regular bill auction sizes and/or CMBs.
        TIPS FINANCING
        Over the February 2023 – April 2023 quarter, Treasury intends to maintain TIPS auction sizes.
        DEBT LIMIT
        Until the debt limit is suspended or increased, debt limit-related constraints will lead to greater-than-normal variability in benchmark bill issuance and significant usage of CMBs.
        BUYBACK OUTREACH
        Treasury continues to study a potential buyback program.
        </body></html>
        """,
        encoding="utf-8",
    )
    downloads = pd.DataFrame(
        [
            {
                "quarter": "2023Q1",
                "href": "https://home.treasury.gov/news/press-releases/jy1238",
                "local_path": str(html_path),
                "doc_type": "official_quarterly_refunding_statement",
            }
        ]
    )

    source_map = build_refunding_statement_source_map(downloads)

    row = source_map.iloc[0]
    assert "Feb-23: 2Y=42" in row["guidance_nominal_coupons"]
    assert "Apr-23: 2Y=42" in row["guidance_nominal_coupons"]
    assert "Nov-22: 2Y=42" not in row["guidance_nominal_coupons"]
    assert "Monthly FRN schedule: Feb-23: 2Y FRN=22" in row["guidance_frns"]
    assert "regular bill auction sizes" in row["bill_guidance"]
    assert "benchmark bill issuance" in row["bill_guidance"]


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


def test_build_quarter_net_issuance_treats_cmb_only_missing_maturity_as_complete() -> None:
    auctions = pd.DataFrame(
        [
            {
                "issue_date": "2023-03-01",
                "security_type": "Bill",
                "cash_management_bill_cmb": "No",
                "offering_amt": 100_000_000_000,
                "est_pub_held_mat_by_type_amt": 90_000_000_000,
            },
            {
                "issue_date": "2023-03-31",
                "security_type": "Bill",
                "cash_management_bill_cmb": "Yes",
                "offering_amt": 50_000_000_000,
                "est_pub_held_mat_by_type_amt": pd.NA,
            },
        ]
    )

    reconstruction = build_quarter_net_issuance_from_auctions(auctions, quarters=["2023Q1"])
    bill_row = reconstruction.loc[reconstruction["bucket"] == "bill_like"].iloc[0]

    assert bill_row["issue_dates_missing_maturing_estimate"] == 1
    assert bill_row["issue_dates_cmb_only_missing_maturity"] == 1
    assert bill_row["reconstruction_status"] == "complete"


def test_enrich_capture_with_auction_reconstruction_promotes_rows() -> None:
    capture = _empty_capture_df()
    capture.loc[0] = {
        "quarter": "2024Q2",
        "qra_release_date": "2024-01-31",
        "market_pricing_marker_minus_1d": "2024-01-30",
        "total_financing_need_bn": "202",
        "source_url": "https://home.treasury.gov/news/press-releases/jy2054",
        "source_doc_local": "/tmp/jy2315.html|/tmp/jy2054.html",
        "source_doc_type": "official_quarterly_refunding_statement|quarterly_refunding_press_release",
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


def test_enrich_capture_with_auction_reconstruction_does_not_promote_without_statement_provenance() -> None:
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
            }
        ]
    )

    enriched = enrich_capture_with_auction_reconstruction(capture, reconstruction)
    row = enriched.iloc[0]

    assert row["qa_status"] == "semi_automated_capture"
    assert "official_auction_reconstruction" in row["source_doc_type"]


def test_enrich_capture_with_auction_reconstruction_strips_seed_provenance_on_promotion() -> None:
    capture = _empty_capture_df()
    capture.loc[0] = {
        "quarter": "2024Q1",
        "qra_release_date": "2023-11-01",
        "market_pricing_marker_minus_1d": "2023-10-31",
        "total_financing_need_bn": "816",
        "source_url": "https://home.treasury.gov/news/press-releases/jy1662",
        "source_doc_local": "/tmp/jy2062.html|/tmp/jy1662.html|data/manual/qra_event_seed.csv|data/manual/quarterly_refunding_seed.csv",
        "source_doc_type": "official_quarterly_refunding_statement|quarterly_refunding_press_release|seed_csv",
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
        "source_doc_type": "official_auction_reconstruction|official_quarterly_refunding_statement|quarterly_refunding_press_release",
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
