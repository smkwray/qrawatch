from __future__ import annotations

from pathlib import Path
import zipfile

import pandas as pd

from ati_shadow_policy import investor_allotments as ia


def test_build_manifest_keeps_only_investor_allotments_files():
    links = pd.DataFrame(
        [
            {"source_page": ia.URL, "text": "Treasury Investor Data", "href": "https://home.treasury.gov/policy-issues/financing-the-government/treasury-investor-data"},
            {
                "source_page": ia.URL,
                "text": "Coupon Auctions – Data from October 2009-present",
                "href": "https://home.treasury.gov/system/files/276/March-9-2026-IC-Coupons.xls",
            },
            {
                "source_page": ia.URL,
                "text": "Bill Auctions – Data from October 2009-present",
                "href": "https://home.treasury.gov/system/files/276/March-9-2026-IC-Bills.xls",
            },
            {
                "source_page": ia.URL,
                "text": "Investor Class Category Descriptions",
                "href": "https://home.treasury.gov/system/files/276/investor-class-descriptions.pdf",
            },
            {"source_page": ia.URL, "text": "Treasury homepage", "href": "https://home.treasury.gov/"},
        ]
    )

    manifest = ia.build_manifest(links)

    assert list(manifest["href"]) == [
        "https://home.treasury.gov/system/files/276/March-9-2026-IC-Coupons.xls",
        "https://home.treasury.gov/system/files/276/March-9-2026-IC-Bills.xls",
        "https://home.treasury.gov/system/files/276/investor-class-descriptions.pdf",
    ]
    assert list(manifest["resource_family"]) == [
        "coupon_auctions",
        "bill_auctions",
        "category_descriptions",
    ]
    assert manifest["source_href_sha1"].str.len().eq(40).all()


def test_build_inventory_merges_download_metadata_and_defaults_to_manifest_only(tmp_path: Path):
    manifest = pd.DataFrame(
        [
            {
                "source_page": ia.URL,
                "start_url": ia.URL,
                "text": "Coupon Auctions – Data from October 2009-present",
                "href": "https://home.treasury.gov/system/files/276/March-9-2026-IC-Coupons.xls",
                "href_extension": ".xls",
                "resource_family": "coupon_auctions",
                "source_href_sha1": "a" * 40,
            },
            {
                "source_page": ia.URL,
                "start_url": ia.URL,
                "text": "Bill Auctions – Data from October 2009-present",
                "href": "https://home.treasury.gov/system/files/276/March-9-2026-IC-Bills.xls",
                "href_extension": ".xls",
                "resource_family": "bill_auctions",
                "source_href_sha1": "b" * 40,
            },
        ]
    )
    coupon_path = tmp_path / "March-9-2026-IC-Coupons.xls"
    coupon_path.write_bytes(b"fake xls bytes")
    downloads = pd.DataFrame(
        [
            {
                "href": manifest.loc[0, "href"],
                "local_path": str(coupon_path),
                "local_filename": coupon_path.name,
                "download_status": "ok",
                "download_attempts": 1,
                "http_status": 200,
                "final_url": manifest.loc[0, "href"],
                "content_type": "application/vnd.ms-excel",
                "content_length": "14",
                "etag": "abc123",
                "last_modified": "Fri, 01 Jan 2021 00:00:00 GMT",
                "downloaded_at_utc": "2026-03-20T00:00:00+00:00",
                "bytes_written": 14,
                "skipped_existing": False,
                "error_type": None,
                "error_message": None,
            }
        ]
    )

    inventory = ia.build_inventory(manifest, downloads)

    assert list(inventory["inventory_status"]) == ["downloaded", "manifest_only"]
    assert bool(inventory.loc[0, "file_available"])
    assert bool(inventory.loc[0, "local_file_exists"])
    assert not bool(inventory.loc[1, "file_available"])
    assert not bool(inventory.loc[1, "local_file_exists"])
    assert inventory.loc[0, "download_status"] == "ok"
    assert pd.isna(inventory.loc[1, "download_status"])
    assert inventory.loc[0, "source_href_sha1"] == "a" * 40


def test_build_inventory_detects_excel_binary_by_content(tmp_path: Path):
    manifest = pd.DataFrame(
        [
            {
                "source_page": ia.URL,
                "start_url": ia.URL,
                "text": "Bill Auctions – Data from October 2009-present",
                "href": "https://home.treasury.gov/system/files/276/March-9-2026-IC-Bills.xls",
                "href_extension": ".xls",
                "resource_family": "bill_auctions",
                "source_href_sha1": "c" * 40,
            }
        ]
    )
    binary_path = tmp_path / "March-9-2026-IC-Bills_wrong.html"
    binary_path.write_bytes(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"\x00" * 64)
    downloads = pd.DataFrame(
        [
            {
                "href": manifest.loc[0, "href"],
                "local_path": str(binary_path),
                "local_filename": binary_path.name,
                "local_extension": ".html",
                "download_status": "ok",
                "download_attempts": 1,
                "http_status": 200,
                "final_url": manifest.loc[0, "href"],
                "content_type": "text/html",
                "content_length": "72",
                "etag": None,
                "last_modified": None,
                "downloaded_at_utc": "2026-03-20T00:00:00+00:00",
                "bytes_written": 72,
                "skipped_existing": False,
                "error_type": None,
                "error_message": None,
            }
        ]
    )

    inventory = ia.build_inventory(manifest, downloads)

    assert inventory.loc[0, "detected_content_kind"] == "excel_xls"
    assert inventory.loc[0, "detected_extension"] == ".xls"
    assert bool(inventory.loc[0, "parser_ready"])


def test_build_normalized_panel_parses_content_sniffed_workbook(tmp_path: Path, monkeypatch):
    disguised = tmp_path / "allotments_payload.html"
    with zipfile.ZipFile(disguised, mode="w") as zf:
        zf.writestr("[Content_Types].xml", "<Types/>")
        zf.writestr("xl/workbook.xml", "<workbook/>")

    rows = [
        [None] * 14,
        ["Auction Allotments By Investor Class", None, None, None, None, None, None, None, None, None, None, None, None, None],
        ["[In billions of dollars.]", None, None, None, None, None, None, None, None, None, None, None, None, None],
        [
            "Issue date",
            "Security term",
            "Auction high rate %",
            "Cusip",
            "Maturity date",
            "Total issue",
            "(SOMA) Federal Reserve banks",
            "Depository institutions",
            "Individuals",
            "Dealers and brokers",
            "Pension and Retirement funds and Ins. Co.",
            "Investment funds",
            "Foreign and international",
            "Other",
        ],
        [None] * 14,
        ["2024-01-04", "13-Week Bill", 5.1, "912XYZ", "2024-04-04", 90.0, 2.0, 0.0, 1.0, 40.0, 0.5, 30.0, 16.0, 0.5],
        ["2024-01-11", "26-Week Bill", 5.2, "912ABC", "2024-07-11", 92.0, 2.5, 0.1, 1.2, 38.0, 0.6, 31.0, 18.0, 0.6],
    ]
    raw_table = pd.DataFrame(rows)
    monkeypatch.setattr(ia, "_read_investor_workbook", lambda path, detected_kind: raw_table.copy())

    manifest = pd.DataFrame(
        [
            {
                "source_page": ia.URL,
                "start_url": ia.URL,
                "text": "Bill Auctions – Data from October 2009-present",
                "href": "https://home.treasury.gov/system/files/276/March-9-2026-IC-Bills.xls",
                "href_extension": ".xls",
                "resource_family": "bill_auctions",
                "source_href_sha1": "d" * 40,
            }
        ]
    )
    downloads = pd.DataFrame(
        [
            {
                "href": manifest.loc[0, "href"],
                "local_path": str(disguised),
                "local_filename": disguised.name,
                "local_extension": ".html",
                "download_status": "ok",
            }
        ]
    )
    inventory = ia.build_inventory(manifest, downloads)

    panel = ia.build_normalized_panel(inventory)

    required = {
        "auction_date",
        "security_family",
        "investor_class",
        "measure",
        "value",
        "units",
        "provenance",
    }
    assert required.issubset(panel.columns)
    assert len(panel) > 0
    assert set(panel["security_family"]) == {"bill"}
    assert set(panel["measure"]) == {"allotment_amount"}
    assert set(panel["units"]) == {"USD billions"}
    assert "total_issue" in set(panel["investor_class"])
    assert panel["provenance"].str.contains("detected_content_kind=excel_xlsx").all()
