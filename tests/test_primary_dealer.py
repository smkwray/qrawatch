from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ati_shadow_policy import primary_dealer as pdm


def test_build_manifest_classifies_primary_dealer_downloads():
    links = pd.DataFrame(
        [
            {
                "source_page": pdm.URLS[0],
                "start_url": pdm.URLS[0],
                "text": "CSV",
                "href": "https://markets.newyorkfed.org/api/pd/latest/SBN2024.csv",
            },
            {
                "source_page": pdm.URLS[0],
                "start_url": pdm.URLS[0],
                "text": "XML",
                "href": "https://markets.newyorkfed.org/api/pd/list/timeseries.xml",
            },
            {
                "source_page": pdm.URLS[0],
                "start_url": pdm.URLS[0],
                "text": "JSON",
                "href": "https://markets.newyorkfed.org/api/marketshare/qtrly/latest.json",
            },
            {
                "source_page": pdm.URLS[0],
                "start_url": pdm.URLS[0],
                "text": "Primary Dealer Statistics",
                "href": "https://www.newyorkfed.org/markets/counterparties/primary-dealers-statistics",
            },
        ]
    )

    manifest = pdm.build_manifest(links)

    assert list(manifest["dataset_type"]) == [
        "latest_series_snapshot",
        "quarterly_marketshare",
        "series_catalog",
    ]
    assert list(manifest["file_family"]) == [
        "dealer_statistics_snapshot",
        "dealer_marketshare_quarterly",
        "dealer_timeseries_catalog",
    ]
    assert list(manifest["series_break"]) == ["SBN2024", "quarterly", "timeseries"]
    assert list(manifest["source_url"]) == [pdm.URLS[0], pdm.URLS[0], pdm.URLS[0]]


def test_build_inventory_merges_downloads_and_infers_shape(tmp_path: Path):
    csv_path = tmp_path / "SBN2024.csv"
    csv_path.write_text('"As Of Date","Time Series","Value (millions)"\n"2026-03-11","PDWOTIPSC","6308"\n', encoding="utf-8")
    json_path = tmp_path / "timeseries.json"
    json_path.write_text('{"pd":{"timeseries":[{"keyid":"A"},{"keyid":"B"}]}}', encoding="utf-8")

    manifest = pd.DataFrame(
        [
            {
                "source_page": pdm.URLS[0],
                "start_url": pdm.URLS[0],
                "source_url": pdm.URLS[0],
                "text": "CSV",
                "href": "https://markets.newyorkfed.org/api/pd/latest/SBN2024.csv",
                "href_extension": ".csv",
                "dataset_type": "latest_series_snapshot",
                "file_family": "dealer_statistics_snapshot",
                "series_break": "SBN2024",
                "release_scope": "weekly",
                "source_href_sha1": "a" * 40,
            },
            {
                "source_page": pdm.URLS[0],
                "start_url": pdm.URLS[0],
                "source_url": pdm.URLS[0],
                "text": "JSON",
                "href": "https://markets.newyorkfed.org/api/pd/list/timeseries.json",
                "href_extension": ".json",
                "dataset_type": "series_catalog",
                "file_family": "dealer_timeseries_catalog",
                "series_break": "timeseries",
                "release_scope": "catalog",
                "source_href_sha1": "b" * 40,
            },
        ]
    )
    downloads = pd.DataFrame(
        [
            {
                "href": manifest.loc[0, "href"],
                "local_path": str(csv_path),
                "local_filename": csv_path.name,
                "local_extension": ".csv",
                "download_status": "ok",
                "download_attempts": 1,
                "http_status": 200,
                "final_url": manifest.loc[0, "href"],
                "content_type": "text/csv",
                "content_length": "80",
                "etag": "abc123",
                "last_modified": "Fri, 01 Jan 2021 00:00:00 GMT",
                "downloaded_at_utc": "2026-03-20T00:00:00+00:00",
                "bytes_written": 80,
                "skipped_existing": False,
                "error_type": None,
                "error_message": None,
            },
            {
                "href": manifest.loc[1, "href"],
                "local_path": str(json_path),
                "local_filename": json_path.name,
                "local_extension": ".json",
                "download_status": "skipped_existing",
                "download_attempts": 0,
                "http_status": None,
                "final_url": manifest.loc[1, "href"],
                "content_type": "application/json",
                "content_length": "48",
                "etag": None,
                "last_modified": None,
                "downloaded_at_utc": None,
                "bytes_written": 0,
                "skipped_existing": True,
                "error_type": None,
                "error_message": None,
            },
        ]
    )

    inventory = pdm.build_inventory(manifest, downloads)

    assert list(inventory["inventory_status"]) == ["downloaded", "downloaded"]
    assert bool(inventory.loc[0, "file_available"])
    assert bool(inventory.loc[1, "file_available"])
    assert inventory.loc[0, "artifact_rows"] == 1
    assert inventory.loc[0, "artifact_columns"] == 3
    assert inventory.loc[1, "artifact_rows"] == 2
    assert inventory.loc[1, "artifact_detail"] == "root.pd.timeseries"


def test_download_manifest_uses_url_extension(monkeypatch, tmp_path: Path):
    calls = []

    def fake_download(url: str, path: Path, skip_existing: bool = True):
        calls.append((url, path, skip_existing))
        path.write_text("downloaded", encoding="utf-8")
        return {
            "download_status": "ok",
            "download_attempts": 1,
            "http_status": 200,
            "final_url": url,
            "content_type": "text/plain",
            "content_length": "10",
            "etag": None,
            "last_modified": None,
            "downloaded_at_utc": "2026-03-20T00:00:00+00:00",
            "bytes_written": 10,
            "skipped_existing": False,
            "error_type": None,
            "error_message": None,
        }

    monkeypatch.setattr(pdm, "download_binary_with_metadata", fake_download)
    manifest = pd.DataFrame(
        [
            {
                "source_page": pdm.URLS[0],
                "start_url": pdm.URLS[0],
                "source_url": pdm.URLS[0],
                "text": "CSV",
                "href": "https://markets.newyorkfed.org/api/pd/latest/SBN2024.csv",
            }
        ]
    )

    downloads = pdm.download_manifest(manifest, tmp_path)

    assert len(calls) == 1
    assert calls[0][1].suffix == ".csv"
    assert downloads.loc[0, "local_extension"] == ".csv"
    assert downloads.loc[0, "download_status"] == "ok"


def test_build_panel_prefers_csv_canonical_snapshot_and_enriches_labels(tmp_path: Path):
    snapshot_csv = tmp_path / "SBN2024_canonical.csv"
    snapshot_csv.write_text(
        '"As Of Date","Time Series","Value (millions)"\n'
        '"2026-03-11","PDWOTIPSC","6308"\n'
        '"2026-03-11","PDABTOTC","2182"\n',
        encoding="utf-8",
    )
    snapshot_json = tmp_path / "SBN2024_duplicate.json"
    snapshot_json.write_text(
        json.dumps(
            {
                "pd": {
                    "timeseries": [
                        {"asofdate": "2026-03-11", "keyid": "PDWOTIPSC", "value": "9999"},
                        {"asofdate": "2026-03-11", "keyid": "PDABTOTC", "value": "8888"},
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    catalog_csv = tmp_path / "timeseries_catalog.csv"
    catalog_csv.write_text(
        "Key Id,Label,Series Break\n"
        "PDWOTIPSC,Weighted Ongoing Treasury Position,SBN2024\n"
        "PDABTOTC,Agency and GSE ABS Position,SBN2024\n",
        encoding="utf-8",
    )

    manifest = pd.DataFrame(
        [
            {
                "href": "https://example.com/SBN2024.csv",
                "text": "CSV",
                "dataset_type": "latest_series_snapshot",
                "source_page": pdm.URLS[0],
            },
            {
                "href": "https://example.com/SBN2024.json",
                "text": "JSON",
                "dataset_type": "latest_series_snapshot",
                "source_page": pdm.URLS[0],
            },
            {
                "href": "https://example.com/timeseries.csv",
                "text": "CSV",
                "dataset_type": "series_catalog",
                "source_page": pdm.URLS[0],
            },
        ]
    )
    downloads = pd.DataFrame(
        [
            {
                "href": "https://example.com/SBN2024.csv",
                "local_path": str(snapshot_csv),
                "local_filename": snapshot_csv.name,
                "local_extension": ".csv",
            },
            {
                "href": "https://example.com/SBN2024.json",
                "local_path": str(snapshot_json),
                "local_filename": snapshot_json.name,
                "local_extension": ".json",
            },
            {
                "href": "https://example.com/timeseries.csv",
                "local_path": str(catalog_csv),
                "local_filename": catalog_csv.name,
                "local_extension": ".csv",
            },
        ]
    )

    panel = pdm.build_panel(manifest, downloads)

    assert len(panel) == 2
    assert set(panel["source_file"]) == {snapshot_csv.name}
    assert set(panel["source_quality"]) == {"csv_canonical"}
    assert set(panel["series_label"]) == {
        "Weighted Ongoing Treasury Position",
        "Agency and GSE ABS Position",
    }
    assert set(panel["units"]) == {"USD millions"}
    assert set(panel["frequency"]) == {"weekly"}
    assert set(panel["value"]) == {6308.0, 2182.0}


def test_build_panel_flattens_repaired_quarterly_marketshare_json(tmp_path: Path):
    marketshare_json = tmp_path / "quarterly_marketshare.json"
    marketshare_json.write_text(
        '{"pd":{"marketshare":{"quarterly":{"releaseDate":"2026-01-08","title":"QUARTER IV 2025","interDealerBrokers":[{"securityType":"U.S. TREASURY SECURITIES (EXCLUDING TIPS)","security":"TREASURY BILLS","percentFirstQuintMktShare":"51.14","percentSecondQuintMktShare":"27.21","percentThirdQuintMktShare":"13.30","percentFourthQuintMktShare":"7.73","percentFifthQuintMktShare":"0.62","dailyAvgVolInMillions": * }],"others":[],"totals":[]}}}}',
        encoding="utf-8",
    )

    manifest = pd.DataFrame(
        [
            {
                "href": "https://example.com/quarterly_marketshare.json",
                "text": "JSON",
                "dataset_type": "quarterly_marketshare",
                "source_page": pdm.URLS[0],
            }
        ]
    )
    downloads = pd.DataFrame(
        [
            {
                "href": "https://example.com/quarterly_marketshare.json",
                "local_path": str(marketshare_json),
                "local_filename": marketshare_json.name,
                "local_extension": ".json",
            }
        ]
    )

    panel = pdm.build_panel(manifest, downloads)

    assert len(panel) == 5
    assert set(panel["source_quality"]) == {"json_repaired"}
    assert set(panel["frequency"]) == {"quarterly"}
    assert set(panel["units"]) == {"percent"}
    assert set(panel["metric_id"]) == {
        "percent_first_quint_mkt_share",
        "percent_second_quint_mkt_share",
        "percent_third_quint_mkt_share",
        "percent_fourth_quint_mkt_share",
        "percent_fifth_quint_mkt_share",
    }
    assert panel["provenance_summary"].str.contains("json_repaired").all()
