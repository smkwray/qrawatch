from __future__ import annotations

from pathlib import Path
from zipfile import ZipFile

import pandas as pd

from ati_shadow_policy import sec_nmfp


def test_build_manifest_classifies_sec_nmfp_resources():
    links = pd.DataFrame(
        [
            {
                "source_page": sec_nmfp.URL,
                "start_url": sec_nmfp.URL,
                "text": "2010q1_nmfp.zip",
                "href": "https://www.sec.gov/files/dera/data/form-n-mfp-data-sets/2010q1_nmfp.zip",
            },
            {
                "source_page": sec_nmfp.URL,
                "start_url": sec_nmfp.URL,
                "text": "20240101-20240131_nmfp.zip",
                "href": "https://www.sec.gov/files/dera/data/form-n-mfp-data-sets/20240101-20240131_nmfp.zip",
            },
            {
                "source_page": sec_nmfp.URL,
                "start_url": sec_nmfp.URL,
                "text": "Readme",
                "href": "https://www.sec.gov/files/dera/data/form-n-mfp-data-sets/readme.htm",
            },
        ]
    )

    manifest = sec_nmfp.build_manifest(links)

    observed = manifest.set_index("href")[
        ["resource_type", "period_family", "dataset_version", "period_label"]
    ].to_dict(orient="index")

    assert observed["https://www.sec.gov/files/dera/data/form-n-mfp-data-sets/readme.htm"] == {
        "resource_type": "readme",
        "period_family": "documentation",
        "dataset_version": "not_applicable",
        "period_label": "documentation",
    }
    assert observed["https://www.sec.gov/files/dera/data/form-n-mfp-data-sets/2010q1_nmfp.zip"] == {
        "resource_type": "dataset_archive",
        "period_family": "quarterly",
        "dataset_version": "legacy_nmfp",
        "period_label": "2010Q1",
    }
    assert observed["https://www.sec.gov/files/dera/data/form-n-mfp-data-sets/20240101-20240131_nmfp.zip"] == {
        "resource_type": "dataset_archive",
        "period_family": "monthly",
        "dataset_version": "nmfp3",
        "period_label": "2024-01-01_to_2024-01-31",
    }


def test_build_inventory_marks_downloaded_files(tmp_path: Path):
    zip_path = tmp_path / "20240101-20240131_nmfp_abc123.zip"
    zip_path.write_text("placeholder zip bytes", encoding="utf-8")

    manifest = pd.DataFrame(
        [
            {
                "source_page": sec_nmfp.URL,
                "start_url": sec_nmfp.URL,
                "text": "20240101-20240131_nmfp.zip",
                "href": "https://www.sec.gov/files/dera/data/form-n-mfp-data-sets/20240101-20240131_nmfp.zip",
                "href_extension": ".zip",
                "resource_type": "dataset_archive",
                "archive_type": "zip",
                "readme_or_archive_type": "archive_zip",
                "period_family": "monthly",
                "dataset_version": "nmfp3",
                "dataset_version_detail": "monthly_zip_filename_pattern",
                "period_start": "2024-01-01",
                "period_end": "2024-01-31",
                "period_label": "2024-01-01_to_2024-01-31",
                "dataset_id": "20240101-20240131_nmfp",
                "source_href_sha1": "a" * 40,
            }
        ]
    )
    downloads = pd.DataFrame(
        [
            {
                "href": manifest.loc[0, "href"],
                "local_path": str(zip_path),
                "local_filename": zip_path.name,
                "local_extension": ".zip",
                "download_status": "ok",
                "download_attempts": 1,
                "http_status": 200,
                "final_url": manifest.loc[0, "href"],
                "content_type": "application/zip",
                "content_length": "21",
                "etag": None,
                "last_modified": None,
                "downloaded_at_utc": "2026-03-20T00:00:00+00:00",
                "bytes_written": 21,
                "skipped_existing": False,
                "error_type": None,
                "error_message": None,
            }
        ]
    )

    inventory = sec_nmfp.build_inventory(manifest, downloads)

    assert list(inventory["inventory_status"]) == ["downloaded"]
    assert bool(inventory.loc[0, "file_available"])
    assert bool(inventory.loc[0, "local_file_exists"])
    assert inventory.loc[0, "period_family"] == "monthly"
    assert inventory.loc[0, "dataset_version"] == "nmfp3"


def test_build_summary_panel_reports_archive_counts_and_field_availability(tmp_path: Path):
    archive_path = tmp_path / "20240101-20240131_nmfp_test.zip"
    with ZipFile(archive_path, "w") as archive:
        archive.writestr(
            "NMFP_SUBMISSION.tsv",
            "\t".join(["ACCESSION_NUMBER", "FILER_CIK", "SERIESID"]) + "\n"
            + "\n".join(
                [
                    "\t".join(["acc-1", "cik-1", "series-a"]),
                    "\t".join(["acc-2", "cik-1", "series-b"]),
                    "\t".join(["acc-2", "cik-2", "series-b"]),
                ]
            ),
        )
        archive.writestr(
            "NMFP_SCHPORTFOLIOSECURITIES.tsv",
            "\t".join(
                [
                    "ACCESSION_NUMBER",
                    "INVESTMENTCATEGORY",
                    "REPURCHASEAGREEMENTOPENFLAG",
                    "INCLUDINGVALUEOFANYSPONSORSUPP",
                    "EXCLUDINGVALUEOFANYSPONSORSUPP",
                    "PERCENTAGEOFMONEYMARKETFUNDNET",
                    "CUSIP_NUMBER",
                ]
            )
            + "\n"
            + "\t".join(["acc-1", "Treasury Debt", "N", "100", "100", "0.2", "912796XYZ"]),
        )
        archive.writestr("NMFP_metadata.json", "{}")
        archive.writestr("NMFP_readme.htm", "<html></html>")

    inventory = pd.DataFrame(
        [
            {
                "resource_type": "dataset_archive",
                "dataset_id": "20240101-20240131_nmfp",
                "dataset_version": "nmfp3",
                "period_family": "monthly",
                "period_label": "2024-01-01_to_2024-01-31",
                "period_start": "2024-01-01",
                "period_end": "2024-01-31",
                "href": "https://www.sec.gov/files/dera/data/form-n-mfp-data-sets/20240101-20240131_nmfp.zip",
                "download_status": "ok",
                "local_file_exists": True,
                "local_filename": archive_path.name,
                "local_path": str(archive_path),
            }
        ]
    )

    panel = sec_nmfp.build_summary_panel(inventory)
    assert not panel.empty

    report_count = panel.loc[
        (panel["summary_type"] == "archive_counts") & (panel["measure"] == "report_count"),
        "value",
    ].iloc[0]
    series_count = panel.loc[
        (panel["summary_type"] == "archive_counts") & (panel["measure"] == "series_count"),
        "value",
    ].iloc[0]
    repo_flag = panel.loc[
        (panel["summary_type"] == "field_availability")
        & (panel["measure"] == "field_repo_open_flag_present"),
        "value",
    ].iloc[0]
    coverage = panel.loc[
        (panel["summary_type"] == "coverage_by_version_period")
        & (panel["measure"] == "archive_count")
        & (panel["dataset_version"] == "nmfp3"),
        "value",
    ].iloc[0]

    assert int(report_count) == 3
    assert int(series_count) == 2
    assert int(repo_flag) == 1
    assert int(coverage) == 1
    assert set(panel.loc[panel["summary_type"] == "archive_status", "parse_status"]) == {"parsed"}


def test_build_summary_panel_marks_missing_local_archive():
    inventory = pd.DataFrame(
        [
            {
                "resource_type": "dataset_archive",
                "dataset_id": "20240301-20240331_nmfp",
                "dataset_version": "nmfp3",
                "period_family": "monthly",
                "period_label": "2024-03-01_to_2024-03-31",
                "period_start": "2024-03-01",
                "period_end": "2024-03-31",
                "href": "https://www.sec.gov/files/dera/data/form-n-mfp-data-sets/20240301-20240331_nmfp.zip",
                "download_status": "error",
                "local_file_exists": False,
                "local_filename": "20240301-20240331_nmfp_missing.zip",
                "local_path": "/tmp/does-not-exist.zip",
            }
        ]
    )

    panel = sec_nmfp.build_summary_panel(inventory)
    status_rows = panel.loc[panel["summary_type"] == "archive_status"].copy()
    local_flag = status_rows.loc[status_rows["measure"] == "local_file_available", "value"].iloc[0]
    parse_statuses = set(status_rows["parse_status"])
    parse_errors = set(status_rows["parse_error"].astype(str))
    parse_error_count = panel.loc[
        (panel["summary_type"] == "coverage_by_version_period")
        & (panel["measure"] == "parse_error_count"),
        "value",
    ].iloc[0]

    assert int(local_flag) == 0
    assert parse_statuses == {"zip_unreadable"}
    assert any("local_file_missing" in message for message in parse_errors)
    assert int(parse_error_count) == 1
