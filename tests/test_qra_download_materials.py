from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path

import pandas as pd

SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "03_download_qra_materials.py"
)

spec = importlib.util.spec_from_file_location("qra_download_materials", SCRIPT_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("Unable to load 03_download_qra_materials.py")
qra_download_materials = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qra_download_materials)


def test_enrich_benchmark_candidate_metadata_classifies_candidates() -> None:
    rows = pd.DataFrame(
        [
            {
                "start_url": "https://home.treasury.gov/.../primary-dealer-auction-size-survey",
                "source_page": "https://home.treasury.gov/.../primary-dealer-auction-size-survey",
                "text": "2nd Quarter",
                "href": "https://home.treasury.gov/system/files/221/auction_survey_20250421.pdf",
                "doc_type": "other",
                "source_family": "",
                "quarter": "2025Q2",
                "is_downloadable_extension": True,
                "preferred_for_download": False,
            },
            {
                "start_url": "https://home.treasury.gov/.../tbac-recommended-financing-tables-by-calendar-year",
                "source_page": "https://home.treasury.gov/.../tbac-recommended-financing-tables-by-calendar-year",
                "text": "TBAC Recommended Financing Tables by Calendar Year",
                "href": "https://home.treasury.gov/.../tbac-recommended-financing-tables-by-calendar-year",
                "doc_type": "other",
                "source_family": "",
                "quarter": "",
                "is_downloadable_extension": False,
                "preferred_for_download": True,
            },
            {
                "start_url": "https://home.treasury.gov/.../quarterly-refunding-financing-estimates-by-calendar-year",
                "source_page": "https://home.treasury.gov/.../quarterly-refunding-financing-estimates-by-calendar-year",
                "text": "Q1 2024 financing tables",
                "href": "https://home.treasury.gov/system/files/221/q1-2024-financing.xlsx",
                "doc_type": "financing_estimates_attachment",
                "source_family": "",
                "quarter": "2024Q1",
                "is_downloadable_extension": True,
                "preferred_for_download": False,
            },
            {
                "start_url": "https://home.treasury.gov/.../office-of-economic-policy-statements-to-tbac",
                "source_page": "https://home.treasury.gov/.../office-of-economic-policy-statements-to-tbac",
                "text": "Charge to TBAC (pdf)",
                "href": "https://home.treasury.gov/system/files/221/tbac-charge.pdf",
                "doc_type": "oep_statement_attachment",
                "source_family": "",
                "quarter": "2025Q2",
                "is_downloadable_extension": True,
                "preferred_for_download": False,
            },
            {
                "start_url": "https://home.treasury.gov/.../official-remarks-on-quarterly-refunding-by-calendar-year",
                "source_page": "https://home.treasury.gov/.../official-remarks-on-quarterly-refunding-by-calendar-year",
                "text": "Official Remarks on Quarterly Refunding by Calendar Year",
                "href": "https://home.treasury.gov/.../official-remarks-on-quarterly-refunding-by-calendar-year",
                "doc_type": "other",
                "source_family": "official_refunding_statement_archive",
                "quarter": "",
                "is_downloadable_extension": False,
                "preferred_for_download": False,
            },
            {
                "start_url": "https://home.treasury.gov/.../quarterly-refunding",
                "source_page": "https://home.treasury.gov/.../quarterly-refunding",
                "text": "Quarterly Refunding home",
                "href": "https://home.treasury.gov/.../quarterly-refunding",
                "doc_type": "other",
                "source_family": "",
                "quarter": "",
                "is_downloadable_extension": False,
                "preferred_for_download": True,
            },
        ]
    )

    enriched = qra_download_materials._enrich_benchmark_candidate_metadata(rows)
    by_href = enriched.set_index("href")

    survey = by_href.loc["https://home.treasury.gov/system/files/221/auction_survey_20250421.pdf"]
    assert survey["benchmark_context_slug"] == "primary_dealer_auction_size_survey_archive"
    assert survey["benchmark_candidate_family"] == "primary_dealer_auction_size_survey"
    assert survey["benchmark_candidate_kind"] == "primary_dealer_survey_file"
    assert survey["benchmark_candidate_priority"] == 0
    assert survey["benchmark_candidate_key"] == "primary_dealer_auction_size_survey__2025q2"
    assert bool(survey["benchmark_download_candidate"]) is True

    tbac_archive = by_href.loc["https://home.treasury.gov/.../tbac-recommended-financing-tables-by-calendar-year"]
    assert tbac_archive["benchmark_candidate_family"] == "tbac_recommended_financing_tables"
    assert tbac_archive["benchmark_candidate_kind"] == "tbac_financing_tables_archive_page"
    assert tbac_archive["benchmark_candidate_priority"] == 1
    assert bool(tbac_archive["benchmark_download_candidate"]) is True

    financing_file = by_href.loc["https://home.treasury.gov/system/files/221/q1-2024-financing.xlsx"]
    assert financing_file["benchmark_candidate_family"] == "financing_estimates_attachment"
    assert financing_file["benchmark_candidate_kind"] == "financing_estimates_file"
    assert financing_file["benchmark_candidate_priority"] == 2
    assert financing_file["benchmark_candidate_key"] == "financing_estimates_attachment__2024q1"

    oep_file = by_href.loc["https://home.treasury.gov/system/files/221/tbac-charge.pdf"]
    assert oep_file["benchmark_candidate_family"] == "oep_statement_to_tbac"
    assert oep_file["benchmark_candidate_kind"] == "oep_statement_file"
    assert oep_file["benchmark_candidate_priority"] == 3

    official_archive = by_href.loc[
        "https://home.treasury.gov/.../official-remarks-on-quarterly-refunding-by-calendar-year"
    ]
    assert official_archive["benchmark_candidate_family"] == "official_refunding_statement_archive"
    assert official_archive["benchmark_candidate_kind"] == "official_refunding_statement_page"
    assert official_archive["benchmark_candidate_priority"] == 4

    non_benchmark = by_href.loc["https://home.treasury.gov/.../quarterly-refunding"]
    assert non_benchmark["benchmark_candidate_family"] == "legacy_qra_archive"
    assert non_benchmark["benchmark_candidate_kind"] == "legacy_archive_page"
    assert non_benchmark["benchmark_candidate_priority"] == 5
    assert bool(non_benchmark["benchmark_download_candidate"]) is True


def test_benchmark_download_candidate_requires_real_candidate() -> None:
    rows = pd.DataFrame(
        [
            {
                "start_url": "https://home.treasury.gov/.../primary-dealer-auction-size-survey",
                "source_page": "https://home.treasury.gov/.../primary-dealer-auction-size-survey",
                "text": "2nd Quarter",
                "href": "https://home.treasury.gov/system/files/221/auction_survey_20250421.pdf",
                "doc_type": "other",
                "is_downloadable_extension": True,
                "preferred_for_download": False,
            },
            {
                "start_url": "https://home.treasury.gov/.../quarterly-refunding",
                "source_page": "https://home.treasury.gov/.../quarterly-refunding",
                "text": "General quarterly refunding archive",
                "href": "https://home.treasury.gov/.../quarterly-refunding",
                "doc_type": "other",
                "is_downloadable_extension": False,
                "preferred_for_download": True,
            },
            {
                "start_url": "https://example.com/not-qra",
                "source_page": "https://example.com/not-qra",
                "text": "Non benchmark row",
                "href": "https://example.com/not-qra",
                "doc_type": "other",
                "is_downloadable_extension": True,
                "preferred_for_download": True,
            },
        ]
    )

    enriched = qra_download_materials._enrich_benchmark_candidate_metadata(rows)
    flags = enriched.set_index("href")["benchmark_download_candidate"].to_dict()

    assert bool(flags["https://home.treasury.gov/system/files/221/auction_survey_20250421.pdf"]) is True
    assert bool(flags["https://home.treasury.gov/.../quarterly-refunding"]) is True
    assert bool(flags["https://example.com/not-qra"]) is False


def test_main_download_manifest_sorts_benchmark_candidates_before_others(
    monkeypatch, tmp_path: Path
) -> None:
    captured: dict[str, pd.DataFrame] = {}

    manifest = pd.DataFrame(
        [
            {
                "text": "legacy preferred",
                "href": "https://example.com/z-legacy",
                "start_url": "https://home.treasury.gov/.../quarterly-refunding",
                "source_page": "https://home.treasury.gov/.../quarterly-refunding",
                "preferred_for_download": True,
                "benchmark_download_candidate": False,
                "benchmark_candidate_priority": 9,
                "download_priority": 4,
                "relevance_score": 20,
            },
            {
                "text": "TBAC recommended financing tables",
                "href": "https://example.com/b-tbac",
                "start_url": "https://home.treasury.gov/.../tbac-recommended-financing-tables-by-calendar-year",
                "source_page": "https://home.treasury.gov/.../tbac-recommended-financing-tables-by-calendar-year",
                "preferred_for_download": True,
                "benchmark_download_candidate": True,
                "benchmark_candidate_priority": 1,
                "download_priority": 3,
                "relevance_score": 15,
            },
            {
                "text": "Primary dealer auction size survey",
                "href": "https://example.com/a-survey",
                "start_url": "https://home.treasury.gov/.../primary-dealer-auction-size-survey",
                "source_page": "https://home.treasury.gov/.../primary-dealer-auction-size-survey",
                "preferred_for_download": True,
                "benchmark_download_candidate": True,
                "benchmark_candidate_priority": 0,
                "download_priority": 9,
                "relevance_score": 10,
            },
        ]
    )

    monkeypatch.setattr(qra_download_materials, "START_URLS", ["https://example.com/start"])
    monkeypatch.setattr(
        qra_download_materials,
        "parse_args",
        lambda: argparse.Namespace(download_files=True, limit=None),
    )
    monkeypatch.setattr(
        qra_download_materials,
        "extract_links",
        lambda url: pd.DataFrame([{"source_page": url, "text": "dummy", "href": f"{url}/doc"}]),
    )
    monkeypatch.setattr(qra_download_materials, "build_qra_manifest", lambda links: manifest.copy())
    monkeypatch.setattr(qra_download_materials, "ensure_project_dirs", lambda: None)
    monkeypatch.setattr(qra_download_materials, "write_df", lambda df, path: None)
    monkeypatch.setattr(qra_download_materials, "RAW_DIR", tmp_path)

    def _capture_download_manifest(df: pd.DataFrame, output_dir: Path, limit: int | None = None) -> pd.DataFrame:
        captured["df"] = df.copy()
        return pd.DataFrame()

    monkeypatch.setattr(qra_download_materials, "download_link_manifest", _capture_download_manifest)

    qra_download_materials.main()

    assert captured["df"]["href"].tolist() == [
        "https://example.com/a-survey",
        "https://example.com/b-tbac",
        "https://example.com/z-legacy",
    ]


def test_main_download_manifest_preserves_stable_order_on_ties(
    monkeypatch, tmp_path: Path
) -> None:
    captured: dict[str, pd.DataFrame] = {}

    manifest = pd.DataFrame(
        [
            {
                "text": "first",
                "preferred_for_download": True,
                "benchmark_download_candidate": True,
                "benchmark_candidate_priority": 1,
                "download_priority": 3,
                "relevance_score": 15,
            },
            {
                "text": "second",
                "preferred_for_download": True,
                "benchmark_download_candidate": True,
                "benchmark_candidate_priority": 1,
                "download_priority": 3,
                "relevance_score": 15,
            },
        ]
    )

    monkeypatch.setattr(qra_download_materials, "START_URLS", ["https://example.com/start"])
    monkeypatch.setattr(
        qra_download_materials,
        "parse_args",
        lambda: argparse.Namespace(download_files=True, limit=None),
    )
    monkeypatch.setattr(
        qra_download_materials,
        "extract_links",
        lambda url: pd.DataFrame([{"source_page": url, "text": "dummy", "href": f"{url}/doc"}]),
    )
    monkeypatch.setattr(qra_download_materials, "build_qra_manifest", lambda links: manifest.copy())
    monkeypatch.setattr(qra_download_materials, "ensure_project_dirs", lambda: None)
    monkeypatch.setattr(qra_download_materials, "write_df", lambda df, path: None)
    monkeypatch.setattr(qra_download_materials, "RAW_DIR", tmp_path)

    def _capture_download_manifest(df: pd.DataFrame, output_dir: Path, limit: int | None = None) -> pd.DataFrame:
        captured["df"] = df.copy()
        return pd.DataFrame()

    monkeypatch.setattr(qra_download_materials, "download_link_manifest", _capture_download_manifest)

    qra_download_materials.main()

    assert captured["df"]["text"].tolist() == ["first", "second"]
