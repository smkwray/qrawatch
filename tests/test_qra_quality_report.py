from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import pandas as pd

SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "14_qra_quality_report.py"
)

spec = importlib.util.spec_from_file_location("qra_quality_report", SCRIPT_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("Unable to load qra_quality_report script module")
qra_quality_report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qra_quality_report)


def test_report_handles_missing_files_without_exception(tmp_path: Path) -> None:
    result = qra_quality_report.build_qra_quality_report(
        downloads_path=tmp_path / "missing_downloads.csv",
        capture_path=tmp_path / "missing_capture.csv",
    )

    assert result["downloads"]["status"].startswith("missing")
    assert result["downloads"]["rows"] == 0
    assert result["official_capture"]["status"].startswith("missing")
    assert result["official_capture"]["rows"] == 0


def test_download_summary_counts_pdf_and_html(tmp_path: Path) -> None:
    downloads_path = tmp_path / "downloads.csv"
    pd.DataFrame(
        [
            {
                "quarter": "2026Q1",
                "doc_type": "quarterly_refunding_press_release",
                "source_family": "quarterly_refunding_press_release",
                "quality_tier": "official_release_page",
                "preferred_for_download": True,
                "local_extension": ".pdf",
                "local_filename": "a",
                "local_path": "a.pdf",
                "content_type": "application/pdf",
            },
            {
                "quarter": "2026Q1",
                "doc_type": "official_quarterly_refunding_statement",
                "source_family": "official_quarterly_refunding_statement_archive",
                "quality_tier": "official_release_page",
                "preferred_for_download": True,
                "local_extension": ".HTML",
                "local_filename": "b",
                "local_path": "b.htm",
                "content_type": "text/html",
            },
            {
                "quarter": "",
                "doc_type": "tbac_attachment",
                "source_family": "",
                "quality_tier": "",
                "preferred_for_download": False,
                "local_extension": "",
                "content_type": "application/pdf",
                "local_path": "c.pdf",
            },
            {
                "quarter": "",
                "doc_type": "",
                "source_family": "",
                "quality_tier": "",
                "preferred_for_download": False,
                "local_extension": "",
                "content_type": "text/plain",
                "local_path": "d.bin",
                "final_url": "https://example.com/doc.txt",
            },
        ]
    ).to_csv(downloads_path, index=False)

    capture_path = tmp_path / "official_quarterly_refunding_capture_template.csv"
    pd.DataFrame(
        {"quarter": ["2026Q1"], "qra_release_date": ["2026-03-20"]}
    ).to_csv(capture_path, index=False)

    result = qra_quality_report.build_qra_quality_report(downloads_path=downloads_path, capture_path=capture_path)

    downloads = result["downloads"]
    assert downloads["status"] == "ok"
    assert downloads["rows"] == 4
    assert downloads["pdf"] == 2
    assert downloads["html"] == 1
    assert downloads["other"] == 1
    assert downloads["extensions"][".pdf"] == 2
    assert downloads["source_family_counts"]["quarterly_refunding_press_release"] == 1
    assert downloads["official_source_family_rows"] == 2
    assert downloads["preferred_for_download_rows"] == 2
    assert downloads["provenance_missing_counts"]["quarter"]["missing"] == 2
    assert downloads["provenance_missing_counts"]["source_family"]["missing"] == 2


def test_capture_summary_required_fields_and_distributions(tmp_path: Path) -> None:
    capture_path = tmp_path / "official_quarterly_refunding_capture_template.csv"
    pd.DataFrame(
        [
            {
                "quarter": "2026Q1",
                "qra_release_date": "2026-03-20",
                "market_pricing_marker_minus_1d": "2026-03-19",
                "total_financing_need_bn": 1.0,
                "net_bill_issuance_bn": 1.0,
                "source_url": "https://example.com/doc1",
                "source_doc_local": "doc1.pdf",
                "source_doc_type": "pdf",
                "qa_status": "manual_official_capture",
                "gross_coupon_schedule_bn": 0,
                "net_coupon_issuance_bn": 0,
            },
            {
                "quarter": "",
                "qra_release_date": "2026-06-20",
                "market_pricing_marker_minus_1d": "",
                "total_financing_need_bn": 2.0,
                "net_bill_issuance_bn": "",
                "source_url": "",
                "source_doc_local": "doc2.html",
                "source_doc_type": "",
                "qa_status": "manual_official_capture",
                "gross_coupon_schedule_bn": 1.0,
                "net_coupon_issuance_bn": 1.0,
            },
            {
                "quarter": "2026Q2",
                "qra_release_date": "",
                "market_pricing_marker_minus_1d": "2026-06-29",
                "total_financing_need_bn": 3.0,
                "net_bill_issuance_bn": 1.0,
                "source_url": "https://example.com/doc3",
                "source_doc_local": "",
                "source_doc_type": "html",
                "qa_status": "",
                "gross_coupon_schedule_bn": 1.0,
                "net_coupon_issuance_bn": 1.0,
            },
        ]
    ).to_csv(capture_path, index=False)

    downloads_path = tmp_path / "downloads.csv"
    pd.DataFrame(columns=["local_extension"]).to_csv(downloads_path, index=False)

    result = qra_quality_report.build_qra_quality_report(downloads_path=downloads_path, capture_path=capture_path)

    capture = result["official_capture"]
    assert capture["status"] == "ok"
    assert capture["rows"] == 3
    assert capture["quarter_coverage"]["filled"] == 2
    assert capture["quarter_coverage"]["pct"] == pytest.approx(66.6667, rel=1e-4)
    assert capture["required_fields"]["quarter"]["missing"] == 1
    assert capture["required_fields"]["qra_release_date"]["missing"] == 1
    assert capture["required_fields"]["source_doc_type"]["missing"] == 1
    assert capture["document_type_counts"]["pdf"] == 1
    assert capture["document_type_counts"]["missing"] == 1
    assert capture["qa_status_counts"]["manual_official_capture"] == 2
    assert capture["qa_status_counts"]["missing"] == 1


def test_capture_summary_handles_known_episode_quarters(tmp_path: Path) -> None:
    capture_path = tmp_path / "official_quarterly_refunding_capture_template.csv"
    pd.DataFrame(
        [
            {
                "quarter": "2022Q3",
                "qra_release_date": "2022-05-04",
                "market_pricing_marker_minus_1d": "2022-05-03",
                "total_financing_need_bn": 137.088,
                "net_bill_issuance_bn": 42.0,
                "source_url": "https://example.com/doc1",
                "source_doc_local": "doc1.pdf",
                "source_doc_type": "pdf",
                "qa_status": "manual_official_capture",
                "gross_coupon_schedule_bn": 1.0,
                "net_coupon_issuance_bn": 1.0,
            },
            {
                "quarter": "2022Q4",
                "qra_release_date": "2022-08-03",
                "market_pricing_marker_minus_1d": "2022-08-02",
                "total_financing_need_bn": 73.149,
                "net_bill_issuance_bn": 20.0,
                "source_url": "https://example.com/doc2",
                "source_doc_local": "doc2.pdf",
                "source_doc_type": "pdf",
                "qa_status": "manual_official_capture",
                "gross_coupon_schedule_bn": 1.0,
                "net_coupon_issuance_bn": 1.0,
            },
        ]
    ).to_csv(capture_path, index=False)

    result = qra_quality_report.build_qra_quality_report(
        downloads_path=tmp_path / "downloads.csv",
        capture_path=capture_path,
    )

    official_capture = result["official_capture"]
    assert official_capture["status"] == "ok"
    assert official_capture["rows"] == 2
    assert official_capture["quarter_coverage"]["filled"] == 2


def test_build_report_contract_check_detects_missing_official_provenance(tmp_path: Path) -> None:
    downloads_path = tmp_path / "downloads.csv"
    pd.DataFrame(columns=["local_extension"]).to_csv(downloads_path, index=False)

    capture_path = tmp_path / "official_quarterly_refunding_capture.csv"
    pd.DataFrame(
        [
            {
                "quarter": "2026Q1",
                "qra_release_date": "2026-03-20",
                "market_pricing_marker_minus_1d": "2026-03-19",
                "total_financing_need_bn": 1.0,
                "net_bill_issuance_bn": 1.0,
                "source_url": "",
                "source_doc_local": "q1.pdf",
                "source_doc_type": "quarterly_refunding_press_release",
                "qa_status": "manual_official_capture",
            },
        ]
    ).to_csv(capture_path, index=False)

    result = qra_quality_report.build_qra_quality_report(
        downloads_path=downloads_path,
        capture_path=capture_path,
        check_contract=True,
    )

    assert result["contract_violations"]
    assert "source_url" in result["contract_violations"][0]


def test_main_exits_nonzero_when_contract_check_fails(tmp_path: Path) -> None:
    downloads_path = tmp_path / "downloads.csv"
    pd.DataFrame(columns=["local_extension"]).to_csv(downloads_path, index=False)

    capture_path = tmp_path / "official_quarterly_refunding_capture.csv"
    pd.DataFrame(
        [
            {
                "quarter": "2026Q1",
                "qra_release_date": "2026-03-20",
                "market_pricing_marker_minus_1d": "2026-03-19",
                "total_financing_need_bn": 1.0,
                "net_bill_issuance_bn": 1.0,
                "source_url": "",
                "source_doc_local": "q1.pdf",
                "source_doc_type": "quarterly_refunding_press_release",
                "qa_status": "manual_official_capture",
            }
        ]
    ).to_csv(capture_path, index=False)

    exit_code = qra_quality_report.main(
        [
            "--downloads",
            str(downloads_path),
            "--official-capture",
            str(capture_path),
            "--fail-on-contract",
        ]
    )

    assert exit_code == 1


def test_main_exits_zero_when_contract_check_passes(tmp_path: Path) -> None:
    downloads_path = tmp_path / "downloads.csv"
    pd.DataFrame(columns=["local_extension"]).to_csv(downloads_path, index=False)

    capture_path = tmp_path / "official_quarterly_refunding_capture.csv"
    pd.DataFrame(
        [
            {
                "quarter": "2026Q1",
                "qra_release_date": "2026-03-20",
                "market_pricing_marker_minus_1d": "2026-03-19",
                "total_financing_need_bn": 1.0,
                "net_bill_issuance_bn": 1.0,
                "source_url": "https://example.com/doc1",
                "source_doc_local": "doc1.pdf",
                "source_doc_type": "quarterly_refunding_press_release",
                "qa_status": "manual_official_capture",
            }
        ]
    ).to_csv(capture_path, index=False)

    exit_code = qra_quality_report.main(
        [
            "--downloads",
            str(downloads_path),
            "--official-capture",
            str(capture_path),
            "--fail-on-contract",
        ]
    )

    assert exit_code == 0


def test_main_fails_when_official_capture_file_is_missing_and_contract_check_is_enabled(tmp_path: Path) -> None:
    downloads_path = tmp_path / "downloads.csv"
    pd.DataFrame(columns=["local_extension"]).to_csv(downloads_path, index=False)

    exit_code = qra_quality_report.main(
        [
            "--downloads",
            str(downloads_path),
            "--official-capture",
            str(tmp_path / "missing_capture.csv"),
            "--fail-on-contract",
        ]
    )

    assert exit_code == 1
