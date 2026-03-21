from __future__ import annotations

import json
from pathlib import Path
import importlib.util
import sys

import pandas as pd

SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "21_validate_backend.py"
)

spec = importlib.util.spec_from_file_location("validate_backend", SCRIPT_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("Unable to load validate_backend script module")
validate_backend_script = importlib.util.module_from_spec(spec)
sys.modules["validate_backend"] = validate_backend_script
spec.loader.exec_module(validate_backend_script)


def _build_publish_artifacts(
    path: Path, schemas: dict[str, list[str]] | None = None
) -> None:
    if schemas is None:
        schemas = validate_backend_script.REQUIRED_PUBLISH_SCHEMAS
    path.mkdir(parents=True, exist_ok=True)
    artifacts: list[str] = []

    for csv_name, required_columns in schemas.items():
        csv_path = path / csv_name
        if csv_name == "dataset_status.csv":
            frame = pd.DataFrame(
                [
                    {
                        "dataset": "extension_investor_allotments",
                        "readiness_tier": "summary_ready",
                        "source_quality": "summary_ready",
                        "headline_ready": True,
                        "fallback_only": False,
                    },
                    {
                        "dataset": "extension_primary_dealer",
                        "readiness_tier": "summary_ready",
                        "source_quality": "summary_ready",
                        "headline_ready": True,
                        "fallback_only": False,
                    },
                    {
                        "dataset": "extension_sec_nmfp",
                        "readiness_tier": "summary_ready",
                        "source_quality": "summary_ready",
                        "headline_ready": True,
                        "fallback_only": False,
                    },
                ]
            )
        elif csv_name == "extension_status.csv":
            frame = pd.DataFrame(
                [
                    {
                        "extension": "investor_allotments",
                        "backend_status": "summary_ready",
                        "raw_dir_exists": True,
                        "manifest_exists": True,
                        "downloads_exists": True,
                        "processed_exists": True,
                        "readiness_tier": "summary_ready",
                        "headline_ready": True,
                    },
                    {
                        "extension": "primary_dealer",
                        "backend_status": "summary_ready",
                        "raw_dir_exists": True,
                        "manifest_exists": True,
                        "downloads_exists": True,
                        "processed_exists": True,
                        "readiness_tier": "summary_ready",
                        "headline_ready": True,
                    },
                    {
                        "extension": "sec_nmfp",
                        "backend_status": "summary_ready",
                        "raw_dir_exists": True,
                        "manifest_exists": True,
                        "downloads_exists": True,
                        "processed_exists": True,
                        "readiness_tier": "summary_ready",
                        "headline_ready": True,
                    },
                    {
                        "extension": "tic",
                        "backend_status": "not_started",
                        "raw_dir_exists": False,
                        "manifest_exists": False,
                        "downloads_exists": False,
                        "processed_exists": False,
                        "readiness_tier": "not_started",
                        "headline_ready": False,
                    },
                ]
            )
        else:
            frame = pd.DataFrame([{column: "" for column in required_columns}])
        frame.to_csv(csv_path, index=False)
        json_path = csv_path.with_suffix(".json")
        json_path.write_text(
            json.dumps({"title": csv_path.stem, "rows": []}, indent=2),
            encoding="utf-8",
        )
        md_path = csv_path.with_suffix(".md")
        md_path.write_text(f"# {csv_path.stem}\n", encoding="utf-8")
        artifacts.extend([csv_name, json_path.name, md_path.name])

    index_payload = {
        "title": "qra watch publish artifacts",
        "artifact_count": len(artifacts) + 1,
        "artifacts": artifacts + ["index.json"],
    }
    (path / "index.json").write_text(json.dumps(index_payload), encoding="utf-8")


def _build_valid_official_capture_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    capture = pd.DataFrame(
        [
            {
                "quarter": "2026Q1",
                "qra_release_date": "2026-04-01",
                "market_pricing_marker_minus_1d": "2026-03-31",
                "total_financing_need_bn": 816.0,
                "net_bill_issuance_bn": 468.0,
                "source_url": "https://home.treasury.gov/news/press-releases/jy2062",
                "source_doc_local": "/tmp/doc.pdf",
                "source_doc_type": "official_quarterly_refunding_statement",
                "qa_status": "manual_official_capture",
            }
        ]
    )
    capture_path = tmp_path / "official_quarterly_refunding_capture.csv"
    capture.to_csv(capture_path, index=False)

    official_ati_path = tmp_path / "ati_index_official_capture.csv"
    pd.DataFrame(
        [
            {
                "quarter": "2026Q1",
                "ati_baseline_bn": 468.0,
                "qa_status": "manual_official_capture",
                "source_doc_local": "/tmp/doc.pdf",
                "source_doc_type": "official_quarterly_refunding_statement",
            }
        ]
    ).to_csv(official_ati_path, index=False)

    return capture_path, official_ati_path, tmp_path / "official_quarterly_refunding_capture_template.csv"


def test_validate_backend_flags_no_exact_official_rows(tmp_path: Path) -> None:
    capture = pd.DataFrame(
        [
            {
                "quarter": "2026Q1",
                "qra_release_date": "2026-01-31",
                "market_pricing_marker_minus_1d": "2026-01-30",
                "qa_status": "semi_automated_capture",
                "source_doc_local": "/tmp/demo.html",
                "source_doc_type": "html",
            }
        ]
    )
    capture_path = tmp_path / "official_quarterly_refunding_capture.csv"
    capture.to_csv(capture_path, index=False)
    _build_publish_artifacts(tmp_path / "publish")
    official_ati_path = tmp_path / "ati_index_official_capture.csv"
    pd.DataFrame().to_csv(official_ati_path, index=False)

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=tmp_path / "official_quarterly_refunding_capture_template.csv",
        publish_dir=tmp_path / "publish",
    )

    assert "official_capture_no_exact_rows" in result.errors
    assert result.summaries["official_capture"]["exact_official_rows"] == 0


def test_validate_backend_detects_seed_contamination_in_official_capture(tmp_path: Path) -> None:
    capture = pd.DataFrame(
        [
            {
                "quarter": "2026Q2",
                "qra_release_date": "2026-04-01",
                "market_pricing_marker_minus_1d": "2026-03-31",
                "total_financing_need_bn": 1.0,
                "net_bill_issuance_bn": 1.0,
                "source_url": "https://example.com",
                "source_doc_local": "seed_csv",
                "source_doc_type": "seed_csv|official_quarterly_refunding_statement",
                "qa_status": "manual_official_capture",
            }
        ]
    )
    capture_path = tmp_path / "official_quarterly_refunding_capture.csv"
    capture.to_csv(capture_path, index=False)
    _build_publish_artifacts(tmp_path / "publish")
    official_ati = pd.DataFrame(
        [
            {
                "quarter": "2026Q2",
                "ati_baseline_bn": 1.1,
                "qa_status": "manual_official_capture",
                "source_doc_local": "official.csv",
                "source_doc_type": "official",
            }
        ]
    )
    official_ati_path = tmp_path / "ati_index_official_capture.csv"
    official_ati.to_csv(official_ati_path, index=False)

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=tmp_path / "official_quarterly_refunding_capture_template.csv",
        publish_dir=tmp_path / "publish",
    )

    assert "official_capture_seed_contamination:2026Q2" in result.errors


def test_validate_backend_detects_seed_contamination_in_official_ati(tmp_path: Path) -> None:
    capture = pd.DataFrame(
        [
            {
                "quarter": "2026Q3",
                "qra_release_date": "2026-07-01",
                "market_pricing_marker_minus_1d": "2026-06-30",
                "total_financing_need_bn": 10.0,
                "net_bill_issuance_bn": 5.0,
                "source_url": "https://example.com",
                "source_doc_local": "/tmp/doc.pdf",
                "source_doc_type": "official_quarterly_refunding_statement",
                "qa_status": "manual_official_capture",
            }
        ]
    )
    capture_path = tmp_path / "official_quarterly_refunding_capture.csv"
    capture.to_csv(capture_path, index=False)
    _build_publish_artifacts(tmp_path / "publish")

    official_ati = pd.DataFrame(
        [
            {
                "quarter": "2026Q3",
                "ati_baseline_bn": 10.0,
                "qa_status": "manual_official_capture",
                "source_doc_local": "seed_csv|/tmp/official.csv",
                "source_doc_type": "official|seed_csv",
            }
        ]
    )
    official_ati_path = tmp_path / "ati_index_official_capture.csv"
    official_ati.to_csv(official_ati_path, index=False)

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=tmp_path / "official_quarterly_refunding_capture_template.csv",
        publish_dir=tmp_path / "publish",
    )

    assert "official_ati_seed_contamination:2026Q3" in result.errors


def test_validate_backend_reports_missing_publish_artifact(tmp_path: Path) -> None:
    capture = pd.DataFrame(
        [
            {
                "quarter": "2026Q4",
                "qra_release_date": "2026-10-01",
                "market_pricing_marker_minus_1d": "2026-09-30",
                "total_financing_need_bn": 1.0,
                "net_bill_issuance_bn": 1.0,
                "source_url": "https://example.com",
                "source_doc_local": "/tmp/doc.pdf",
                "source_doc_type": "official_quarterly_refunding_statement",
                "qa_status": "manual_official_capture",
            }
        ]
    )
    capture_path = tmp_path / "official_quarterly_refunding_capture.csv"
    capture.to_csv(capture_path, index=False)
    publish_path = tmp_path / "publish"
    _build_publish_artifacts(publish_path)
    artifact_to_remove = publish_path / "plumbing_regression_summary.csv"
    artifact_to_remove.unlink()
    official_ati = pd.DataFrame(
        [
            {
                "quarter": "2026Q4",
                "ati_baseline_bn": 10.0,
                "qa_status": "manual_official_capture",
                "source_doc_local": "/tmp/official.csv",
                "source_doc_type": "official_quarterly_refunding_statement",
            }
        ]
    )
    official_ati_path = tmp_path / "ati_index_official_capture.csv"
    official_ati.to_csv(official_ati_path, index=False)

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=tmp_path / "official_quarterly_refunding_capture_template.csv",
        publish_dir=publish_path,
    )

    assert any(
        message.startswith("publish_artifact_missing:plumbing_regression_summary.csv")
        for message in result.errors
    )


def test_validate_backend_flags_missing_extension_summary_artifacts(tmp_path: Path, monkeypatch) -> None:
    capture_path, official_ati_path, manual_capture_path = _build_valid_official_capture_inputs(
        tmp_path
    )
    publish_path = tmp_path / "publish"
    _build_publish_artifacts(path=publish_path)
    (publish_path / "investor_allotments_summary.json").unlink()
    (publish_path / "primary_dealer_summary.md").unlink()

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=manual_capture_path,
        publish_dir=publish_path,
    )

    assert any(
        message.startswith(
            "publish_artifact_missing:investor_allotments_summary.json"
        )
        for message in result.errors
    )
    assert any(
        message.startswith(
            "publish_artifact_missing:primary_dealer_summary.md"
        )
        for message in result.errors
    )


def test_validate_backend_accepts_extension_summary_artifacts_when_present(tmp_path: Path, monkeypatch) -> None:
    capture_path, official_ati_path, manual_capture_path = _build_valid_official_capture_inputs(
        tmp_path
    )
    publish_path = tmp_path / "publish"
    _build_publish_artifacts(path=publish_path)

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=manual_capture_path,
        publish_dir=publish_path,
    )

    assert not any(
        message.startswith("publish_artifact_missing:investor_allotments_summary")
        for message in result.errors
    )
    assert not any(
        message.startswith("publish_artifact_missing:primary_dealer_summary")
        for message in result.errors
    )
