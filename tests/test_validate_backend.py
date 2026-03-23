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
    path: Path,
    schemas: dict[str, list[str]] | None = None,
    *,
    official_quarter: str = "2026Q1",
    official_financing_need_bn: float = 816.0,
    official_net_bills_bn: float = 468.0,
    official_ati_baseline_bn: float = 468.0,
) -> None:
    if schemas is None:
        schemas = validate_backend_script.REQUIRED_PUBLISH_SCHEMAS
    path.mkdir(parents=True, exist_ok=True)
    artifacts: list[str] = []

    for csv_name, required_columns in schemas.items():
        csv_path = path / csv_name
        if csv_name == "ati_quarter_table.csv":
            frame = pd.DataFrame(
                [
                    {
                        "quarter": official_quarter,
                        "financing_need_bn": official_financing_need_bn,
                        "net_bills_bn": official_net_bills_bn,
                        "ati_baseline_bn": official_ati_baseline_bn,
                        "source_quality": "exact_official_numeric",
                        "public_role": "headline",
                    }
                ]
            )
        elif csv_name == "ati_seed_forecast_table.csv":
            frame = pd.DataFrame(
                [
                    {
                        "quarter": "2026Q2",
                        "financing_need_bn": 900.0,
                        "ati_baseline_bn": 100.0,
                        "seed_source": "user_note_table_3_forecast",
                        "seed_quality": "note_forecast",
                        "non_headline_reason": "seed_forecast_without_official_capture",
                        "public_role": "supporting",
                    }
                ]
            )
        elif csv_name == "dataset_status.csv":
            frame = pd.DataFrame(
                [
                    {
                        "dataset": "extension_investor_allotments",
                        "readiness_tier": "summary_ready",
                        "source_quality": "summary_ready",
                        "headline_ready": False,
                        "fallback_only": False,
                        "public_role": "supporting",
                    },
                    {
                        "dataset": "extension_primary_dealer",
                        "readiness_tier": "summary_ready",
                        "source_quality": "summary_ready",
                        "headline_ready": False,
                        "fallback_only": False,
                        "public_role": "supporting",
                    },
                    {
                        "dataset": "extension_sec_nmfp",
                        "readiness_tier": "summary_ready",
                        "source_quality": "summary_ready",
                        "headline_ready": False,
                        "fallback_only": False,
                        "public_role": "supporting",
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
                        "headline_ready": False,
                        "public_role": "supporting",
                    },
                    {
                        "extension": "primary_dealer",
                        "backend_status": "summary_ready",
                        "raw_dir_exists": True,
                        "manifest_exists": True,
                        "downloads_exists": True,
                        "processed_exists": True,
                        "readiness_tier": "summary_ready",
                        "headline_ready": False,
                        "public_role": "supporting",
                    },
                    {
                        "extension": "sec_nmfp",
                        "backend_status": "summary_ready",
                        "raw_dir_exists": True,
                        "manifest_exists": True,
                        "downloads_exists": True,
                        "processed_exists": True,
                        "readiness_tier": "summary_ready",
                        "headline_ready": False,
                        "public_role": "supporting",
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
                        "public_role": "supporting",
                    },
                ]
            )
        elif csv_name == "series_metadata_catalog.csv":
            frame = pd.DataFrame(
                [
                    {
                        "dataset": "investor_allotments",
                        "series_id": "investor_allotments_summary",
                        "frequency": "auction-event",
                        "value_units": "reported units",
                        "source_quality": "summary_ready",
                        "series_role": "supporting",
                        "public_role": "supporting",
                    },
                    {
                        "dataset": "primary_dealer",
                        "series_id": "primary_dealer_summary",
                        "frequency": "mixed",
                        "value_units": "reported units",
                        "source_quality": "summary_ready",
                        "series_role": "supporting",
                        "public_role": "supporting",
                    },
                    {
                        "dataset": "sec_nmfp",
                        "series_id": "sec_nmfp_summary",
                        "frequency": "mixed",
                        "value_units": "reported units",
                        "source_quality": "summary_ready",
                        "series_role": "supporting",
                        "public_role": "supporting",
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


def _add_publish_artifact(
    publish_path: Path,
    csv_name: str,
    frame: pd.DataFrame,
) -> None:
    csv_path = publish_path / csv_name
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(csv_path, index=False)
    (csv_path.with_suffix(".json")).write_text(
        json.dumps({"title": csv_path.stem, "rows": []}),
        encoding="utf-8",
    )
    (csv_path.with_suffix(".md")).write_text(
        f"# {csv_path.stem}\n",
        encoding="utf-8",
    )

    index_payload = json.loads((publish_path / "index.json").read_text(encoding="utf-8"))
    artifact_names = {csv_path.name, csv_path.with_suffix(".json").name, csv_path.with_suffix(".md").name}
    for item in artifact_names:
        if item not in index_payload["artifacts"]:
            index_payload["artifacts"].append(item)
    index_payload["artifact_count"] = len(index_payload["artifacts"])
    (publish_path / "index.json").write_text(json.dumps(index_payload), encoding="utf-8")


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
                "financing_need_bn": 816.0,
                "net_bills_bn": 468.0,
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
    _build_publish_artifacts(
        tmp_path / "publish",
        official_quarter="2026Q2",
        official_financing_need_bn=1.0,
        official_net_bills_bn=1.0,
        official_ati_baseline_bn=1.1,
    )
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
    _build_publish_artifacts(
        tmp_path / "publish",
        official_quarter="2026Q3",
        official_financing_need_bn=10.0,
        official_net_bills_bn=5.0,
        official_ati_baseline_bn=10.0,
    )
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
    _build_publish_artifacts(
        publish_path,
        official_quarter="2026Q4",
        official_financing_need_bn=1.0,
        official_net_bills_bn=1.0,
        official_ati_baseline_bn=10.0,
    )
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


def test_validate_backend_flags_absolute_paths_in_manual_capture_template(tmp_path: Path) -> None:
    capture_path, official_ati_path, manual_capture_path = _build_valid_official_capture_inputs(
        tmp_path
    )
    pd.DataFrame(
        [
            {
                "quarter": "2026Q1",
                "qra_release_date": "2026-04-01",
                "market_pricing_marker_minus_1d": "2026-03-31",
                "total_financing_need_bn": 816.0,
                "net_bill_issuance_bn": 468.0,
                "source_url": "https://example.com",
                "source_doc_local": "/tmp/doc.pdf|data/raw/qra/files/doc.html",
                "source_doc_type": "official_quarterly_refunding_statement|quarterly_refunding_press_release",
                "qa_status": "manual_official_capture",
            }
        ]
    ).to_csv(manual_capture_path, index=False)
    publish_path = tmp_path / "publish"
    _build_publish_artifacts(path=publish_path)

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=manual_capture_path,
        publish_dir=publish_path,
    )

    assert "official_capture_template_absolute_source_doc_local:2026Q1" in result.errors


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


def test_validate_backend_treats_qra_event_elasticity_as_optional(tmp_path: Path) -> None:
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
        message.startswith("publish_artifact_missing:qra_event_elasticity")
        for message in result.errors
    )


def test_validate_backend_validates_qra_event_elasticity_when_present(tmp_path: Path) -> None:
    capture_path, official_ati_path, manual_capture_path = _build_valid_official_capture_inputs(
        tmp_path
    )
    publish_path = tmp_path / "publish"
    _build_publish_artifacts(path=publish_path)
    pd.DataFrame(
        [
            {
                "quarter": "2024Q4",
                "event_id": "qra_2024_07",
                "event_date_type": "official_release_date",
                "headline_bucket": "tightening",
                "classification_review_status": "reviewed",
                "shock_review_status": "reviewed",
                "series": "DGS10",
                "window": "d3",
                "shock_bn": 25.0,
                "schedule_diff_10y_eq_bn": 25.0,
                "schedule_diff_dynamic_10y_eq_bn": 27.0,
                "schedule_diff_dv01_usd": 2400000.0,
                "shock_construction": "schedule_diff_primary",
                "elasticity_bp_per_100bn": 12.0,
                "usable_for_headline": True,
            }
        ]
    ).to_csv(publish_path / "qra_event_elasticity.csv", index=False)
    (publish_path / "qra_event_elasticity.json").write_text(
        json.dumps({"title": "qra_event_elasticity", "rows": []}),
        encoding="utf-8",
    )
    (publish_path / "qra_event_elasticity.md").write_text(
        "# qra_event_elasticity\n",
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "quarter": "2024Q4",
                "event_id": "qra_2024_07",
                "event_date_type": "official_release_date",
                "headline_bucket": "tightening",
                "classification_review_status": "reviewed",
                "shock_review_status": "reviewed",
                "series": "DGS10",
                "window": "d3",
                "shock_bn": 25.0,
                "schedule_diff_10y_eq_bn": 25.0,
                "schedule_diff_dynamic_10y_eq_bn": 27.0,
                "schedule_diff_dv01_usd": 2400000.0,
                "shock_construction": "schedule_diff_primary",
                "elasticity_bp_per_100bn": 12.0,
                "usable_for_headline": True,
            }
        ]
    ).to_csv(publish_path / "qra_event_elasticity_diagnostic.csv", index=False)
    (publish_path / "qra_event_elasticity_diagnostic.json").write_text(
        json.dumps({"title": "qra_event_elasticity_diagnostic", "rows": []}),
        encoding="utf-8",
    )
    (publish_path / "qra_event_elasticity_diagnostic.md").write_text(
        "# qra_event_elasticity_diagnostic\n",
        encoding="utf-8",
    )
    index_payload = json.loads((publish_path / "index.json").read_text(encoding="utf-8"))
    index_payload["artifacts"].extend(
        [
            "qra_event_elasticity.csv",
            "qra_event_elasticity.json",
            "qra_event_elasticity.md",
            "qra_event_elasticity_diagnostic.csv",
            "qra_event_elasticity_diagnostic.json",
            "qra_event_elasticity_diagnostic.md",
        ]
    )
    index_payload["artifact_count"] = len(index_payload["artifacts"])
    (publish_path / "index.json").write_text(json.dumps(index_payload), encoding="utf-8")

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=manual_capture_path,
        publish_dir=publish_path,
    )

    assert not any(
        message.startswith("publish_artifact_missing:qra_event_elasticity")
        for message in result.errors
    )


def test_validate_backend_detects_official_ati_publish_mismatch(tmp_path: Path) -> None:
    capture_path, official_ati_path, manual_capture_path = _build_valid_official_capture_inputs(tmp_path)
    publish_path = tmp_path / "publish"
    _build_publish_artifacts(path=publish_path)
    pd.DataFrame(
        [
            {
                "quarter": "2026Q1",
                "financing_need_bn": 999.0,
                "net_bills_bn": 468.0,
                "ati_baseline_bn": 400.0,
                "source_quality": "exact_official_numeric",
                "public_role": "headline",
            }
        ]
    ).to_csv(publish_path / "ati_quarter_table.csv", index=False)

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=manual_capture_path,
        publish_dir=publish_path,
    )

    assert "ati_quarter_table_value_mismatch:2026Q1:financing_need_bn" in result.errors
    assert "ati_quarter_table_value_mismatch:2026Q1:ati_baseline_bn" in result.errors


def test_validate_backend_detects_publish_hygiene_leak(tmp_path: Path) -> None:
    capture_path, official_ati_path, manual_capture_path = _build_valid_official_capture_inputs(tmp_path)
    publish_path = tmp_path / "publish"
    _build_publish_artifacts(path=publish_path)
    (publish_path / "ati_seed_vs_official.md").write_text(
        "# leak\n\nLocal source: /Users/demo/file.csv\n",
        encoding="utf-8",
    )

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=manual_capture_path,
        publish_dir=publish_path,
    )

    assert "publish_hygiene_violation:absolute_local_path:ati_seed_vs_official.md" in result.errors


def test_validate_backend_detects_seed_forecast_overlap_with_official(tmp_path: Path) -> None:
    capture_path, official_ati_path, manual_capture_path = _build_valid_official_capture_inputs(tmp_path)
    publish_path = tmp_path / "publish"
    _build_publish_artifacts(path=publish_path)
    pd.DataFrame(
        [
            {
                "quarter": "2026Q1",
                "financing_need_bn": 816.0,
                "ati_baseline_bn": 468.0,
                "seed_source": "user_note_table_3_forecast",
                "seed_quality": "note_forecast",
                "non_headline_reason": "seed_forecast_without_official_capture",
                "public_role": "supporting",
            }
        ]
    ).to_csv(publish_path / "ati_seed_forecast_table.csv", index=False)

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=manual_capture_path,
        publish_dir=publish_path,
    )

    assert "ati_seed_forecast_contains_official_quarters:2026Q1" in result.errors


def test_validate_backend_detects_noncanonical_qra_elasticity_rows(tmp_path: Path) -> None:
    capture_path, official_ati_path, manual_capture_path = _build_valid_official_capture_inputs(tmp_path)
    publish_path = tmp_path / "publish"
    _build_publish_artifacts(path=publish_path)
    pd.DataFrame(
        [
            {
                "quarter": "2024Q4",
                "event_id": "qra_2024_07",
                "event_date_type": "market_pricing_marker_minus_1d",
                "headline_bucket": "tightening",
                "classification_review_status": "reviewed",
                "shock_review_status": "reviewed",
                "series": "DGS10",
                "window": "d3",
                "shock_bn": 25.0,
                "schedule_diff_10y_eq_bn": 25.0,
                "schedule_diff_dynamic_10y_eq_bn": 27.0,
                "schedule_diff_dv01_usd": 2400000.0,
                "shock_construction": "schedule_diff_primary",
                "elasticity_bp_per_100bn": 12.0,
                "usable_for_headline": True,
            }
        ]
    ).to_csv(publish_path / "qra_event_elasticity.csv", index=False)
    (publish_path / "qra_event_elasticity.json").write_text(
        json.dumps({"title": "qra_event_elasticity", "rows": []}),
        encoding="utf-8",
    )
    (publish_path / "qra_event_elasticity.md").write_text("# qra_event_elasticity\n", encoding="utf-8")
    pd.DataFrame(
        [
            {
                "quarter": "2024Q4",
                "event_id": "qra_2024_07",
                "event_date_type": "market_pricing_marker_minus_1d",
                "headline_bucket": "tightening",
                "classification_review_status": "reviewed",
                "shock_review_status": "reviewed",
                "series": "DGS10",
                "window": "d3",
                "shock_bn": 25.0,
                "schedule_diff_10y_eq_bn": 25.0,
                "schedule_diff_dynamic_10y_eq_bn": 27.0,
                "schedule_diff_dv01_usd": 2400000.0,
                "shock_construction": "schedule_diff_primary",
                "elasticity_bp_per_100bn": 12.0,
                "usable_for_headline": True,
            }
        ]
    ).to_csv(publish_path / "qra_event_elasticity_diagnostic.csv", index=False)
    (publish_path / "qra_event_elasticity_diagnostic.json").write_text(
        json.dumps({"title": "qra_event_elasticity_diagnostic", "rows": []}),
        encoding="utf-8",
    )
    (publish_path / "qra_event_elasticity_diagnostic.md").write_text(
        "# qra_event_elasticity_diagnostic\n",
        encoding="utf-8",
    )
    index_payload = json.loads((publish_path / "index.json").read_text(encoding="utf-8"))
    index_payload["artifacts"].extend(
        [
            "qra_event_elasticity.csv",
            "qra_event_elasticity.json",
            "qra_event_elasticity.md",
            "qra_event_elasticity_diagnostic.csv",
            "qra_event_elasticity_diagnostic.json",
            "qra_event_elasticity_diagnostic.md",
        ]
    )
    index_payload["artifact_count"] = len(index_payload["artifacts"])
    (publish_path / "index.json").write_text(json.dumps(index_payload), encoding="utf-8")

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=manual_capture_path,
        publish_dir=publish_path,
    )

    assert "qra_publish_noncanonical_date_type:qra_event_elasticity.csv" in result.errors


def test_validate_backend_validates_qra_event_shock_components_schema_when_present(tmp_path: Path) -> None:
    capture_path, official_ati_path, manual_capture_path = _build_valid_official_capture_inputs(tmp_path)
    publish_path = tmp_path / "publish"
    _build_publish_artifacts(path=publish_path)
    pd.DataFrame(
        [
            {
                "event_id": "qra_2024_07",
                "quarter": "2024Q3",
                "previous_event_id": "qra_2024_05",
                "previous_quarter": "2024Q2",
                "tenor": "10Y",
                "issue_type": "nominal_coupon",
                "current_total_bn": 9.0,
                "previous_total_bn": 6.0,
                "delta_bn": 3.0,
                "yield_date": "2024-07-01",
                "yield_curve_source": "fred_constant_maturity_prior_business_day",
                "tenor_yield_pct": 4.0,
                "tenor_modified_duration": 5.4,
                "duration_factor_source": "fred_exact",
                "dynamic_10y_eq_weight": 0.54,
                "contribution_dynamic_10y_eq_bn": 1.62,
                "dv01_per_1bn_usd": 540000.0,
                "dv01_contribution_usd": 1620000.0,
                "tenor_weight_10y_eq": 1.0,
                "contribution_10y_eq_bn": 3.0,
            }
        ]
    ).to_csv(publish_path / "qra_event_shock_components.csv", index=False)
    (publish_path / "qra_event_shock_components.json").write_text(
        json.dumps({"title": "qra_event_shock_components", "rows": []}),
        encoding="utf-8",
    )
    (publish_path / "qra_event_shock_components.md").write_text(
        "# qra_event_shock_components\n",
        encoding="utf-8",
    )
    index_payload = json.loads((publish_path / "index.json").read_text(encoding="utf-8"))
    index_payload["artifacts"].extend(
        [
            "qra_event_shock_components.csv",
            "qra_event_shock_components.json",
            "qra_event_shock_components.md",
        ]
    )
    index_payload["artifact_count"] = len(index_payload["artifacts"])
    (publish_path / "index.json").write_text(json.dumps(index_payload), encoding="utf-8")

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=manual_capture_path,
        publish_dir=publish_path,
    )

    assert not any(
        message.startswith("publish_artifact_missing_columns:qra_event_shock_components.csv")
        for message in result.errors
    )
    assert not any(
        message.startswith("publish_artifact_missing:qra_event_shock_components")
        for message in result.errors
    )


def test_validate_backend_detects_extension_headline_semantics_violation(tmp_path: Path) -> None:
    capture_path, official_ati_path, manual_capture_path = _build_valid_official_capture_inputs(tmp_path)
    publish_path = tmp_path / "publish"
    _build_publish_artifacts(path=publish_path)

    extension_status = pd.read_csv(publish_path / "extension_status.csv")
    extension_status.loc[
        extension_status["extension"] == "investor_allotments", "headline_ready"
    ] = True
    extension_status.to_csv(publish_path / "extension_status.csv", index=False)

    dataset_status = pd.read_csv(publish_path / "dataset_status.csv")
    dataset_status.loc[
        dataset_status["dataset"] == "extension_investor_allotments", "headline_ready"
    ] = True
    dataset_status.to_csv(publish_path / "dataset_status.csv", index=False)

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=manual_capture_path,
        publish_dir=publish_path,
    )

    assert "dataset_status_extension_marked_headline:extension_investor_allotments" in result.errors
    assert "extension_status_marked_headline:investor_allotments" in result.errors


def test_validate_backend_enforces_qra_event_registry_v2_required_schema(tmp_path: Path) -> None:
    capture_path, official_ati_path, manual_capture_path = _build_valid_official_capture_inputs(
        tmp_path
    )
    publish_path = tmp_path / "publish"
    _build_publish_artifacts(path=publish_path)

    registry_row = {
        "event_id": "qra_2022_05",
        "quarter": "2022Q3",
        "release_bundle_type": "explicit_multi_stage_release",
        "policy_statement_url": "https://example.com/qra_2022_05",
        "financing_estimates_url": "https://example.com/qra_2022_05/estimates",
        "timing_quality": "timestamped",
        "overlap_severity": "none",
        "overlap_label": "none",
        "financing_need_news_flag": True,
        "composition_news_flag": False,
        "forward_guidance_flag": False,
        "reviewer": "qa_engine",
        "review_date": "2026-01-01",
        "treatment_version_id": "spec_duration_v1",
        "headline_eligibility_reason": "notional_gap",
    }
    # Intentionally omit release_timestamp_et to trigger required-schema failure.
    _add_publish_artifact(
        publish_path=publish_path,
        csv_name="qra_event_registry_v2.csv",
        frame=pd.DataFrame([registry_row]),
    )

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=manual_capture_path,
        publish_dir=publish_path,
    )

    assert any(
        error.startswith("publish_artifact_missing_columns:qra_event_registry_v2.csv:")
        and "release_timestamp_et" in error
        for error in result.errors
    )


def test_validate_backend_flags_overclaimed_event_timestamp_and_causal_status(tmp_path: Path) -> None:
    capture_path, official_ati_path, manual_capture_path = _build_valid_official_capture_inputs(
        tmp_path
    )
    publish_path = tmp_path / "publish"
    _build_publish_artifacts(path=publish_path)

    _add_publish_artifact(
        publish_path=publish_path,
        csv_name="qra_event_registry_v2.csv",
        frame=pd.DataFrame(
            [
                {
                    "event_id": "qra_2022_05",
                    "quarter": "2022Q3",
                    "release_timestamp_et": "2022-05-04T00:00:00-04:00",
                    "release_timestamp_kind": "release_component_registry_timestamp_with_time",
                    "release_bundle_type": "explicit_multi_stage_release",
                    "policy_statement_url": "https://example.com/policy",
                    "financing_estimates_url": "https://example.com/financing",
                    "timing_quality": "explicit_multi_stage_release",
                    "overlap_severity": "none",
                    "overlap_label": "",
                    "financing_need_news_flag": True,
                    "composition_news_flag": True,
                    "forward_guidance_flag": False,
                    "reviewer": "qa_engine",
                    "review_date": "2026-01-01",
                    "quality_tier": "Tier B",
                    "eligibility_blockers": "missing_expectation_benchmark",
                    "timestamp_precision": "exact_time",
                    "separability_status": "separable_component",
                    "expectation_status": "missing_benchmark",
                    "contamination_status": "pending_review",
                    "release_component_count": 2,
                    "causal_eligible_component_count": 0,
                    "treatment_version_id": "spec_duration_treatment_v1",
                    "headline_eligibility_reason": "usable",
                    "spec_id": "spec_qra_event_v2",
                    "treatment_variant": "canonical_shock_bn",
                }
            ]
        ),
    )
    _add_publish_artifact(
        publish_path=publish_path,
        csv_name="qra_release_component_registry.csv",
        frame=pd.DataFrame(
            [
                {
                    "release_component_id": "qra_2022_05__policy_statement",
                    "event_id": "qra_2022_05",
                    "quarter": "2022Q3",
                    "component_type": "policy_statement",
                    "release_timestamp_et": "2022-05-04T08:30:00-04:00",
                    "timestamp_precision": "exact_time",
                    "source_url": "https://example.com/policy",
                    "bundle_id": "qra_2022_05",
                    "release_sequence_label": "financing_then_policy",
                    "separable_component_flag": True,
                    "review_status": "reviewed",
                    "review_notes": "",
                    "benchmark_timestamp_et": "",
                    "benchmark_source": "",
                    "expected_composition_bn": "",
                    "realized_composition_bn": "",
                    "composition_surprise_bn": "",
                    "benchmark_stale_flag": False,
                    "expectation_review_status": "",
                    "expectation_notes": "",
                    "expectation_status": "missing_benchmark",
                    "contamination_flag": False,
                    "contamination_status": "pending_review",
                    "contamination_review_status": "",
                    "contamination_label": "",
                    "contamination_notes": "",
                    "separability_status": "separable_component",
                    "eligibility_blockers": "missing_expectation_benchmark",
                    "quality_tier": "Tier B",
                    "causal_eligible": False,
                }
            ]
        ),
    )
    _add_publish_artifact(
        publish_path=publish_path,
        csv_name="event_design_status.csv",
        frame=pd.DataFrame(
            [
                {"metric": "tier_a_count", "value": 0, "notes": "No causal-eligible components."},
                {"metric": "reviewed_surprise_ready_count", "value": 0, "notes": "No reviewed surprise-ready components."},
            ]
        ),
    )
    dataset_status = pd.read_csv(publish_path / "dataset_status.csv")
    dataset_status = pd.concat(
        [
            dataset_status,
            pd.DataFrame(
                [
                    {
                        "dataset": "qra_event_registry_v2",
                        "readiness_tier": "supporting_ready",
                        "source_quality": "derived_event_ledger",
                        "headline_ready": False,
                        "fallback_only": False,
                        "public_role": "supporting",
                    },
                    {
                        "dataset": "qra_release_component_registry",
                        "readiness_tier": "supporting_ready",
                        "source_quality": "derived_component_registry",
                        "headline_ready": False,
                        "fallback_only": False,
                        "public_role": "supporting",
                    },
                    {
                        "dataset": "qra_causal_qa_ledger",
                        "readiness_tier": "supporting_ready",
                        "source_quality": "derived_causal_qa",
                        "headline_ready": False,
                        "fallback_only": False,
                        "public_role": "supporting",
                    },
                    {
                        "dataset": "event_design_status",
                        "readiness_tier": "supporting_ready",
                        "source_quality": "derived_event_design_status",
                        "headline_ready": False,
                        "fallback_only": False,
                        "public_role": "supporting",
                    },
                ]
            ),
        ],
        ignore_index=True,
    )
    dataset_status.to_csv(publish_path / "dataset_status.csv", index=False)

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=manual_capture_path,
        publish_dir=publish_path,
    )

    assert "qra_publish_inexact_event_timestamp_claim:qra_event_registry_v2.csv:qra_2022_05" in result.errors
    assert "qra_publish_consistency_registry_component_timestamp_mismatch:qra_2022_05" in result.errors


def test_validate_backend_flags_invalid_causal_component_claims_and_status_metric_drift(
    tmp_path: Path,
) -> None:
    capture_path, official_ati_path, manual_capture_path = _build_valid_official_capture_inputs(
        tmp_path
    )
    publish_path = tmp_path / "publish"
    _build_publish_artifacts(path=publish_path)

    _add_publish_artifact(
        publish_path=publish_path,
        csv_name="qra_event_registry_v2.csv",
        frame=pd.DataFrame(
            [
                {
                    "event_id": "qra_2022_05",
                    "quarter": "2022Q3",
                    "release_timestamp_et": "2022-05-04T08:30:00-04:00",
                    "release_timestamp_kind": "release_component_registry_timestamp_with_time",
                    "release_bundle_type": "explicit_multi_stage_release",
                    "policy_statement_url": "https://example.com/policy",
                    "financing_estimates_url": "https://example.com/financing",
                    "timing_quality": "explicit_multi_stage_release",
                    "overlap_severity": "none",
                    "overlap_label": "",
                    "financing_need_news_flag": True,
                    "composition_news_flag": True,
                    "forward_guidance_flag": False,
                    "reviewer": "qa_engine",
                    "review_date": "2026-01-01",
                    "quality_tier": "Tier A",
                    "eligibility_blockers": "",
                    "timestamp_precision": "exact_time",
                    "separability_status": "separable_component",
                    "expectation_status": "reviewed_surprise_ready",
                    "contamination_status": "reviewed_clean",
                    "release_component_count": 1,
                    "causal_eligible_component_count": 1,
                    "treatment_version_id": "spec_duration_treatment_v1",
                    "headline_eligibility_reason": "usable",
                    "spec_id": "spec_qra_event_v2",
                    "treatment_variant": "canonical_shock_bn",
                }
            ]
        ),
    )
    _add_publish_artifact(
        publish_path=publish_path,
        csv_name="qra_release_component_registry.csv",
        frame=pd.DataFrame(
            [
                {
                    "release_component_id": "qra_2022_05__policy_statement",
                    "event_id": "qra_2022_05",
                    "quarter": "2022Q3",
                    "component_type": "policy_statement",
                    "release_timestamp_et": "2022-05-04T08:30:00-04:00",
                    "timestamp_precision": "exact_time",
                    "source_url": "https://example.com/policy",
                    "bundle_id": "qra_2022_05",
                    "release_sequence_label": "financing_then_policy",
                    "separable_component_flag": True,
                    "review_status": "reviewed",
                    "review_notes": "",
                    "benchmark_timestamp_et": "",
                    "benchmark_source": "",
                    "expected_composition_bn": "",
                    "realized_composition_bn": "",
                    "composition_surprise_bn": "",
                    "benchmark_stale_flag": False,
                    "expectation_review_status": "",
                    "expectation_notes": "",
                    "expectation_status": "",
                    "contamination_flag": False,
                    "contamination_status": "",
                    "contamination_review_status": "",
                    "contamination_label": "",
                    "contamination_notes": "",
                    "separability_status": "separable_component",
                    "eligibility_blockers": "",
                    "quality_tier": "Tier A",
                    "causal_eligible": True,
                }
            ]
        ),
    )
    _add_publish_artifact(
        publish_path=publish_path,
        csv_name="qra_causal_qa_ledger.csv",
        frame=pd.DataFrame(
            [
                {
                    "event_id": "qra_2022_05",
                    "quality_tier": "Tier A",
                    "eligibility_blockers": "",
                    "timestamp_precision": "exact_time",
                    "separability_status": "separable_component",
                    "expectation_status": "reviewed_surprise_ready",
                    "contamination_status": "reviewed_clean",
                    "release_component_count": 1,
                    "causal_eligible_component_count": 1,
                }
            ]
        ),
    )
    _add_publish_artifact(
        publish_path=publish_path,
        csv_name="event_design_status.csv",
        frame=pd.DataFrame(
            [
                {"metric": "release_component_count", "value": 2, "notes": "Incorrect on purpose."},
                {"metric": "tier_a_count", "value": 1, "notes": "Incorrectly claims one Tier A row."},
                {"metric": "reviewed_surprise_ready_count", "value": 1, "notes": "Incorrectly claims reviewed surprise readiness."},
                {"metric": "reviewed_clean_component_count", "value": 1, "notes": "Incorrectly claims clean contamination review."},
            ]
        ),
    )
    dataset_status = pd.read_csv(publish_path / "dataset_status.csv")
    dataset_status = pd.concat(
        [
            dataset_status,
            pd.DataFrame(
                [
                    {
                        "dataset": "qra_event_registry_v2",
                        "readiness_tier": "supporting_ready",
                        "source_quality": "derived_event_ledger",
                        "headline_ready": False,
                        "fallback_only": False,
                        "public_role": "supporting",
                    },
                    {
                        "dataset": "qra_release_component_registry",
                        "readiness_tier": "supporting_ready",
                        "source_quality": "derived_component_registry",
                        "headline_ready": False,
                        "fallback_only": False,
                        "public_role": "supporting",
                    },
                    {
                        "dataset": "qra_causal_qa_ledger",
                        "readiness_tier": "supporting_ready",
                        "source_quality": "derived_causal_qa",
                        "headline_ready": False,
                        "fallback_only": False,
                        "public_role": "supporting",
                    },
                    {
                        "dataset": "event_design_status",
                        "readiness_tier": "supporting_ready",
                        "source_quality": "derived_event_design_status",
                        "headline_ready": False,
                        "fallback_only": False,
                        "public_role": "supporting",
                    },
                ]
            ),
        ],
        ignore_index=True,
    )
    dataset_status.to_csv(publish_path / "dataset_status.csv", index=False)

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=manual_capture_path,
        publish_dir=publish_path,
    )

    assert "qra_component_registry_missing_expectation_status:qra_2022_05__policy_statement" in result.errors
    assert "qra_component_registry_missing_contamination_status:qra_2022_05__policy_statement" in result.errors
    assert "qra_component_registry_invalid_causal_eligible_claim:qra_2022_05__policy_statement" in result.errors
    assert "qra_event_design_status_metric_mismatch:release_component_count,reviewed_clean_component_count,reviewed_surprise_ready_count" in result.errors


def test_validate_qra_publish_consistency_ignores_historical_blank_component_timestamps() -> None:
    component_registry = pd.DataFrame(
        [
            {
                "release_component_id": "qra_2010_02__policy_statement",
                "event_id": "qra_2010_02",
                "quarter": "2010Q1",
                "component_type": "policy_statement",
                "release_timestamp_et": "",
                "timestamp_precision": "missing",
                "source_url": "https://example.com/policy",
                "bundle_id": "qra_2010_02",
                "release_sequence_label": "unknown",
                "separable_component_flag": False,
                "review_status": "pending",
                "review_notes": "historical scaffold",
                "benchmark_timestamp_et": "",
                "benchmark_source": "",
                "benchmark_source_family": "",
                "benchmark_timing_status": "same_release_placeholder",
                "external_benchmark_ready": False,
                "expected_composition_bn": "",
                "realized_composition_bn": "",
                "composition_surprise_bn": "",
                "benchmark_stale_flag": False,
                "expectation_review_status": "pending",
                "expectation_notes": "historical scaffold",
                "expectation_status": "missing_benchmark",
                "contamination_flag": False,
                "contamination_status": "pending_review",
                "contamination_review_status": "pending",
                "contamination_label": "",
                "contamination_notes": "historical scaffold",
                "separability_status": "same_day_inseparable_bundle",
                "eligibility_blockers": "review_not_complete|missing_exact_timestamp",
                "quality_tier": "Tier C",
                "causal_eligible": False,
            }
        ]
    )
    frames = {
        "qra_event_registry_v2.csv": pd.DataFrame(
            [
                {
                    "event_id": "qra_2010_02",
                    "release_timestamp_et": "2010-02-03T00:00:00-05:00",
                    "timestamp_precision": "missing",
                    "headline_eligibility_reason": "shock_missing",
                }
            ]
        ),
        "qra_release_component_registry.csv": component_registry,
        "event_design_status.csv": pd.DataFrame(
            [
                {"metric": "release_component_count", "value": 1},
                {"metric": "tier_a_count", "value": 0},
                {"metric": "tier_b_count", "value": 0},
                {"metric": "tier_c_count", "value": 1},
                {"metric": "tier_d_count", "value": 0},
                {"metric": "exact_time_component_count", "value": 0},
                {"metric": "reviewed_surprise_ready_count", "value": 0},
                {"metric": "reviewed_clean_component_count", "value": 0},
            ]
        ),
    }

    errors = validate_backend_script._validate_qra_publish_consistency(frames)

    assert "qra_publish_consistency_registry_component_timestamp_mismatch:qra_2010_02" not in errors


def test_validate_qra_publish_consistency_ignores_t_minus_one_usability_rows() -> None:
    component_registry = pd.DataFrame(
        [
            {
                "release_component_id": "qra_2022_05__financing_estimates",
                "event_id": "qra_2022_05",
                "quarter": "2022Q3",
                "component_type": "financing_estimates",
                "release_timestamp_et": "2022-05-02T15:00:00-04:00",
                "timestamp_precision": "exact_time",
                "source_url": "https://example.com/financing",
                "bundle_id": "qra_2022_05",
                "release_sequence_label": "financing_then_policy",
                "separable_component_flag": True,
                "review_status": "reviewed",
                "review_notes": "",
                "benchmark_timestamp_et": "",
                "benchmark_source": "https://example.com/benchmark",
                "benchmark_source_family": "tbac_recommended_financing_tables",
                "benchmark_timing_status": "external_timing_unverified",
                "external_benchmark_ready": False,
                "expected_composition_bn": 100.0,
                "realized_composition_bn": 125.0,
                "composition_surprise_bn": 25.0,
                "benchmark_stale_flag": False,
                "expectation_review_status": "reviewed",
                "expectation_notes": "",
                "expectation_status": "benchmark_timing_unverified",
                "contamination_flag": False,
                "contamination_status": "reviewed_clean",
                "contamination_review_status": "reviewed",
                "contamination_label": "",
                "contamination_notes": "",
                "separability_status": "separable_component",
                "eligibility_blockers": "missing_pre_release_external_benchmark",
                "quality_tier": "Tier B",
                "causal_eligible": False,
            }
        ]
    )
    frames = {
        "qra_event_registry_v2.csv": pd.DataFrame(
            [
                {
                    "event_id": "qra_2022_05",
                    "release_timestamp_et": "2022-05-02T15:00:00-04:00",
                    "timestamp_precision": "exact_time",
                    "headline_eligibility_reason": "usable",
                }
            ]
        ),
        "qra_release_component_registry.csv": component_registry,
        "event_design_status.csv": pd.DataFrame(
            [
                {"metric": "release_component_count", "value": 1},
                {"metric": "tier_a_count", "value": 0},
                {"metric": "tier_b_count", "value": 1},
                {"metric": "tier_c_count", "value": 0},
                {"metric": "tier_d_count", "value": 0},
                {"metric": "exact_time_component_count", "value": 1},
                {"metric": "reviewed_surprise_ready_count", "value": 0},
                {"metric": "reviewed_clean_component_count", "value": 1},
            ]
        ),
        "qra_shock_crosswalk_v1.csv": pd.DataFrame(
            [
                {
                    "event_id": "qra_2022_05",
                    "event_date_type": "official_release_date",
                    "spec_id": "spec_qra_event_v2",
                    "treatment_variant": "canonical_shock_bn",
                    "usable_for_headline_reason": "usable",
                }
            ]
        ),
        "qra_event_shock_summary.csv": pd.DataFrame(
            [
                {
                    "event_id": "qra_2022_05",
                    "event_date_type": "official_release_date",
                    "headline_bucket": "tightening",
                    "classification_review_status": "reviewed",
                    "shock_review_status": "reviewed",
                    "overlap_severity": "none",
                    "usable_for_headline": True,
                    "usable_for_headline_reason": "usable",
                    "spec_id": "spec_qra_event_v2",
                    "treatment_variant": "canonical_shock_bn",
                }
            ]
        ),
        "event_usability_table.csv": pd.DataFrame(
            [
                {
                    "headline_bucket": "tightening",
                    "event_date_type": "market_pricing_marker_minus_1d",
                    "classification_review_status": "reviewed",
                    "shock_review_status": "reviewed",
                    "overlap_severity": "none",
                    "usable_for_headline": False,
                    "usable_for_headline_reason": "non_official_event_date_type",
                    "event_count": 1,
                    "spec_id": "spec_qra_event_v2",
                    "treatment_variant": "canonical_shock_bn",
                },
                {
                    "headline_bucket": "tightening",
                    "event_date_type": "official_release_date",
                    "classification_review_status": "reviewed",
                    "shock_review_status": "reviewed",
                    "overlap_severity": "none",
                    "usable_for_headline": True,
                    "usable_for_headline_reason": "usable",
                    "event_count": 1,
                    "spec_id": "spec_qra_event_v2",
                    "treatment_variant": "canonical_shock_bn",
                },
            ]
        ),
    }

    errors = validate_backend_script._validate_qra_publish_consistency(frames)

    assert "qra_publish_consistency_usability_count_mismatch" not in errors


def test_validate_qra_publish_consistency_flags_benchmark_coverage_metric_mismatch() -> None:
    component_registry = pd.DataFrame(
        [
            {
                "release_component_id": "qra_2024_05__financing_estimates",
                "event_id": "qra_2024_05",
                "quarter": "2024Q3",
                "component_type": "financing_estimates",
                "benchmark_timing_status": "pre_release_external",
                "external_benchmark_ready": True,
                "expectation_status": "reviewed_surprise_ready",
                "contamination_status": "reviewed_contaminated_context_only",
                "quality_tier": "Tier B",
                "causal_eligible": False,
                "timestamp_precision": "exact_time",
                "source_url": "https://example.com/financing",
                "eligibility_blockers": "contamination_context_only",
            }
        ]
    )
    frames = {
        "qra_event_registry_v2.csv": pd.DataFrame(
            [
                {
                    "event_id": "qra_2024_05",
                    "release_timestamp_et": "2024-04-29T15:00:00-04:00",
                    "timestamp_precision": "exact_time",
                    "headline_eligibility_reason": "usable",
                }
            ]
        ),
        "qra_release_component_registry.csv": component_registry,
        "qra_benchmark_coverage.csv": pd.DataFrame(
            [
                {"scope": "current_sample_financing_estimates", "metric": "release_component_count", "value": 1},
                {"scope": "current_sample_financing_estimates", "metric": "reviewed_contaminated_context_only_count", "value": 0},
            ]
        ),
    }

    errors = validate_backend_script._validate_qra_publish_consistency(frames)

    assert (
        "qra_benchmark_coverage_metric_mismatch:"
        "current_sample_financing_estimates:reviewed_contaminated_context_only_count"
    ) in errors


def test_validate_manual_causal_review_inputs_requires_current_sample_financing_rows(tmp_path: Path) -> None:
    manual_dir = tmp_path / "manual"
    manual_dir.mkdir()
    pd.DataFrame(
        [
            {
                "release_component_id": "qra_2024_05__financing_estimates",
                "quarter": "2024Q3",
                "component_type": "financing_estimates",
            }
        ]
    ).to_csv(manual_dir / "qra_release_component_registry.csv", index=False)
    pd.DataFrame(
        [
            {
                "release_component_id": "qra_2024_05__financing_estimates",
                "benchmark_timing_status": "pre_release_external",
                "expectation_review_status": "reviewed",
                "expectation_notes": "Reviewed.",
                "benchmark_source": "https://example.com/survey.pdf",
                "benchmark_source_family": "primary_dealer_auction_size_survey",
                "benchmark_release_timestamp_et": "2024-04-23T15:32:09-04:00",
                "benchmark_pre_release_verified_flag": True,
                "benchmark_observed_before_component_flag": True,
            }
        ]
    ).to_csv(manual_dir / "qra_component_expectation_template.csv", index=False)
    pd.DataFrame(columns=["release_component_id"]).to_csv(
        manual_dir / "qra_event_contamination_reviews.csv", index=False
    )

    errors, warnings = validate_backend_script.validate_manual_causal_review_inputs(manual_dir)

    assert warnings == []
    assert (
        "manual_causal_contamination_missing_current_sample_financing_rows:"
        "qra_2024_05__financing_estimates"
    ) in errors


def test_canonical_qra_review_frame_does_not_collapse_aggregate_usability_rows() -> None:
    frame = pd.DataFrame(
        [
            {
                "event_date_type": "official_release_date",
                "headline_bucket": "control_hold",
                "treatment_variant": "canonical_shock_bn",
                "event_count": 1,
            },
            {
                "event_date_type": "official_release_date",
                "headline_bucket": "tightening",
                "treatment_variant": "canonical_shock_bn",
                "event_count": 2,
            },
        ]
    )

    out = validate_backend_script._canonical_qra_review_frame(frame)

    assert len(out) == 2


def test_validate_backend_flags_shock_drift_for_known_events(tmp_path: Path) -> None:
    capture_path, official_ati_path, manual_capture_path = _build_valid_official_capture_inputs(
        tmp_path
    )
    publish_path = tmp_path / "publish"
    _build_publish_artifacts(path=publish_path)

    event_ids = ["qra_2023_05", "qra_2023_08"]
    _add_publish_artifact(
        publish_path=publish_path,
        csv_name="qra_shock_crosswalk_v1.csv",
        frame=pd.DataFrame(
            [
                {
                    "event_id": event_id,
                    "event_date_type": "official_release_date",
                    "canonical_shock_id": "manual_v1",
                    "shock_bn": 0.0,
                    "schedule_diff_10y_eq_bn": 120.0,
                    "schedule_diff_dynamic_10y_eq_bn": 125.0,
                    "schedule_diff_dv01_usd": 5_000_000.0,
                    "shock_source": "manual_statement_review_v1",
                    "manual_override_reason": "reviewed manual row pending alt treatment completion",
                    "shock_review_status": "reviewed",
                    "alternative_treatment_complete": False,
                    "alternative_treatment_missing_reason": "manual_statement_primary_only_pending_alt_treatments",
                }
                for event_id in event_ids
            ]
        ),
    )

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=manual_capture_path,
        publish_dir=publish_path,
    )

    for event_id in event_ids:
        assert any(f":{event_id}:" in message for message in result.warnings), f"missing drift warning for {event_id}"


def test_validate_backend_ignores_non_actionable_shock_drift_rows(tmp_path: Path) -> None:
    capture_path, official_ati_path, manual_capture_path = _build_valid_official_capture_inputs(
        tmp_path
    )
    publish_path = tmp_path / "publish"
    _build_publish_artifacts(path=publish_path)

    _add_publish_artifact(
        publish_path=publish_path,
        csv_name="qra_shock_crosswalk_v1.csv",
        frame=pd.DataFrame(
            [
                {
                    "event_id": "qra_2022_08",
                    "event_date_type": "official_release_date",
                    "canonical_shock_id": "manual_v1",
                    "shock_bn": 0.0,
                    "schedule_diff_10y_eq_bn": -602.2,
                    "schedule_diff_dynamic_10y_eq_bn": -557.9,
                    "schedule_diff_dv01_usd": 5_000_000.0,
                    "shock_source": "manual_statement_review_v1",
                    "manual_override_reason": "reviewed hold quarter",
                    "shock_review_status": "reviewed",
                    "alternative_treatment_complete": True,
                },
                {
                    "event_id": "qra_2024_07",
                    "event_date_type": "official_release_date",
                    "canonical_shock_id": "auto_v1",
                    "shock_bn": 0.0,
                    "schedule_diff_10y_eq_bn": 476.9,
                    "schedule_diff_dynamic_10y_eq_bn": 440.7,
                    "schedule_diff_dv01_usd": 5_000_000.0,
                    "shock_source": "auto_tenor_parser_v1",
                    "shock_review_status": "provisional",
                    "alternative_treatment_complete": True,
                },
            ]
        ),
    )

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=manual_capture_path,
        publish_dir=publish_path,
    )

    assert not any("qra_publish_shock_drift_alert:" in message for message in result.warnings)


def test_validate_shock_drift_alerts_ignores_elasticity_duplicates() -> None:
    warnings: list[str] = []
    frame = pd.DataFrame(
        [
            {
                "event_id": "e1",
                "event_date_type": "official_release_date",
                "treatment_variant": "canonical_shock_bn",
                "shock_bn": 0.0,
                "schedule_diff_10y_eq_bn": 120.0,
                "schedule_diff_dynamic_10y_eq_bn": 125.0,
                "shock_review_status": "reviewed",
            },
            {
                "event_id": "e1",
                "event_date_type": "official_release_date",
                "treatment_variant": "fixed_10y_eq_bn",
                "shock_bn": 0.0,
                "schedule_diff_10y_eq_bn": 120.0,
                "schedule_diff_dynamic_10y_eq_bn": 125.0,
                "shock_review_status": "reviewed",
            },
        ]
    )

    validate_backend_script._validate_shock_drift_alerts(
        "qra_event_elasticity.csv",
        frame,
        warnings=warnings,
    )

    assert warnings == []


def test_validate_backend_checks_readme_coverage_consistency(tmp_path: Path) -> None:
    capture_path, official_ati_path, manual_capture_path = _build_valid_official_capture_inputs(
        tmp_path
    )
    publish_path = tmp_path / "publish"
    _build_publish_artifacts(path=publish_path, official_quarter="2022Q3")

    pd.DataFrame(
        [
            {
                "quarter": "2022Q3",
                "readiness_tier": "headline_ready",
                "source_quality": "exact_official",
                "headline_ready": True,
                "fallback_only": False,
                "missing_critical_fields": "",
                "provenance_summary": "test",
            },
            {
                "quarter": "2022Q4",
                "readiness_tier": "headline_ready",
                "source_quality": "exact_official",
                "headline_ready": True,
                "fallback_only": False,
                "missing_critical_fields": "",
                "provenance_summary": "test",
            },
            {
                "quarter": "2023Q3",
                "readiness_tier": "headline_ready",
                "source_quality": "exact_official",
                "headline_ready": True,
                "fallback_only": False,
                "missing_critical_fields": "",
                "provenance_summary": "test",
            },
        ]
    ).to_csv(publish_path / "official_capture_readiness.csv", index=False)
    pd.DataFrame(
        [
            {
                "quarter": "2022Q3",
                "completion_tier": "exact_official_numeric",
                "qa_status": "manual_official_capture",
                "uses_seed_source": False,
                "net_bill_issuance_bn": 1.0,
                "reconstructed_net_bill_issuance_bn": 1.0,
                "reconstruction_status_bill": "complete",
                "is_headline_ready": True,
            },
            {
                "quarter": "2022Q4",
                "completion_tier": "exact_official_numeric",
                "qa_status": "manual_official_capture",
                "uses_seed_source": False,
                "net_bill_issuance_bn": 1.0,
                "reconstructed_net_bill_issuance_bn": 1.0,
                "reconstruction_status_bill": "complete",
                "is_headline_ready": True,
            },
            {
                "quarter": "2023Q3",
                "completion_tier": "exact_official_numeric",
                "qa_status": "manual_official_capture",
                "uses_seed_source": False,
                "net_bill_issuance_bn": 1.0,
                "reconstructed_net_bill_issuance_bn": 1.0,
                "reconstruction_status_bill": "complete",
                "is_headline_ready": True,
            },
        ]
    ).to_csv(publish_path / "official_capture_completion.csv", index=False)

    readme_path = tmp_path / "README.md"
    readme_path.write_text(
        "# test\nExact official quarter coverage currently spans `2023Q4` through `2025Q4`.\n",
        encoding="utf-8",
    )

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=manual_capture_path,
        publish_dir=publish_path,
        readme_path=readme_path,
    )

    assert any(msg.startswith("readme_official_coverage_mismatch:") for msg in result.warnings)
    assert "official_coverage_statement_contiguous_while_data_noncontiguous" in result.warnings


def test_validate_backend_validates_optional_event_and_absorption_artifacts(tmp_path: Path) -> None:
    capture_path, official_ati_path, manual_capture_path = _build_valid_official_capture_inputs(
        tmp_path
    )
    publish_path = tmp_path / "publish"
    _build_publish_artifacts(path=publish_path)

    _add_publish_artifact(
        publish_path=publish_path,
        csv_name="treatment_comparison_table.csv",
        frame=pd.DataFrame(
            [
                {
                    "spec_id": "spec_duration_treatment_v1",
                    "event_date_type": "official_release_date",
                    "series": "DGS10",
                    "window": "d3",
                    "treatment_variant": "canonical_shock_bn",
                    "comparison_family": "bp_per_100bn",
                    "comparison_family_label": "bp-per-100bn family",
                    "elasticity_units": "bp_per_100bn",
                    "n_rows": 2,
                    "n_events": 2,
                    "n_headline_eligible_events": 2,
                    "headline_eligible_share": 1.0,
                    "mean_elasticity_value": 12.0,
                    "median_elasticity_value": 12.0,
                    "std_elasticity_value": 1.0,
                    "min_elasticity_value": 11.0,
                    "max_elasticity_value": 13.0,
                    "mean_abs_elasticity_value": 12.0,
                    "family_reference_variant": "canonical_shock_bn",
                    "family_reference_mean_elasticity_value": 12.0,
                    "delta_vs_family_reference_mean_elasticity_value": 0.0,
                    "bp_family_spread_elasticity_value": 2.0,
                    "headline_recommendation_status": "retain_canonical_contract",
                    "headline_recommendation_reason": "comparison_fallback_loaded",
                    "primary_treatment_variant": "canonical_shock_bn",
                    "primary_treatment_reason": "canonical_shock_bn remains the headline contract; fixed, dynamic, and DV01 variants are comparison diagnostics.",
                }
            ]
        ),
    )
    _add_publish_artifact(
        publish_path=publish_path,
        csv_name="event_usability_table.csv",
        frame=pd.DataFrame(
            [
                {
                    "headline_bucket": "tightening",
                    "event_date_type": "official_release_date",
                    "classification_review_status": "reviewed",
                    "shock_review_status": "reviewed",
                    "overlap_severity": "none",
                    "headline_usable_count": 1,
                    "event_count": 3,
                }
            ]
        ),
    )
    _add_publish_artifact(
        publish_path=publish_path,
        csv_name="leave_one_event_out_table.csv",
        frame=pd.DataFrame(
            [
                {
                    "left_out_event_id": "qra_2022_05",
                    "event_date_type": "official_release_date",
                    "headline_bucket": "tightening",
                    "series": "DGS10",
                    "window": "d3",
                    "estimate": 1.2,
                    "p_value": 0.31,
                    "n_events": 4,
                }
            ]
        ),
    )
    _add_publish_artifact(
        publish_path=publish_path,
        csv_name="auction_absorption_table.csv",
        frame=pd.DataFrame(
            [
                {
                    "qra_event_id": "qra_2022_05",
                    "quarter": "2022Q3",
                    "auction_date": "2022-05-03",
                    "security_family": "bill",
                    "investor_class": "money_market",
                    "measure": "tail",
                    "value": 1.5,
                    "units": "bps",
                    "source_quality": "summary_ready",
                    "provenance_summary": "bounded_validation_pack",
                }
            ]
        ),
    )

    result = validate_backend_script.validate_backend(
        official_capture_path=capture_path,
        official_ati_path=official_ati_path,
        manual_capture_path=manual_capture_path,
        publish_dir=publish_path,
    )

    for artifact in (
        "treatment_comparison_table",
        "event_usability_table",
        "leave_one_event_out_table",
        "auction_absorption_table",
    ):
        assert not any(
            message.startswith(f"publish_artifact_missing_columns:{artifact}.csv:")
            for message in result.errors
        )
