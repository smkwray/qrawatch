from __future__ import annotations

from pathlib import Path

import pandas as pd

from ati_shadow_policy import publish


def test_build_extension_status_table_defaults_to_not_started(tmp_path, monkeypatch):
    monkeypatch.setattr(publish, "RAW_DIR", tmp_path / "raw")
    monkeypatch.setattr(publish, "PROCESSED_DIR", tmp_path / "processed")
    monkeypatch.setattr(publish, "OUTPUT_DIR", tmp_path / "output")

    table = publish.build_extension_status_table()

    assert list(table["extension"]) == ["investor_allotments", "primary_dealer", "sec_nmfp", "tic"]
    assert set(table["backend_status"]) == {"not_started"}


def test_build_data_sources_publish_table_reports_existing_files(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw"
    qra_dir = raw_dir / "qra"
    qra_dir.mkdir(parents=True)
    (qra_dir / "manifest.csv").write_text("href\nhttps://example.com\n", encoding="utf-8")
    (qra_dir / "downloads.csv").write_text("local_path\nfile\n", encoding="utf-8")
    (qra_dir / "example.html").write_text("<html></html>", encoding="utf-8")

    monkeypatch.setattr(publish, "RAW_DIR", raw_dir)

    table = publish.build_data_sources_publish_table()
    qra_row = table.loc[table["source_family"] == "qra"].iloc[0]

    assert bool(qra_row["raw_dir_exists"])
    assert bool(qra_row["manifest_exists"])
    assert bool(qra_row["downloads_exists"])
    assert qra_row["file_count"] == 3


def test_build_duration_publish_table_returns_latest_window(tmp_path, monkeypatch):
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir(parents=True)
    df = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=20, freq="W-WED"),
            "coupon_like_total": range(20),
            "qt_proxy": range(20),
            "buybacks_accepted": range(20),
            "provisional_public_duration_supply": range(20),
            "notes": ["note"] * 20,
        }
    )
    df.to_csv(processed_dir / "public_duration_supply_provisional.csv", index=False)
    monkeypatch.setattr(publish, "PROCESSED_DIR", processed_dir)

    latest = publish.build_duration_publish_table()

    assert len(latest) == 12
    assert latest.iloc[0]["date"] == "2024-02-28"
    assert latest.iloc[-1]["date"] == "2024-05-15"


def test_build_ati_seed_vs_official_comparison_merges_rows(tmp_path, monkeypatch):
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "quarter": ["2024Q1", "2024Q2"],
            "ati_baseline_bn": [10.0, 20.0],
        }
    ).to_csv(processed_dir / "ati_index_seed.csv", index=False)
    pd.DataFrame(
        {
            "quarter": ["2024Q2"],
            "ati_baseline_bn": [22.5],
            "qa_status": ["manual_official_capture"],
        }
    ).to_csv(processed_dir / "ati_index_official_capture.csv", index=False)
    pd.DataFrame(
        {
            "quarter": ["2024Q2"],
            "qra_release_date": ["2024-01-31"],
            "market_pricing_marker_minus_1d": ["2024-01-30"],
            "total_financing_need_bn": [202],
            "net_bill_issuance_bn": [55.0],
            "source_url": ["https://home.treasury.gov/news/press-releases/jy2315"],
            "source_doc_local": ["/tmp/jy2315.html"],
            "source_doc_type": ["official_quarterly_refunding_statement"],
            "qa_status": ["manual_official_capture"],
        }
    ).to_csv(processed_dir / "official_quarterly_refunding_capture.csv", index=False)
    monkeypatch.setattr(publish, "PROCESSED_DIR", processed_dir)

    comparison = publish.build_ati_seed_vs_official_comparison()

    assert list(comparison["quarter"]) == ["2024Q1", "2024Q2"]
    assert pd.isna(comparison.loc[0, "ati_official_bn"])
    assert comparison.loc[0, "comparison_status"] == "seed_only"
    assert not bool(comparison.loc[0, "official_capture_present"])
    assert comparison.loc[1, "ati_diff_official_minus_seed"] == 2.5
    assert comparison.loc[1, "source_quality"] == "exact_official"
    assert bool(comparison.loc[1, "headline_ready"])
    assert comparison.loc[1, "readiness_tier"] == "headline_ready"


def test_build_official_capture_readiness_table_distinguishes_exact_and_hybrid(tmp_path, monkeypatch):
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "quarter": ["2024Q1", "2024Q2"],
            "qra_release_date": ["2023-11-01", "2024-01-31"],
            "market_pricing_marker_minus_1d": ["2023-10-31", "2024-01-30"],
            "total_financing_need_bn": [816, 202],
            "net_bill_issuance_bn": [468.0, ""],
            "source_url": [
                "https://home.treasury.gov/news/press-releases/jy2062",
                "https://home.treasury.gov/news/press-releases/jy2315",
            ],
            "source_doc_local": ["/tmp/jy2062.html", "/tmp/jy2315.html|data/manual/qra_event_seed.csv"],
            "source_doc_type": [
                "official_quarterly_refunding_statement",
                "official_quarterly_refunding_statement|quarterly_refunding_press_release|seed_csv",
            ],
            "qa_status": ["manual_official_capture", "semi_automated_capture"],
        }
    ).to_csv(processed_dir / "official_quarterly_refunding_capture.csv", index=False)
    monkeypatch.setattr(publish, "PROCESSED_DIR", processed_dir)

    table = publish.build_official_capture_readiness_table()

    exact = table.loc[table["quarter"] == "2024Q1"].iloc[0]
    hybrid = table.loc[table["quarter"] == "2024Q2"].iloc[0]

    assert exact["source_quality"] == "exact_official"
    assert exact["readiness_tier"] == "headline_ready"
    assert bool(exact["headline_ready"])
    assert not bool(exact["fallback_only"])
    assert exact["missing_critical_fields"] == ""
    assert "doc_type=official_quarterly_refunding_statement" in exact["provenance_summary"]

    assert hybrid["source_quality"] == "official_hybrid"
    assert hybrid["readiness_tier"] == "incomplete"
    assert not bool(hybrid["headline_ready"])
    assert bool(hybrid["fallback_only"])
    assert "net_bill_issuance_bn" in hybrid["missing_critical_fields"]


def _write_extension_summary_publish_file(path: Path, stem: str) -> None:
    (path / f"{stem}.csv").write_text(
        "extension,readiness_tier,source_quality,rows\n"
        f"{stem},summary_ready,exact_official,100\n",
        encoding="utf-8",
    )
    (path / f"{stem}.json").write_text(
        f'{{"title":"{stem}","rows":[{{"extension":"{stem}","readiness_tier":"summary_ready"}}]}}',
        encoding="utf-8",
    )
    (path / f"{stem}.md").write_text(f"# {stem}\n", encoding="utf-8")


def test_extension_status_promotes_ready_tier_to_summary_when_summaries_present(
    tmp_path, monkeypatch
) -> None:
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    publish_dir = tmp_path / "output" / "publish"
    processed_dir.mkdir(parents=True)
    publish_dir.mkdir(parents=True)

    for extension in ("investor_allotments", "primary_dealer"):
        ext_raw = raw_dir / extension
        ext_raw.mkdir(parents=True)
        (ext_raw / "manifest.csv").write_text("href\n", encoding="utf-8")
        (ext_raw / "downloads.csv").write_text("href\n", encoding="utf-8")

    (processed_dir / "investor_allotments.csv").write_text("col\n1\n", encoding="utf-8")
    (processed_dir / "investor_allotments_panel.csv").write_text(
        "auction_date,security_family,investor_class,measure,value,units,summary_type,as_of_date,source_quality,source_file\n"
        "2026-03-11,bill,foreign_and_international,allotment_amount,10.0,USD billions,auction_observation,2026-03-11,official_treasury_download,investor.csv\n",
        encoding="utf-8",
    )
    (processed_dir / "primary_dealer_inventory.csv").write_text(
        "col\n1\n", encoding="utf-8"
    )
    (processed_dir / "primary_dealer_panel.csv").write_text(
        "date,series_id,metric_id,series_label,metric_label,value,units,frequency,source_file,source_quality,source_dataset_type\n"
        "2026-03-11,pdwotipsc,daily_avg_vol_in_millions,Weighted Ongoing Treasury Position,Daily Avg Vol,12.0,USD millions,weekly,primary.csv,csv_canonical,latest_series_snapshot\n",
        encoding="utf-8",
    )
    _write_extension_summary_publish_file(publish_dir, "investor_allotments_summary")
    _write_extension_summary_publish_file(publish_dir, "primary_dealer_summary")

    monkeypatch.setattr(publish, "RAW_DIR", raw_dir)
    monkeypatch.setattr(publish, "PROCESSED_DIR", processed_dir)
    monkeypatch.setattr(publish, "OUTPUT_DIR", publish_dir.parent)

    table = publish.build_extension_status_table()

    investor = table.loc[table["extension"] == "investor_allotments"].iloc[0]
    primary = table.loc[table["extension"] == "primary_dealer"].iloc[0]

    assert investor["backend_status"] == "summary_ready"
    assert investor["readiness_tier"] == "summary_ready"
    assert bool(investor["headline_ready"])
    assert primary["backend_status"] == "summary_ready"
    assert primary["readiness_tier"] == "summary_ready"
    assert bool(primary["headline_ready"])


def test_dataset_status_derives_summary_ready_from_extension_status_rows(monkeypatch, tmp_path) -> None:
    fake_extension_status = pd.DataFrame(
        [
            {
                "extension": "investor_allotments",
                "backend_status": "summary_ready",
                "readiness_tier": "summary_ready",
            },
            {
                "extension": "primary_dealer",
                "backend_status": "summary_ready",
                "readiness_tier": "summary_ready",
            },
            {
                "extension": "sec_nmfp",
                "backend_status": "processed",
                "readiness_tier": "inventory_ready",
            },
            {
                "extension": "tic",
                "backend_status": "not_started",
                "readiness_tier": "not_started",
            },
        ]
    )

    monkeypatch.setattr(publish, "build_extension_status_table", lambda: fake_extension_status)

    processed_dir = tmp_path / "processed"
    tables_dir = tmp_path / "tables"
    processed_dir.mkdir()
    tables_dir.mkdir()
    (processed_dir / "official_quarterly_refunding_capture.csv").write_text(
        "quarter,qra_release_date,market_pricing_marker_minus_1d,total_financing_need_bn,net_bill_issuance_bn,source_url,source_doc_local,source_doc_type,qa_status\n",
        encoding="utf-8",
    )
    (processed_dir / "ati_index_official_capture.csv").write_text(
        "quarter,ati_baseline_bn,qa_status,source_doc_local,source_doc_type\n",
        encoding="utf-8",
    )
    (tables_dir / "plumbing_regressions.csv").write_text(
        "dependent_variable,term,coef,p_value\n", encoding="utf-8"
    )
    (processed_dir / "public_duration_supply.csv").write_text("date,val\n", encoding="utf-8")

    monkeypatch.setattr(publish, "PROCESSED_DIR", processed_dir)
    monkeypatch.setattr(publish, "TABLES_DIR", tables_dir)

    table = publish.build_dataset_status_table()

    investor = table.loc[table["dataset"] == "extension_investor_allotments"].iloc[0]
    primary = table.loc[table["dataset"] == "extension_primary_dealer"].iloc[0]

    assert investor["readiness_tier"] == "summary_ready"
    assert primary["readiness_tier"] == "summary_ready"
    assert investor["source_quality"] == "summary_ready"
    assert primary["source_quality"] == "summary_ready"
    assert bool(investor["headline_ready"])
    assert bool(primary["headline_ready"])
    assert not bool(investor["fallback_only"])
    assert not bool(primary["fallback_only"])
