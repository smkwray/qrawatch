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
    assert not bool(table["headline_ready"].any())
    assert set(table["public_role"]) == {"supporting"}


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


def test_build_ati_publish_table_prefers_official_rows(tmp_path, monkeypatch):
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "quarter": ["2024Q1", "2024Q2", "2024Q3"],
            "financing_need_bn": [816.0, 202.0, 847.0],
            "net_bills_bn": [409.117, -296.523, 239.382],
            "bill_share": [0.5, -1.46, 0.28],
            "missing_coupons_15_bn": [286.717, -326.823, 112.332],
            "missing_coupons_18_bn": [262.237, -332.883, 86.922],
            "missing_coupons_20_bn": [245.917, -336.923, 69.982],
            "ati_baseline_bn": [262.237, -332.883, 86.922],
            "qa_status": ["manual_official_capture", "manual_official_capture", "manual_official_capture"],
            "source_doc_local": ["/tmp/doc1.pdf", "/tmp/doc2.pdf", "seed_csv|/tmp/doc3.pdf"],
            "source_doc_type": [
                "official_quarterly_refunding_statement",
                "official_quarterly_refunding_statement",
                "official_quarterly_refunding_statement|seed_csv",
            ],
        }
    ).to_csv(processed_dir / "ati_index_official_capture.csv", index=False)
    pd.DataFrame(
        {
            "quarter": ["2024Q1", "2024Q3"],
            "ati_baseline_bn": [321.12, 132.54],
            "seed_source": ["user_note_table_2", "user_note_table_2_adjusted"],
            "seed_quality": ["note_estimate", "note_estimate_adjusted"],
        }
    ).to_csv(processed_dir / "ati_index_seed.csv", index=False)
    monkeypatch.setattr(publish, "PROCESSED_DIR", processed_dir)

    table = publish.build_ati_publish_table()

    assert list(table["quarter"]) == ["2024Q1", "2024Q2"]
    assert "seed_source" not in table.columns
    assert set(table["source_quality"]) == {"exact_official_numeric"}
    assert set(table["public_role"]) == {"headline"}
    assert table.loc[table["quarter"] == "2024Q2", "ati_baseline_bn"].iloc[0] == -332.883


def test_build_ati_seed_forecast_table_excludes_official_overlap(tmp_path, monkeypatch):
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "quarter": ["2024Q1"],
            "financing_need_bn": [816.0],
            "net_bills_bn": [409.117],
            "bill_share": [0.5],
            "missing_coupons_15_bn": [286.717],
            "missing_coupons_18_bn": [262.237],
            "missing_coupons_20_bn": [245.917],
            "ati_baseline_bn": [262.237],
            "qa_status": ["manual_official_capture"],
            "source_doc_local": ["/tmp/doc1.pdf"],
            "source_doc_type": ["official_quarterly_refunding_statement"],
        }
    ).to_csv(processed_dir / "ati_index_official_capture.csv", index=False)
    pd.DataFrame(
        {
            "quarter": ["2024Q1", "2024Q4", "2025Q1"],
            "financing_need_bn": [816.0, 665.0, 770.0],
            "net_bills_bn": [468.0, 208.0, 200.0],
            "bill_share": [0.57, 0.31, 0.26],
            "missing_coupons_15_bn": [345.6, 108.25, 84.5],
            "missing_coupons_18_bn": [321.12, 88.3, 61.4],
            "missing_coupons_20_bn": [304.8, 75.0, 46.0],
            "ati_baseline_bn": [321.12, 88.3, 61.4],
            "seed_source": ["user_note_table_2", "user_note_table_3_forecast", "user_note_table_3_forecast"],
            "seed_quality": ["note_estimate", "note_forecast", "note_forecast"],
        }
    ).to_csv(processed_dir / "ati_index_seed.csv", index=False)
    monkeypatch.setattr(publish, "PROCESSED_DIR", processed_dir)

    table = publish.build_ati_seed_forecast_table()

    assert list(table["quarter"]) == ["2024Q4", "2025Q1"]
    assert set(table["public_role"]) == {"supporting"}
    assert not bool(table["headline_ready"].any())
    assert set(table["non_headline_reason"]) == {"seed_forecast_without_official_capture"}


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


def test_build_official_capture_backfill_queue_table_tracks_missing_numeric_fields(tmp_path, monkeypatch):
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "quarter": ["2010Q1", "2010Q2"],
            "qra_release_date": ["2010-02-03", "2010-05-05"],
            "market_pricing_marker_minus_1d": ["2010-02-02", "2010-05-04"],
            "total_financing_need_bn": [392, ""],
            "net_bill_issuance_bn": [49.943, -39.009],
            "financing_source_url": [
                "https://home.treasury.gov/news/press-releases/tg524",
                "",
            ],
            "financing_source_doc_local": ["/tmp/tg524.html", ""],
            "financing_source_doc_type": ["quarterly_refunding_press_release", ""],
            "refunding_statement_source_url": [
                "https://home.treasury.gov/news/press-releases/tg527",
                "https://home.treasury.gov/news/press-releases/tg679",
            ],
            "refunding_statement_source_doc_local": ["/tmp/tg527.html", "/tmp/tg679.html"],
            "refunding_statement_source_doc_type": [
                "official_quarterly_refunding_statement",
                "official_quarterly_refunding_statement",
            ],
            "auction_reconstruction_source_url": [
                "https://fiscaldata.treasury.gov/datasets/treasury-securities-auctions-data/auctions-query",
                "https://fiscaldata.treasury.gov/datasets/treasury-securities-auctions-data/auctions-query",
            ],
            "auction_reconstruction_source_doc_local": [
                "data/raw/fiscaldata/auctions_query.csv",
                "data/raw/fiscaldata/auctions_query.csv",
            ],
            "auction_reconstruction_source_doc_type": [
                "official_auction_reconstruction",
                "official_auction_reconstruction",
            ],
            "source_url": [
                "https://home.treasury.gov/news/press-releases/tg524|https://home.treasury.gov/news/press-releases/tg527|https://fiscaldata.treasury.gov/datasets/treasury-securities-auctions-data/auctions-query",
                "https://home.treasury.gov/news/press-releases/tg679|https://fiscaldata.treasury.gov/datasets/treasury-securities-auctions-data/auctions-query",
            ],
            "source_doc_local": [
                "/tmp/tg524.html|/tmp/tg527.html|data/raw/fiscaldata/auctions_query.csv",
                "/tmp/tg679.html|data/raw/fiscaldata/auctions_query.csv",
            ],
            "source_doc_type": [
                "quarterly_refunding_press_release|official_quarterly_refunding_statement|official_auction_reconstruction",
                "official_quarterly_refunding_statement|official_auction_reconstruction",
            ],
            "qa_status": ["manual_official_capture", "semi_automated_capture"],
        }
    ).to_csv(processed_dir / "official_quarterly_refunding_capture.csv", index=False)
    monkeypatch.setattr(publish, "PROCESSED_DIR", processed_dir)

    table = publish.build_official_capture_backfill_queue_table()

    ready = table.loc[table["quarter"] == "2010Q1"].iloc[0]
    pending = table.loc[table["quarter"] == "2010Q2"].iloc[0]

    assert bool(ready["numeric_official_capture_ready"])
    assert ready["next_action"] == "complete"
    assert pending["missing_numeric_fields"] == "total_financing_need_bn"
    assert pending["next_action"] == "attach_financing_release_and_populate_total_financing_need"


def test_build_qra_long_rate_translation_panel_derives_governed_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        publish,
        "build_qra_shock_crosswalk_publish_table",
        lambda: pd.DataFrame(
            [
                {
                    "event_id": "qra_2024_10",
                    "event_date_type": "official_release_date",
                    "schedule_diff_10y_eq_bn": 25.0,
                    "schedule_diff_dynamic_10y_eq_bn": 27.5,
                    "schedule_diff_dv01_usd": 2_600_000.0,
                    "gross_notional_delta_bn": 40.0,
                    "shock_source": "manual_schedule_diff",
                    "shock_review_status": "reviewed",
                    "alternative_treatment_complete": True,
                    "alternative_treatment_missing_fields": "",
                    "alternative_treatment_missing_reason": "",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "build_qra_event_registry_publish_table",
        lambda: pd.DataFrame(
            [
                {
                    "event_id": "qra_2024_10",
                    "quarter": "2024Q4",
                    "quality_tier": "Tier A",
                    "eligibility_blockers": "policy_statement_descriptive_only",
                    "timestamp_precision": "exact_time",
                    "separability_status": "separable_component",
                    "expectation_status": "reviewed_surprise_ready",
                    "contamination_status": "reviewed_clean",
                    "causal_eligible_component_count": 1,
                }
            ]
        ),
    )

    panel = publish.build_qra_long_rate_translation_panel()

    assert set(panel["translation_variant"]) == {"fixed_10y_eq_bn", "dynamic_10y_eq_bn", "dv01_usd"}
    fixed_row = panel.loc[panel["translation_variant"] == "fixed_10y_eq_bn"].iloc[0]
    assert fixed_row["translation_value"] == 25.0
    assert bool(fixed_row["long_rate_pilot_ready"])
    assert fixed_row["duration_assumption_source"] == "fixed_duration_weights"
    assert fixed_row["claim_scope"] == "causal_pilot_only"


def test_build_qra_benchmark_evidence_registry_table_derives_terminal_status_and_claim_scope(monkeypatch) -> None:
    monkeypatch.setattr(
        publish,
        "build_qra_release_component_registry_publish_table",
        lambda: pd.DataFrame(
            [
                {
                    "release_component_id": "qra_2024_10__financing_estimates",
                    "event_id": "qra_2024_10",
                    "quarter": "2025Q1",
                    "component_type": "financing_estimates",
                    "benchmark_timing_status": "pre_release_external",
                    "external_benchmark_ready": True,
                    "expectation_status": "reviewed_surprise_ready",
                    "benchmark_search_disposition": "upgraded_pre_release_external",
                    "contamination_status": "reviewed_clean",
                    "macro_crosswalk_status": "pending_external_crosswalk",
                    "quality_tier": "Tier A",
                    "causal_eligible": True,
                    "eligibility_blockers": "",
                },
                {
                    "release_component_id": "qra_2024_05__financing_estimates",
                    "event_id": "qra_2024_05",
                    "quarter": "2024Q3",
                    "component_type": "financing_estimates",
                    "benchmark_timing_status": "pre_release_external",
                    "external_benchmark_ready": True,
                    "expectation_status": "reviewed_surprise_ready",
                    "benchmark_search_disposition": "upgraded_pre_release_external",
                    "contamination_status": "reviewed_contaminated_context_only",
                    "macro_crosswalk_status": "reviewed_external_overlap",
                    "quality_tier": "Tier B",
                    "causal_eligible": False,
                    "eligibility_blockers": "contamination_context_only",
                },
            ]
        ),
    )

    table = publish.build_qra_benchmark_evidence_registry_table()

    pilot_ready = table.loc[table["release_component_id"] == "qra_2024_10__financing_estimates"].iloc[0]
    context_only = table.loc[table["release_component_id"] == "qra_2024_05__financing_estimates"].iloc[0]

    assert pilot_ready["terminal_disposition"] == "tier_a_causal_pilot_ready"
    assert pilot_ready["claim_scope"] == "causal_pilot_only"
    assert pilot_ready["benchmark_search_disposition"] == "upgraded_pre_release_external"
    assert context_only["terminal_disposition"] == "reviewed_contaminated_context_only"
    assert context_only["claim_scope"] == "descriptive_only"
    assert context_only["macro_crosswalk_status"] == "reviewed_external_overlap"


def test_build_causal_claims_status_table_summarizes_current_sample_pilot(monkeypatch) -> None:
    monkeypatch.setattr(
        publish,
        "build_event_design_status_publish_table",
        lambda: pd.DataFrame(
            [
                {"metric": "current_sample_financing_component_count", "value": 14, "notes": ""},
                {"metric": "current_sample_financing_reviewed_surprise_ready_count", "value": 6, "notes": ""},
                {"metric": "current_sample_financing_tier_a_count", "value": 5, "notes": ""},
                {"metric": "current_sample_financing_reviewed_contaminated_context_only_count", "value": 1, "notes": ""},
                {"metric": "current_sample_financing_post_release_invalid_count", "value": 8, "notes": ""},
                {"metric": "current_sample_financing_source_family_exhausted_count", "value": 8, "notes": ""},
                {"metric": "current_sample_financing_open_candidate_count", "value": 0, "notes": ""},
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "_artifact_mtime",
        lambda path: "2026-03-23T00:00:00+00:00",
    )

    table = publish.build_causal_claims_status_table()
    row = table.iloc[0]

    assert row["claim_id"] == "current_sample_financing_pilot"
    assert row["claim_scope"] == "causal_pilot_only"
    assert row["benchmark_ready_count"] == 6
    assert row["tier_a_count"] == 5
    assert row["context_only_count"] == 1
    assert row["post_release_invalid_count"] == 8
    assert row["source_family_exhausted_count"] == 8
    assert row["open_candidate_count"] == 0
    assert "settled or full-sample causal estimate" in row["cannot_claim"]


def test_build_qra_benchmark_coverage_table_tracks_contamination_and_surprise_states(monkeypatch) -> None:
    monkeypatch.setattr(
        publish,
        "build_qra_release_component_registry_publish_table",
        lambda: pd.DataFrame(
            [
                {
                    "release_component_id": "qra_2024_05__financing_estimates",
                    "quarter": "2024Q3",
                    "component_type": "financing_estimates",
                    "benchmark_timing_status": "pre_release_external",
                    "external_benchmark_ready": True,
                    "expectation_status": "reviewed_surprise_ready",
                    "contamination_status": "reviewed_contaminated_context_only",
                    "causal_eligible": False,
                },
                {
                    "release_component_id": "qra_2024_10__financing_estimates",
                    "quarter": "2025Q1",
                    "component_type": "financing_estimates",
                    "benchmark_timing_status": "post_release_invalid",
                    "external_benchmark_ready": False,
                    "expectation_status": "post_release_invalid",
                    "contamination_status": "reviewed_clean",
                    "causal_eligible": False,
                },
                {
                    "release_component_id": "qra_2024_10__policy_statement",
                    "quarter": "2025Q1",
                    "component_type": "policy_statement",
                    "benchmark_timing_status": "same_release_placeholder",
                    "external_benchmark_ready": False,
                    "expectation_status": "same_release_placeholder",
                    "contamination_status": "pending_review",
                    "causal_eligible": False,
                },
            ]
        ),
    )

    table = publish.build_qra_benchmark_coverage_table()

    def metric(scope: str, name: str) -> int:
        return int(table.loc[(table["scope"] == scope) & (table["metric"] == name), "value"].iloc[0])

    assert metric("current_sample_financing_estimates", "reviewed_surprise_ready_count") == 1
    assert metric("current_sample_financing_estimates", "reviewed_contaminated_context_only_count") == 1
    assert metric("current_sample_financing_estimates", "reviewed_clean_count") == 1
    assert metric("current_sample_all_components", "pending_review_count") == 1


def test_build_qra_event_elasticity_publish_table_is_optional(tmp_path, monkeypatch):
    tables_dir = tmp_path / "tables"
    tables_dir.mkdir(parents=True)
    monkeypatch.setattr(publish, "TABLES_DIR", tables_dir)

    empty = publish.build_qra_event_elasticity_publish_table()
    assert list(empty.columns) == [
        "quarter",
        "event_id",
        "event_label",
        "event_date_requested",
        "event_date_aligned",
        "event_date_type",
        "policy_statement_url",
        "financing_estimates_url",
        "timing_quality",
        "current_quarter_action",
        "forward_guidance_bias",
        "headline_bucket",
        "shock_sign_curated",
        "classification_confidence",
        "classification_review_status",
        "spec_id",
        "treatment_variant",
        "series",
        "window",
        "delta_pp",
        "delta_bp",
        "shock_bn",
        "previous_event_id",
        "previous_quarter",
        "gross_notional_delta_bn",
        "schedule_diff_10y_eq_bn",
        "schedule_diff_dynamic_10y_eq_bn",
        "schedule_diff_dv01_usd",
        "shock_construction",
        "shock_source",
        "shock_notes",
        "shock_review_status",
        "shock_missing_flag",
        "small_denominator_flag",
        "descriptive_headline_reason",
        "usable_for_descriptive_headline",
        "usable_for_headline_reason",
        "review_maturity",
        "elasticity_bp_per_100bn",
        "sign_flip_flag",
        "usable_for_headline",
        "claim_scope",
    ]


def test_build_qra_event_registry_publish_table_includes_causal_summary(tmp_path, monkeypatch):
    tables_dir = tmp_path / "tables"
    tables_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "event_id": "e1",
                "quarter": "2024Q1",
                "release_timestamp_et": "2024-01-31T08:30:00-05:00",
                "release_timestamp_kind": "official_release_date_timestamp_with_time",
                "release_bundle_type": "explicit_multi_stage_release",
                "policy_statement_url": "https://example.com/policy",
                "financing_estimates_url": "https://example.com/financing",
                "timing_quality": "explicit_multi_stage_release",
                "overlap_severity": "none",
                "overlap_label": "",
                "financing_need_news_flag": True,
                "composition_news_flag": True,
                "forward_guidance_flag": False,
                "reviewer": "manual_review",
                "review_date": "2024-02-01",
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
    ).to_csv(tables_dir / "qra_event_registry_v2.csv", index=False)
    monkeypatch.setattr(publish, "TABLES_DIR", tables_dir)

    table = publish.build_qra_event_registry_publish_table()

    row = table.iloc[0]
    assert row["quality_tier"] == "Tier B"
    assert row["timestamp_precision"] == "exact_time"
    assert row["release_component_count"] == 2


def test_build_event_design_status_publish_table_reads_optional_artifact(tmp_path, monkeypatch):
    tables_dir = tmp_path / "tables"
    tables_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {"metric": "tier_a_count", "value": 1, "notes": "Causal-eligible components."},
            {"metric": "tier_b_count", "value": 2, "notes": "Reviewed descriptive-only components."},
        ]
    ).to_csv(tables_dir / "event_design_status.csv", index=False)
    monkeypatch.setattr(publish, "TABLES_DIR", tables_dir)

    table = publish.build_event_design_status_publish_table()

    assert list(table["metric"]) == ["tier_a_count", "tier_b_count"]


def test_build_qra_event_elasticity_publish_table_populated(tmp_path, monkeypatch):
    tables_dir = tmp_path / "tables"
    tables_dir.mkdir(parents=True)
    monkeypatch.setattr(publish, "TABLES_DIR", tables_dir)

    pd.DataFrame(
        [
            {
                "quarter": "2024Q4",
                "event_id": "qra_2024_07",
                "event_label": "2024 Jul QRA",
                "event_date_requested": "2024-07-31",
                "event_date_aligned": "2024-07-31",
                "series": "DGS10",
                "window": "d3",
                "event_date_type": "official_release_date",
                "policy_statement_url": "https://example.com/policy",
                "financing_estimates_url": "https://example.com/financing",
                "timing_quality": "same_day_release_bundle",
                "current_quarter_action": "pending",
                "forward_guidance_bias": "pending",
                "headline_bucket": "pending",
                "shock_sign_curated": "",
                "classification_confidence": "pending",
                "classification_review_status": "pending",
                "delta_pp": 0.12,
                "delta_bp": 12.0,
                "shock_bn": 25.0,
                "previous_event_id": "qra_2024_05",
                "previous_quarter": "2024Q3",
                "gross_notional_delta_bn": 40.0,
                "schedule_diff_10y_eq_bn": 25.0,
                "schedule_diff_dynamic_10y_eq_bn": 27.0,
                "schedule_diff_dv01_usd": 2400000.0,
                "shock_construction": "schedule_diff_primary",
                "shock_source": "manual",
                "shock_notes": "",
                "shock_review_status": "provisional",
                "shock_missing_flag": False,
                "elasticity_bp_per_100bn": 12.0,
                "sign_flip_flag": False,
                "small_denominator_flag": False,
                "usable_for_headline": False,
            }
        ]
    ).to_csv(tables_dir / "qra_event_elasticity.csv", index=False)

    populated = publish.build_qra_event_elasticity_publish_table()
    assert populated.loc[0, "series"] == "DGS10"
    assert populated.loc[0, "elasticity_bp_per_100bn"] == 12.0
    assert populated.loc[0, "schedule_diff_10y_eq_bn"] == 25.0
    assert populated.loc[0, "spec_id"] == "spec_qra_event_v2"
    assert populated.loc[0, "treatment_variant"] == "schedule_diff_primary"
    assert populated.loc[0, "usable_for_headline_reason"] == "classification_not_reviewed"
    assert populated.loc[0, "review_maturity"] == "provisional_supporting"

    diagnostic = publish.build_qra_event_elasticity_diagnostic_publish_table()
    assert diagnostic.loc[0, "event_date_type"] == "official_release_date"


def test_build_qra_event_registry_publish_table_is_optional(tmp_path, monkeypatch):
    tables_dir = tmp_path / "tables"
    tables_dir.mkdir(parents=True)
    monkeypatch.setattr(publish, "TABLES_DIR", tables_dir)

    empty = publish.build_qra_event_registry_publish_table()
    assert "headline_eligibility_reason" in empty.columns

    pd.DataFrame(
        [
            {
                "event_id": "qra_2024_07",
                "quarter": "2024Q3",
                "release_timestamp_et": "2024-07-31T00:00:00-04:00",
                "release_timestamp_kind": "date_proxy",
                "release_bundle_type": "same_day_release_bundle",
                "policy_statement_url": "https://example.com/policy",
                "financing_estimates_url": "https://example.com/financing",
                "timing_quality": "same_day_release_bundle",
                "overlap_severity": "none",
                "overlap_label": "",
                "financing_need_news_flag": True,
                "composition_news_flag": True,
                "forward_guidance_flag": False,
            }
        ]
    ).to_csv(tables_dir / "qra_event_registry_v2.csv", index=False)

    populated = publish.build_qra_event_registry_publish_table()
    assert populated.loc[0, "event_id"] == "qra_2024_07"
    assert populated.loc[0, "spec_id"] == "spec_qra_event_v2"
    assert populated.loc[0, "treatment_variant"] == "canonical_shock_bn"
    assert populated.loc[0, "headline_eligibility_reason"] == "missing_shock_summary"


def test_build_qra_shock_crosswalk_publish_table_is_optional(tmp_path, monkeypatch):
    tables_dir = tmp_path / "tables"
    tables_dir.mkdir(parents=True)
    monkeypatch.setattr(publish, "TABLES_DIR", tables_dir)

    empty = publish.build_qra_shock_crosswalk_publish_table()
    assert "usable_for_headline_reason" in empty.columns

    pd.DataFrame(
        [
            {
                "event_id": "qra_2024_07",
                "event_date_type": "official_release_date",
                "canonical_shock_id": "schedule_diff_primary",
                "shock_bn": 25.0,
                "schedule_diff_10y_eq_bn": 25.0,
                "schedule_diff_dynamic_10y_eq_bn": 30.0,
                "schedule_diff_dv01_usd": 2600000.0,
                "gross_notional_delta_bn": 40.0,
                "shock_source": "manual",
                "manual_override_reason": "",
                "alternative_treatment_complete": False,
                "alternative_treatment_missing_fields": "schedule_diff_dv01_usd",
                "alternative_treatment_missing_reason": "manual_statement_primary_only_pending_alt_treatments",
                "shock_review_status": "reviewed",
            }
        ]
    ).to_csv(tables_dir / "qra_shock_crosswalk_v1.csv", index=False)

    populated = publish.build_qra_shock_crosswalk_publish_table()
    assert populated.loc[0, "canonical_shock_id"] == "schedule_diff_primary"
    assert populated.loc[0, "spec_id"] == "spec_qra_event_v2"
    assert populated.loc[0, "treatment_variant"] == "canonical_shock_bn"
    assert populated.loc[0, "usable_for_headline_reason"] == "missing_shock_summary"
    assert populated.loc[0, "alternative_treatment_missing_reason"] == "manual_statement_primary_only_pending_alt_treatments"


def test_build_event_usability_publish_table_is_optional(tmp_path, monkeypatch):
    tables_dir = tmp_path / "tables"
    tables_dir.mkdir(parents=True)
    monkeypatch.setattr(publish, "TABLES_DIR", tables_dir)

    empty = publish.build_event_usability_publish_table()
    assert "n_events" in empty.columns

    pd.DataFrame(
        [
            {
                "event_date_type": "official_release_date",
                "headline_bucket": "tightening",
                "classification_review_status": "reviewed",
                "shock_review_status": "reviewed",
                "overlap_severity": "none",
                "usable_for_headline": True,
                "n_rows": 2,
                "n_events": 1,
            }
        ]
    ).to_csv(tables_dir / "event_usability_table.csv", index=False)

    populated = publish.build_event_usability_publish_table()
    assert populated.loc[0, "spec_id"] == "spec_qra_event_v2"
    assert populated.loc[0, "treatment_variant"] == "canonical_shock_bn"
    assert populated.loc[0, "n_events"] == 1


def test_build_leave_one_event_out_publish_table_is_optional(tmp_path, monkeypatch):
    tables_dir = tmp_path / "tables"
    tables_dir.mkdir(parents=True)
    monkeypatch.setattr(publish, "TABLES_DIR", tables_dir)

    empty = publish.build_leave_one_event_out_publish_table()
    assert "leave_one_out_delta" in empty.columns

    pd.DataFrame(
        [
            {
                "event_id": "qra_2024_07",
                "event_date_type": "official_release_date",
                "series": "DGS10",
                "window": "d3",
                "n_observations": 3,
                "leave_one_out_coefficient": 0.12,
                "leave_one_out_std_err": 0.03,
                "leave_one_out_delta": 0.01,
            }
        ]
    ).to_csv(tables_dir / "leave_one_event_out_table.csv", index=False)

    populated = publish.build_leave_one_event_out_publish_table()
    assert populated.loc[0, "spec_id"] == "spec_qra_event_v2"
    assert populated.loc[0, "treatment_variant"] == "shock_bn"
    assert populated.loc[0, "leave_one_out_delta"] == 0.01


def test_build_treatment_comparison_publish_table_is_optional(tmp_path, monkeypatch):
    tables_dir = tmp_path / "tables"
    tables_dir.mkdir(parents=True)
    monkeypatch.setattr(publish, "TABLES_DIR", tables_dir)

    empty = publish.build_treatment_comparison_publish_table()
    assert "headline_recommendation_status" in empty.columns

    pd.DataFrame(
        [
            {
                "spec_id": "spec_duration_treatment_v1",
                "event_date_type": "official_release_date",
                "series": "DGS10",
                "window": "d1",
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
    ).to_csv(tables_dir / "treatment_comparison_table.csv", index=False)

    populated = publish.build_treatment_comparison_publish_table()
    assert populated.loc[0, "treatment_variant"] == "canonical_shock_bn"
    assert populated.loc[0, "headline_recommendation_status"] == "retain_canonical_contract"


def test_build_auction_absorption_publish_table_is_optional(tmp_path, monkeypatch):
    tables_dir = tmp_path / "tables"
    tables_dir.mkdir(parents=True)
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir(parents=True)
    monkeypatch.setattr(publish, "TABLES_DIR", tables_dir)
    monkeypatch.setattr(publish, "PROCESSED_DIR", processed_dir)

    empty = publish.build_auction_absorption_publish_table()
    assert "spec_id" in empty.columns

    pd.DataFrame(
        [
            {
                "qra_event_id": "qra_2024_07",
                "quarter": "2024Q3",
                "auction_date": "2024-07-31",
                "security_family": "coupon",
                "investor_class": "indirect",
                "measure": "allotment_amount",
                "value": 12.5,
                "units": "USD billions",
                "source_quality": "summary_ready",
                "provenance_summary": "source_file=investor_allotments_panel.csv",
                "source_family": "investor_allotments",
            }
        ]
    ).to_csv(tables_dir / "auction_absorption_table.csv", index=False)

    populated = publish.build_auction_absorption_publish_table()
    assert populated.loc[0, "qra_event_id"] == "qra_2024_07"
    assert populated.loc[0, "spec_id"] == "spec_auction_absorption_v1"
    assert populated.loc[0, "source_family"] == "investor_allotments"


def test_build_qra_event_shock_components_publish_table_is_optional(tmp_path, monkeypatch):
    tables_dir = tmp_path / "tables"
    tables_dir.mkdir(parents=True)
    monkeypatch.setattr(publish, "TABLES_DIR", tables_dir)

    empty = publish.build_qra_event_shock_components_publish_table()
    assert "contribution_10y_eq_bn" in empty.columns

    pd.DataFrame(
        [
            {
                "event_id": "qra_2023_05",
                "quarter": "2023Q3",
                "previous_event_id": "qra_2023_02",
                "previous_quarter": "2023Q2",
                "tenor": "10Y",
                "issue_type": "nominal_coupon",
                "current_total_bn": 9.0,
                "previous_total_bn": 6.0,
                "delta_bn": 3.0,
                "yield_date": "2023-06-30",
                "yield_curve_source": "fred_constant_maturity_prior_business_day",
                "tenor_yield_pct": 4.1,
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
    ).to_csv(tables_dir / "qra_event_shock_components.csv", index=False)

    populated = publish.build_qra_event_shock_components_publish_table()
    assert populated.loc[0, "tenor"] == "10Y"
    assert populated.loc[0, "yield_curve_source"] == "fred_constant_maturity_prior_business_day"
    assert populated.loc[0, "duration_factor_source"] == "fred_exact"


def test_build_qra_event_shock_summary_publish_table_is_canonical(tmp_path, monkeypatch):
    tables_dir = tmp_path / "tables"
    tables_dir.mkdir(parents=True)
    monkeypatch.setattr(publish, "TABLES_DIR", tables_dir)

    empty = publish.build_qra_event_shock_summary_publish_table()
    assert "usable_for_headline" in empty.columns

    pd.DataFrame(
        [
            {
                "quarter": "2024Q3",
                "event_id": "qra_2024_07",
                "event_label": "2024 Jul QRA",
                "event_date_requested": "2024-07-31",
                "event_date_aligned": "2024-07-31",
                "event_date_type": "official_release_date",
                "policy_statement_url": "https://example.com/policy",
                "financing_estimates_url": "https://example.com/financing",
                "timing_quality": "same_day_release_bundle",
                "current_quarter_action": "tightening",
                "forward_guidance_bias": "neutral",
                "headline_bucket": "tightening",
                "shock_sign_curated": "1",
                "classification_confidence": "exact_statement",
                "classification_review_status": "reviewed",
                "series": "DGS10",
                "window": "d1",
                "delta_pp": 0.10,
                "delta_bp": 10.0,
                "shock_bn": 25.0,
                "previous_event_id": "qra_2024_05",
                "previous_quarter": "2024Q2",
                "gross_notional_delta_bn": 40.0,
                "schedule_diff_10y_eq_bn": 25.0,
                "schedule_diff_dynamic_10y_eq_bn": 30.0,
                "schedule_diff_dv01_usd": 2600000.0,
                "shock_construction": "schedule_diff_primary",
                "shock_source": "manual",
                "shock_notes": "",
                "shock_review_status": "reviewed",
                "shock_missing_flag": False,
                "small_denominator_flag": False,
                "elasticity_bp_per_100bn": 40.0,
                "sign_flip_flag": False,
                "usable_for_headline": True,
            },
            {
                "quarter": "2024Q3",
                "event_id": "qra_2024_07",
                "event_label": "2024 Jul QRA",
                "event_date_requested": "2024-07-31",
                "event_date_aligned": "2024-07-31",
                "event_date_type": "official_release_date",
                "policy_statement_url": "https://example.com/policy",
                "financing_estimates_url": "https://example.com/financing",
                "timing_quality": "same_day_release_bundle",
                "current_quarter_action": "tightening",
                "forward_guidance_bias": "neutral",
                "headline_bucket": "tightening",
                "shock_sign_curated": "1",
                "classification_confidence": "exact_statement",
                "classification_review_status": "reviewed",
                "series": "DGS10",
                "window": "d3",
                "delta_pp": 0.12,
                "delta_bp": 12.0,
                "shock_bn": 25.0,
                "previous_event_id": "qra_2024_05",
                "previous_quarter": "2024Q2",
                "gross_notional_delta_bn": 40.0,
                "schedule_diff_10y_eq_bn": 25.0,
                "schedule_diff_dynamic_10y_eq_bn": 30.0,
                "schedule_diff_dv01_usd": 2600000.0,
                "shock_construction": "schedule_diff_primary",
                "shock_source": "manual",
                "shock_notes": "",
                "shock_review_status": "reviewed",
                "shock_missing_flag": False,
                "small_denominator_flag": False,
                "elasticity_bp_per_100bn": 48.0,
                "sign_flip_flag": False,
                "usable_for_headline": True,
            },
            {
                "quarter": "2024Q3",
                "event_id": "qra_2024_07",
                "event_label": "2024 Jul QRA",
                "event_date_requested": "2024-07-30",
                "event_date_aligned": "2024-07-30",
                "event_date_type": "market_pricing_marker_minus_1d",
                "policy_statement_url": "https://example.com/policy",
                "financing_estimates_url": "https://example.com/financing",
                "timing_quality": "same_day_release_bundle",
                "current_quarter_action": "tightening",
                "forward_guidance_bias": "neutral",
                "headline_bucket": "tightening",
                "shock_sign_curated": "1",
                "classification_confidence": "exact_statement",
                "classification_review_status": "reviewed",
                "series": "DGS10",
                "window": "d1",
                "delta_pp": 0.10,
                "delta_bp": 10.0,
                "shock_bn": 25.0,
                "previous_event_id": "qra_2024_05",
                "previous_quarter": "2024Q2",
                "gross_notional_delta_bn": 40.0,
                "schedule_diff_10y_eq_bn": 25.0,
                "schedule_diff_dynamic_10y_eq_bn": 30.0,
                "schedule_diff_dv01_usd": 2600000.0,
                "shock_construction": "schedule_diff_primary",
                "shock_source": "manual",
                "shock_notes": "",
                "shock_review_status": "reviewed",
                "shock_missing_flag": False,
                "small_denominator_flag": False,
                "elasticity_bp_per_100bn": 40.0,
                "sign_flip_flag": False,
                "usable_for_headline": False,
            },
        ]
    ).to_csv(tables_dir / "qra_event_elasticity.csv", index=False)

    summary = publish.build_qra_event_shock_summary_publish_table()
    assert len(summary) == 1
    assert summary.loc[0, "event_date_type"] == "official_release_date"
    assert summary.loc[0, "schedule_diff_10y_eq_bn"] == 25.0
    assert summary.loc[0, "schedule_diff_dynamic_10y_eq_bn"] == 30.0
    assert summary.loc[0, "schedule_diff_dv01_usd"] == 2600000.0
    assert summary.loc[0, "usable_for_headline"]


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
    assert not bool(investor["headline_ready"])
    assert investor["public_role"] == "supporting"
    assert primary["backend_status"] == "summary_ready"
    assert primary["readiness_tier"] == "summary_ready"
    assert not bool(primary["headline_ready"])
    assert primary["public_role"] == "supporting"


def test_dataset_status_derives_summary_ready_from_extension_status_rows(monkeypatch, tmp_path) -> None:
    fake_extension_status = pd.DataFrame(
        [
            {
                "extension": "investor_allotments",
                "backend_status": "summary_ready",
                "readiness_tier": "summary_ready",
                "headline_ready": False,
                "public_role": "supporting",
            },
            {
                "extension": "primary_dealer",
                "backend_status": "summary_ready",
                "readiness_tier": "summary_ready",
                "headline_ready": False,
                "public_role": "supporting",
            },
            {
                "extension": "sec_nmfp",
                "backend_status": "processed",
                "readiness_tier": "inventory_ready",
                "headline_ready": False,
                "public_role": "supporting",
            },
            {
                "extension": "tic",
                "backend_status": "not_started",
                "readiness_tier": "not_started",
                "headline_ready": False,
                "public_role": "supporting",
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

    official_capture = table.loc[table["dataset"] == "official_capture"].iloc[0]
    qra_elasticity = table.loc[table["dataset"] == "qra_event_elasticity"].iloc[0]
    treatment = table.loc[table["dataset"] == "treatment_comparison_table"].iloc[0]
    investor = table.loc[table["dataset"] == "extension_investor_allotments"].iloc[0]
    primary = table.loc[table["dataset"] == "extension_primary_dealer"].iloc[0]

    assert not bool(official_capture["headline_ready"])
    assert official_capture["public_role"] == "supporting"
    assert qra_elasticity["readiness_tier"] == "not_started"
    assert qra_elasticity["review_maturity"] == "not_started"
    assert treatment["readiness_tier"] == "fallback_only"
    assert investor["readiness_tier"] == "summary_ready"
    assert primary["readiness_tier"] == "summary_ready"
    assert investor["source_quality"] == "summary_ready"
    assert primary["source_quality"] == "summary_ready"
    assert not bool(investor["headline_ready"])
    assert not bool(primary["headline_ready"])
    assert not bool(investor["fallback_only"])
    assert not bool(primary["fallback_only"])
    assert investor["public_role"] == "supporting"
    assert primary["public_role"] == "supporting"


def test_dataset_status_keeps_official_capture_headline_ready_with_seed_history(monkeypatch) -> None:
    monkeypatch.setattr(
        publish,
        "build_extension_status_table",
        lambda: pd.DataFrame(
            [
                {
                    "extension": "investor_allotments",
                    "backend_status": "summary_ready",
                    "readiness_tier": "summary_ready",
                    "headline_ready": False,
                    "public_role": "supporting",
                },
                {
                    "extension": "primary_dealer",
                    "backend_status": "summary_ready",
                    "readiness_tier": "summary_ready",
                    "headline_ready": False,
                    "public_role": "supporting",
                },
                {
                    "extension": "sec_nmfp",
                    "backend_status": "summary_ready",
                    "readiness_tier": "summary_ready",
                    "headline_ready": False,
                    "public_role": "supporting",
                },
                {
                    "extension": "tic",
                    "backend_status": "not_started",
                    "readiness_tier": "not_started",
                    "headline_ready": False,
                    "public_role": "supporting",
                },
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "build_official_capture_readiness_table",
        lambda: pd.DataFrame(
            [
                {
                    "headline_ready": True,
                    "fallback_only": False,
                    "missing_critical_fields": "",
                    "source_quality": "exact_official",
                },
                {
                    "headline_ready": False,
                    "fallback_only": True,
                    "missing_critical_fields": "total_financing_need_bn",
                    "source_quality": "seed_only",
                },
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "_official_ati_headline_table",
        lambda: pd.DataFrame([{"quarter": "2024Q3", "ati_baseline_bn": 1.0}]),
    )
    monkeypatch.setattr(
        publish,
        "build_official_capture_backfill_queue_table",
        lambda: pd.DataFrame(),
    )

    table = publish.build_dataset_status_table()
    official_capture = table.loc[table["dataset"] == "official_capture"].iloc[0]
    backfill_queue = table.loc[table["dataset"] == "official_capture_backfill_queue"].iloc[0]

    assert bool(official_capture["headline_ready"])
    assert not bool(official_capture["fallback_only"])
    assert official_capture["readiness_tier"] == "headline_ready"
    assert official_capture["source_quality"] == "exact_official_window_plus_seed_history"
    assert official_capture["missing_critical_fields"] == ""
    assert backfill_queue["readiness_tier"] == "not_started"


def test_dataset_status_marks_qra_elasticity_provisional_when_published(monkeypatch, tmp_path) -> None:
    fake_extension_status = pd.DataFrame(
        [
            {
                "extension": "investor_allotments",
                "backend_status": "summary_ready",
                "readiness_tier": "summary_ready",
                "headline_ready": False,
                "public_role": "supporting",
            },
            {
                "extension": "primary_dealer",
                "backend_status": "summary_ready",
                "readiness_tier": "summary_ready",
                "headline_ready": False,
                "public_role": "supporting",
            },
            {
                "extension": "sec_nmfp",
                "backend_status": "processed",
                "readiness_tier": "inventory_ready",
                "headline_ready": False,
                "public_role": "supporting",
            },
            {
                "extension": "tic",
                "backend_status": "not_started",
                "readiness_tier": "not_started",
                "headline_ready": False,
                "public_role": "supporting",
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
    (tables_dir / "qra_event_elasticity.csv").write_text(
        "quarter,event_id,event_date_type,series,window,headline_bucket,classification_review_status,shock_review_status,usable_for_headline,elasticity_bp_per_100bn\n"
        "2024Q3,qra_2024_07,official_release_date,DGS10,d3,tightening,reviewed,reviewed,true,12.0\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(publish, "PROCESSED_DIR", processed_dir)
    monkeypatch.setattr(publish, "TABLES_DIR", tables_dir)

    table = publish.build_dataset_status_table()

    qra_elasticity = table.loc[table["dataset"] == "qra_event_elasticity"].iloc[0]
    treatment = table.loc[table["dataset"] == "treatment_comparison_table"].iloc[0]
    assert qra_elasticity["readiness_tier"] == "supporting_provisional"
    assert qra_elasticity["review_maturity"] == "provisional_supporting"
    assert treatment["readiness_tier"] == "supporting_ready"


def test_dataset_status_ignores_treatment_variant_rows_when_checking_qra_duplicates(monkeypatch, tmp_path) -> None:
    fake_extension_status = pd.DataFrame(
        [
            {
                "extension": "investor_allotments",
                "backend_status": "summary_ready",
                "readiness_tier": "summary_ready",
                "headline_ready": False,
                "public_role": "supporting",
            }
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
    pd.DataFrame(
        [
            {
                "quarter": "2024Q3",
                "event_id": "qra_2024_07",
                "event_date_type": "official_release_date",
                "series": "DGS10",
                "window": "d3",
                "treatment_variant": "canonical_shock_bn",
                "headline_bucket": "tightening",
                "classification_review_status": "reviewed",
                "shock_review_status": "reviewed",
                "usable_for_headline": True,
                "elasticity_bp_per_100bn": 12.0,
            },
            {
                "quarter": "2024Q3",
                "event_id": "qra_2024_07",
                "event_date_type": "official_release_date",
                "series": "DGS10",
                "window": "d3",
                "treatment_variant": "fixed_10y_eq_bn",
                "headline_bucket": "tightening",
                "classification_review_status": "reviewed",
                "shock_review_status": "reviewed",
                "usable_for_headline": False,
                "elasticity_bp_per_100bn": 8.0,
            },
        ]
    ).to_csv(tables_dir / "qra_event_elasticity.csv", index=False)

    monkeypatch.setattr(publish, "PROCESSED_DIR", processed_dir)
    monkeypatch.setattr(publish, "TABLES_DIR", tables_dir)

    table = publish.build_dataset_status_table()
    qra_elasticity = table.loc[table["dataset"] == "qra_event_elasticity"].iloc[0]

    assert qra_elasticity["readiness_tier"] == "supporting_provisional"
    assert "missing_review_surface" in qra_elasticity["missing_critical_fields"]


def test_qra_review_surface_integrity_ignores_t_minus_one_usability_rows() -> None:
    registry = pd.DataFrame(
        [
            {
                "event_id": "qra_2023_05",
                "headline_eligibility_reason": "usable",
            }
        ]
    )
    crosswalk = pd.DataFrame(
        [
            {
                "event_id": "qra_2023_05",
                "event_date_type": "official_release_date",
                "usable_for_headline_reason": "usable",
                "spec_id": "spec_qra_event_v2",
                "treatment_variant": "canonical_shock_bn",
            }
        ]
    )
    event_usability = pd.DataFrame(
        [
            {
                "event_date_type": "official_release_date",
                "headline_bucket": "easing",
                "classification_review_status": "reviewed",
                "shock_review_status": "reviewed",
                "overlap_severity": "none",
                "usable_for_headline": True,
                "usable_for_headline_reason": "usable",
                "event_count": 1,
                "treatment_variant": "canonical_shock_bn",
            },
            {
                "event_date_type": "market_pricing_marker_minus_1d",
                "headline_bucket": "easing",
                "classification_review_status": "reviewed",
                "shock_review_status": "reviewed",
                "overlap_severity": "none",
                "usable_for_headline": False,
                "usable_for_headline_reason": "non_official_event_date_type",
                "event_count": 1,
                "treatment_variant": "canonical_shock_bn",
            },
        ]
    )
    shock_summary = pd.DataFrame(
        [
            {
                "event_id": "qra_2023_05",
                "event_date_type": "official_release_date",
                "headline_bucket": "easing",
                "classification_review_status": "reviewed",
                "shock_review_status": "reviewed",
                "usable_for_headline": True,
                "usable_for_headline_reason": "usable",
                "spec_id": "spec_qra_event_v2",
                "treatment_variant": "canonical_shock_bn",
            }
        ]
    )
    qra_event_robustness = pd.DataFrame(
        [
            {"sample_variant": "all_events"},
            {"sample_variant": "overlap_excluded"},
        ]
    )

    result = publish._qra_review_surface_integrity(
        qra_event_registry=registry,
        qra_shock_crosswalk=crosswalk,
        event_usability=event_usability,
        qra_shock_summary=shock_summary,
        qra_event_robustness=qra_event_robustness,
    )

    assert result == {"ready": True, "issues": []}


def test_dataset_status_uses_qra_robustness_publish_table(monkeypatch) -> None:
    monkeypatch.setattr(
        publish,
        "build_extension_status_table",
        lambda: pd.DataFrame(
            [
                {
                    "extension": "investor_allotments",
                    "backend_status": "summary_ready",
                    "readiness_tier": "summary_ready",
                    "headline_ready": False,
                    "public_role": "supporting",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "build_official_capture_readiness_table",
        lambda: pd.DataFrame(
            [
                {
                    "headline_ready": True,
                    "fallback_only": False,
                    "missing_critical_fields": "",
                    "source_quality": "exact_official",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "_official_ati_headline_table",
        lambda: pd.DataFrame([{"quarter": "2024Q3", "ati_baseline_bn": 1.0}]),
    )
    monkeypatch.setattr(
        publish,
        "_qra_elasticity_readiness",
        lambda path: {
            "readiness_tier": "not_started",
            "fallback_only": True,
            "missing_critical_fields": "",
        },
    )
    monkeypatch.setattr(
        publish,
        "build_qra_event_registry_publish_table",
        lambda: pd.DataFrame(
            [
                {
                    "event_id": "qra_2023_05",
                    "headline_eligibility_reason": "usable",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "build_qra_release_component_registry_publish_table",
        lambda: pd.DataFrame([{"release_component_id": "qra_2023_05__policy_statement"}]),
    )
    monkeypatch.setattr(
        publish,
        "build_qra_causal_qa_publish_table",
        lambda: pd.DataFrame([{"event_id": "qra_2023_05"}]),
    )
    monkeypatch.setattr(
        publish,
        "build_event_design_status_publish_table",
        lambda: pd.DataFrame(
            [
                {"metric": "tier_a_count", "value": 0, "notes": "No causal-eligible components yet."},
                {"metric": "reviewed_surprise_ready_count", "value": 0, "notes": "No reviewed surprise-ready components yet."},
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "build_qra_shock_crosswalk_publish_table",
        lambda: pd.DataFrame(
            [
                {
                    "event_id": "qra_2023_05",
                    "event_date_type": "official_release_date",
                    "usable_for_headline_reason": "usable",
                    "spec_id": "spec_qra_event_v2",
                    "treatment_variant": "canonical_shock_bn",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "build_event_usability_publish_table",
        lambda: pd.DataFrame(
            [
                {
                    "event_date_type": "official_release_date",
                    "headline_bucket": "easing",
                    "classification_review_status": "reviewed",
                    "shock_review_status": "reviewed",
                    "overlap_severity": "none",
                    "usable_for_headline": True,
                    "usable_for_headline_reason": "usable",
                    "event_count": 1,
                    "treatment_variant": "canonical_shock_bn",
                },
                {
                    "event_date_type": "market_pricing_marker_minus_1d",
                    "headline_bucket": "easing",
                    "classification_review_status": "reviewed",
                    "shock_review_status": "reviewed",
                    "overlap_severity": "none",
                    "usable_for_headline": False,
                    "usable_for_headline_reason": "non_official_event_date_type",
                    "event_count": 1,
                    "treatment_variant": "canonical_shock_bn",
                },
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "build_qra_event_shock_summary_publish_table",
        lambda: pd.DataFrame(
            [
                {
                    "event_id": "qra_2023_05",
                    "event_date_type": "official_release_date",
                    "headline_bucket": "easing",
                    "classification_review_status": "reviewed",
                    "shock_review_status": "reviewed",
                    "usable_for_headline": True,
                    "usable_for_headline_reason": "usable",
                    "spec_id": "spec_qra_event_v2",
                    "treatment_variant": "canonical_shock_bn",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "build_qra_robustness_publish_table",
        lambda: pd.DataFrame(
            [
                {"sample_variant": "all_events"},
                {"sample_variant": "overlap_excluded"},
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "build_treatment_comparison_publish_table",
        lambda: pd.DataFrame([{"spec_id": "spec_qra_event_v2"}]),
    )
    monkeypatch.setattr(
        publish,
        "build_leave_one_event_out_publish_table",
        lambda: pd.DataFrame([{"event_id": "qra_2023_05"}]),
    )
    monkeypatch.setattr(
        publish,
        "build_auction_absorption_publish_table",
        lambda: pd.DataFrame([{"event_id": "qra_2023_05"}]),
    )

    table = publish.build_dataset_status_table()

    registry = table.loc[table["dataset"] == "qra_event_registry_v2"].iloc[0]
    component_registry = table.loc[table["dataset"] == "qra_release_component_registry"].iloc[0]
    causal_qa = table.loc[table["dataset"] == "qra_causal_qa_ledger"].iloc[0]
    design_status = table.loc[table["dataset"] == "event_design_status"].iloc[0]
    usability = table.loc[table["dataset"] == "event_usability_table"].iloc[0]

    assert registry["readiness_tier"] == "supporting_provisional"
    assert component_registry["readiness_tier"] == "supporting_provisional"
    assert causal_qa["readiness_tier"] == "supporting_provisional"
    assert design_status["readiness_tier"] == "supporting_provisional"
    assert usability["readiness_tier"] == "supporting_ready"
    assert registry["missing_critical_fields"] == "no_tier_a_components|no_reviewed_surprise_components"
    assert usability["missing_critical_fields"] == ""


def test_dataset_status_keeps_qra_elasticity_provisional_when_review_surface_is_ready(monkeypatch) -> None:
    monkeypatch.setattr(
        publish,
        "build_extension_status_table",
        lambda: pd.DataFrame(
            [
                {
                    "extension": "investor_allotments",
                    "backend_status": "summary_ready",
                    "readiness_tier": "summary_ready",
                    "headline_ready": False,
                    "public_role": "supporting",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "build_official_capture_readiness_table",
        lambda: pd.DataFrame(
            [
                {
                    "headline_ready": True,
                    "fallback_only": False,
                    "missing_critical_fields": "",
                    "source_quality": "exact_official",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "_official_ati_headline_table",
        lambda: pd.DataFrame([{"quarter": "2024Q3", "ati_baseline_bn": 1.0}]),
    )
    monkeypatch.setattr(
        publish,
        "_qra_elasticity_readiness",
        lambda path: {
            "readiness_tier": "supporting_ready",
            "fallback_only": False,
            "missing_critical_fields": "",
        },
    )
    monkeypatch.setattr(
        publish,
        "build_qra_event_registry_publish_table",
        lambda: pd.DataFrame(
            [
                {
                    "event_id": "qra_2023_05",
                    "headline_eligibility_reason": "usable",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "build_qra_shock_crosswalk_publish_table",
        lambda: pd.DataFrame(
            [
                {
                    "event_id": "qra_2023_05",
                    "event_date_type": "official_release_date",
                    "usable_for_headline_reason": "usable",
                    "spec_id": "spec_qra_event_v2",
                    "treatment_variant": "canonical_shock_bn",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "build_event_usability_publish_table",
        lambda: pd.DataFrame(
            [
                {
                    "event_date_type": "official_release_date",
                    "headline_bucket": "easing",
                    "classification_review_status": "reviewed",
                    "shock_review_status": "reviewed",
                    "overlap_severity": "none",
                    "usable_for_headline": True,
                    "usable_for_headline_reason": "usable",
                    "event_count": 1,
                    "treatment_variant": "canonical_shock_bn",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "build_qra_event_shock_summary_publish_table",
        lambda: pd.DataFrame(
            [
                {
                    "event_id": "qra_2023_05",
                    "event_date_type": "official_release_date",
                    "headline_bucket": "easing",
                    "classification_review_status": "reviewed",
                    "shock_review_status": "reviewed",
                    "usable_for_headline": True,
                    "usable_for_headline_reason": "usable",
                    "spec_id": "spec_qra_event_v2",
                    "treatment_variant": "canonical_shock_bn",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "build_qra_robustness_publish_table",
        lambda: pd.DataFrame(
            [
                {"sample_variant": "all_events"},
                {"sample_variant": "overlap_excluded"},
            ]
        ),
    )
    monkeypatch.setattr(
        publish,
        "build_treatment_comparison_publish_table",
        lambda: pd.DataFrame([{"spec_id": "spec_qra_event_v2"}]),
    )
    monkeypatch.setattr(
        publish,
        "build_leave_one_event_out_publish_table",
        lambda: pd.DataFrame([{"event_id": "qra_2023_05"}]),
    )
    monkeypatch.setattr(
        publish,
        "build_auction_absorption_publish_table",
        lambda: pd.DataFrame([{"event_id": "qra_2023_05"}]),
    )

    table = publish.build_dataset_status_table()
    qra_elasticity = table.loc[table["dataset"] == "qra_event_elasticity"].iloc[0]

    assert qra_elasticity["readiness_tier"] == "supporting_provisional"
    assert bool(qra_elasticity["fallback_only"])
    assert qra_elasticity["missing_critical_fields"] == ""


def test_series_metadata_catalog_marks_extensions_supporting() -> None:
    catalog = publish.build_series_metadata_catalog()

    extensions = catalog.loc[
        catalog["dataset"].isin(["investor_allotments", "primary_dealer", "sec_nmfp"])
    ].copy()
    assert not extensions.empty
    assert set(extensions["series_role"]) == {"supporting"}
    assert set(extensions["public_role"]) == {"supporting"}


def test_build_pricing_publish_tables_and_dataset_status(tmp_path, monkeypatch) -> None:
    tables_dir = tmp_path / "tables"
    processed_dir = tmp_path / "processed"
    output_dir = tmp_path / "output"
    raw_dir = tmp_path / "raw"
    tables_dir.mkdir(parents=True)
    processed_dir.mkdir(parents=True)
    output_dir.mkdir(parents=True)
    raw_dir.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "spec_id": "release_flow_baseline_63bd",
                "spec_family": "release_flow",
                "headline_flag": True,
                "anchor_role": "credibility_anchor",
                "window_definition": "release_plus_63bd",
                "sample_start": "2009-01-31",
                "sample_end": "2026-03-31",
                "outcome": "DGS10",
                "predictor_set": "ati_baseline_bn",
                "control_set": "delta_dff_release_plus_63bd|debt_limit_dummy",
                "frequency": "release-event",
                "notes": "Primary unique-release fixed-horizon specification using Maturity-Tilt Flow.",
            }
        ]
    ).to_csv(tables_dir / "pricing_spec_registry.csv", index=False)
    pd.DataFrame(
        [
            {
                "model_id": "release_flow_baseline_63bd",
                "model_family": "release_flow",
                "model_mode": "headline_baseline",
                "panel_key": "pricing_release_flow_panel",
                "panel_frequency": "release-event",
                "window_definition": "release_plus_63bd",
                "anchor_role": "credibility_anchor",
                "dependent_variable": "DGS10",
                "dependent_label": "10-year Treasury constant maturity yield",
                "outcome_role": "headline",
                "term": "ati_baseline_bn",
                "coef": -0.05,
                "std_err": 0.02,
                "t_stat": -2.5,
                "p_value": 0.03,
                "nobs": 68,
                "effective_shock_count": 68,
                "rsquared": 0.2,
                "term_role": "primary_predictor",
                "term_label": "Maturity-Tilt Flow (USD bn)",
                "term_units": "USD 100bn",
                "outcome_units": "basis points",
                "cov_type": "HAC",
                "cov_maxlags": 4,
                "term_mode": "baseline",
                "sample_start": "2009-01-31",
                "sample_end": "2026-03-31",
                "notes": "Primary unique-release fixed-horizon specification using Maturity-Tilt Flow.",
            }
        ]
    ).to_csv(tables_dir / "pricing_regression_summary.csv", index=False)
    pd.DataFrame(
        [
            {
                "spec_id": "release_flow_baseline_63bd",
                "spec_family": "release_flow",
                "variant_id": "post_2014",
                "variant_family": "post_2014",
                "frequency": "release-event",
                "window_definition": "release_plus_63bd",
                "dependent_variable": "DGS10",
                "dependent_label": "10-year Treasury constant maturity yield",
                "outcome_role": "headline",
                "term": "ati_baseline_bn",
                "term_label": "Maturity-Tilt Flow (USD bn)",
                "coef": -0.04,
                "std_err": 0.02,
                "t_stat": -2.0,
                "p_value": 0.05,
                "nobs": 45,
                "effective_shock_count": 45,
                "rsquared": 0.18,
                "cov_type": "HAC",
                "cov_maxlags": 4,
                "sample_start": "2014-01-31",
                "sample_end": "2026-03-31",
                "notes": "Post-2014 release-level flow baseline.",
            }
        ]
    ).to_csv(tables_dir / "pricing_subsample_grid.csv", index=False)
    pd.DataFrame(
        [
            {
                "dependent_variable": "DGS10",
                "dependent_label": "10-year Treasury constant maturity yield",
                "model_id": "monthly_stock_baseline_standardized",
                "model_family": "monthly_stock",
                "variant_id": "standardized_predictors",
                "variant_family": "standardized_predictors",
                "panel_frequency": "monthly",
                "window_definition": "carry_forward_monthly",
                "term": "stock_excess_bills_bn",
                "coef": 0.01,
                "std_err": 0.01,
                "t_stat": 1.0,
                "p_value": 0.30,
                "nobs": 200,
                "effective_shock_count": 200,
                "rsquared": 0.2,
                "term_role": "primary_predictor",
                "term_label": "Excess Bills Stock (USD bn) (1 SD)",
                "term_units": "standard deviations",
                "outcome_units": "basis points",
                "cov_type": "HAC",
                "cov_maxlags": 4,
                "term_mode": "standardized_predictors",
                "model_mode": "robustness",
                "sample_start": "2009-01-31",
                "sample_end": "2026-03-31",
                "notes": "Monthly stock baseline standardized.",
            }
        ]
    ).to_csv(tables_dir / "pricing_regression_robustness.csv", index=False)
    pd.DataFrame(
        [
            {
                "scenario_id": "plus_100bn_duration_supply",
                "scenario_label": "Plus $100bn Public Duration Supply shock",
                "scenario_role": "supporting",
                "scenario_shock_bn": 100.0,
                "scenario_shock_scale_bn": 100.0,
                "model_id": "weekly_duration_baseline",
                "model_family": "weekly_duration",
                "dependent_variable": "DGS10",
                "dependent_label": "10-year Treasury constant maturity yield",
                "outcome_role": "headline",
                "term": "headline_public_duration_supply",
                "term_label": "Public Duration Supply (USD bn)",
                "coef_bp_per_100bn": -0.05,
                "implied_bp_change": -0.05,
                "nobs": 200,
                "effective_shock_count": 200,
                "p_value": 0.03,
                "notes": "Weekly duration baseline.",
            }
        ]
    ).to_csv(tables_dir / "pricing_scenario_translation.csv", index=False)
    pd.DataFrame(
        [
            {
                "release_id": "2024-01-15__2024Q1",
                "quarter": "2024Q1",
                "source_quarters": "2024Q1",
                "release_row_count": 1,
                "qra_release_date": "2024-01-15",
                "market_pricing_marker_minus_1d": "2024-01-12",
                "release_plus_63bd_end_date": "2024-04-11",
                "release_plus_21bd_end_date": "2024-02-13",
                "bill_share": 0.20,
                "ati_baseline_bn": 2.0,
                "ati_baseline_bn_posonly": 2.0,
                "debt_limit_dummy": 0,
                "target_tau": 0.18,
                "DGS10": 400.0,
                "THREEFYTP10": 120.0,
                "DGS30": 430.0,
                "delta_dgs10_release_plus_63bd": -4.0,
                "delta_threefytp10_release_plus_63bd": -2.0,
                "delta_dgs30_release_plus_63bd": -3.0,
                "delta_dff_release_plus_63bd": 0.1,
                "delta_dgs10_release_plus_21bd": -2.0,
                "delta_threefytp10_release_plus_21bd": -1.0,
                "delta_dgs30_release_plus_21bd": -1.5,
                "delta_dff_release_plus_21bd": 0.05,
                "release_minus_21bd_to_minus_1bd_start_date": "2023-12-15",
                "delta_dgs10_release_minus_21bd_to_minus_1bd": 0.2,
                "delta_threefytp10_release_minus_21bd_to_minus_1bd": 0.1,
                "delta_dgs30_release_minus_21bd_to_minus_1bd": 0.15,
                "delta_dff_release_minus_21bd_to_minus_1bd": 0.01,
            }
        ]
    ).to_csv(processed_dir / "pricing_release_flow_panel.csv", index=False)
    pd.DataFrame(
        [
            {
                "spec_id": "release_flow_baseline_63bd",
                "window_definition": "release_plus_63bd",
                "dependent_variable": "DGS10",
                "dependent_label": "10-year Treasury constant maturity yield",
                "omitted_release_id": "2024-01-15__2024Q1",
                "coef": -0.05,
                "std_err": 0.02,
                "t_stat": -2.5,
                "p_value": 0.03,
                "nobs": 40,
                "effective_shock_count": 40,
                "sample_start": "2009-01-31",
                "sample_end": "2026-03-31",
                "notes": "Release leave-one-out diagnostic.",
            }
        ]
    ).to_csv(tables_dir / "pricing_release_flow_leave_one_out.csv", index=False)
    pd.DataFrame(
        [
            {
                "tau": 0.18,
                "model_id": "monthly_stock_tau_18",
                "model_family": "monthly_stock_tau_sensitivity",
                "dependent_variable": "DGS10",
                "dependent_label": "10-year Treasury constant maturity yield",
                "term": "stock_excess_bills_bn",
                "term_label": "Excess Bills Stock (USD bn)",
                "coef": 0.01,
                "std_err": 0.02,
                "t_stat": 0.5,
                "p_value": 0.61,
                "nobs": 200,
                "effective_shock_count": 200,
                "rsquared": 0.2,
                "sample_start": "2009-01-31",
                "sample_end": "2026-03-31",
                "notes": "Tau sensitivity.",
            }
        ]
    ).to_csv(tables_dir / "pricing_tau_sensitivity_grid.csv", index=False)

    monkeypatch.setattr(publish, "TABLES_DIR", tables_dir)
    monkeypatch.setattr(publish, "PROCESSED_DIR", processed_dir)
    monkeypatch.setattr(publish, "OUTPUT_DIR", output_dir)
    monkeypatch.setattr(publish, "RAW_DIR", raw_dir)

    spec_registry = publish.build_pricing_spec_registry_publish_table()
    summary = publish.build_pricing_regression_summary_publish_table()
    subsample = publish.build_pricing_subsample_grid_publish_table()
    robustness = publish.build_pricing_regression_robustness_publish_table()
    scenarios = publish.build_pricing_scenario_translation_publish_table()
    release_flow_panel = publish.build_pricing_release_flow_panel_publish_table()
    leave_one_out = publish.build_pricing_release_flow_leave_one_out_publish_table()
    tau_grid = publish.build_pricing_tau_sensitivity_grid_publish_table()

    assert list(spec_registry["spec_id"]) == ["release_flow_baseline_63bd"]
    assert list(summary["model_id"]) == ["release_flow_baseline_63bd"]
    assert list(subsample["variant_id"]) == ["post_2014"]
    assert list(robustness["term"]) == ["stock_excess_bills_bn"]
    assert list(scenarios["scenario_id"]) == ["plus_100bn_duration_supply"]
    assert list(release_flow_panel["release_id"]) == ["2024-01-15__2024Q1"]
    assert list(leave_one_out["omitted_release_id"]) == ["2024-01-15__2024Q1"]
    assert list(tau_grid["tau"]) == [0.18]

    dataset_status = publish.build_dataset_status_table()
    pricing_row = dataset_status.loc[dataset_status["dataset"] == "pricing"].iloc[0]
    assert pricing_row["readiness_tier"] == "supporting_provisional"
    assert pricing_row["public_role"] == "supporting"
    assert bool(pricing_row["fallback_only"])
    assert "pricing_spec_registry" in set(dataset_status["dataset"])
    assert "pricing_subsample_grid" in set(dataset_status["dataset"])
    assert "pricing_release_flow_panel" in set(dataset_status["dataset"])
    assert "pricing_tau_sensitivity_grid" in set(dataset_status["dataset"])


def test_series_metadata_catalog_includes_pricing_series() -> None:
    catalog = publish.build_series_metadata_catalog()

    pricing_rows = catalog.loc[
        catalog["dataset"].isin(["pricing", "pricing_scenario_translation", "pricing_spec_registry", "pricing_subsample_grid"])
    ].copy()

    assert not pricing_rows.empty
    assert {"ati_baseline_bn", "stock_excess_bills_bn", "headline_public_duration_supply", "THREEFYTP10", "DGS10"}.issubset(
        set(pricing_rows["series_id"])
    )
    assert {
        "pricing_spec_registry",
        "pricing_subsample_grid",
        "pricing_release_flow_panel",
        "pricing_release_flow_leave_one_out",
        "pricing_tau_sensitivity_grid",
    }.issubset(set(pricing_rows["series_id"]))
    assert set(pricing_rows["public_role"]) == {"supporting"}
