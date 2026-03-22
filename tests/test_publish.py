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
        "usable_for_headline_reason",
        "review_maturity",
        "elasticity_bp_per_100bn",
        "sign_flip_flag",
        "usable_for_headline",
    ]

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


def test_series_metadata_catalog_marks_extensions_supporting() -> None:
    catalog = publish.build_series_metadata_catalog()

    extensions = catalog.loc[
        catalog["dataset"].isin(["investor_allotments", "primary_dealer", "sec_nmfp"])
    ].copy()
    assert not extensions.empty
    assert set(extensions["series_role"]) == {"supporting"}
    assert set(extensions["public_role"]) == {"supporting"}
