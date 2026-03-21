from __future__ import annotations

import pandas as pd

from ati_shadow_policy.research.auction_absorption import (
    build_auction_absorption_panel_v1,
    build_auction_absorption_table,
)


def test_build_auction_absorption_panel_v1_maps_event_windows_and_dealer_quarters() -> None:
    investor = pd.DataFrame(
        {
            "auction_date": ["2024-05-02", "2024-05-15"],
            "security_family": ["nominal_coupon", "tips"],
            "investor_class": ["foreign_and_international", "dealers_and_brokers"],
            "measure": ["allotment_amount", "allotment_amount"],
            "value": [10.0, 5.0],
            "units": ["USD billions", "USD billions"],
            "provenance": ["investor_prov", "investor_prov"],
            "source_quality": ["official_treasury_download", "official_treasury_download"],
        }
    )
    dealer = pd.DataFrame(
        {
            "date": ["2025-01-08"],
            "series_label": ["U.S. TREASURY SECURITIES (EXCLUDING TIPS) | COUPONS DUE > 3 YRS, BUT <= 6 YRS"],
            "source_dataset_type": ["quarterly_marketshare"],
            "source_title": ["QUARTER IV 2024"],
            "metric_id": ["daily_avg_vol_in_millions"],
            "value": [100.0],
            "units": ["USD millions"],
            "source_quality": ["json_repaired"],
            "source_section": ["interDealerBrokers"],
            "provenance_summary": ["dealer_prov"],
        }
    )
    registry = pd.DataFrame(
        {
            "event_id": ["qra_2024_05", "qra_2024_07"],
            "quarter": ["2024Q3", "2024Q4"],
            "release_timestamp_et": ["2024-05-01T00:00:00-04:00", "2024-07-31T00:00:00-04:00"],
        }
    )
    auction_results = pd.DataFrame(
        {
            "auction_date": ["2024-05-06"],
            "security_type": ["Note"],
            "security_term": ["10-Year"],
            "bid_to_cover_ratio": [2.45],
            "high_yield": [4.50],
            "avg_med_yield": [4.45],
            "offering_amt": [39_000_000_000],
            "total_tendered": [120_000_000_000],
            "total_accepted": [42_000_000_000],
            "primary_dealer_accepted": [8_000_000_000],
            "indirect_bidder_accepted": [24_000_000_000],
            "direct_bidder_accepted": [10_000_000_000],
        }
    )

    panel = build_auction_absorption_panel_v1(investor, dealer, registry, auction_results=auction_results)

    assert "event_adjacent_auction" in set(panel["view_type"])
    assert "quarterly_marketshare_snapshot" in set(panel["view_type"])
    assert "event_adjacent_auction_outcome" in set(panel["view_type"])
    assert set(panel["qra_event_id"].dropna()) >= {"qra_2024_05"}
    assert "accepted_share_pct" in set(panel["measure"])
    assert "when_issued_spread_available" in set(panel["measure"])


def test_build_auction_absorption_table_aggregates_panel_rows() -> None:
    panel = pd.DataFrame(
        {
            "qra_event_id": ["e1", "e1"],
            "quarter": ["2024Q3", "2024Q3"],
            "source_family": ["investor_allotments", "investor_allotments"],
            "view_type": ["event_adjacent_auction", "event_adjacent_auction"],
            "auction_date": ["2024-05-01", "2024-05-01"],
            "security_family": ["nominal_coupon", "nominal_coupon"],
            "investor_class": ["foreign_and_international", "foreign_and_international"],
            "measure": ["allotment_amount", "allotment_amount"],
            "value": [10.0, 12.0],
            "units": ["USD billions", "USD billions"],
            "source_quality": ["official_treasury_download", "official_treasury_download"],
            "aggregation_method": ["sum", "sum"],
            "coverage_status": ["observed", "observed"],
            "coverage_note": [pd.NA, pd.NA],
            "spec_id": ["spec_auction_absorption_v1", "spec_auction_absorption_v1"],
        }
    )

    out = build_auction_absorption_table(panel)

    assert len(out) == 1
    assert out.loc[0, "value"] == 22.0


def test_build_auction_absorption_table_means_ratio_measures() -> None:
    panel = pd.DataFrame(
        {
            "qra_event_id": ["e1", "e1"],
            "quarter": ["2024Q3", "2024Q3"],
            "source_family": ["auction_results", "auction_results"],
            "view_type": ["event_adjacent_auction_outcome", "event_adjacent_auction_outcome"],
            "auction_date": ["2024-05-06", "2024-05-06"],
            "security_family": ["nominal_coupon", "nominal_coupon"],
            "investor_class": ["all", "all"],
            "measure": ["bid_to_cover_ratio", "bid_to_cover_ratio"],
            "value": [2.0, 4.0],
            "units": ["ratio", "ratio"],
            "source_quality": ["official_fiscaldata_download", "official_fiscaldata_download"],
            "aggregation_method": ["mean", "mean"],
            "coverage_status": ["observed", "observed"],
            "coverage_note": [pd.NA, pd.NA],
            "spec_id": ["spec_auction_absorption_v1", "spec_auction_absorption_v1"],
        }
    )

    out = build_auction_absorption_table(panel)

    assert len(out) == 1
    assert out.loc[0, "value"] == 3.0
