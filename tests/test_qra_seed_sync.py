from __future__ import annotations

import pandas as pd

from ati_shadow_policy.qra_capture import CAPTURE_COLUMNS
from ati_shadow_policy.research.qra_seed_sync import build_seed_rows, sync_capture_template


def _capture_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    for col in CAPTURE_COLUMNS:
        if col not in frame.columns:
            frame[col] = ""
    return frame[list(CAPTURE_COLUMNS)]


def test_sync_capture_template_backward_append_is_idempotent() -> None:
    template = _capture_frame(
        [
            {
                "quarter": "2023Q4",
                "qra_release_date": "2023-08-02",
                "market_pricing_marker_minus_1d": "2023-08-01",
                "qa_status": "manual_official_capture",
                "source_url": "https://home.treasury.gov/news/press-releases/jy1864",
                "source_doc_local": "data/raw/qra/files/jy1864_demo.html",
                "source_doc_type": "official_quarterly_refunding_statement",
                "total_financing_need_bn": "852",
                "net_bill_issuance_bn": "437.423",
            }
        ]
    )
    historical_seed = _capture_frame(
        [
            {
                "quarter": "2022Q3",
                "qra_release_date": "2022-05-04",
                "market_pricing_marker_minus_1d": "2022-05-03",
                "source_doc_local": "data/manual/official_quarterly_refunding_historical_seed.csv",
                "source_doc_type": "seed_csv",
                "qa_status": "seed_only",
            },
            {
                "quarter": "2023Q3",
                "qra_release_date": "2023-05-03",
                "market_pricing_marker_minus_1d": "2023-05-02",
                "source_doc_local": "data/manual/official_quarterly_refunding_historical_seed.csv",
                "source_doc_type": "seed_csv",
                "qa_status": "seed_only",
            },
        ]
    )

    seed_rows = build_seed_rows(direction="backward", historical_seed_df=historical_seed)
    first = sync_capture_template(template, seed_rows)

    assert first.rows_added == 2
    assert first.cells_enriched == 0
    assert first.dataframe["quarter"].tolist() == ["2022Q3", "2023Q3", "2023Q4"]

    second = sync_capture_template(first.dataframe, seed_rows)
    assert second.rows_added == 0
    assert second.cells_enriched == 0
    assert second.conflicting_cells_skipped == 0
    assert second.dataframe.equals(first.dataframe)


def test_sync_capture_template_preserves_richer_existing_rows() -> None:
    template = _capture_frame(
        [
            {
                "quarter": "2024Q1",
                "qra_release_date": "2023-11-01",
                "market_pricing_marker_minus_1d": "2023-10-31",
                "total_financing_need_bn": "816",
                "net_bill_issuance_bn": "",
                "source_url": "https://home.treasury.gov/news/press-releases/jy2062",
                "source_doc_local": "data/raw/qra/files/jy2062_demo.html",
                "source_doc_type": "official_quarterly_refunding_statement",
                "qa_status": "manual_official_capture",
                "notes": "Richer existing capture.",
            }
        ]
    )
    seed_rows = _capture_frame(
        [
            {
                "quarter": "2024Q1",
                "qra_release_date": "2023-11-01",
                "market_pricing_marker_minus_1d": "2023-10-31",
                "total_financing_need_bn": "999",
                "net_bill_issuance_bn": "409.117",
                "source_url": "",
                "source_doc_local": "data/manual/official_quarterly_refunding_historical_seed.csv",
                "source_doc_type": "seed_csv",
                "qa_status": "seed_only",
                "notes": "Scaffold row",
            }
        ]
    )

    result = sync_capture_template(template, seed_rows)
    row = result.dataframe.iloc[0]

    assert result.rows_added == 0
    assert result.cells_enriched == 1
    assert result.conflicting_cells_skipped >= 1
    assert row["total_financing_need_bn"] == "816"
    assert row["net_bill_issuance_bn"] == "409.117"
    assert row["qa_status"] == "manual_official_capture"
    assert row["source_url"] == "https://home.treasury.gov/news/press-releases/jy2062"
    assert row["notes"] == "Richer existing capture."


def test_build_seed_rows_both_combines_backward_and_forward_sources() -> None:
    historical_seed = _capture_frame(
        [
            {
                "quarter": "2022Q3",
                "qra_release_date": "2022-05-04",
                "market_pricing_marker_minus_1d": "2022-05-03",
                "source_doc_local": "data/manual/official_quarterly_refunding_historical_seed.csv",
                "source_doc_type": "seed_csv",
                "qa_status": "seed_only",
            },
            {
                "quarter": "2024Q2",
                "qra_release_date": "2024-01-31",
                "market_pricing_marker_minus_1d": "2024-01-30",
                "source_doc_local": "data/manual/official_quarterly_refunding_historical_seed.csv",
                "source_doc_type": "seed_csv",
                "qa_status": "seed_only",
            },
        ]
    )
    qra_event_seed = pd.DataFrame(
        [
            {
                "official_release_date": "2024-01-31",
                "market_pricing_marker_minus_1d": "2024-01-30",
            }
        ]
    )
    quarterly_refunding_seed = pd.DataFrame(
        [{"quarter": "2024Q2", "financing_need_bn": "202.0", "net_bills_bn": "-296.523"}]
    )

    combined = build_seed_rows(
        direction="both",
        historical_seed_df=historical_seed,
        qra_event_seed_df=qra_event_seed,
        quarterly_refunding_seed_df=quarterly_refunding_seed,
    )

    assert combined["quarter"].tolist() == ["2022Q3", "2024Q2"]
    row_2024q2 = combined.loc[combined["quarter"] == "2024Q2"].iloc[0]
    assert row_2024q2["total_financing_need_bn"] == "202.0"
    assert row_2024q2["net_bill_issuance_bn"] == "-296.523"
