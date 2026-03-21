from __future__ import annotations

import pandas as pd

from ati_shadow_policy.paths import MANUAL_DIR
from ati_shadow_policy.qra_capture import build_ati_input_from_official_capture


def test_build_ati_input_from_official_capture_drops_incomplete_rows():
    capture = pd.DataFrame(
        {
            "quarter": ["2024Q1", "2024Q2", "2024Q3"],
            "total_financing_need_bn": ["100", "", "90"],
            "net_bill_issuance_bn": ["20", "10", "30"],
            "qa_status": ["manual_official_capture", "seed_only", "manual_official_capture"],
            "source_doc_local": [
                "data/raw/fiscaldata/auctions_query.csv|statement1.html|release1.html",
                "doc2.pdf",
                "doc3.pdf|data/manual/quarterly_refunding_seed.csv",
            ],
            "source_url": ["https://example.com/1", "https://example.com/2", "https://example.com/3"],
            "source_doc_type": [
                "official_auction_reconstruction|official_quarterly_refunding_statement|quarterly_refunding_press_release",
                "seed_csv",
                "seed_csv",
            ],
            "notes": ["ok", "missing financing", "seed assisted"],
        }
    )

    ati_input = build_ati_input_from_official_capture(capture)

    assert len(ati_input) == 1
    assert ati_input.loc[0, "quarter"] == "2024Q1"
    assert ati_input.loc[0, "financing_need_bn"] == "100"
    assert ati_input.loc[0, "net_bills_bn"] == "20"
    assert ati_input.loc[0, "capture_quality"] == "manual_official_capture"
    assert ati_input.loc[0, "capture_source"] == "data/raw/fiscaldata/auctions_query.csv|statement1.html|release1.html"


def test_build_ati_input_from_official_capture_requires_statement_provenance():
    capture = pd.DataFrame(
        {
            "quarter": ["2024Q4"],
            "total_financing_need_bn": ["565"],
            "net_bill_issuance_bn": ["182.504"],
            "qa_status": ["manual_official_capture"],
            "source_doc_local": ["data/raw/fiscaldata/auctions_query.csv|release1.html"],
            "source_url": ["https://example.com/1"],
            "source_doc_type": ["official_auction_reconstruction|quarterly_refunding_press_release"],
            "notes": ["missing statement provenance"],
        }
    )

    ati_input = build_ati_input_from_official_capture(capture)

    assert ati_input.empty


def test_manual_capture_template_extends_official_frontier_through_2025q4():
    capture = pd.read_csv(
        MANUAL_DIR / "official_quarterly_refunding_capture_template.csv",
        dtype=str,
    ).fillna("")

    ati_input = build_ati_input_from_official_capture(capture)
    quarters = set(ati_input["quarter"].tolist())

    assert {"2024Q4", "2025Q1", "2025Q2", "2025Q3", "2025Q4"}.issubset(quarters)
    assert "2026Q1" not in quarters
