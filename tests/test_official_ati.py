from __future__ import annotations

import pandas as pd

from ati_shadow_policy.qra_capture import build_ati_input_from_official_capture


def test_build_ati_input_from_official_capture_drops_incomplete_rows():
    capture = pd.DataFrame(
        {
            "quarter": ["2024Q1", "2024Q2", "2024Q3"],
            "total_financing_need_bn": ["100", "", "90"],
            "net_bill_issuance_bn": ["20", "10", "30"],
            "qa_status": ["manual_official_capture", "seed_only", "manual_official_capture"],
            "source_doc_local": ["doc1.pdf", "doc2.pdf", "doc3.pdf|data/manual/quarterly_refunding_seed.csv"],
            "source_url": ["https://example.com/1", "https://example.com/2", "https://example.com/3"],
            "source_doc_type": ["official_auction_reconstruction|pdf", "seed_csv", "seed_csv"],
            "notes": ["ok", "missing financing", "seed assisted"],
        }
    )

    ati_input = build_ati_input_from_official_capture(capture)

    assert len(ati_input) == 1
    assert ati_input.loc[0, "quarter"] == "2024Q1"
    assert ati_input.loc[0, "financing_need_bn"] == "100"
    assert ati_input.loc[0, "net_bills_bn"] == "20"
    assert ati_input.loc[0, "capture_quality"] == "manual_official_capture"
    assert ati_input.loc[0, "capture_source"] == "doc1.pdf"
