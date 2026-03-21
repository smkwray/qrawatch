import pandas as pd

from ati_shadow_policy.research.ati_index import build_ati_index

def test_missing_coupons_arithmetic():
    df = pd.DataFrame({
        "quarter": ["2023Q4"],
        "financing_need_bn": [852],
        "net_bills_bn": [513],
    })
    out = build_ati_index(df)
    assert round(out.loc[0, "bill_share"], 6) == round(513 / 852, 6)
    assert round(out.loc[0, "missing_coupons_18_bn"], 2) == round(513 - 0.18 * 852, 2)
    assert round(out.loc[0, "net_coupons_bn_implied"], 2) == 339.00

def test_positive_only_variant():
    df = pd.DataFrame({
        "quarter": ["x"],
        "financing_need_bn": [100],
        "net_bills_bn": [10],
    })
    out = build_ati_index(df)
    assert out.loc[0, "missing_coupons_18_bn"] == -8
    assert out.loc[0, "missing_coupons_18_bn_posonly"] == 0
