from ati_shadow_policy.research.ati_index import classify_security_bucket

def test_classify_security_bucket():
    assert classify_security_bucket("Bill") == "bill_like"
    assert classify_security_bucket("Cash Management Bill") == "bill_like"
    assert classify_security_bucket("Floating Rate Note") == "frn"
    assert classify_security_bucket("Treasury Note") == "nominal_coupon"
    assert classify_security_bucket("Treasury Bond") == "nominal_coupon"
    assert classify_security_bucket("Treasury Inflation-Protected Security") == "tips"


def test_classify_security_bucket_preserves_baseline_edges():
    assert classify_security_bucket("  CMB  ") == "bill_like"
    assert classify_security_bucket("2-Year Floating Rate Note") == "frn"
    assert classify_security_bucket("10-Year TIPS") == "tips"
    assert classify_security_bucket("  ") == "unknown"
