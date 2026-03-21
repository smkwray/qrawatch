from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from ati_shadow_policy.io_utils import coerce_numeric, pick_first_existing, write_df
from ati_shadow_policy.paths import PROCESSED_DIR, RAW_DIR, TABLES_DIR, ensure_project_dirs
from ati_shadow_policy.research.ati_index import aggregate_auction_flows, aggregate_auction_net_flows

BUYBACK_DATE_CANDIDATES = ["operation_date", "buyback_date", "record_date"]
BUYBACK_AMOUNT_CANDIDATES = [
    "accepted_amt",
    "buyback_accepted_amt",
    "total_accepted",
    "accepted_amount",
    "total_par_amt_accepted",
]

VALUE_UNITS = "USD notional (weekly sum)"
SIGN_CONVENTION = "Higher values imply more duration supply to the public."
FREQUENCY = "weekly (W-WED)"


def add_duration_supply_variants(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.copy()
    qt = out["qt_proxy"].fillna(0)
    buybacks = out["buybacks_accepted"].fillna(0)
    if "nonbill_net_exact" not in out.columns:
        out["nonbill_net_exact"] = out["coupon_like_total"]
    out["headline_treasury_nonbill_net_exact"] = out["nonbill_net_exact"]
    out["headline_public_duration_supply"] = out["headline_treasury_nonbill_net_exact"] + qt - buybacks
    out["treasury_only_duration_nominal_plus_tips"] = out["coupon_like_total"]
    out["treasury_only_duration_nominal_only"] = out["nominal_coupon"]
    out["combined_duration_nominal_plus_tips"] = out["coupon_like_total"] + qt - buybacks
    out["combined_duration_nominal_plus_tips_plus_frn"] = out["coupon_plus_frn_total"] + qt - buybacks
    out["combined_duration_nominal_only"] = out["nominal_coupon"] + qt - buybacks
    out["provisional_public_duration_supply"] = out["combined_duration_nominal_plus_tips"]
    out["headline_source_quality"] = "hybrid_exact_nonbill_net_plus_qt_proxy"
    out["fallback_source_quality"] = "fallback_gross_coupon_proxy_plus_qt_proxy"
    out["frequency"] = FREQUENCY
    out["value_units"] = VALUE_UNITS
    out["sign_convention"] = SIGN_CONVENTION
    out["notes"] = (
        "Headline measure = exact combined non-bill net issuance + QT proxy - buybacks accepted. "
        "Gross coupon-flow constructions remain explicit fallback columns."
    )
    return out


def build_duration_supply_comparison(panel: pd.DataFrame, latest_rows: int = 12) -> pd.DataFrame:
    keep = panel.sort_values("date").tail(latest_rows).copy()
    constructions = [
        {
            "construction_id": "headline_nonbill_net_exact_qt_minus_buybacks",
            "construction_family": "headline",
            "value_col": "headline_public_duration_supply",
            "treasury_proxy_label": "Exact combined non-bill net issuance",
            "includes_frn": True,
            "includes_tips": True,
            "includes_qt_proxy": True,
            "subtracts_buybacks": True,
            "source_quality": "hybrid_exact_nonbill_net_plus_qt_proxy",
        },
        {
            "construction_id": "treasury_only_nominal_plus_tips",
            "construction_family": "treasury_only",
            "value_col": "treasury_only_duration_nominal_plus_tips",
            "treasury_proxy_label": "Nominal coupons + TIPS",
            "includes_frn": False,
            "includes_tips": True,
            "includes_qt_proxy": False,
            "subtracts_buybacks": False,
            "source_quality": "fallback_gross_coupon_proxy",
        },
        {
            "construction_id": "combined_nominal_plus_tips_qt_minus_buybacks",
            "construction_family": "combined_duration",
            "value_col": "combined_duration_nominal_plus_tips",
            "treasury_proxy_label": "Nominal coupons + TIPS",
            "includes_frn": False,
            "includes_tips": True,
            "includes_qt_proxy": True,
            "subtracts_buybacks": True,
            "source_quality": "fallback_gross_coupon_proxy_plus_qt_proxy",
        },
        {
            "construction_id": "combined_nominal_plus_tips_plus_frn_qt_minus_buybacks",
            "construction_family": "combined_duration",
            "value_col": "combined_duration_nominal_plus_tips_plus_frn",
            "treasury_proxy_label": "Nominal coupons + TIPS + FRNs",
            "includes_frn": True,
            "includes_tips": True,
            "includes_qt_proxy": True,
            "subtracts_buybacks": True,
            "source_quality": "fallback_gross_coupon_proxy_plus_qt_proxy",
        },
        {
            "construction_id": "combined_nominal_only_qt_minus_buybacks",
            "construction_family": "combined_duration",
            "value_col": "combined_duration_nominal_only",
            "treasury_proxy_label": "Nominal coupons only (TIPS excluded)",
            "includes_frn": False,
            "includes_tips": False,
            "includes_qt_proxy": True,
            "subtracts_buybacks": True,
            "source_quality": "fallback_gross_coupon_proxy_plus_qt_proxy",
        },
    ]
    rows = []
    for spec in constructions:
        part = keep[["date", spec["value_col"]]].copy()
        part = part.rename(columns={spec["value_col"]: "value"})
        part["construction_id"] = spec["construction_id"]
        part["construction_family"] = spec["construction_family"]
        part["treasury_proxy_label"] = spec["treasury_proxy_label"]
        part["includes_frn"] = spec["includes_frn"]
        part["includes_tips"] = spec["includes_tips"]
        part["includes_qt_proxy"] = spec["includes_qt_proxy"]
        part["subtracts_buybacks"] = spec["subtracts_buybacks"]
        part["source_quality"] = spec["source_quality"]
        part["frequency"] = FREQUENCY
        part["value_units"] = VALUE_UNITS
        part["sign_convention"] = SIGN_CONVENTION
        rows.append(part)
    return pd.concat(rows, ignore_index=True)


def main() -> None:
    ensure_project_dirs()
    auctions = pd.read_csv(RAW_DIR / "fiscaldata" / "auctions_query.csv", low_memory=False)
    auction_flows = aggregate_auction_flows(auctions)
    auction_flows["date"] = pd.to_datetime(auction_flows["date"])
    auction_net = aggregate_auction_net_flows(auctions)
    auction_net["date"] = pd.to_datetime(auction_net["date"])

    fred = pd.read_csv(RAW_DIR / "fred" / "core_wide.csv")
    fred["date"] = pd.to_datetime(fred["date"])
    keep = [c for c in ["date", "TREAST"] if c in fred.columns]
    fred = fred[keep].copy().sort_values("date")
    if "TREAST" in fred.columns:
        fred["qt_proxy"] = -fred["TREAST"].diff()
    else:
        fred["qt_proxy"] = pd.NA

    buybacks_path = RAW_DIR / "fiscaldata" / "buybacks_operations.csv"
    buybacks = pd.read_csv(buybacks_path, low_memory=False)
    date_col = pick_first_existing(buybacks.columns, BUYBACK_DATE_CANDIDATES)
    amount_col = pick_first_existing(buybacks.columns, BUYBACK_AMOUNT_CANDIDATES)
    buybacks[date_col] = pd.to_datetime(buybacks[date_col], errors="coerce")
    buybacks[amount_col] = coerce_numeric(buybacks[amount_col])
    buybacks_weekly = (
        buybacks.dropna(subset=[date_col, amount_col])
        .groupby(pd.Grouper(key=date_col, freq="W-WED"))[amount_col]
        .sum()
        .reset_index()
        .rename(columns={date_col: "date", amount_col: "buybacks_accepted"})
    )

    panel = (
        auction_flows.merge(auction_net, on="date", how="left")
        .merge(fred[["date", "qt_proxy"]], on="date", how="left")
        .merge(buybacks_weekly, on="date", how="left")
    )
    panel["buybacks_accepted"] = panel["buybacks_accepted"].fillna(0)
    panel = add_duration_supply_variants(panel)
    write_df(panel, PROCESSED_DIR / "public_duration_supply.csv")
    write_df(panel, PROCESSED_DIR / "public_duration_supply_provisional.csv")

    comparison = build_duration_supply_comparison(panel)
    write_df(comparison, TABLES_DIR / "duration_supply_comparison.csv")
    comparison_md = "# Duration Supply Comparison\n\n" + comparison.to_markdown(index=False)
    (TABLES_DIR / "duration_supply_comparison.md").write_text(comparison_md + "\n", encoding="utf-8")
    print(f"Saved public duration supply panel with {len(panel):,} rows")

if __name__ == "__main__":
    main()
