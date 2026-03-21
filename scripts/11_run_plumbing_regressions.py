from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd
import statsmodels.api as sm

from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import PROCESSED_DIR, RAW_DIR, TABLES_DIR, ensure_project_dirs
from ati_shadow_policy.research.ati_index import aggregate_auction_flows, aggregate_auction_net_flows

FRED_SERIES = ["date", "WRESBAL", "WDTGAL", "WTREGEN", "WLRRAL", "TREAST"]
BASELINE_VARIANT_ID = "baseline_frn_separate_tips_included"

REGRESSION_VARIANTS = (
    {
        "variant_id": BASELINE_VARIANT_ID,
        "variant_group": "baseline",
        "bill_proxy_col": "bill_net_exact",
        "duration_proxy_col": "nonbill_net_exact",
        "bill_proxy_label": "Bills + CMBs exact net issuance (official auction reconstruction)",
        "duration_proxy_label": "Combined non-bill exact net issuance (official auction reconstruction)",
        "bill_proxy_source_quality": "exact_official_net",
        "duration_proxy_source_quality": "exact_official_net",
        "series_role": "headline",
    },
    {
        "variant_id": "robustness_frn_with_bills_tips_included",
        "variant_group": "frn_robustness",
        "bill_proxy_col": "bill_plus_frn_total",
        "duration_proxy_col": "coupon_like_total",
        "bill_proxy_label": "Bills + CMBs + FRNs gross auction flow",
        "duration_proxy_label": "Nominal coupons + TIPS gross auction flow",
        "bill_proxy_source_quality": "fallback_gross_proxy",
        "duration_proxy_source_quality": "fallback_gross_proxy",
        "series_role": "fallback",
    },
    {
        "variant_id": "robustness_frn_with_coupons_tips_included",
        "variant_group": "frn_robustness",
        "bill_proxy_col": "bill_like",
        "duration_proxy_col": "coupon_plus_frn_total",
        "bill_proxy_label": "Bills + CMBs gross auction flow",
        "duration_proxy_label": "Nominal coupons + TIPS + FRNs gross auction flow",
        "bill_proxy_source_quality": "fallback_gross_proxy",
        "duration_proxy_source_quality": "fallback_gross_proxy",
        "series_role": "fallback",
    },
    {
        "variant_id": "robustness_frn_separate_tips_excluded",
        "variant_group": "tips_robustness",
        "bill_proxy_col": "bill_like",
        "duration_proxy_col": "nominal_coupon",
        "bill_proxy_label": "Bills + CMBs gross auction flow (FRNs separate)",
        "duration_proxy_label": "Nominal coupons only gross auction flow (TIPS excluded)",
        "bill_proxy_source_quality": "fallback_gross_proxy",
        "duration_proxy_source_quality": "fallback_gross_proxy",
        "series_role": "fallback",
    },
)
DEPENDENT_SPECS = (
    {
        "dependent_variable": "delta_wlrral",
        "dependent_label": "Change in weekly reverse-repo liabilities (WLRRAL)",
        "expected_sign_bill_proxy": "positive",
        "expected_sign_duration_proxy": "lower_than_bill_proxy",
    },
    {
        "dependent_variable": "delta_wresbal",
        "dependent_label": "Change in reserve balances (WRESBAL)",
        "expected_sign_bill_proxy": "higher_than_duration_proxy",
        "expected_sign_duration_proxy": "more_negative_than_bill_proxy",
    },
)
COMMON_CONTROLS = ["delta_wdtgal", "qt_proxy"]
FREQUENCY = "weekly (W-WED)"
SIGN_CONVENTION = "Positive issuance variables imply more Treasury supply to the public."


def add_plumbing_proxy_columns(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.copy()
    out["bill_plus_frn_total"] = out["bill_like"].fillna(0) + out["frn"].fillna(0)
    if "bill_net_exact" not in out.columns:
        out["bill_net_exact"] = out["bill_like"]
    if "nonbill_net_exact" not in out.columns:
        out["nonbill_net_exact"] = out["coupon_like_total"]
    out["headline_proxy_status"] = "exact_bill_and_exact_nonbill_net"
    out["fallback_proxy_status"] = "gross_auction_flow_proxy"
    return out

def newey_west_table(result, dep_var: str) -> pd.DataFrame:
    params = result.params
    se = result.bse
    out = pd.DataFrame({
        "dependent_variable": dep_var,
        "term": params.index,
        "coef": params.values,
        "std_err": se.values,
        "t_stat": result.tvalues.values,
        "p_value": result.pvalues.values,
        "nobs": result.nobs,
        "rsquared": result.rsquared,
    })
    return out

def run_regression_variants(panel: pd.DataFrame) -> pd.DataFrame:
    results_frames = []
    for dep_spec in DEPENDENT_SPECS:
        dep_var = dep_spec["dependent_variable"]
        for variant in REGRESSION_VARIANTS:
            bill_proxy = variant["bill_proxy_col"]
            duration_proxy = variant["duration_proxy_col"]
            required = {dep_var, bill_proxy, duration_proxy, *COMMON_CONTROLS}
            if not required.issubset(panel.columns):
                continue
            xvars = [bill_proxy, duration_proxy, *COMMON_CONTROLS]
            reg = panel[[dep_var, *xvars]].dropna()
            if len(reg) < 10:
                continue
            X = sm.add_constant(reg[xvars])
            model = sm.OLS(reg[dep_var], X).fit(cov_type="HAC", cov_kwds={"maxlags": 4})
            table = newey_west_table(model, dep_var)
            table["variant_id"] = variant["variant_id"]
            table["variant_group"] = variant["variant_group"]
            table["bill_proxy_col"] = bill_proxy
            table["duration_proxy_col"] = duration_proxy
            table["bill_proxy_label"] = variant["bill_proxy_label"]
            table["duration_proxy_label"] = variant["duration_proxy_label"]
            table["bill_proxy_source_quality"] = variant["bill_proxy_source_quality"]
            table["duration_proxy_source_quality"] = variant["duration_proxy_source_quality"]
            table["series_role"] = variant["series_role"]
            table["dependent_label"] = dep_spec["dependent_label"]
            table["expected_sign_bill_proxy"] = dep_spec["expected_sign_bill_proxy"]
            table["expected_sign_duration_proxy"] = dep_spec["expected_sign_duration_proxy"]
            table["proxy_units"] = "USD notional (weekly sum)"
            table["frequency"] = FREQUENCY
            table["sign_convention"] = SIGN_CONVENTION
            if variant["series_role"] == "headline":
                table["notes"] = (
                    "Headline plumbing variant uses exact bill net issuance and exact combined non-bill "
                    "net issuance reconstructed from official auction settlement data; controls are weekly "
                    "changes in TGA and a QT proxy."
                )
            else:
                table["notes"] = (
                    "Fallback robustness variant uses gross auction-flow proxies; controls are weekly "
                    "changes in TGA and a QT proxy."
                )
            results_frames.append(table)

    if not results_frames:
        return pd.DataFrame(
            columns=[
                "dependent_variable",
                "term",
                "coef",
                "std_err",
                "t_stat",
                "p_value",
                "nobs",
                "rsquared",
                "variant_id",
                "variant_group",
                "bill_proxy_col",
                "duration_proxy_col",
                "bill_proxy_label",
                "duration_proxy_label",
                "bill_proxy_source_quality",
                "duration_proxy_source_quality",
                "series_role",
                "dependent_label",
                "expected_sign_bill_proxy",
                "expected_sign_duration_proxy",
                "proxy_units",
                "frequency",
                "sign_convention",
                "notes",
            ]
        )
    return pd.concat(results_frames, ignore_index=True)


def build_robustness_summary(results: pd.DataFrame) -> pd.DataFrame:
    if results.empty:
        return results
    focus_terms = {"const"} | set(COMMON_CONTROLS)
    summary = results.loc[~results["term"].isin(focus_terms)].copy()
    summary["proxy_role"] = summary.apply(
        lambda row: "bill_proxy"
        if row["term"] == row["bill_proxy_col"]
        else ("duration_proxy" if row["term"] == row["duration_proxy_col"] else "other"),
        axis=1,
    )
    keep = [
        "dependent_variable",
        "dependent_label",
        "variant_id",
        "variant_group",
        "proxy_role",
        "term",
        "coef",
        "std_err",
        "t_stat",
        "p_value",
        "bill_proxy_label",
        "duration_proxy_label",
        "bill_proxy_source_quality",
        "duration_proxy_source_quality",
        "series_role",
        "expected_sign_bill_proxy",
        "expected_sign_duration_proxy",
        "proxy_units",
        "frequency",
        "sign_convention",
        "nobs",
        "rsquared",
        "notes",
    ]
    return summary[keep].copy()


def main() -> None:
    ensure_project_dirs()
    auctions = pd.read_csv(RAW_DIR / "fiscaldata" / "auctions_query.csv", low_memory=False)
    auction_flows = aggregate_auction_flows(auctions)
    auction_flows["date"] = pd.to_datetime(auction_flows["date"])
    auction_net = aggregate_auction_net_flows(auctions)
    auction_net["date"] = pd.to_datetime(auction_net["date"])

    fred = pd.read_csv(RAW_DIR / "fred" / "core_wide.csv")
    fred["date"] = pd.to_datetime(fred["date"])
    keep = [col for col in FRED_SERIES if col in fred.columns]
    fred = fred[keep].copy().sort_values("date")

    panel = (
        auction_flows.merge(auction_net, on="date", how="left")
        .merge(fred, on="date", how="left")
        .sort_values("date")
    )
    panel = add_plumbing_proxy_columns(panel)
    for col in ["WRESBAL", "WDTGAL", "WTREGEN", "WLRRAL", "TREAST"]:
        if col in panel.columns:
            panel[f"delta_{col.lower()}"] = panel[col].diff()

    if "delta_treast" in panel.columns:
        panel["qt_proxy"] = -panel["delta_treast"]

    write_df(panel, PROCESSED_DIR / "plumbing_weekly_panel.csv")

    all_results = run_regression_variants(panel)
    baseline_output = all_results.loc[all_results["variant_id"] == BASELINE_VARIANT_ID].copy()
    if baseline_output.empty:
        baseline_output = pd.DataFrame(
            columns=["dependent_variable", "term", "coef", "std_err", "t_stat", "p_value", "nobs", "rsquared", "notes"]
        )

    write_df(baseline_output, TABLES_DIR / "plumbing_regressions.csv")
    md = "# Plumbing Regressions\n\n" + baseline_output.to_markdown(index=False)
    (TABLES_DIR / "plumbing_regressions.md").write_text(md + "\n", encoding="utf-8")

    robustness = build_robustness_summary(all_results)
    write_df(robustness, TABLES_DIR / "plumbing_robustness.csv")
    robustness_md = "# Plumbing Robustness\n\n" + robustness.to_markdown(index=False)
    (TABLES_DIR / "plumbing_robustness.md").write_text(robustness_md + "\n", encoding="utf-8")
    print(
        "Saved plumbing panel with "
        f"{len(panel):,} rows, baseline regression rows {len(baseline_output):,}, "
        f"and robustness rows {len(robustness):,}"
    )

if __name__ == "__main__":
    main()
