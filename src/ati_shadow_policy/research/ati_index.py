from __future__ import annotations

from typing import Iterable, Sequence

import numpy as np
import pandas as pd

from ..io_utils import coerce_numeric, pick_first_existing
from ..qra_capture import bill_settlement_flows


TARGETS = (0.15, 0.18, 0.20)


def build_ati_index(df: pd.DataFrame, targets: Sequence[float] = TARGETS) -> pd.DataFrame:
    out = df.copy()
    out["financing_need_bn"] = coerce_numeric(out["financing_need_bn"])
    out["net_bills_bn"] = coerce_numeric(out["net_bills_bn"])
    out["bill_share"] = out["net_bills_bn"] / out["financing_need_bn"]
    out["net_coupons_bn_implied"] = out["financing_need_bn"] - out["net_bills_bn"]

    for tau in targets:
        pct = int(round(tau * 100))
        out[f"missing_coupons_{pct}_bn"] = out["net_bills_bn"] - tau * out["financing_need_bn"]
        out[f"missing_coupons_{pct}_bn_posonly"] = out[f"missing_coupons_{pct}_bn"].clip(lower=0)

    out["baseline_target"] = 0.18
    out["ati_baseline_bn"] = out["missing_coupons_18_bn"]
    out["ati_baseline_bn_posonly"] = out["missing_coupons_18_bn_posonly"]
    return out


def classify_security_bucket(value: str) -> str:
    text = (value or "").strip().lower()
    if not text:
        return "unknown"
    if "cash management bill" in text or text == "cmb":
        return "bill_like"
    if "bill" in text:
        return "bill_like"
    if "floating" in text or "frn" in text:
        return "frn"
    if "inflation" in text or "tips" in text:
        return "tips"
    if "note" in text or "bond" in text:
        return "nominal_coupon"
    return "unknown"


AUCTION_DATE_CANDIDATES = ["issue_date", "record_date", "auction_date"]
AUCTION_TYPE_CANDIDATES = ["security_type", "security_desc", "security_type_desc", "security"]
AUCTION_AMOUNT_CANDIDATES = ["offering_amt", "total_accepted", "auctioned_amt", "offering_amount"]
AUCTION_MATURITY_CANDIDATES = ["est_pub_held_mat_by_type_amt"]


def _require_auction_column(columns: Iterable[str], candidates: Sequence[str], role: str) -> str:
    try:
        return pick_first_existing(columns, candidates)
    except KeyError as err:
        available = [str(col) for col in columns]
        raise ValueError(
            f"aggregate_auction_flows missing required {role} column. "
            f"Expected one of {list(candidates)}; available columns: {available}"
        ) from err


def _classify_auction_row(row: pd.Series, type_col: str) -> str:
    if str(row.get("cash_management_bill_cmb", "") or "").strip() == "Yes":
        return "bill_like"
    if str(row.get("floating_rate", "") or "").strip() == "Yes":
        return "frn"
    if str(row.get("inflation_index_security", "") or "").strip() == "Yes":
        return "tips"
    return classify_security_bucket(str(row.get(type_col, "") or ""))


def aggregate_auction_flows(df: pd.DataFrame, freq: str = "W-WED") -> pd.DataFrame:
    date_col = _require_auction_column(df.columns, AUCTION_DATE_CANDIDATES, "date")
    type_col = _require_auction_column(df.columns, AUCTION_TYPE_CANDIDATES, "security-type")
    amount_col = _require_auction_column(df.columns, AUCTION_AMOUNT_CANDIDATES, "amount")

    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    out[amount_col] = coerce_numeric(out[amount_col])
    out["bucket"] = out.apply(lambda row: _classify_auction_row(row, type_col), axis=1)
    out = out.dropna(subset=[date_col, amount_col])

    grouped = (
        out.groupby([pd.Grouper(key=date_col, freq=freq), "bucket"], dropna=False)[amount_col]
        .sum()
        .reset_index()
        .pivot(index=date_col, columns="bucket", values=amount_col)
        .fillna(0)
        .reset_index()
        .rename(columns={date_col: "date"})
        .sort_values("date")
    )

    for bucket in ["bill_like", "frn", "nominal_coupon", "tips", "unknown"]:
        if bucket not in grouped.columns:
            grouped[bucket] = 0.0

    grouped["coupon_like_total"] = grouped["nominal_coupon"] + grouped["tips"]
    grouped["coupon_plus_frn_total"] = grouped["coupon_like_total"] + grouped["frn"]
    grouped["gross_total"] = (
        grouped["bill_like"] + grouped["frn"] + grouped["nominal_coupon"] + grouped["tips"] + grouped["unknown"]
    )
    stable_columns = [
        "date",
        "bill_like",
        "frn",
        "nominal_coupon",
        "tips",
        "coupon_like_total",
        "coupon_plus_frn_total",
        "gross_total",
        "unknown",
    ]
    return grouped[stable_columns]


def aggregate_auction_net_flows(df: pd.DataFrame, freq: str = "W-WED") -> pd.DataFrame:
    """Aggregate settlement flows within the input's observed issue-date span.

    Bill maturities use the same reconstruction as the quarterly capture, including
    maturity-only dates. Future maturities beyond the last observed issue date do
    not establish future net financing. Nonbill maturity estimates retain their
    issue-date convention.
    """
    date_col = _require_auction_column(df.columns, AUCTION_DATE_CANDIDATES, "date")
    type_col = _require_auction_column(df.columns, AUCTION_TYPE_CANDIDATES, "security-type")
    amount_col = _require_auction_column(df.columns, AUCTION_AMOUNT_CANDIDATES, "amount")
    maturity_col = _require_auction_column(df.columns, AUCTION_MATURITY_CANDIDATES, "maturity-estimate")

    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    out[amount_col] = coerce_numeric(out[amount_col])
    out[maturity_col] = coerce_numeric(out[maturity_col])
    out["bucket"] = out.apply(lambda row: _classify_auction_row(row, type_col), axis=1)
    out["net_group"] = np.where(out["bucket"] == "bill_like", "bill_net", "nonbill_net")
    scope_start, scope_end = out[date_col].min(), out[date_col].max()
    has_bills = out["bucket"].eq("bill_like").any()
    out = out.dropna(subset=[date_col, amount_col])

    by_issue_date = (
        out.loc[out["bucket"].ne("bill_like")].groupby([date_col, "net_group"], as_index=False, dropna=False)
        .agg(
            gross_offering_amt=(amount_col, "sum"),
            maturing_estimate_amt=(maturity_col, "min"),
        )
        .sort_values([date_col, "net_group"], kind="stable")
    )
    by_issue_date["issue_dates"] = 1
    if has_bills:
        bill_daily = bill_settlement_flows(df)
        bill_daily = bill_daily.loc[bill_daily["date"].between(scope_start, scope_end)].copy()
        bill_daily = bill_daily.rename(columns={
            "date": date_col,
            "offering_amt": "gross_offering_amt",
            "publicly_held_maturing_amt": "maturing_estimate_amt",
        })
        bill_daily["net_group"] = "bill_net"
        bill_daily["issue_dates"] = bill_daily["bill_issue"].astype(int)
        by_issue_date = pd.concat([by_issue_date, bill_daily[by_issue_date.columns]], ignore_index=True)
    by_issue_date["has_maturity_estimate"] = by_issue_date["maturing_estimate_amt"].notna()
    by_issue_date["has_required_amounts"] = by_issue_date[["gross_offering_amt", "maturing_estimate_amt"]].notna().all(axis=1)
    by_issue_date["net_issuance_amt"] = (
        by_issue_date["gross_offering_amt"] - by_issue_date["maturing_estimate_amt"]
    )

    weekly = (
        by_issue_date.groupby([pd.Grouper(key=date_col, freq=freq), "net_group"], as_index=False, dropna=False)
        .agg(
            gross_offering_amt=("gross_offering_amt", lambda values: values.sum(min_count=len(values))),
            maturing_estimate_amt=("maturing_estimate_amt", lambda values: values.sum(min_count=len(values))),
            net_issuance_amt=("net_issuance_amt", lambda values: values.sum(min_count=len(values))),
            issue_dates=("issue_dates", "sum"),
            issue_dates_missing_maturity=("has_maturity_estimate", lambda values: int((~values).sum())),
            dates_missing_amounts=("has_required_amounts", lambda values: int((~values).sum())),
        )
        .sort_values([date_col, "net_group"], kind="stable")
    )
    weekly["reconstruction_status"] = weekly["dates_missing_amounts"].map(
        lambda count: "complete" if int(count) == 0 else "partial"
    )

    rows: list[dict[str, object]] = []
    for date, group in weekly.groupby(date_col, dropna=False):
        row: dict[str, object] = {"date": date}
        for net_group in ("bill_net", "nonbill_net"):
            part = group.loc[group["net_group"] == net_group]
            if part.empty:
                row[f"{net_group}_exact"] = 0.0
                row[f"{net_group}_gross"] = 0.0
                row[f"{net_group}_maturing"] = 0.0
                row[f"{net_group}_issue_dates"] = 0
                row[f"{net_group}_issue_dates_missing_maturity"] = 0
                row[f"{net_group}_reconstruction_status"] = "complete"
                continue
            record = part.iloc[0]
            row[f"{net_group}_exact"] = float(record["net_issuance_amt"])
            row[f"{net_group}_gross"] = float(record["gross_offering_amt"])
            row[f"{net_group}_maturing"] = (
                float(record["maturing_estimate_amt"])
                if not pd.isna(record["maturing_estimate_amt"])
                else np.nan
            )
            row[f"{net_group}_issue_dates"] = int(record["issue_dates"])
            row[f"{net_group}_issue_dates_missing_maturity"] = int(record["issue_dates_missing_maturity"])
            row[f"{net_group}_reconstruction_status"] = str(record["reconstruction_status"])
        row["headline_treasury_net_exact"] = row["bill_net_exact"] + row["nonbill_net_exact"]
        rows.append(row)

    if not rows:
        return pd.DataFrame(
            columns=[
                "date",
                "bill_net_exact",
                "nonbill_net_exact",
                "headline_treasury_net_exact",
                "bill_net_gross",
                "nonbill_net_gross",
                "bill_net_maturing",
                "nonbill_net_maturing",
                "bill_net_issue_dates",
                "nonbill_net_issue_dates",
                "bill_net_issue_dates_missing_maturity",
                "nonbill_net_issue_dates_missing_maturity",
                "bill_net_reconstruction_status",
                "nonbill_net_reconstruction_status",
            ]
        )

    result = pd.DataFrame(rows).sort_values("date", kind="stable").reset_index(drop=True)
    return result[
        [
            "date",
            "bill_net_exact",
            "nonbill_net_exact",
            "headline_treasury_net_exact",
            "bill_net_gross",
            "nonbill_net_gross",
            "bill_net_maturing",
            "nonbill_net_maturing",
            "bill_net_issue_dates",
            "nonbill_net_issue_dates",
            "bill_net_issue_dates_missing_maturity",
            "nonbill_net_issue_dates_missing_maturity",
            "bill_net_reconstruction_status",
            "nonbill_net_reconstruction_status",
        ]
    ]
