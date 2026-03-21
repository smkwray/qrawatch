from __future__ import annotations

import re

import pandas as pd

from ati_shadow_policy.specs import SPEC_AUCTION_ABSORPTION_V1


_ROMAN_QUARTER = {"I": 1, "II": 2, "III": 3, "IV": 4}


def _quarter_from_title(value: object) -> str:
    text = " ".join(str(value or "").split())
    match = re.search(r"QUARTER\s+(I|II|III|IV)\s+(\d{4})", text, flags=re.IGNORECASE)
    if not match:
        return ""
    quarter_num = _ROMAN_QUARTER[match.group(1).upper()]
    year = match.group(2)
    return f"{year}Q{quarter_num}"


def _load_event_calendar(event_registry: pd.DataFrame) -> pd.DataFrame:
    calendar = event_registry.copy()
    calendar = calendar[["event_id", "quarter", "release_timestamp_et"]].copy()
    calendar["release_timestamp_et"] = pd.to_datetime(
        calendar["release_timestamp_et"],
        errors="coerce",
        utc=True,
    )
    calendar["window_start"] = calendar["release_timestamp_et"].dt.tz_convert("America/New_York").dt.tz_localize(None)
    calendar = calendar.sort_values("window_start", kind="stable").reset_index(drop=True)
    calendar["window_end"] = calendar["window_start"].shift(-1)
    return calendar


def _numeric(value: object) -> float | None:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return None
    return float(parsed)


def _auction_security_family(row: pd.Series) -> str:
    security_type = str(row.get("security_type", "") or "").strip().lower()
    security_term = str(row.get("security_term", "") or "").strip().lower()
    if str(row.get("inflation_index_security", "") or "").strip().lower() == "yes":
        return "tips"
    if str(row.get("floating_rate", "") or "").strip().lower() == "yes":
        return "frn"
    if security_type == "bill":
        return "bill"
    if "tips" in security_term:
        return "tips"
    return "nominal_coupon"


def _auction_stop_out_measure(row: pd.Series) -> tuple[str, float | None, str]:
    if _numeric(row.get("high_yield")) is not None:
        return "stop_out_yield", _numeric(row.get("high_yield")), "percent"
    if _numeric(row.get("high_investment_rate")) is not None:
        return "stop_out_investment_rate", _numeric(row.get("high_investment_rate")), "percent"
    if _numeric(row.get("high_discnt_rate")) is not None:
        return "stop_out_discount_rate", _numeric(row.get("high_discnt_rate")), "percent"
    return "stop_out_rate", None, "percent"


def _auction_tail_bp(row: pd.Series) -> float | None:
    high_yield = _numeric(row.get("high_yield"))
    avg_med_yield = _numeric(row.get("avg_med_yield"))
    if high_yield is not None and avg_med_yield is not None:
        return (high_yield - avg_med_yield) * 100.0
    high_discount = _numeric(row.get("high_discnt_rate"))
    avg_discount = _numeric(row.get("avg_med_discnt_rate"))
    if high_discount is not None and avg_discount is not None:
        return (high_discount - avg_discount) * 100.0
    return None


def _append_measure_row(
    rows: list[dict[str, object]],
    *,
    qra_event_id: object,
    quarter: object,
    auction_date: object,
    security_family: object,
    investor_class: object,
    measure: str,
    value: object,
    units: str,
    source_quality: object,
    provenance_summary: object,
    source_family: str,
    view_type: str,
    aggregation_method: str,
    coverage_status: str = "observed",
    coverage_note: object = pd.NA,
) -> None:
    if value is None or pd.isna(value):
        return
    rows.append(
        {
            "qra_event_id": qra_event_id,
            "quarter": quarter,
            "auction_date": auction_date,
            "security_family": security_family,
            "investor_class": investor_class,
            "measure": measure,
            "value": value,
            "units": units,
            "source_quality": source_quality,
            "provenance_summary": provenance_summary,
            "source_family": source_family,
            "view_type": view_type,
            "aggregation_method": aggregation_method,
            "coverage_status": coverage_status,
            "coverage_note": coverage_note,
            "spec_id": SPEC_AUCTION_ABSORPTION_V1,
        }
    )


def _empty_panel_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "qra_event_id",
            "quarter",
            "auction_date",
            "security_family",
            "investor_class",
            "measure",
            "value",
            "units",
            "source_quality",
            "provenance_summary",
            "source_family",
            "view_type",
            "aggregation_method",
            "coverage_status",
            "coverage_note",
            "spec_id",
        ]
    )


def build_auction_absorption_panel_v1(
    investor_allotments: pd.DataFrame,
    primary_dealer: pd.DataFrame,
    event_registry: pd.DataFrame,
    auction_results: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if event_registry.empty:
        return _empty_panel_frame()

    calendar = _load_event_calendar(event_registry)
    rows: list[dict[str, object]] = []

    if not investor_allotments.empty:
        allotments = investor_allotments.copy()
        allotments = allotments.loc[
            allotments.get("measure", pd.Series(dtype=str)).eq("allotment_amount")
            & allotments.get("investor_class", pd.Series(dtype=str)).ne("total_issue")
        ].copy()
        allotments["auction_date"] = pd.to_datetime(allotments["auction_date"], errors="coerce")
        for _, event in calendar.iterrows():
            start = event["window_start"]
            end = event["window_end"]
            subset = allotments.loc[allotments["auction_date"] >= start].copy()
            if pd.notna(end):
                subset = subset.loc[subset["auction_date"] < end].copy()
            if subset.empty:
                continue
            for _, row in subset.iterrows():
                rows.append(
                    {
                        "qra_event_id": event["event_id"],
                        "quarter": event["quarter"],
                        "auction_date": row["auction_date"].strftime("%Y-%m-%d"),
                        "security_family": row.get("security_family", pd.NA),
                        "investor_class": row.get("investor_class", pd.NA),
                        "measure": row.get("measure", pd.NA),
                        "value": row.get("value", pd.NA),
                        "units": row.get("units", pd.NA),
                        "source_quality": row.get("source_quality", pd.NA),
                        "provenance_summary": row.get("provenance", pd.NA),
                        "source_family": "investor_allotments",
                        "view_type": "event_adjacent_auction",
                        "aggregation_method": "sum",
                        "coverage_status": "observed",
                        "coverage_note": pd.NA,
                        "spec_id": SPEC_AUCTION_ABSORPTION_V1,
                    }
                )

            aggregate = (
                subset.groupby(["security_family", "investor_class", "measure", "units", "source_quality"], dropna=False)
                .agg(
                    value=("value", "sum"),
                    provenance_summary=("provenance", "first"),
                )
                .reset_index()
            )
            for _, row in aggregate.iterrows():
                rows.append(
                    {
                        "qra_event_id": event["event_id"],
                        "quarter": event["quarter"],
                        "auction_date": pd.NA,
                        "security_family": row.get("security_family", pd.NA),
                        "investor_class": row.get("investor_class", pd.NA),
                        "measure": row.get("measure", pd.NA),
                        "value": row.get("value", pd.NA),
                        "units": row.get("units", pd.NA),
                        "source_quality": row.get("source_quality", pd.NA),
                        "provenance_summary": row.get("provenance_summary", pd.NA),
                        "source_family": "investor_allotments",
                        "view_type": "qra_quarter_aggregate",
                        "aggregation_method": "sum",
                        "coverage_status": "observed",
                        "coverage_note": pd.NA,
                        "spec_id": SPEC_AUCTION_ABSORPTION_V1,
                    }
                )

    if not primary_dealer.empty:
        dealer = primary_dealer.copy()
        dealer = dealer.loc[dealer.get("source_dataset_type", pd.Series(dtype=str)).eq("quarterly_marketshare")].copy()
        dealer = dealer.loc[
            dealer["series_label"].fillna("").str.contains("U.S. TREASURY", case=False)
            | dealer["series_label"].fillna("").str.contains("TIPS", case=False)
        ].copy()
        dealer["quarter"] = dealer["source_title"].map(_quarter_from_title)
        dealer = dealer.loc[dealer["quarter"].astype(str).str.len() > 0].copy()
        if not dealer.empty:
            dealer["security_family"] = dealer["series_label"].map(
                lambda value: "tips" if "tips" in str(value).lower() else "nominal_coupon"
            )
            dealer["auction_date"] = pd.to_datetime(dealer["date"], errors="coerce").dt.strftime("%Y-%m-%d")
            quarter_to_event = event_registry[["event_id", "quarter"]].drop_duplicates()
            dealer = dealer.merge(quarter_to_event, on="quarter", how="left")
            for _, row in dealer.iterrows():
                rows.append(
                    {
                        "qra_event_id": row.get("event_id", pd.NA),
                        "quarter": row.get("quarter", pd.NA),
                        "auction_date": row.get("auction_date", pd.NA),
                        "security_family": row.get("security_family", pd.NA),
                        "investor_class": row.get("source_section", "primary_dealer_marketshare"),
                        "measure": row.get("metric_id", pd.NA),
                        "value": row.get("value", pd.NA),
                        "units": row.get("units", pd.NA),
                        "source_quality": row.get("source_quality", pd.NA),
                        "provenance_summary": row.get("provenance_summary", pd.NA),
                        "source_family": "primary_dealer",
                        "view_type": "quarterly_marketshare_snapshot",
                        "aggregation_method": "mean",
                        "coverage_status": "observed",
                        "coverage_note": pd.NA,
                        "spec_id": SPEC_AUCTION_ABSORPTION_V1,
                    }
                )

    if auction_results is not None and not auction_results.empty:
        auctions = auction_results.copy()
        auctions["auction_date"] = pd.to_datetime(auctions.get("auction_date"), errors="coerce")
        auctions = auctions.loc[auctions["auction_date"].notna()].copy()
        auctions = auctions.loc[
            auctions.get("security_type", pd.Series(index=auctions.index, dtype=object))
            .astype(str)
            .str.lower()
            .isin({"bill", "note", "bond"})
        ].copy()
        auctions["security_family"] = auctions.apply(_auction_security_family, axis=1)
        auctions["auction_date_iso"] = auctions["auction_date"].dt.strftime("%Y-%m-%d")
        for _, event in calendar.iterrows():
            start = event["window_start"]
            end = event["window_end"]
            subset = auctions.loc[auctions["auction_date"] >= start].copy()
            if pd.notna(end):
                subset = subset.loc[subset["auction_date"] < end].copy()
            if subset.empty:
                continue
            for _, row in subset.iterrows():
                provenance = (
                    "fiscaldata_auctions_query"
                    f"|security_type={row.get('security_type', '')}"
                    f"|security_term={row.get('security_term', '')}"
                )
                stop_out_measure, stop_out_value, stop_out_units = _auction_stop_out_measure(row)
                tail_bp = _auction_tail_bp(row)
                total_accepted = _numeric(row.get("total_accepted"))
                offering_amt = _numeric(row.get("offering_amt"))
                total_tendered = _numeric(row.get("total_tendered"))
                primary_accepted = _numeric(row.get("primary_dealer_accepted"))
                indirect_accepted = _numeric(row.get("indirect_bidder_accepted"))
                direct_accepted = _numeric(row.get("direct_bidder_accepted"))
                common = {
                    "qra_event_id": event["event_id"],
                    "quarter": event["quarter"],
                    "auction_date": row["auction_date_iso"],
                    "security_family": row["security_family"],
                    "source_quality": "official_fiscaldata_download",
                    "provenance_summary": provenance,
                    "source_family": "auction_results",
                    "view_type": "event_adjacent_auction_outcome",
                }
                _append_measure_row(
                    rows,
                    investor_class="all",
                    measure="bid_to_cover_ratio",
                    value=_numeric(row.get("bid_to_cover_ratio")),
                    units="ratio",
                    aggregation_method="mean",
                    **common,
                )
                _append_measure_row(
                    rows,
                    investor_class="all",
                    measure=stop_out_measure,
                    value=stop_out_value,
                    units=stop_out_units,
                    aggregation_method="mean",
                    **common,
                )
                _append_measure_row(
                    rows,
                    investor_class="all",
                    measure="tail_bp",
                    value=tail_bp,
                    units="basis_points",
                    aggregation_method="mean",
                    **common,
                )
                _append_measure_row(
                    rows,
                    investor_class="all",
                    measure="offering_amount_bn",
                    value=(offering_amt / 1_000_000_000.0) if offering_amt is not None else None,
                    units="USD billions",
                    aggregation_method="sum",
                    **common,
                )
                _append_measure_row(
                    rows,
                    investor_class="all",
                    measure="total_tendered_bn",
                    value=(total_tendered / 1_000_000_000.0) if total_tendered is not None else None,
                    units="USD billions",
                    aggregation_method="sum",
                    **common,
                )
                _append_measure_row(
                    rows,
                    investor_class="all",
                    measure="total_accepted_bn",
                    value=(total_accepted / 1_000_000_000.0) if total_accepted is not None else None,
                    units="USD billions",
                    aggregation_method="sum",
                    **common,
                )
                for investor_class, accepted_value in (
                    ("primary_dealer", primary_accepted),
                    ("indirect_bidder", indirect_accepted),
                    ("direct_bidder", direct_accepted),
                ):
                    _append_measure_row(
                        rows,
                        investor_class=investor_class,
                        measure="accepted_amount_bn",
                        value=(accepted_value / 1_000_000_000.0) if accepted_value is not None else None,
                        units="USD billions",
                        aggregation_method="sum",
                        **common,
                    )
                    share_value = None
                    if total_accepted not in (None, 0.0) and accepted_value is not None:
                        share_value = (accepted_value / total_accepted) * 100.0
                    _append_measure_row(
                        rows,
                        investor_class=investor_class,
                        measure="accepted_share_pct",
                        value=share_value,
                        units="percent",
                        aggregation_method="mean",
                        **common,
                    )
                _append_measure_row(
                    rows,
                    investor_class="all",
                    measure="when_issued_spread_available",
                    value=0.0,
                    units="indicator",
                    aggregation_method="mean",
                    coverage_status="not_in_source",
                    coverage_note="FiscalData auctions_query does not include when-issued pricing.",
                    **common,
                )

    output = pd.DataFrame(rows)
    if output.empty:
        return _empty_panel_frame()
    return output.sort_values(
        ["quarter", "qra_event_id", "source_family", "view_type", "auction_date", "security_family", "investor_class", "measure"],
        kind="stable",
        na_position="last",
    ).reset_index(drop=True)


def build_auction_absorption_table(panel: pd.DataFrame) -> pd.DataFrame:
    if panel.empty:
        return _empty_panel_frame()
    working = panel.copy()
    if "auction_date" not in working.columns:
        working["auction_date"] = pd.NA
    if "aggregation_method" not in working.columns:
        working["aggregation_method"] = "sum"
    if "coverage_status" not in working.columns:
        working["coverage_status"] = "observed"
    if "coverage_note" not in working.columns:
        working["coverage_note"] = pd.NA
    group_cols = [
        "qra_event_id",
        "quarter",
        "source_family",
        "view_type",
        "auction_date",
        "security_family",
        "investor_class",
        "measure",
        "units",
        "source_quality",
        "aggregation_method",
        "coverage_status",
        "coverage_note",
        "spec_id",
    ]
    grouped_frames: list[pd.DataFrame] = []
    for aggregation_method, subset in working.groupby("aggregation_method", dropna=False):
        if str(aggregation_method) == "mean":
            aggregated = subset.groupby(group_cols, dropna=False).agg(value=("value", "mean")).reset_index()
        else:
            aggregated = subset.groupby(group_cols, dropna=False).agg(value=("value", "sum")).reset_index()
        grouped_frames.append(aggregated)
    grouped = pd.concat(grouped_frames, ignore_index=True) if grouped_frames else _empty_panel_frame()
    return grouped.sort_values(
        ["quarter", "qra_event_id", "source_family", "view_type", "auction_date", "security_family", "investor_class", "measure"],
        kind="stable",
        na_position="last",
    ).reset_index(drop=True)
