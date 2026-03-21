from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

from .io_utils import ensure_dir, write_df, write_json
from .paths import OUTPUT_DIR, PROCESSED_DIR, RAW_DIR, TABLES_DIR

def get_publish_dir() -> Path:
    return OUTPUT_DIR / "publish"


PUBLISH_DIR = get_publish_dir()
OFFICIAL_CAPTURE_REQUIRED_FIELDS = [
    "quarter",
    "qra_release_date",
    "market_pricing_marker_minus_1d",
    "total_financing_need_bn",
    "net_bill_issuance_bn",
    "source_url",
    "source_doc_local",
    "source_doc_type",
    "qa_status",
]
OFFICIAL_QA_STATUSES = {"manual_official_capture", "parser_verified"}
EXTENSION_REGISTRY = {
    "investor_allotments": {
        "inventory_path": PROCESSED_DIR / "investor_allotments.csv",
        "panel_path": PROCESSED_DIR / "investor_allotments_panel.csv",
        "publish_name": "investor_allotments_summary",
        "dataset_status": "summary_ready",
    },
    "primary_dealer": {
        "inventory_path": PROCESSED_DIR / "primary_dealer_inventory.csv",
        "panel_path": PROCESSED_DIR / "primary_dealer_panel.csv",
        "publish_name": "primary_dealer_summary",
        "dataset_status": "summary_ready",
    },
    "sec_nmfp": {
        "inventory_path": PROCESSED_DIR / "sec_nmfp_inventory.csv",
        "panel_path": PROCESSED_DIR / "sec_nmfp_summary_panel.csv",
        "publish_name": "sec_nmfp_summary",
        "dataset_status": "summary_ready",
    },
    "tic": {
        "inventory_path": PROCESSED_DIR / "tic_inventory.csv",
        "panel_path": PROCESSED_DIR / "tic_panel.csv",
        "publish_name": "tic_summary",
        "dataset_status": "not_started",
    },
}


def _extension_inventory_path(config: dict[str, object]) -> Path:
    return PROCESSED_DIR / Path(str(config["inventory_path"])).name


def _extension_panel_path(config: dict[str, object]) -> Path:
    return PROCESSED_DIR / Path(str(config["panel_path"])).name


def _write_markdown_table(title: str, df: pd.DataFrame, path: Path) -> None:
    ensure_dir(path.parent)
    if df.empty:
        body = "_No rows available._"
    else:
        body = df.to_markdown(index=False)
    path.write_text(f"# {title}\n\n{body}\n", encoding="utf-8")


def _write_records_json(records: list[dict], path: Path, title: str) -> None:
    write_json({"title": title, "rows": records}, path)


def publish_table(name: str, title: str, df: pd.DataFrame) -> None:
    base = get_publish_dir() / name
    write_df(df, base.with_suffix(".csv"))
    _write_records_json(df.to_dict(orient="records"), base.with_suffix(".json"), title)
    _write_markdown_table(title, df, base.with_suffix(".md"))


def _read_processed_csv(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        if columns is None:
            return pd.DataFrame()
        return pd.DataFrame(columns=columns)
    df = pd.read_csv(path)
    if columns is None:
        return df
    keep = [col for col in columns if col in df.columns]
    return df[keep].copy()


def _split_pipe_values(value: object) -> list[str]:
    text = str(value or "").strip()
    if not text or text in {"nan", "None"}:
        return []
    parts = []
    for part in text.split("|"):
        cleaned = part.strip()
        if cleaned and cleaned not in parts:
            parts.append(cleaned)
    return parts


def _has_seed_dependency(source_doc_type: object, source_doc_local: object) -> bool:
    combined = " ".join(_split_pipe_values(source_doc_type) + _split_pipe_values(source_doc_local))
    return "seed_csv" in combined


def _missing_critical_fields(row: pd.Series, required_fields: Iterable[str]) -> list[str]:
    missing: list[str] = []
    for field in required_fields:
        if field not in row.index:
            missing.append(field)
            continue
        value = row[field]
        if pd.isna(value) or str(value).strip() == "":
            missing.append(field)
    return missing


def _source_quality(row: pd.Series) -> str:
    qa_status = str(row.get("qa_status", "") or "").strip()
    source_doc_type = row.get("source_doc_type", "")
    source_doc_local = row.get("source_doc_local", "")
    if qa_status in OFFICIAL_QA_STATUSES and not _has_seed_dependency(source_doc_type, source_doc_local):
        return "exact_official"
    if qa_status == "semi_automated_capture":
        return "official_hybrid"
    if qa_status == "seed_only":
        return "seed_only"
    return "unknown"


def _readiness_tier(source_quality: str, missing_fields: list[str]) -> str:
    if missing_fields:
        return "incomplete"
    if source_quality == "exact_official":
        return "headline_ready"
    if source_quality == "official_hybrid":
        return "hybrid_ready"
    return "fallback_only"


def _provenance_summary(row: pd.Series) -> str:
    parts: list[str] = []
    source_url = str(row.get("source_url", "") or "").strip()
    if source_url:
        parts.append(f"source={source_url.split('|')[0]}")
    source_doc_type = str(row.get("source_doc_type", "") or "").strip()
    if source_doc_type:
        parts.append(f"doc_type={source_doc_type}")
    qa_status = str(row.get("qa_status", "") or "").strip()
    if qa_status:
        parts.append(f"qa={qa_status}")
    return "; ".join(parts)


def build_ati_publish_table() -> pd.DataFrame:
    return _official_ati_headline_table()


def build_ati_seed_forecast_table() -> pd.DataFrame:
    seed = _read_processed_csv(PROCESSED_DIR / "ati_index_seed.csv")
    if seed.empty:
        return pd.DataFrame(
            columns=[
                "quarter",
                "financing_need_bn",
                "net_bills_bn",
                "bill_share",
                "missing_coupons_15_bn",
                "missing_coupons_18_bn",
                "missing_coupons_20_bn",
                "ati_baseline_bn",
                "seed_source",
                "seed_quality",
                "headline_ready",
                "non_headline_reason",
                "public_role",
            ]
        )
    official = _official_ati_headline_table()
    official_quarters = set(official.get("quarter", pd.Series(dtype=str)).dropna().astype(str))
    seed = seed.loc[~seed["quarter"].astype(str).isin(official_quarters)].copy()
    if seed.empty:
        return pd.DataFrame(
            columns=[
                "quarter",
                "financing_need_bn",
                "net_bills_bn",
                "bill_share",
                "missing_coupons_15_bn",
                "missing_coupons_18_bn",
                "missing_coupons_20_bn",
                "ati_baseline_bn",
                "seed_source",
                "seed_quality",
                "headline_ready",
                "non_headline_reason",
                "public_role",
            ]
        )
    seed["headline_ready"] = False
    seed["non_headline_reason"] = seed["seed_quality"].apply(
        lambda value: (
            "seed_forecast_without_official_capture"
            if "forecast" in str(value or "")
            else "seed_estimate_without_official_capture"
        )
    )
    seed["public_role"] = "supporting"
    keep = [
        "quarter",
        "financing_need_bn",
        "net_bills_bn",
        "bill_share",
        "missing_coupons_15_bn",
        "missing_coupons_18_bn",
        "missing_coupons_20_bn",
        "ati_baseline_bn",
        "seed_source",
        "seed_quality",
        "headline_ready",
        "non_headline_reason",
        "public_role",
    ]
    return seed[keep].sort_values("quarter").reset_index(drop=True)


def build_official_capture_publish_table() -> pd.DataFrame:
    path = PROCESSED_DIR / "official_quarterly_refunding_capture.csv"
    if not path.exists():
        return pd.DataFrame(
            columns=[
                "quarter",
                "qra_release_date",
                "market_pricing_marker_minus_1d",
                "total_financing_need_bn",
                "net_bill_issuance_bn",
                "source_url",
                "source_doc_type",
                "qa_status",
                "readiness_tier",
                "source_quality",
                "headline_ready",
                "fallback_only",
                "missing_critical_fields",
                "provenance_summary",
            ]
        )
    df = pd.read_csv(path)
    df = _add_official_capture_status_columns(df)
    keep = [
        "quarter",
        "qra_release_date",
        "market_pricing_marker_minus_1d",
        "total_financing_need_bn",
        "net_bill_issuance_bn",
        "source_url",
        "source_doc_type",
        "qa_status",
        "readiness_tier",
        "source_quality",
        "headline_ready",
        "fallback_only",
        "missing_critical_fields",
        "provenance_summary",
    ]
    return df[[c for c in keep if c in df.columns]].copy()


def build_official_capture_readiness_table() -> pd.DataFrame:
    path = PROCESSED_DIR / "official_quarterly_refunding_capture.csv"
    if not path.exists():
        return pd.DataFrame(
            columns=[
                "quarter",
                "readiness_tier",
                "source_quality",
                "headline_ready",
                "fallback_only",
                "missing_critical_fields",
                "provenance_summary",
            ]
        )
    df = pd.read_csv(path)
    df = _add_official_capture_status_columns(df)
    keep = [
        "quarter",
        "readiness_tier",
        "source_quality",
        "headline_ready",
        "fallback_only",
        "missing_critical_fields",
        "provenance_summary",
    ]
    return df[keep].copy()


def _add_official_capture_status_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["source_quality"] = out.apply(_source_quality, axis=1)
    out["missing_critical_fields"] = out.apply(
        lambda row: "|".join(_missing_critical_fields(row, OFFICIAL_CAPTURE_REQUIRED_FIELDS)),
        axis=1,
    )
    out["readiness_tier"] = out.apply(
        lambda row: _readiness_tier(
            str(row.get("source_quality", "") or ""),
            _missing_critical_fields(row, OFFICIAL_CAPTURE_REQUIRED_FIELDS),
        ),
        axis=1,
    )
    out["headline_ready"] = out["readiness_tier"].eq("headline_ready")
    out["fallback_only"] = ~out["headline_ready"]
    out["provenance_summary"] = out.apply(_provenance_summary, axis=1)
    return out


def _official_capture_with_status_columns() -> pd.DataFrame:
    path = PROCESSED_DIR / "official_quarterly_refunding_capture.csv"
    if not path.exists():
        return pd.DataFrame(
            columns=[
                "quarter",
                "readiness_tier",
                "source_quality",
                "headline_ready",
                "fallback_only",
                "missing_critical_fields",
                "provenance_summary",
            ]
        )
    df = pd.read_csv(path)
    return _add_official_capture_status_columns(df)


def _official_ati_headline_table() -> pd.DataFrame:
    columns = [
        "quarter",
        "financing_need_bn",
        "net_bills_bn",
        "bill_share",
        "missing_coupons_15_bn",
        "missing_coupons_18_bn",
        "missing_coupons_20_bn",
        "ati_baseline_bn",
    ]
    path = PROCESSED_DIR / "ati_index_official_capture.csv"
    if not path.exists():
        return pd.DataFrame(columns=columns + ["source_quality", "public_role"])
    official = pd.read_csv(path)
    if official.empty:
        return pd.DataFrame(columns=columns + ["source_quality", "public_role"])

    qa_status = official.get("qa_status")
    if qa_status is None:
        quality_mask = pd.Series(True, index=official.index, dtype=bool)
    else:
        quality_mask = qa_status.astype(str).str.strip().isin(OFFICIAL_QA_STATUSES)

    seed_mask = official.apply(
        lambda row: _has_seed_dependency(row.get("source_doc_type", ""), row.get("source_doc_local", "")),
        axis=1,
    )
    headline = official.loc[quality_mask & ~seed_mask].copy()
    if headline.empty:
        return pd.DataFrame(columns=columns + ["source_quality", "public_role"])

    for col in columns:
        if col not in headline.columns:
            headline[col] = pd.NA
    headline["source_quality"] = "exact_official_numeric"
    headline["public_role"] = "headline"
    return headline[columns + ["source_quality", "public_role"]].sort_values("quarter").reset_index(drop=True)


def build_ati_seed_vs_official_comparison() -> pd.DataFrame:
    seed = _read_processed_csv(PROCESSED_DIR / "ati_index_seed.csv")
    seed = seed[["quarter", "ati_baseline_bn"]].rename(columns={"ati_baseline_bn": "ati_seed_bn"})

    official_path = PROCESSED_DIR / "ati_index_official_capture.csv"
    if official_path.exists():
        official = pd.read_csv(official_path)
        official = official[["quarter", "ati_baseline_bn", "qa_status"]].rename(columns={"ati_baseline_bn": "ati_official_bn"})
    else:
        official = pd.DataFrame(columns=["quarter", "ati_official_bn", "qa_status"])

    comparison = seed.merge(official, on="quarter", how="outer")
    comparison["ati_diff_official_minus_seed"] = comparison["ati_official_bn"] - comparison["ati_seed_bn"]
    capture = _official_capture_with_status_columns()
    capture = capture[
        [
            "quarter",
            "readiness_tier",
            "source_quality",
            "headline_ready",
            "fallback_only",
            "missing_critical_fields",
            "provenance_summary",
        ]
    ]
    comparison = comparison.merge(capture, on="quarter", how="left")
    comparison["comparison_status"] = comparison["ati_official_bn"].apply(
        lambda value: "official_matched" if pd.notna(value) else "seed_only"
    )
    comparison["official_capture_present"] = comparison["ati_official_bn"].notna()
    comparison["source_quality"] = comparison["source_quality"].fillna("seed_only")
    comparison["readiness_tier"] = comparison["readiness_tier"].fillna("seed_only")
    comparison["headline_ready"] = comparison["headline_ready"].fillna(False).astype(bool)
    comparison["fallback_only"] = comparison["fallback_only"].fillna(True).astype(bool)
    comparison["missing_critical_fields"] = comparison["missing_critical_fields"].fillna("official_capture_missing")
    comparison["provenance_summary"] = comparison["provenance_summary"].fillna("seed-only comparison row")
    return comparison.sort_values("quarter").reset_index(drop=True)


def build_qra_event_publish_table() -> pd.DataFrame:
    df = pd.read_csv(PROCESSED_DIR / "qra_event_panel.csv")
    id_cols = [
        "quarter",
        "event_id",
        "event_label",
        "official_release_date",
        "market_pricing_marker_minus_1d",
        "event_date_requested",
        "event_date_aligned",
        "event_date_type",
        "policy_statement_url",
        "financing_estimates_url",
        "timing_quality",
        "expected_direction",
        "current_quarter_action",
        "forward_guidance_bias",
        "headline_bucket",
        "shock_sign_curated",
        "classification_confidence",
        "classification_review_status",
    ]
    metric_cols = [c for c in df.columns if c.endswith("_d1") or c.endswith("_d3")]
    keep = [c for c in id_cols if c in df.columns]
    return df[keep + sorted(metric_cols)].copy()


def build_qra_summary_publish_table() -> pd.DataFrame:
    return pd.read_csv(TABLES_DIR / "qra_event_summary.csv")

def build_qra_robustness_publish_table() -> pd.DataFrame:
    path = TABLES_DIR / "qra_event_summary_robustness.csv"
    if not path.exists():
        return pd.DataFrame(
            columns=["sample_variant", "event_date_type", "headline_bucket", "n_events"]
        )
    return pd.read_csv(path)


def _qra_elasticity_publish_columns() -> list[str]:
    return [
        "quarter",
        "event_id",
        "event_label",
        "event_date_requested",
        "event_date_aligned",
        "event_date_type",
        "policy_statement_url",
        "financing_estimates_url",
        "timing_quality",
        "current_quarter_action",
        "forward_guidance_bias",
        "headline_bucket",
        "shock_sign_curated",
        "classification_confidence",
        "classification_review_status",
        "series",
        "window",
        "delta_pp",
        "delta_bp",
        "shock_bn",
        "previous_event_id",
        "previous_quarter",
        "gross_notional_delta_bn",
        "schedule_diff_10y_eq_bn",
        "schedule_diff_dynamic_10y_eq_bn",
        "schedule_diff_dv01_usd",
        "shock_construction",
        "shock_source",
        "shock_notes",
        "shock_review_status",
        "shock_missing_flag",
        "small_denominator_flag",
        "elasticity_bp_per_100bn",
        "sign_flip_flag",
        "usable_for_headline",
    ]


def _empty_qra_elasticity_publish_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=_qra_elasticity_publish_columns())


def _qra_event_shock_summary_publish_columns() -> list[str]:
    return [
        "quarter",
        "event_id",
        "event_label",
        "event_date_requested",
        "event_date_aligned",
        "event_date_type",
        "policy_statement_url",
        "financing_estimates_url",
        "timing_quality",
        "current_quarter_action",
        "forward_guidance_bias",
        "headline_bucket",
        "shock_sign_curated",
        "classification_confidence",
        "classification_review_status",
        "shock_bn",
        "previous_event_id",
        "previous_quarter",
        "gross_notional_delta_bn",
        "schedule_diff_10y_eq_bn",
        "schedule_diff_dynamic_10y_eq_bn",
        "schedule_diff_dv01_usd",
        "shock_construction",
        "shock_source",
        "shock_notes",
        "shock_review_status",
        "shock_missing_flag",
        "small_denominator_flag",
        "sign_flip_flag",
        "usable_for_headline",
    ]


def _empty_qra_event_shock_summary_publish_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=_qra_event_shock_summary_publish_columns())


def build_qra_event_elasticity_publish_table() -> pd.DataFrame:
    path = TABLES_DIR / "qra_event_elasticity.csv"
    if not path.exists():
        return _empty_qra_elasticity_publish_frame()
    df = pd.read_csv(path)
    df = df.loc[df.get("event_date_type", pd.Series(dtype=str)).astype(str) == "official_release_date"].copy()
    keep = [column for column in _qra_elasticity_publish_columns() if column in df.columns]
    return df[keep].reset_index(drop=True)


def build_qra_event_elasticity_diagnostic_publish_table() -> pd.DataFrame:
    path = TABLES_DIR / "qra_event_elasticity.csv"
    if not path.exists():
        return _empty_qra_elasticity_publish_frame()
    df = pd.read_csv(path)
    keep = [column for column in _qra_elasticity_publish_columns() if column in df.columns]
    return df[keep].copy()


def build_qra_event_shock_summary_publish_table() -> pd.DataFrame:
    path = TABLES_DIR / "qra_event_elasticity.csv"
    if not path.exists():
        return _empty_qra_event_shock_summary_publish_frame()
    df = pd.read_csv(path)
    df = df.loc[df.get("event_date_type", pd.Series(dtype=str)).astype(str) == "official_release_date"].copy()
    if df.empty:
        return _empty_qra_event_shock_summary_publish_frame()
    keep = [column for column in _qra_event_shock_summary_publish_columns() if column in df.columns]
    return (
        df[keep]
        .drop_duplicates(subset=["event_id", "event_date_type"], keep="first")
        .sort_values(["event_date_requested", "event_id"], kind="stable")
        .reset_index(drop=True)
    )


def build_qra_event_shock_components_publish_table() -> pd.DataFrame:
    path = TABLES_DIR / "qra_event_shock_components.csv"
    if not path.exists():
        return pd.DataFrame(
            columns=[
                "event_id",
                "quarter",
                "previous_event_id",
                "previous_quarter",
                "tenor",
                "issue_type",
                "current_total_bn",
                "previous_total_bn",
                "delta_bn",
                "yield_date",
                "yield_curve_source",
                "tenor_yield_pct",
                "tenor_modified_duration",
                "duration_factor_source",
                "dynamic_10y_eq_weight",
                "contribution_dynamic_10y_eq_bn",
                "dv01_per_1bn_usd",
                "dv01_contribution_usd",
                "tenor_weight_10y_eq",
                "contribution_10y_eq_bn",
            ]
        )
    return pd.read_csv(path)


def build_plumbing_publish_table() -> pd.DataFrame:
    df = pd.read_csv(TABLES_DIR / "plumbing_regressions.csv")
    keep = [
        "dependent_variable",
        "term",
        "coef",
        "std_err",
        "t_stat",
        "p_value",
        "nobs",
        "rsquared",
        "series_role",
        "bill_proxy_source_quality",
        "duration_proxy_source_quality",
        "proxy_units",
        "frequency",
        "sign_convention",
        "notes",
    ]
    return df[[c for c in keep if c in df.columns]].copy()

def build_plumbing_robustness_publish_table() -> pd.DataFrame:
    path = TABLES_DIR / "plumbing_robustness.csv"
    if not path.exists():
        return pd.DataFrame(
            columns=["dependent_variable", "variant_id", "proxy_role", "term", "coef", "p_value"]
        )
    return pd.read_csv(path)


def build_duration_publish_table() -> pd.DataFrame:
    preferred = PROCESSED_DIR / "public_duration_supply.csv"
    fallback = PROCESSED_DIR / "public_duration_supply_provisional.csv"
    df = pd.read_csv(preferred if preferred.exists() else fallback)
    keep = [
        "date",
        "coupon_like_total",
        "headline_public_duration_supply",
        "provisional_public_duration_supply",
        "headline_source_quality",
        "fallback_source_quality",
        "qt_proxy",
        "buybacks_accepted",
        "value_units",
        "frequency",
        "sign_convention",
        "notes",
    ]
    for col in keep:
        if col not in df.columns:
            df[col] = pd.NA
    df["date"] = pd.to_datetime(df["date"])
    latest = (
        df.sort_values("date")
        .tail(12)[keep]
        .copy()
    )
    latest["date"] = latest["date"].dt.strftime("%Y-%m-%d")
    return latest


def build_duration_comparison_publish_table() -> pd.DataFrame:
    path = TABLES_DIR / "duration_supply_comparison.csv"
    if not path.exists():
        return pd.DataFrame(
            columns=["date", "construction_id", "construction_family", "value", "value_units"]
        )
    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    return df


def _raw_file_count(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for child in path.rglob("*") if child.is_file())


def build_data_sources_publish_table() -> pd.DataFrame:
    rows = []
    registry = [
        ("fiscaldata", RAW_DIR / "fiscaldata"),
        ("fred", RAW_DIR / "fred"),
        ("qra", RAW_DIR / "qra"),
        ("investor_allotments", RAW_DIR / "investor_allotments"),
        ("primary_dealer", RAW_DIR / "primary_dealer"),
        ("sec_nmfp", RAW_DIR / "sec_nmfp"),
    ]
    for source_family, path in registry:
        manifest_path = path / "manifest.csv"
        downloads_path = path / "downloads.csv"
        rows.append(
            {
                "source_family": source_family,
                "raw_dir_exists": path.exists(),
                "file_count": _raw_file_count(path),
                "manifest_exists": manifest_path.exists(),
                "downloads_exists": downloads_path.exists(),
            }
        )
    return pd.DataFrame(rows)


def build_extension_status_table() -> pd.DataFrame:
    rows = []
    for name, config in EXTENSION_REGISTRY.items():
        raw_dir = RAW_DIR / name
        processed_path = _extension_inventory_path(config)
        panel_path = _extension_panel_path(config)
        publish_path = get_publish_dir() / f"{config['publish_name']}.csv"
        manifest_exists = (raw_dir / "manifest.csv").exists()
        downloads_exists = (raw_dir / "downloads.csv").exists()
        processed_exists = processed_path.exists()
        panel_exists = panel_path.exists()
        publish_exists = publish_path.exists()
        processed_rows = 0
        if processed_exists:
            try:
                processed_rows = len(pd.read_csv(processed_path))
            except Exception:
                processed_rows = 0
        panel_rows = 0
        if panel_exists:
            try:
                panel_rows = len(pd.read_csv(panel_path))
            except Exception:
                panel_rows = 0
        if panel_exists and publish_exists and name != "tic":
            backend_status = "summary_ready"
            readiness_tier = "summary_ready"
        elif processed_exists:
            backend_status = "processed"
            readiness_tier = "inventory_ready"
        elif downloads_exists or manifest_exists:
            backend_status = "raw_only"
            readiness_tier = "raw_only"
        else:
            backend_status = "not_started"
            readiness_tier = "not_started"
        rows.append(
            {
                "extension": name,
                "raw_dir_exists": raw_dir.exists(),
                "manifest_exists": manifest_exists,
                "downloads_exists": downloads_exists,
                "processed_exists": processed_exists,
                "processed_rows": processed_rows,
                "panel_exists": panel_exists,
                "panel_rows": panel_rows,
                "publish_exists": publish_exists,
                "backend_status": backend_status,
                "readiness_tier": readiness_tier,
                "headline_ready": False,
                "public_role": "supporting",
            }
        )
    return pd.DataFrame(rows)


def _build_extension_summary_table(name: str, columns: list[str]) -> pd.DataFrame:
    config = EXTENSION_REGISTRY[name]
    panel_path = _extension_panel_path(config)
    if not panel_path.exists():
        return pd.DataFrame(columns=columns)
    df = pd.read_csv(panel_path)
    for col in columns:
        if col not in df.columns:
            df[col] = pd.NA
    return df[columns].copy()


def build_investor_allotments_summary_table() -> pd.DataFrame:
    path = _extension_panel_path(EXTENSION_REGISTRY["investor_allotments"])
    columns = [
        "summary_type",
        "security_family",
        "investor_class",
        "measure",
        "value",
        "units",
        "as_of_date",
        "source_quality",
        "source_file",
    ]
    if not path.exists():
        return pd.DataFrame(columns=columns)
    panel = pd.read_csv(path)
    if panel.empty:
        return pd.DataFrame(columns=columns)
    amount = panel.loc[panel.get("measure", pd.Series(dtype=str)).eq("allotment_amount")].copy()
    amount = amount.loc[amount.get("investor_class", pd.Series(dtype=str)).ne("total_issue")].copy()
    total_issue = panel.loc[
        panel.get("measure", pd.Series(dtype=str)).eq("allotment_amount")
        & panel.get("investor_class", pd.Series(dtype=str)).eq("total_issue")
    ].copy()
    latest_date = str(amount["auction_date"].max()) if not amount.empty else ""
    coverage = (
        amount.groupby("security_family", dropna=False)
        .agg(value=("auction_date", "size"), as_of_date=("auction_date", "max"))
        .reset_index()
        .assign(
            summary_type="coverage",
            investor_class="all",
            measure="auction_rows",
            units="count",
            source_quality="summary_ready",
            source_file=path.name,
        )
    )
    latest_snapshot = (
        amount.loc[amount["auction_date"] == latest_date]
        .groupby(["security_family", "investor_class"], dropna=False)
        .agg(value=("value", "sum"))
        .reset_index()
        .assign(
            summary_type="latest_snapshot",
            measure="allotment_amount",
            units="USD billions",
            as_of_date=latest_date,
            source_quality="summary_ready",
            source_file=path.name,
        )
    )
    bill_coupon = (
        total_issue.groupby("security_family", dropna=False)
        .agg(value=("value", "sum"))
        .reset_index()
        .assign(
            summary_type="bill_vs_coupon_split",
            investor_class="all",
            measure="total_issue_amount",
            units="USD billions",
            as_of_date=latest_date,
            source_quality="summary_ready",
            source_file=path.name,
        )
    )
    return pd.concat([coverage, latest_snapshot, bill_coupon], ignore_index=True)[columns]


def build_primary_dealer_summary_table() -> pd.DataFrame:
    path = _extension_panel_path(EXTENSION_REGISTRY["primary_dealer"])
    columns = [
        "summary_type",
        "dataset_type",
        "series_id",
        "measure",
        "value",
        "units",
        "as_of_date",
        "frequency",
        "source_quality",
        "source_file",
    ]
    if not path.exists():
        return pd.DataFrame(columns=columns)
    panel = pd.read_csv(path)
    if panel.empty:
        return pd.DataFrame(columns=columns)
    available = (
        panel.groupby(["source_dataset_type", "source_quality", "frequency"], dropna=False)
        .agg(
            value=("series_id", pd.Series.nunique),
            as_of_date=("date", "max"),
        )
        .reset_index()
        .rename(columns={"source_dataset_type": "dataset_type"})
        .assign(
            summary_type="available_series",
            series_id="all",
            measure="series_count",
            units="count",
            source_file=path.name,
        )
    )
    latest = (
        panel.groupby(["source_dataset_type", "source_quality", "frequency"], dropna=False)
        .agg(as_of_date=("date", "max"))
        .reset_index()
        .rename(columns={"source_dataset_type": "dataset_type"})
    )
    latest_rows = panel.merge(
        latest,
        left_on=["source_dataset_type", "source_quality", "frequency", "date"],
        right_on=["dataset_type", "source_quality", "frequency", "as_of_date"],
        how="inner",
    )
    latest_summary = (
        latest_rows.groupby(["dataset_type", "source_quality", "frequency"], dropna=False)
        .agg(value=("series_id", "size"), as_of_date=("date", "max"))
        .reset_index()
        .assign(
            summary_type="latest_snapshot",
            series_id="all",
            measure="latest_row_count",
            units="count",
            source_file=path.name,
        )
    )
    marketshare = (
        panel.loc[panel["source_dataset_type"].isin(["quarterly_marketshare", "ytd_marketshare"])]
        .groupby(["source_dataset_type", "source_quality", "frequency"], dropna=False)
        .agg(value=("series_id", pd.Series.nunique), as_of_date=("date", "max"))
        .reset_index()
        .rename(columns={"source_dataset_type": "dataset_type"})
        .assign(
            summary_type="marketshare_coverage",
            series_id="all",
            measure="series_count",
            units="count",
            source_file=path.name,
        )
    )
    return pd.concat([available, latest_summary, marketshare], ignore_index=True)[columns]


def build_sec_nmfp_summary_table() -> pd.DataFrame:
    return _build_extension_summary_table(
        "sec_nmfp",
        [
            "summary_type",
            "dataset_version",
            "period_family",
            "period_label",
            "measure",
            "value",
            "units",
            "source_quality",
            "source_file",
        ],
    )


def build_official_capture_completion_publish_table() -> pd.DataFrame:
    path = PROCESSED_DIR / "official_capture_completion_status.csv"
    return _read_processed_csv(
        path,
        columns=[
            "quarter",
            "completion_tier",
            "qa_status",
            "uses_seed_source",
            "net_bill_issuance_bn",
            "reconstructed_net_bill_issuance_bn",
            "reconstruction_status_bill",
            "is_headline_ready",
        ],
    )


def build_series_metadata_catalog() -> pd.DataFrame:
    rows = [
        {
            "dataset": "official_ati",
            "series_id": "ati_baseline_bn",
            "frequency": "quarterly",
            "value_units": "USD billions",
            "sign_convention": "Positive values imply bill issuance above the 18% baseline share.",
            "source_quality": "exact_official_numeric",
            "series_role": "headline",
            "public_role": "headline",
        },
        {
            "dataset": "plumbing",
            "series_id": "bill_net_exact",
            "frequency": "weekly (W-WED)",
            "value_units": "USD notional (weekly sum)",
            "sign_convention": "Positive values imply more bill supply to the public.",
            "source_quality": "exact_official_net",
            "series_role": "headline",
            "public_role": "headline",
        },
        {
            "dataset": "plumbing",
            "series_id": "nonbill_net_exact",
            "frequency": "weekly (W-WED)",
            "value_units": "USD notional (weekly sum)",
            "sign_convention": "Positive values imply more combined non-bill supply to the public.",
            "source_quality": "exact_official_net",
            "series_role": "headline",
            "public_role": "headline",
        },
        {
            "dataset": "duration",
            "series_id": "headline_public_duration_supply",
            "frequency": "weekly (W-WED)",
            "value_units": "USD notional (weekly sum)",
            "sign_convention": "Positive values imply more duration supply to the public.",
            "source_quality": "hybrid_exact_nonbill_net_plus_qt_proxy",
            "series_role": "headline",
            "public_role": "headline",
        },
        {
            "dataset": "duration",
            "series_id": "provisional_public_duration_supply",
            "frequency": "weekly (W-WED)",
            "value_units": "USD notional (weekly sum)",
            "sign_convention": "Positive values imply more duration supply to the public.",
            "source_quality": "fallback_gross_coupon_proxy_plus_qt_proxy",
            "series_role": "fallback",
            "public_role": "supporting",
        },
        {
            "dataset": "qra_event_elasticity",
            "series_id": "elasticity_bp_per_100bn",
            "frequency": "QRA event window",
            "value_units": "basis points per $100bn 10y-equivalent duration shock",
            "sign_convention": "Positive values imply higher yields per positive announced duration shock.",
            "source_quality": "manual_qra_shock_template_plus_event_panel",
            "series_role": "supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "investor_allotments",
            "series_id": "investor_allotments_summary",
            "frequency": "auction-event",
            "value_units": "reported Treasury allotment units",
            "sign_convention": "Shares and amounts follow Treasury investor allotment source files.",
            "source_quality": "summary_ready",
            "series_role": "supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "primary_dealer",
            "series_id": "primary_dealer_summary",
            "frequency": "mixed",
            "value_units": "reported New York Fed units",
            "sign_convention": "Values follow the canonical dealer export source for each summary row.",
            "source_quality": "summary_ready",
            "series_role": "supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "sec_nmfp",
            "series_id": "sec_nmfp_summary",
            "frequency": "mixed",
            "value_units": "counts / availability flags",
            "sign_convention": "Positive counts indicate greater archive and field coverage.",
            "source_quality": "summary_ready",
            "series_role": "supporting",
            "public_role": "supporting",
        },
    ]
    return pd.DataFrame(rows)


def _artifact_mtime(path: Path) -> str:
    if not path.exists():
        return ""
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()


def _qra_elasticity_readiness(path: Path) -> dict[str, object]:
    if not path.exists():
        return {
            "readiness_tier": "not_started",
            "fallback_only": True,
            "missing_critical_fields": "",
        }
    try:
        df = pd.read_csv(path)
    except Exception:
        return {
            "readiness_tier": "schema_incomplete",
            "fallback_only": True,
            "missing_critical_fields": "read_error",
        }

    missing: list[str] = []
    required_columns = {
        "quarter",
        "event_id",
        "event_date_type",
        "series",
        "window",
        "headline_bucket",
        "classification_review_status",
        "shock_review_status",
        "usable_for_headline",
        "elasticity_bp_per_100bn",
    }
    for column in required_columns:
        if column not in df.columns:
            missing.append(f"missing_column:{column}")
    if "quarter" in df.columns and df["quarter"].fillna("").astype(str).str.strip().eq("").any():
        missing.append("null_quarter")
    if {"event_id", "event_date_type", "series", "window"}.issubset(df.columns):
        if df.duplicated(subset=["event_id", "event_date_type", "series", "window"]).any():
            missing.append("duplicate_event_series_window")
    if "shock_review_status" in df.columns:
        invalid = sorted(
            {
                str(value)
                for value in df["shock_review_status"].dropna().astype(str)
                if str(value).strip() and str(value).strip() not in {"reviewed", "provisional", "pending"}
            }
        )
        if invalid:
            missing.append("invalid_shock_review_status")
    if "classification_review_status" in df.columns:
        invalid = sorted(
            {
                str(value)
                for value in df["classification_review_status"].dropna().astype(str)
                if str(value).strip() and str(value).strip() not in {"reviewed", "provisional", "pending"}
            }
        )
        if invalid:
            missing.append("invalid_classification_review_status")

    readiness_tier = "supporting_ready" if not missing else "supporting_provisional"
    return {
        "readiness_tier": readiness_tier,
        "fallback_only": readiness_tier != "supporting_ready",
        "missing_critical_fields": "|".join(missing),
    }


def build_dataset_status_table() -> pd.DataFrame:
    official_capture = build_official_capture_readiness_table()
    extension_status = build_extension_status_table()
    official_ati_headline = _official_ati_headline_table()
    official_ati_headline_ready = not official_ati_headline.empty
    qra_elasticity_readiness = _qra_elasticity_readiness(TABLES_DIR / "qra_event_elasticity.csv")
    rows = [
        {
            "dataset": "official_capture",
            "readiness_tier": (
                "headline_ready"
                if not official_capture.empty and bool(official_capture["headline_ready"].all())
                else "fallback_only"
            ),
            "source_quality": (
                "exact_official"
                if not official_capture.empty and set(official_capture["source_quality"]) == {"exact_official"}
                else "mixed"
            ),
            "headline_ready": bool(not official_capture.empty and official_capture["headline_ready"].all()),
            "fallback_only": bool(official_capture.empty or official_capture["fallback_only"].any()),
            "missing_critical_fields": (
                ""
                if official_capture.empty
                else "|".join(sorted({value for value in official_capture["missing_critical_fields"] if str(value).strip()}))
            ),
            "last_regenerated_utc": _artifact_mtime(PROCESSED_DIR / "official_quarterly_refunding_capture.csv"),
            "public_role": "supporting",
        },
        {
            "dataset": "official_ati",
            "readiness_tier": "headline_ready" if official_ati_headline_ready else "missing",
            "source_quality": "exact_official_numeric",
            "headline_ready": official_ati_headline_ready,
            "fallback_only": not official_ati_headline_ready,
            "missing_critical_fields": "",
            "last_regenerated_utc": _artifact_mtime(PROCESSED_DIR / "ati_index_official_capture.csv"),
            "public_role": "headline",
        },
        {
            "dataset": "qra_event_elasticity",
            "readiness_tier": str(qra_elasticity_readiness["readiness_tier"]),
            "source_quality": "manual_qra_shock_template_plus_event_panel",
            "headline_ready": False,
            "fallback_only": bool(qra_elasticity_readiness["fallback_only"]),
            "missing_critical_fields": str(qra_elasticity_readiness["missing_critical_fields"]),
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "qra_event_elasticity.csv"),
            "public_role": "supporting",
        },
        {
            "dataset": "plumbing",
            "readiness_tier": "headline_ready" if (TABLES_DIR / "plumbing_regressions.csv").exists() else "missing",
            "source_quality": "headline_exact_net_with_labeled_fallbacks",
            "headline_ready": (TABLES_DIR / "plumbing_regressions.csv").exists(),
            "fallback_only": False,
            "missing_critical_fields": "",
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "plumbing_regressions.csv"),
            "public_role": "headline",
        },
        {
            "dataset": "duration",
            "readiness_tier": "headline_ready" if (PROCESSED_DIR / "public_duration_supply.csv").exists() else "fallback_only",
            "source_quality": "headline_hybrid_exact_with_labeled_fallbacks",
            "headline_ready": (PROCESSED_DIR / "public_duration_supply.csv").exists(),
            "fallback_only": False,
            "missing_critical_fields": "",
            "last_regenerated_utc": _artifact_mtime(PROCESSED_DIR / "public_duration_supply.csv"),
            "public_role": "headline",
        },
    ]
    for _, row in extension_status.iterrows():
        extension_name = str(row["extension"])
        config = EXTENSION_REGISTRY[extension_name]
        panel_path = _extension_panel_path(config)
        inventory_path = _extension_inventory_path(config)
        freshness_path = panel_path if panel_path.exists() else inventory_path
        rows.append(
            {
                "dataset": f"extension_{extension_name}",
                "readiness_tier": row["readiness_tier"],
                "source_quality": row["backend_status"],
                "headline_ready": False,
                "fallback_only": bool(row["backend_status"] not in {"summary_ready", "processed", "raw_only"}),
                "missing_critical_fields": "",
                "last_regenerated_utc": _artifact_mtime(freshness_path),
                "public_role": "supporting",
            }
        )
    return pd.DataFrame(rows)


def build_index_metadata() -> dict:
    publish_dir = get_publish_dir()
    files = sorted(p.name for p in publish_dir.glob("*") if p.is_file())
    return {
        "title": "qrawatch publish artifacts",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "artifact_count": len(files),
        "artifacts": files,
    }


def build_publish_artifacts() -> None:
    publish_dir = get_publish_dir()
    ensure_dir(publish_dir)
    publish_table("ati_quarter_table", "ATI Quarter Table", build_ati_publish_table())
    publish_table("ati_seed_forecast_table", "ATI Seed Forecast Table", build_ati_seed_forecast_table())
    publish_table("official_qra_capture", "Official QRA Capture", build_official_capture_publish_table())
    publish_table("official_capture_readiness", "Official Capture Readiness", build_official_capture_readiness_table())
    publish_table("official_capture_completion", "Official Capture Completion", build_official_capture_completion_publish_table())
    publish_table("ati_seed_vs_official", "ATI Seed vs Official Comparison", build_ati_seed_vs_official_comparison())
    publish_table("qra_event_table", "QRA Event Table", build_qra_event_publish_table())
    publish_table("qra_event_summary", "QRA Event Summary", build_qra_summary_publish_table())
    publish_table("qra_event_robustness", "QRA Event Robustness", build_qra_robustness_publish_table())
    if (TABLES_DIR / "qra_event_elasticity.csv").exists():
        publish_table("qra_event_elasticity", "QRA Event Elasticity", build_qra_event_elasticity_publish_table())
        publish_table(
            "qra_event_elasticity_diagnostic",
            "QRA Event Elasticity Diagnostic",
            build_qra_event_elasticity_diagnostic_publish_table(),
        )
        publish_table(
            "qra_event_shock_summary",
            "QRA Event Shock Summary",
            build_qra_event_shock_summary_publish_table(),
        )
    if (TABLES_DIR / "qra_event_shock_components.csv").exists():
        publish_table(
            "qra_event_shock_components",
            "QRA Event Shock Components",
            build_qra_event_shock_components_publish_table(),
        )
    publish_table("plumbing_regression_summary", "Plumbing Regression Summary", build_plumbing_publish_table())
    publish_table("plumbing_robustness", "Plumbing Robustness", build_plumbing_robustness_publish_table())
    publish_table("duration_supply_summary", "Duration Supply Summary", build_duration_publish_table())
    publish_table("duration_supply_comparison", "Duration Supply Comparison", build_duration_comparison_publish_table())
    publish_table("data_sources_summary", "Data Sources Summary", build_data_sources_publish_table())
    publish_table("investor_allotments_summary", "Investor Allotments Summary", build_investor_allotments_summary_table())
    publish_table("primary_dealer_summary", "Primary Dealer Summary", build_primary_dealer_summary_table())
    publish_table("sec_nmfp_summary", "SEC N-MFP Summary", build_sec_nmfp_summary_table())
    publish_table("extension_status", "Extension Backend Status", build_extension_status_table())
    publish_table("dataset_status", "Dataset Status", build_dataset_status_table())
    publish_table("series_metadata_catalog", "Series Metadata Catalog", build_series_metadata_catalog())
    write_json(build_index_metadata(), publish_dir / "index.json")
