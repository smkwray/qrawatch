from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

from .io_utils import ensure_dir, write_df, write_json
from .paths import OUTPUT_DIR, PROCESSED_DIR, RAW_DIR, TABLES_DIR
from .research.pricing_models import (
    PRICING_REGRESSION_ROBUSTNESS_COLUMNS,
    PRICING_REGRESSION_SUMMARY_COLUMNS,
    PRICING_RELEASE_FLOW_LEAVE_ONE_OUT_COLUMNS,
    PRICING_SPEC_REGISTRY_COLUMNS,
    PRICING_SUBSAMPLE_GRID_COLUMNS,
    PRICING_TAU_SENSITIVITY_GRID_COLUMNS,
    SCENARIO_TRANSLATION_COLUMNS as PRICING_SCENARIO_TRANSLATION_COLUMNS,
)
from .research.pricing_panels import RELEASE_FLOW_PANEL_COLUMNS
from .research.qra_classification import SUMMARY_HEADLINE_BUCKETS
from .research.qra_elasticity import build_treatment_comparison_table

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
OFFICIAL_CAPTURE_NUMERIC_FIELDS = [
    "total_financing_need_bn",
    "net_bill_issuance_bn",
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
QRA_EVENT_SPEC_ID = "spec_qra_event_v2"
QRA_DURATION_SPEC_ID = "spec_duration_treatment_v1"
QRA_AUCTION_SPEC_ID = "spec_auction_absorption_v1"
LONG_RATE_TRANSLATION_VARIANTS = (
    (
        "fixed_10y_eq_bn",
        "schedule_diff_10y_eq_bn",
        "USD billions",
        "fixed_10y_eq_from_schedule_diff",
        "fixed_duration_weights",
    ),
    (
        "dynamic_10y_eq_bn",
        "schedule_diff_dynamic_10y_eq_bn",
        "USD billions",
        "dynamic_10y_eq_from_schedule_diff",
        "fred_prior_business_day_yield_curve_plus_frn_convention",
    ),
    (
        "dv01_usd",
        "schedule_diff_dv01_usd",
        "USD",
        "dv01_from_schedule_diff",
        "fred_prior_business_day_yield_curve_plus_frn_convention",
    ),
)
CLAIM_SCOPE_DESCRIPTIVE_ONLY = "descriptive_only"
CLAIM_SCOPE_CAUSAL_PILOT_ONLY = "causal_pilot_only"
CLAIM_SCOPE_HEADLINE = "headline"
CLAIM_SCOPE_VALUES = {
    CLAIM_SCOPE_DESCRIPTIVE_ONLY,
    CLAIM_SCOPE_CAUSAL_PILOT_ONLY,
    CLAIM_SCOPE_HEADLINE,
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


def _first_existing_path(*paths: Path) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def _read_optional_source_csv(
    stem: str,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    path = _first_existing_path(TABLES_DIR / f"{stem}.csv", PROCESSED_DIR / f"{stem}.csv")
    if path is None:
        return pd.DataFrame(columns=columns or [])
    df = pd.read_csv(path)
    if columns is None:
        return df
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
    return df[columns].copy()


def _ensure_columns(df: pd.DataFrame, defaults: dict[str, object]) -> pd.DataFrame:
    out = df.copy()
    for column, default in defaults.items():
        if column not in out.columns:
            out[column] = default
        else:
            out[column] = out[column].fillna(default)
            if isinstance(default, str):
                out[column] = out[column].replace("", default)
    return out


def _quarter_from_timestamp(value: object) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return ""
    quarter = (int(ts.month) - 1) // 3 + 1
    return f"{int(ts.year)}Q{quarter}"


def _quarter_sort_key(value: object) -> tuple[int, int]:
    text = str(value or "").strip()
    if len(text) != 6 or text[4] != "Q":
        return (0, 0)
    try:
        return (int(text[:4]), int(text[5]))
    except ValueError:
        return (0, 0)


def _metric_int(frame: pd.DataFrame, metric: str) -> int:
    if frame.empty or "metric" not in frame.columns or "value" not in frame.columns:
        return 0
    match = frame.loc[frame["metric"].astype(str) == metric, "value"]
    if match.empty:
        return 0
    numeric = pd.to_numeric(match, errors="coerce")
    if numeric.empty or pd.isna(numeric.iloc[0]):
        return 0
    return int(numeric.iloc[0])


def _causal_design_supporting_ready(event_design_status: pd.DataFrame) -> bool:
    if event_design_status.empty:
        return False
    return (
        _metric_int(event_design_status, "tier_a_count") > 0
        and _metric_int(event_design_status, "reviewed_surprise_ready_count") > 0
    )


def _causal_design_missing_fields(event_design_status: pd.DataFrame) -> str:
    if event_design_status.empty:
        return ""
    missing: list[str] = []
    if _metric_int(event_design_status, "tier_a_count") == 0:
        missing.append("no_tier_a_components")
    if _metric_int(event_design_status, "reviewed_surprise_ready_count") == 0:
        missing.append("no_reviewed_surprise_components")
    return "|".join(missing)


def _timestamp_proxy_et(value: object) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return ""
    return f"{pd.Timestamp(ts).strftime('%Y-%m-%d')}T00:00:00-04:00"


def _truthy_text(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    text = str(value).strip().lower()
    return text not in {"", "0", "false", "none", "nan", "null", "pending", "unknown"}


def _coerce_bool_value(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _coerce_int_value(value: object) -> int:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce")
    if numeric.empty or pd.isna(numeric.iloc[0]):
        return 0
    return int(numeric.iloc[0])


def _claim_scope_for_event_row(row: pd.Series) -> str:
    existing = str(row.get("claim_scope", "") or "").strip()
    if existing in CLAIM_SCOPE_VALUES:
        return existing

    event_date_type = str(row.get("event_date_type", "") or "").strip()
    if event_date_type and event_date_type != "official_release_date":
        return CLAIM_SCOPE_DESCRIPTIVE_ONLY

    causal_count = _coerce_int_value(row.get("causal_eligible_component_count"))
    causal_flag = _coerce_bool_value(row.get("causal_eligible"))
    usable_for_headline = _coerce_bool_value(
        row.get("usable_for_headline", row.get("usable_for_descriptive_headline", False))
    )
    if (causal_count > 0 or causal_flag) and usable_for_headline:
        return CLAIM_SCOPE_CAUSAL_PILOT_ONLY

    if (
        str(row.get("quality_tier", "") or "").strip() == "Tier A"
        and str(row.get("timestamp_precision", "") or "").strip() == "exact_time"
        and str(row.get("separability_status", "") or "").strip() == "separable_component"
        and str(row.get("expectation_status", "") or "").strip() == "reviewed_surprise_ready"
        and str(row.get("contamination_status", "") or "").strip() == "reviewed_clean"
        and usable_for_headline
    ):
        return CLAIM_SCOPE_CAUSAL_PILOT_ONLY
    return CLAIM_SCOPE_DESCRIPTIVE_ONLY


def _apply_event_claim_scope(df: pd.DataFrame, default: str = CLAIM_SCOPE_DESCRIPTIVE_ONLY) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        if "claim_scope" not in out.columns:
            out["claim_scope"] = pd.Series(dtype=str)
        return out
    computed = out.apply(_claim_scope_for_event_row, axis=1)
    if "claim_scope" not in out.columns:
        out["claim_scope"] = computed
    else:
        normalized = out["claim_scope"].fillna("").astype(str).str.strip()
        out["claim_scope"] = normalized.where(normalized.isin(CLAIM_SCOPE_VALUES), computed)
    out["claim_scope"] = out["claim_scope"].fillna(default).replace("", default)
    return out


def _qra_overlap_severity(row: pd.Series) -> str:
    if _truthy_text(row.get("overlap_flag")):
        label = str(row.get("overlap_label", "") or "").strip()
        return label if label else "overlap_flagged"
    return "none"


def _qra_financing_need_news_flag(row: pd.Series) -> bool:
    return str(row.get("current_quarter_action", "") or "").strip().lower() in {"tightening", "easing"}


def _qra_composition_news_flag(row: pd.Series) -> bool:
    return str(row.get("headline_bucket", "") or "").strip().lower() in {"tightening", "easing"}


def _qra_forward_guidance_flag(row: pd.Series) -> bool:
    return str(row.get("forward_guidance_bias", "") or "").strip().lower() in {"hawkish", "dovish"}


def _qra_headline_eligibility_reason(row: pd.Series) -> str:
    if _truthy_text(row.get("usable_for_descriptive_headline")):
        return "usable_for_descriptive_headline"
    if _truthy_text(row.get("usable_for_headline")):
        return "usable_for_headline"
    descriptive_reason = str(row.get("descriptive_headline_reason", "") or "").strip()
    if descriptive_reason:
        return descriptive_reason
    if _truthy_text(row.get("shock_missing_flag")):
        return "missing_shock"
    if _truthy_text(row.get("small_denominator_flag")):
        return "small_denominator"
    if str(row.get("classification_review_status", "") or "").strip().lower() != "reviewed":
        return "classification_not_reviewed"
    if str(row.get("shock_review_status", "") or "").strip().lower() != "reviewed":
        return "shock_not_reviewed"
    if str(row.get("headline_bucket", "") or "").strip().lower() not in SUMMARY_HEADLINE_BUCKETS:
        return "non_headline_bucket"
    if _truthy_text(row.get("sign_flip_flag")):
        return "sign_flip"
    return "non_headline_or_unreviewed"


def _qra_review_maturity(df: pd.DataFrame) -> str:
    if df.empty:
        return "not_started"
    required = {"classification_review_status", "shock_review_status"}
    if not required.issubset(df.columns):
        return "provisional_supporting"
    headline_column = (
        "usable_for_descriptive_headline"
        if "usable_for_descriptive_headline" in df.columns
        else "usable_for_headline"
    )
    statuses = {
        str(value).strip().lower()
        for value in df["classification_review_status"].dropna().astype(str)
        if str(value).strip()
    } | {
        str(value).strip().lower()
        for value in df["shock_review_status"].dropna().astype(str)
        if str(value).strip()
    }
    if statuses.issubset({"reviewed"}) and headline_column in df.columns and bool(df[headline_column].fillna(False).astype(bool).any()):
        return "supporting_ready"
    return "provisional_supporting"


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


def _qra_event_table_frame() -> pd.DataFrame:
    return _read_optional_source_csv("qra_event_table")


def _qra_shock_summary_frame() -> pd.DataFrame:
    return _read_optional_source_csv("qra_event_shock_summary")


def _qra_elasticity_source_frame() -> pd.DataFrame:
    return _read_optional_source_csv("qra_event_elasticity")


def _augment_qra_elasticity_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        return out
    original_spec = out.get("spec_id", pd.Series(index=out.index, dtype=object))
    event_spec = out.get("event_spec_id", pd.Series(index=out.index, dtype=object))
    if "treatment_version_id" not in out.columns:
        out["treatment_version_id"] = original_spec.fillna(QRA_DURATION_SPEC_ID).replace("", QRA_DURATION_SPEC_ID)
    else:
        out["treatment_version_id"] = out["treatment_version_id"].fillna(QRA_DURATION_SPEC_ID).replace("", QRA_DURATION_SPEC_ID)
    out["spec_id"] = event_spec.where(
        event_spec.notna() & event_spec.astype(str).str.strip().ne(""),
        original_spec,
    ).fillna(QRA_EVENT_SPEC_ID).replace("", QRA_EVENT_SPEC_ID)
    if "treatment_variant" not in out.columns:
        out["treatment_variant"] = out.get("treatment_variant", pd.Series(index=out.index, dtype=object)).fillna(
            out.get("shock_construction", pd.Series(index=out.index, dtype=object)).fillna("shock_bn")
        )
    else:
        out["treatment_variant"] = out["treatment_variant"].fillna(
            out.get("shock_construction", pd.Series(index=out.index, dtype=object)).fillna("shock_bn")
        )
    if "usable_for_headline_reason" not in out.columns:
        out["usable_for_headline_reason"] = out.apply(_qra_headline_eligibility_reason, axis=1)
    else:
        out["usable_for_headline_reason"] = out["usable_for_headline_reason"].fillna(
            out.apply(_qra_headline_eligibility_reason, axis=1)
        )
    if "descriptive_headline_reason" not in out.columns:
        out["descriptive_headline_reason"] = out["usable_for_headline_reason"]
    else:
        out["descriptive_headline_reason"] = out["descriptive_headline_reason"].fillna(
            out["usable_for_headline_reason"]
        )
    if "usable_for_descriptive_headline" not in out.columns:
        out["usable_for_descriptive_headline"] = out.get(
            "usable_for_headline",
            pd.Series(index=out.index, dtype=bool),
        ).fillna(False).astype(bool)
    else:
        out["usable_for_descriptive_headline"] = (
            out["usable_for_descriptive_headline"]
            .fillna(out.get("usable_for_headline", pd.Series(index=out.index, dtype=bool)).fillna(False))
            .astype(bool)
        )
    if "usable_for_headline" not in out.columns:
        out["usable_for_headline"] = out["usable_for_descriptive_headline"]
    else:
        out["usable_for_headline"] = (
            out["usable_for_headline"]
            .fillna(out["usable_for_descriptive_headline"])
            .astype(bool)
        )
    if "review_maturity" not in out.columns:
        out["review_maturity"] = _qra_review_maturity(out)
    else:
        out["review_maturity"] = out["review_maturity"].fillna(_qra_review_maturity(out))
    if "elasticity_value" not in out.columns:
        if "elasticity_bp_per_100bn" in out.columns:
            out["elasticity_value"] = out["elasticity_bp_per_100bn"]
        elif "elasticity" in out.columns:
            out["elasticity_value"] = out["elasticity"]
    if "elasticity_units" not in out.columns:
        if "elasticity_bp_per_100bn" in out.columns:
            out["elasticity_units"] = "bp_per_100bn"
        else:
            out["elasticity_units"] = pd.NA
    if "comparison_family" not in out.columns:
        out["comparison_family"] = out["treatment_variant"].map(
            lambda value: "dv01" if str(value).strip().lower() == "dv01_usd" else "bp_per_100bn"
        )
    return out


def _empty_qra_registry_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "event_id",
            "quarter",
            "release_timestamp_et",
            "release_timestamp_kind",
            "release_bundle_type",
            "policy_statement_url",
            "financing_estimates_url",
            "timing_quality",
            "overlap_severity",
            "overlap_label",
            "financing_need_news_flag",
            "composition_news_flag",
            "forward_guidance_flag",
            "reviewer",
            "review_date",
            "quality_tier",
            "eligibility_blockers",
            "timestamp_precision",
            "separability_status",
            "expectation_status",
            "contamination_status",
            "release_component_count",
            "causal_eligible_component_count",
            "treatment_version_id",
            "headline_eligibility_reason",
            "spec_id",
            "treatment_variant",
        ]
    )


def build_qra_event_registry_publish_table() -> pd.DataFrame:
    source = _read_optional_source_csv("qra_event_registry_v2")
    if source.empty:
        return _empty_qra_registry_frame()
    source = _ensure_columns(
        source,
        {
            "spec_id": QRA_EVENT_SPEC_ID,
            "treatment_variant": "canonical_shock_bn",
            "release_timestamp_kind": "date_proxy",
            "release_bundle_type": "",
            "overlap_severity": "none",
            "overlap_label": "",
            "financing_need_news_flag": False,
            "composition_news_flag": False,
            "forward_guidance_flag": False,
            "reviewer": "",
            "review_date": "",
            "quality_tier": "Tier D",
            "eligibility_blockers": "",
            "timestamp_precision": "missing",
            "separability_status": "same_day_inseparable_bundle",
            "expectation_status": "missing_benchmark",
            "contamination_status": "pending_review",
            "release_component_count": 0,
            "causal_eligible_component_count": 0,
            "treatment_version_id": QRA_DURATION_SPEC_ID,
            "headline_eligibility_reason": "missing_shock_summary",
        },
    )
    columns = [
        "event_id",
        "quarter",
        "release_timestamp_et",
        "release_timestamp_kind",
        "release_bundle_type",
        "policy_statement_url",
        "financing_estimates_url",
        "timing_quality",
        "overlap_severity",
        "overlap_label",
        "financing_need_news_flag",
        "composition_news_flag",
        "forward_guidance_flag",
        "reviewer",
        "review_date",
        "quality_tier",
        "eligibility_blockers",
        "timestamp_precision",
        "separability_status",
        "expectation_status",
        "contamination_status",
        "release_component_count",
        "causal_eligible_component_count",
        "treatment_version_id",
        "headline_eligibility_reason",
        "spec_id",
        "treatment_variant",
    ]
    for column in columns:
        if column not in source.columns:
            source[column] = pd.NA
    return source[columns].drop_duplicates(subset=["event_id"], keep="first").sort_values(["quarter", "event_id"]).reset_index(drop=True)


def build_qra_release_component_registry_publish_table() -> pd.DataFrame:
    columns = [
        "release_component_id",
        "event_id",
        "quarter",
        "component_type",
        "release_timestamp_et",
        "timestamp_precision",
        "release_timestamp_source_method",
        "timestamp_evidence_url",
        "timestamp_evidence_note",
        "release_timezone_asserted",
        "bundle_decomposition_evidence",
        "source_url",
        "bundle_id",
        "release_sequence_label",
        "separable_component_flag",
        "review_status",
        "review_notes",
        "benchmark_timestamp_et",
        "benchmark_source",
        "benchmark_source_family",
        "benchmark_document_url",
        "benchmark_document_local",
        "benchmark_release_timestamp_et",
        "benchmark_release_timestamp_precision",
        "benchmark_timestamp_source_method",
        "benchmark_pre_release_verified_flag",
        "benchmark_observed_before_component_flag",
        "benchmark_timing_status",
        "external_benchmark_ready",
        "expected_composition_bn",
        "realized_composition_bn",
        "composition_surprise_bn",
        "surprise_construction_method",
        "surprise_units",
        "benchmark_stale_flag",
        "expectation_review_status",
        "expectation_notes",
        "benchmark_search_disposition",
        "benchmark_search_note",
        "expectation_status",
        "contamination_flag",
        "contamination_status",
        "contamination_review_status",
        "contamination_label",
        "contamination_window_start_et",
        "contamination_window_end_et",
        "confound_release_type",
        "confound_release_timestamp_et",
        "decision_rule",
        "exclude_from_causal_pool",
        "decision_confidence",
        "contamination_notes",
        "macro_crosswalk_status",
        "macro_crosswalk_note",
        "separability_status",
        "eligibility_blockers",
        "quality_tier",
        "causal_eligible",
    ]
    source = _read_optional_source_csv("qra_release_component_registry", columns=columns)
    for column in columns:
        if column not in source.columns:
            source[column] = pd.NA
    return source[columns]


def build_qra_causal_qa_publish_table() -> pd.DataFrame:
    columns = [
        "event_id",
        "quality_tier",
        "eligibility_blockers",
        "timestamp_precision",
        "separability_status",
        "expectation_status",
        "contamination_status",
        "release_component_count",
        "causal_eligible_component_count",
    ]
    source = _read_optional_source_csv("qra_causal_qa_ledger", columns=columns)
    for column in columns:
        if column not in source.columns:
            source[column] = pd.NA
    return source[columns]


def build_qra_benchmark_blockers_by_event_publish_table() -> pd.DataFrame:
    columns = [
        "event_id",
        "quarter",
        "release_component_count",
        "pre_release_external_count",
        "external_timing_unverified_count",
        "same_release_placeholder_count",
        "post_release_invalid_count",
        "benchmark_verification_incomplete_count",
        "reviewed_surprise_ready_count",
        "tier_a_count",
        "benchmark_blockers",
    ]
    source = _read_optional_source_csv("qra_benchmark_blockers_by_event", columns=columns)
    for column in columns:
        if column not in source.columns:
            source[column] = pd.NA
    return source[columns]


def build_event_design_status_publish_table() -> pd.DataFrame:
    columns = ["metric", "value", "notes"]
    source = _read_optional_source_csv("event_design_status", columns=columns)
    for column in columns:
        if column not in source.columns:
            source[column] = pd.NA
    return source[columns]


def build_qra_benchmark_coverage_table() -> pd.DataFrame:
    columns = ["scope", "metric", "value", "notes"]
    registry = build_qra_release_component_registry_publish_table()
    if registry.empty:
        return pd.DataFrame(columns=columns)
    if "quarter" not in registry.columns:
        return pd.DataFrame(columns=columns)
    current_sample = registry.loc[
        registry["quarter"].map(_quarter_sort_key) >= (2022, 3)
    ].copy()
    if current_sample.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, object]] = []
    scopes = {
        "current_sample_all_components": current_sample,
        "current_sample_financing_estimates": current_sample.loc[
            current_sample.get("component_type", pd.Series(dtype=object)).astype(str).eq("financing_estimates")
        ].copy(),
    }
    for scope, frame in scopes.items():
        if frame.empty:
            continue
        rows.extend(
            [
                {
                    "scope": scope,
                    "metric": "release_component_count",
                    "value": int(len(frame)),
                    "notes": "Components in scope.",
                },
                {
                    "scope": scope,
                    "metric": "pre_release_external_benchmark_count",
                    "value": int(frame.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("pre_release_external").sum()),
                    "notes": "Rows with benchmark timing classified as pre-release external.",
                },
                {
                    "scope": scope,
                    "metric": "same_release_placeholder_count",
                    "value": int(frame.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("same_release_placeholder").sum()),
                    "notes": "Rows still blocked by same-release placeholder benchmark semantics.",
                },
                {
                    "scope": scope,
                    "metric": "post_release_invalid_count",
                    "value": int(frame.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("post_release_invalid").sum()),
                    "notes": "Rows with post-release benchmark timestamps.",
                },
                {
                    "scope": scope,
                    "metric": "external_timing_unverified_count",
                    "value": int(frame.get("benchmark_timing_status", pd.Series(dtype=object)).astype(str).eq("external_timing_unverified").sum()),
                    "notes": "Rows with a benchmark source family attached but no validated pre-release timestamp.",
                },
                {
                    "scope": scope,
                    "metric": "external_benchmark_ready_count",
                    "value": int(frame.get("external_benchmark_ready", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()),
                    "notes": "Rows structurally ready for causal surprise use.",
                },
                {
                    "scope": scope,
                    "metric": "reviewed_surprise_ready_count",
                    "value": int(frame.get("expectation_status", pd.Series(dtype=object)).astype(str).eq("reviewed_surprise_ready").sum()),
                    "notes": "Rows with reviewed surprise inputs that pass benchmark-readiness checks.",
                },
                {
                    "scope": scope,
                    "metric": "reviewed_clean_count",
                    "value": int(frame.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("reviewed_clean").sum()),
                    "notes": "Rows with reviewed clean contamination status.",
                },
                {
                    "scope": scope,
                    "metric": "reviewed_contaminated_context_only_count",
                    "value": int(frame.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("reviewed_contaminated_context_only").sum()),
                    "notes": "Rows retained only as context because reviewed contamination was not fully clean.",
                },
                {
                    "scope": scope,
                    "metric": "reviewed_contaminated_exclude_count",
                    "value": int(frame.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("reviewed_contaminated_exclude").sum()),
                    "notes": "Rows excluded from the causal pool after reviewed contamination.",
                },
                {
                    "scope": scope,
                    "metric": "pending_review_count",
                    "value": int(frame.get("contamination_status", pd.Series(dtype=object)).astype(str).eq("pending_review").sum()),
                    "notes": "Rows whose contamination review is still pending.",
                },
                {
                    "scope": scope,
                    "metric": "causal_eligible_count",
                    "value": int(frame.get("causal_eligible", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()),
                    "notes": "Rows that currently pass all causal gates.",
                },
            ]
        )
    return pd.DataFrame(rows, columns=columns)


def _benchmark_terminal_disposition(row: pd.Series) -> str:
    if _coerce_bool_value(row.get("causal_eligible")):
        return "tier_a_causal_pilot_ready"
    contamination_status = str(row.get("contamination_status", "") or "").strip()
    if contamination_status == "reviewed_contaminated_context_only":
        return "reviewed_contaminated_context_only"
    if contamination_status == "reviewed_contaminated_exclude":
        return "reviewed_contaminated_exclude"
    expectation_status = str(row.get("expectation_status", "") or "").strip()
    if expectation_status == "post_release_invalid":
        search_disposition = str(row.get("benchmark_search_disposition", "")).strip()
        if search_disposition.lower() in {"", "nan", "<na>"}:
            search_disposition = ""
        if search_disposition in {"blocked_source_family_exhausted", "blocked_open_candidate"}:
            return search_disposition
        return "post_release_invalid"
    if expectation_status == "benchmark_timing_unverified":
        return "external_timing_unverified"
    if expectation_status == "same_release_placeholder":
        return "same_release_placeholder"
    if expectation_status == "benchmark_verification_incomplete":
        return "benchmark_verification_incomplete"
    if contamination_status == "pending_review":
        return "pending_contamination_review"
    if expectation_status == "reviewed_surprise_ready":
        return "reviewed_surprise_ready_not_tier_a"
    return expectation_status or contamination_status or "review_pending"


def build_qra_benchmark_evidence_registry_table() -> pd.DataFrame:
    columns = [
        "release_component_id",
        "event_id",
        "quarter",
        "component_type",
        "release_timestamp_et",
        "benchmark_timestamp_et",
        "benchmark_source",
        "benchmark_source_family",
        "benchmark_document_url",
        "benchmark_document_local",
        "benchmark_release_timestamp_et",
        "benchmark_release_timestamp_precision",
        "benchmark_timestamp_source_method",
        "benchmark_pre_release_verified_flag",
        "benchmark_observed_before_component_flag",
        "benchmark_timing_status",
        "external_benchmark_ready",
        "expectation_review_status",
        "expectation_status",
        "expectation_notes",
        "benchmark_search_disposition",
        "benchmark_search_note",
        "contamination_flag",
        "contamination_status",
        "contamination_review_status",
        "contamination_label",
        "confound_release_type",
        "confound_release_timestamp_et",
        "decision_rule",
        "exclude_from_causal_pool",
        "decision_confidence",
        "contamination_notes",
        "macro_crosswalk_status",
        "macro_crosswalk_note",
        "quality_tier",
        "causal_eligible",
        "terminal_disposition",
        "terminal_status_reason",
        "claim_scope",
    ]
    registry = build_qra_release_component_registry_publish_table()
    if registry.empty or "quarter" not in registry.columns:
        return pd.DataFrame(columns=columns)
    current_sample = registry.loc[
        registry["quarter"].map(_quarter_sort_key) >= (2022, 3)
    ].copy()
    current_sample = current_sample.loc[
        current_sample.get("component_type", pd.Series(dtype=object)).astype(str).eq("financing_estimates")
    ].copy()
    if current_sample.empty:
        return pd.DataFrame(columns=columns)
    current_sample["terminal_disposition"] = current_sample.apply(_benchmark_terminal_disposition, axis=1)
    current_sample["terminal_status_reason"] = current_sample.get(
        "eligibility_blockers",
        pd.Series(index=current_sample.index, dtype=object),
    ).fillna("")
    current_sample["claim_scope"] = current_sample.get(
        "causal_eligible",
        pd.Series(index=current_sample.index, dtype=bool),
    ).fillna(False).astype(bool).map(
        lambda eligible: CLAIM_SCOPE_CAUSAL_PILOT_ONLY if eligible else CLAIM_SCOPE_DESCRIPTIVE_ONLY
    )
    for column in columns:
        if column not in current_sample.columns:
            current_sample[column] = pd.NA
    return current_sample[columns].sort_values(
        ["quarter", "event_id", "release_component_id"],
        key=lambda s: s.map(_quarter_sort_key) if s.name == "quarter" else s,
        kind="stable",
    ).reset_index(drop=True)


def build_causal_claims_status_table() -> pd.DataFrame:
    columns = [
        "claim_id",
        "claim_name",
        "claim_scope",
        "readiness_tier",
        "public_role",
        "headline_ready",
        "causal_pilot_ready",
        "source_quality",
        "current_sample_financing_component_count",
        "benchmark_ready_count",
        "tier_a_count",
        "context_only_count",
        "post_release_invalid_count",
        "source_family_exhausted_count",
        "open_candidate_count",
        "can_claim",
        "cannot_claim",
        "boundary_reason",
        "last_regenerated_utc",
    ]
    event_design_status = build_event_design_status_publish_table()
    if event_design_status.empty:
        return pd.DataFrame(columns=columns)
    component_count = _metric_int(event_design_status, "current_sample_financing_component_count")
    benchmark_ready_count = _metric_int(event_design_status, "current_sample_financing_reviewed_surprise_ready_count")
    tier_a_count = _metric_int(event_design_status, "current_sample_financing_tier_a_count")
    context_only_count = _metric_int(event_design_status, "current_sample_financing_reviewed_contaminated_context_only_count")
    post_release_invalid_count = _metric_int(event_design_status, "current_sample_financing_post_release_invalid_count")
    source_family_exhausted_count = _metric_int(event_design_status, "current_sample_financing_source_family_exhausted_count")
    open_candidate_count = _metric_int(event_design_status, "current_sample_financing_open_candidate_count")
    causal_pilot_ready = benchmark_ready_count > 0 and tier_a_count > 0
    readiness_tier = "supporting_ready" if component_count > 0 else "not_started"
    can_claim = (
        f"A narrow post-2022Q3 financing-estimates pilot with {component_count} reviewed current-sample financing components, "
        f"{benchmark_ready_count} benchmark-ready rows, and {tier_a_count} Tier A components."
    )
    cannot_claim = (
        "A settled or full-sample causal estimate of Treasury issuance effects on long rates; "
        f"{post_release_invalid_count} financing rows remain post-release-invalid, "
        f"{source_family_exhausted_count} are source-family-exhausted, "
        f"{open_candidate_count} remain open benchmark candidates, "
        "and the long-rate translation layer is still supporting/provisional."
    )
    boundary_reason = (
        f"{component_count} current-sample financing rows; {benchmark_ready_count} benchmark-ready; "
        f"{tier_a_count} Tier A; {context_only_count} context-only; {post_release_invalid_count} post-release-invalid; "
        f"{source_family_exhausted_count} source-family-exhausted; {open_candidate_count} open candidates."
    )
    row = {
        "claim_id": "current_sample_financing_pilot",
        "claim_name": "Current-sample financing pilot",
        "claim_scope": CLAIM_SCOPE_CAUSAL_PILOT_ONLY if causal_pilot_ready else CLAIM_SCOPE_DESCRIPTIVE_ONLY,
        "readiness_tier": readiness_tier,
        "public_role": "supporting",
        "headline_ready": False,
        "causal_pilot_ready": causal_pilot_ready,
        "source_quality": "derived_causal_claims_status",
        "current_sample_financing_component_count": component_count,
        "benchmark_ready_count": benchmark_ready_count,
        "tier_a_count": tier_a_count,
        "context_only_count": context_only_count,
        "post_release_invalid_count": post_release_invalid_count,
        "source_family_exhausted_count": source_family_exhausted_count,
        "open_candidate_count": open_candidate_count,
        "can_claim": can_claim,
        "cannot_claim": cannot_claim,
        "boundary_reason": boundary_reason,
        "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "event_design_status.csv"),
    }
    return pd.DataFrame([row], columns=columns)


def build_official_capture_history_status_table() -> pd.DataFrame:
    columns = [
        "quarter",
        "qra_release_date",
        "qa_status",
        "source_quality",
        "readiness_tier",
        "headline_ready",
        "has_financing_estimates_source",
        "has_refunding_statement_source",
        "has_auction_reconstruction",
        "financing_provenance_ready",
        "refunding_statement_provenance_ready",
        "auction_reconstruction_ready",
        "numeric_official_capture_ready",
        "source_completeness",
    ]
    capture = _official_capture_with_status_columns()
    if capture.empty:
        return pd.DataFrame(columns=columns)

    def _has_token(value: object, needles: tuple[str, ...]) -> bool:
        tokens = [str(item or "").strip().lower() for item in _split_pipe_values(value)]
        return any(any(needle in token for needle in needles) for token in tokens)

    out = capture.copy()
    out["financing_provenance_ready"] = out.apply(
        lambda row: _truthy_text(row.get("financing_source_doc_type"))
        and (
            _truthy_text(row.get("financing_source_url"))
            or _truthy_text(row.get("financing_source_doc_local"))
        ),
        axis=1,
    )
    out["refunding_statement_provenance_ready"] = out.apply(
        lambda row: _truthy_text(row.get("refunding_statement_source_doc_type"))
        and (
            _truthy_text(row.get("refunding_statement_source_url"))
            or _truthy_text(row.get("refunding_statement_source_doc_local"))
        ),
        axis=1,
    )
    out["auction_reconstruction_ready"] = out.apply(
        lambda row: (
            _truthy_text(row.get("auction_reconstruction_source_doc_type"))
            and (
                _truthy_text(row.get("auction_reconstruction_source_url"))
                or _truthy_text(row.get("auction_reconstruction_source_doc_local"))
            )
        )
        or pd.notna(pd.to_numeric(pd.Series([row.get("reconstructed_net_bill_issuance_bn")]), errors="coerce").iloc[0]),
        axis=1,
    )
    out["has_financing_estimates_source"] = out["financing_provenance_ready"] | out.apply(
        lambda row: _has_token(row.get("source_doc_type"), ("quarterly_refunding_press_release", "financing"))
        or _has_token(row.get("source_doc_local"), ("borrowing", "financing")),
        axis=1,
    )
    out["has_refunding_statement_source"] = out["refunding_statement_provenance_ready"] | out.apply(
        lambda row: _has_token(row.get("source_doc_type"), ("refunding", "statement"))
        or _has_token(row.get("source_doc_local"), ("refunding", "statement")),
        axis=1,
    )
    out["has_auction_reconstruction"] = out["auction_reconstruction_ready"]
    out["numeric_official_capture_ready"] = out.apply(
        lambda row: str(row.get("readiness_tier", "")).strip() == "headline_ready",
        axis=1,
    )
    completeness: list[str] = []
    for _, row in out.iterrows():
        parts: list[str] = []
        if bool(row.get("financing_provenance_ready")):
            parts.append("financing_estimates")
        if bool(row.get("refunding_statement_provenance_ready")):
            parts.append("refunding_statement")
        if bool(row.get("auction_reconstruction_ready")):
            parts.append("auction_reconstruction")
        completeness.append("|".join(parts) if parts else "source_incomplete")
    out["source_completeness"] = completeness
    keep = [column for column in columns if column in out.columns]
    return out[keep].sort_values("quarter", key=lambda s: s.map(_quarter_sort_key)).reset_index(drop=True)


def _official_capture_missing_numeric_fields(row: pd.Series) -> str:
    missing = []
    for field in OFFICIAL_CAPTURE_NUMERIC_FIELDS:
        value = row.get(field)
        if field not in row.index or pd.isna(value) or str(value).strip() == "":
            missing.append(field)
    return "|".join(missing)


def _official_capture_next_action(row: pd.Series) -> str:
    missing_numeric = _split_pipe_values(row.get("missing_numeric_fields"))
    if bool(row.get("numeric_official_capture_ready")):
        return "complete"
    if "total_financing_need_bn" in missing_numeric and not bool(row.get("financing_provenance_ready")):
        return "attach_financing_release_and_populate_total_financing_need"
    if "total_financing_need_bn" in missing_numeric:
        return "populate_total_financing_need"
    if "net_bill_issuance_bn" in missing_numeric and not bool(row.get("auction_reconstruction_ready")):
        return "add_auction_reconstruction_and_populate_net_bill_issuance"
    if "net_bill_issuance_bn" in missing_numeric:
        return "populate_net_bill_issuance"
    if not bool(row.get("refunding_statement_provenance_ready")):
        return "attach_refunding_statement_provenance"
    if not bool(row.get("financing_provenance_ready")):
        return "attach_financing_release_provenance"
    if not bool(row.get("auction_reconstruction_ready")):
        return "attach_auction_reconstruction_provenance"
    if str(row.get("source_quality", "")).strip() == "official_hybrid":
        return "promote_manual_official_capture"
    return "review_row"


def build_official_capture_backfill_queue_table() -> pd.DataFrame:
    columns = [
        "quarter",
        "source_quality",
        "readiness_tier",
        "numeric_official_capture_ready",
        "financing_provenance_ready",
        "refunding_statement_provenance_ready",
        "auction_reconstruction_ready",
        "missing_numeric_fields",
        "next_action",
    ]
    history = build_official_capture_history_status_table()
    capture = _official_capture_with_status_columns()
    if history.empty or capture.empty:
        return pd.DataFrame(columns=columns)

    out = history.merge(
        capture[["quarter"] + OFFICIAL_CAPTURE_NUMERIC_FIELDS],
        on="quarter",
        how="left",
    )
    out["missing_numeric_fields"] = out.apply(_official_capture_missing_numeric_fields, axis=1)
    out["next_action"] = out.apply(_official_capture_next_action, axis=1)
    return out[columns].sort_values("quarter", key=lambda s: s.map(_quarter_sort_key)).reset_index(drop=True)


def _empty_qra_crosswalk_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "event_id",
            "event_date_type",
            "canonical_shock_id",
            "shock_bn",
            "schedule_diff_10y_eq_bn",
            "schedule_diff_dynamic_10y_eq_bn",
            "schedule_diff_dv01_usd",
            "gross_notional_delta_bn",
            "shock_source",
            "manual_override_reason",
            "alternative_treatment_complete",
            "alternative_treatment_missing_fields",
            "alternative_treatment_missing_reason",
            "shock_review_status",
            "spec_id",
            "treatment_variant",
            "usable_for_headline_reason",
            "claim_scope",
        ]
    )


def build_qra_shock_crosswalk_publish_table() -> pd.DataFrame:
    source = _read_optional_source_csv("qra_shock_crosswalk_v1")
    if source.empty:
        return _empty_qra_crosswalk_frame()
    out = source.copy()

    out = _ensure_columns(
        out,
        {
            "spec_id": QRA_EVENT_SPEC_ID,
            "treatment_version_id": QRA_DURATION_SPEC_ID,
            "treatment_variant": "canonical_shock_bn",
            "usable_for_headline_reason": "missing_shock_summary",
            "manual_override_reason": "",
            "alternative_treatment_complete": False,
            "alternative_treatment_missing_fields": "",
            "alternative_treatment_missing_reason": "",
            "shock_review_status": "",
        },
    )
    if out.empty:
        return _empty_qra_crosswalk_frame()
    for column in ("shock_bn", "schedule_diff_10y_eq_bn", "schedule_diff_dynamic_10y_eq_bn", "schedule_diff_dv01_usd", "gross_notional_delta_bn"):
        if column not in out.columns:
            out[column] = pd.NA
    out["canonical_shock_id"] = (
        out.get("canonical_shock_id", pd.Series(index=out.index, dtype=object))
        .fillna(out.get("shock_construction", pd.Series(index=out.index, dtype=object)))
        .fillna("shock_bn")
    )
    out["manual_override_reason"] = (
        out.get("manual_override_reason", pd.Series(index=out.index, dtype=object))
        .fillna(out.get("shock_notes", pd.Series(index=out.index, dtype=object)))
        .fillna("")
    )
    out = _apply_event_claim_scope(out)
    columns = [
        "event_id",
        "event_date_type",
        "canonical_shock_id",
        "shock_bn",
        "schedule_diff_10y_eq_bn",
        "schedule_diff_dynamic_10y_eq_bn",
        "schedule_diff_dv01_usd",
        "gross_notional_delta_bn",
        "shock_source",
        "manual_override_reason",
        "alternative_treatment_complete",
        "alternative_treatment_missing_fields",
        "alternative_treatment_missing_reason",
        "shock_review_status",
        "treatment_version_id",
        "spec_id",
        "treatment_variant",
        "usable_for_headline_reason",
        "claim_scope",
    ]
    for column in columns:
        if column not in out.columns:
            out[column] = pd.NA
    return out[columns].drop_duplicates(subset=["event_id", "event_date_type"], keep="first").sort_values(["event_id", "event_date_type"]).reset_index(drop=True)


def _empty_event_usability_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "event_date_type",
            "headline_bucket",
            "classification_review_status",
            "shock_review_status",
            "overlap_severity",
            "usable_for_descriptive_headline",
            "descriptive_headline_reason",
            "usable_for_headline",
            "usable_for_headline_reason",
            "headline_usable_count",
            "n_rows",
            "n_events",
            "event_count",
            "spec_id",
            "treatment_version_id",
            "treatment_variant",
            "claim_scope",
        ]
    )


def build_event_usability_publish_table() -> pd.DataFrame:
    source = _read_optional_source_csv("event_usability_table")
    if source.empty:
        return _empty_event_usability_frame()
    source = _ensure_columns(
        source,
        {
            "spec_id": QRA_EVENT_SPEC_ID,
            "treatment_version_id": QRA_DURATION_SPEC_ID,
            "treatment_variant": "canonical_shock_bn",
            "overlap_severity": "none",
        },
    )
    if "event_count" not in source.columns:
        source["event_count"] = source.get("n_events", source.get("n_rows", pd.NA))
    if "n_rows" not in source.columns:
        source["n_rows"] = source.get("event_count", pd.NA)
    if "n_events" not in source.columns:
        source["n_events"] = source.get("event_count", pd.NA)
    if "headline_usable_count" not in source.columns:
        event_count = pd.to_numeric(source.get("event_count", pd.Series(index=source.index, dtype=object)), errors="coerce").fillna(0)
        usable_mask = source.get("usable_for_headline", pd.Series(False, index=source.index)).fillna(False).map(_coerce_bool_value)
        source["headline_usable_count"] = event_count.where(usable_mask, 0).astype(int)
    if "descriptive_headline_reason" not in source.columns:
        source["descriptive_headline_reason"] = source.get("usable_for_headline_reason", pd.NA)
    if "usable_for_descriptive_headline" not in source.columns:
        source["usable_for_descriptive_headline"] = source.get("usable_for_headline", False)
    return _apply_event_claim_scope(source)


def _empty_leave_one_out_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "event_id",
            "event_date_type",
            "headline_bucket",
            "series",
            "window",
            "n_events",
            "p_value",
            "n_observations",
            "leave_one_out_coefficient",
            "leave_one_out_std_err",
            "leave_one_out_delta",
            "spec_id",
            "treatment_variant",
            "claim_scope",
        ]
    )


def build_leave_one_event_out_publish_table() -> pd.DataFrame:
    source = _read_optional_source_csv("leave_one_event_out_table")
    if source.empty:
        return _empty_leave_one_out_frame()
    out = _ensure_columns(
        source,
        {
            "spec_id": QRA_EVENT_SPEC_ID,
            "treatment_variant": "shock_bn",
        },
    )
    if "left_out_event_id" not in out.columns:
        out["left_out_event_id"] = out.get("dropped_event_id", out.get("event_id", pd.NA))
    if "estimate" not in out.columns:
        out["estimate"] = out.get("leave_one_out_coefficient", out.get("mean_elasticity", pd.NA))
    if "n_events" not in out.columns:
        out["n_events"] = out.get("n_remaining_events", out.get("n_observations", pd.NA))
    if "p_value" not in out.columns:
        out["p_value"] = pd.NA
    if "headline_bucket" in out.columns:
        out["headline_bucket"] = out["headline_bucket"].replace({"headline_usable_pool": "pending"})
    return _apply_event_claim_scope(out)


def _empty_treatment_comparison_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "spec_id",
            "event_date_type",
            "series",
            "window",
            "treatment_variant",
            "comparison_family",
            "comparison_family_label",
            "elasticity_units",
            "n_rows",
            "n_events",
            "n_headline_eligible_events",
            "headline_eligible_share",
            "mean_elasticity_value",
            "median_elasticity_value",
            "std_elasticity_value",
            "min_elasticity_value",
            "max_elasticity_value",
            "mean_abs_elasticity_value",
            "family_reference_variant",
            "family_reference_mean_elasticity_value",
            "delta_vs_family_reference_mean_elasticity_value",
            "bp_family_spread_elasticity_value",
            "headline_recommendation_status",
            "headline_recommendation_reason",
            "primary_treatment_variant",
            "primary_treatment_reason",
            "claim_scope",
        ]
    )


def build_treatment_comparison_publish_table() -> pd.DataFrame:
    source = _read_optional_source_csv("treatment_comparison_table")
    if source.empty:
        elasticity = _augment_qra_elasticity_frame(_qra_elasticity_source_frame())
        if elasticity.empty:
            return _empty_treatment_comparison_frame()
        out = build_treatment_comparison_table(elasticity)
        out["claim_scope"] = CLAIM_SCOPE_DESCRIPTIVE_ONLY
        return out
    return _ensure_columns(
        source,
        {
            "spec_id": QRA_DURATION_SPEC_ID,
            "event_date_type": "official_release_date",
            "comparison_family": "bp_per_100bn",
            "comparison_family_label": "bp-per-100bn family",
            "elasticity_units": "bp_per_100bn",
            "n_rows": pd.NA,
            "n_events": pd.NA,
            "n_headline_eligible_events": pd.NA,
            "headline_eligible_share": pd.NA,
            "mean_elasticity_value": pd.NA,
            "median_elasticity_value": pd.NA,
            "std_elasticity_value": pd.NA,
            "min_elasticity_value": pd.NA,
            "max_elasticity_value": pd.NA,
            "mean_abs_elasticity_value": pd.NA,
            "family_reference_variant": "canonical_shock_bn",
            "family_reference_mean_elasticity_value": pd.NA,
            "delta_vs_family_reference_mean_elasticity_value": pd.NA,
            "bp_family_spread_elasticity_value": pd.NA,
            "headline_recommendation_status": "retain_canonical_contract",
            "headline_recommendation_reason": "comparison_fallback_loaded",
            "primary_treatment_variant": "canonical_shock_bn",
            "primary_treatment_reason": "canonical_shock_bn remains the headline contract; fixed, dynamic, and DV01 variants are comparison diagnostics.",
            "claim_scope": CLAIM_SCOPE_DESCRIPTIVE_ONLY,
        },
    )


def build_qra_long_rate_translation_panel() -> pd.DataFrame:
    columns = [
        "event_id",
        "quarter",
        "event_date_type",
        "translation_variant",
        "translation_value",
        "translation_value_units",
        "translation_source_field",
        "translation_method",
        "duration_assumption_source",
        "translation_review_status",
        "shock_source",
        "shock_review_status",
        "alternative_treatment_complete",
        "alternative_treatment_missing_fields",
        "alternative_treatment_missing_reason",
        "gross_notional_delta_bn",
        "quality_tier",
        "eligibility_blockers",
        "timestamp_precision",
        "separability_status",
        "expectation_status",
        "contamination_status",
        "causal_eligible_component_count",
        "long_rate_pilot_ready",
        "long_rate_pilot_blocker",
        "public_role",
        "claim_scope",
    ]
    crosswalk = build_qra_shock_crosswalk_publish_table()
    if crosswalk.empty:
        return pd.DataFrame(columns=columns)

    registry = build_qra_event_registry_publish_table()
    registry_columns = [
        "event_id",
        "quarter",
        "quality_tier",
        "eligibility_blockers",
        "timestamp_precision",
        "separability_status",
        "expectation_status",
        "contamination_status",
        "causal_eligible_component_count",
    ]
    if registry.empty:
        registry = pd.DataFrame(columns=registry_columns)
    else:
        registry = registry[[column for column in registry_columns if column in registry.columns]].copy()

    merged = crosswalk.merge(registry, on="event_id", how="left")
    rows: list[dict[str, object]] = []
    for _, row in merged.iterrows():
        for variant, source_field, units, method, duration_source in LONG_RATE_TRANSLATION_VARIANTS:
            value = pd.to_numeric(pd.Series([row.get(source_field)]), errors="coerce").iloc[0]
            blockers: list[str] = []
            if str(row.get("event_date_type", "")).strip() != "official_release_date":
                blockers.append("non_official_event_date_type")
            if pd.isna(value):
                blockers.append("translation_value_missing")
            if str(row.get("shock_review_status", "")).strip().lower() != "reviewed":
                blockers.append("translation_not_reviewed")
            if str(row.get("quality_tier", "")).strip() != "Tier A":
                blockers.append("event_not_tier_a")
            rows.append(
                {
                    "event_id": row.get("event_id", pd.NA),
                    "quarter": row.get("quarter", pd.NA),
                    "event_date_type": row.get("event_date_type", pd.NA),
                    "translation_variant": variant,
                    "translation_value": value if not pd.isna(value) else pd.NA,
                    "translation_value_units": units,
                    "translation_source_field": source_field,
                    "translation_method": method,
                    "duration_assumption_source": duration_source,
                    "translation_review_status": row.get("shock_review_status", pd.NA),
                    "shock_source": row.get("shock_source", pd.NA),
                    "shock_review_status": row.get("shock_review_status", pd.NA),
                    "alternative_treatment_complete": row.get("alternative_treatment_complete", pd.NA),
                    "alternative_treatment_missing_fields": row.get("alternative_treatment_missing_fields", pd.NA),
                    "alternative_treatment_missing_reason": row.get("alternative_treatment_missing_reason", pd.NA),
                    "gross_notional_delta_bn": row.get("gross_notional_delta_bn", pd.NA),
                    "quality_tier": row.get("quality_tier", pd.NA),
                    "eligibility_blockers": row.get("eligibility_blockers", pd.NA),
                    "timestamp_precision": row.get("timestamp_precision", pd.NA),
                    "separability_status": row.get("separability_status", pd.NA),
                    "expectation_status": row.get("expectation_status", pd.NA),
                    "contamination_status": row.get("contamination_status", pd.NA),
                    "causal_eligible_component_count": row.get("causal_eligible_component_count", pd.NA),
                    "long_rate_pilot_ready": not blockers,
                    "long_rate_pilot_blocker": "|".join(blockers),
                    "public_role": "supporting",
                    "claim_scope": (
                        CLAIM_SCOPE_CAUSAL_PILOT_ONLY
                        if not blockers
                        else CLAIM_SCOPE_DESCRIPTIVE_ONLY
                    ),
                }
            )
    return pd.DataFrame(rows, columns=columns)


def _empty_auction_absorption_frame() -> pd.DataFrame:
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
            "spec_id",
            "claim_scope",
        ]
    )


def _build_event_quarter_map() -> pd.DataFrame:
    events = _qra_event_table_frame()
    if events.empty:
        return pd.DataFrame(columns=["quarter", "qra_event_id", "event_label"])
    events = events.loc[events.get("event_date_type", pd.Series(dtype=str)).astype(str) == "official_release_date"].copy()
    if events.empty:
        return pd.DataFrame(columns=["quarter", "qra_event_id", "event_label"])
    return (
        events[["quarter", "event_id", "event_label"]]
        .drop_duplicates(subset=["quarter", "event_id"], keep="first")
        .rename(columns={"event_id": "qra_event_id"})
    )


def _quarter_series(value: pd.Series) -> pd.Series:
    return value.map(_quarter_from_timestamp)


def _normalize_security_family(label: object) -> str:
    text = str(label or "").upper()
    if "TIPS" in text:
        return "tips"
    if "COUPONS" in text or "COUPON" in text:
        return "nominal_coupon"
    if "BILL" in text:
        return "bill"
    return "other"


def build_auction_absorption_publish_table() -> pd.DataFrame:
    source = _read_optional_source_csv("auction_absorption_table")
    if not source.empty:
        out = _ensure_columns(
            source,
            {
                "spec_id": QRA_AUCTION_SPEC_ID,
                "source_family": "unknown",
                "source_quality": "summary_ready",
                "provenance_summary": "",
                "claim_scope": CLAIM_SCOPE_DESCRIPTIVE_ONLY,
            },
        )
        return _apply_event_claim_scope(out, default=CLAIM_SCOPE_DESCRIPTIVE_ONLY)

    event_map = _build_event_quarter_map()
    rows: list[pd.DataFrame] = []

    investor_path = PROCESSED_DIR / "investor_allotments_panel.csv"
    if investor_path.exists():
        investor = pd.read_csv(investor_path)
        if not investor.empty and {"auction_date", "measure", "value"}.issubset(investor.columns):
            investor = investor.loc[investor["measure"].astype(str).eq("allotment_amount")].copy()
            if "investor_class" in investor.columns:
                investor = investor.loc[investor["investor_class"].astype(str).ne("total_issue")].copy()
            investor["quarter"] = _quarter_series(pd.to_datetime(investor["auction_date"], errors="coerce"))
            investor = investor.merge(event_map, on="quarter", how="left")
            investor["qra_event_id"] = investor["qra_event_id"].fillna("")
            investor["source_family"] = "investor_allotments"
            if "source_quality" not in investor.columns:
                investor["source_quality"] = "summary_ready"
            if "provenance_summary" not in investor.columns:
                investor["provenance_summary"] = investor.get("source_file", pd.Series(index=investor.index, dtype=object)).map(
                    lambda value: f"source_file={value}" if str(value).strip() else "investor_allotments_panel"
                )
            rows.append(
                investor[
                    [
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
                    ]
                ].copy()
            )

    dealer_path = PROCESSED_DIR / "primary_dealer_panel.csv"
    if dealer_path.exists():
        dealer = pd.read_csv(dealer_path)
        if not dealer.empty and {"date", "value"}.issubset(dealer.columns):
            label_source = dealer.get("series_label", pd.Series(index=dealer.index, dtype=object)).fillna("")
            series_text = label_source.astype(str)
            mask = series_text.str.contains("U.S. TREASURY", case=False, na=False) & series_text.str.contains(
                "COUPONS|TIPS", case=False, na=False, regex=True
            )
            dealer = dealer.loc[mask].copy()
            if not dealer.empty:
                dealer["quarter"] = _quarter_series(pd.to_datetime(dealer["date"], errors="coerce"))
                dealer = dealer.merge(event_map, on="quarter", how="left")
                dealer["qra_event_id"] = dealer["qra_event_id"].fillna("")
                dealer["source_family"] = "primary_dealer"
                dealer["security_family"] = series_text.loc[dealer.index].map(_normalize_security_family)
                if "source_quality" not in dealer.columns:
                    dealer["source_quality"] = "summary_ready"
                if "provenance_summary" not in dealer.columns:
                    dealer["provenance_summary"] = dealer.get("source_file", pd.Series(index=dealer.index, dtype=object)).map(
                        lambda value: f"source_file={value}" if str(value).strip() else "primary_dealer_panel"
                    )
                if "measure" not in dealer.columns:
                    dealer["measure"] = dealer.get("metric_label", pd.Series(index=dealer.index, dtype=object))
                dealer["auction_date"] = dealer.get("date", pd.Series(index=dealer.index, dtype=object))
                dealer["investor_class"] = "all"
                dealer["units"] = dealer.get("units", pd.Series(index=dealer.index, dtype=object))
                rows.append(
                    dealer[
                        [
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
                        ]
                    ].copy()
                )

    if not rows:
        return _empty_auction_absorption_frame()

    out = pd.concat(rows, ignore_index=True)
    out["spec_id"] = QRA_AUCTION_SPEC_ID
    out["claim_scope"] = CLAIM_SCOPE_DESCRIPTIVE_ONLY
    return out[
        [
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
            "spec_id",
            "claim_scope",
        ]
    ].sort_values(["quarter", "qra_event_id", "auction_date", "security_family", "measure"], kind="stable").reset_index(drop=True)


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
        "spec_id",
        "treatment_variant",
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
        "descriptive_headline_reason",
        "usable_for_descriptive_headline",
        "usable_for_headline_reason",
        "review_maturity",
        "elasticity_bp_per_100bn",
        "sign_flip_flag",
        "usable_for_headline",
        "claim_scope",
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
        "overlap_severity",
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
        "spec_id",
        "treatment_variant",
        "descriptive_headline_reason",
        "usable_for_descriptive_headline",
        "usable_for_headline_reason",
        "review_maturity",
        "usable_for_headline",
        "claim_scope",
    ]


def _empty_qra_event_shock_summary_publish_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=_qra_event_shock_summary_publish_columns())


def build_qra_event_elasticity_publish_table() -> pd.DataFrame:
    path = TABLES_DIR / "qra_event_elasticity.csv"
    if not path.exists():
        return _empty_qra_elasticity_publish_frame()
    df = pd.read_csv(path)
    df = df.loc[df.get("event_date_type", pd.Series(dtype=str)).astype(str) == "official_release_date"].copy()
    df = _augment_qra_elasticity_frame(df)
    df = _apply_event_claim_scope(df)
    keep = [column for column in _qra_elasticity_publish_columns() if column in df.columns]
    return df[keep].reset_index(drop=True)


def build_qra_event_elasticity_diagnostic_publish_table() -> pd.DataFrame:
    path = TABLES_DIR / "qra_event_elasticity.csv"
    if not path.exists():
        return _empty_qra_elasticity_publish_frame()
    df = pd.read_csv(path)
    df = _augment_qra_elasticity_frame(df)
    df = _apply_event_claim_scope(df)
    keep = [column for column in _qra_elasticity_publish_columns() if column in df.columns]
    return df[keep].copy()


def build_qra_event_shock_summary_publish_table() -> pd.DataFrame:
    path = _first_existing_path(
        TABLES_DIR / "qra_event_shock_summary.csv",
        TABLES_DIR / "qra_event_elasticity.csv",
        PROCESSED_DIR / "qra_event_shock_summary.csv",
    )
    if path is None:
        return _empty_qra_event_shock_summary_publish_frame()
    df = pd.read_csv(path)
    df = df.loc[df.get("event_date_type", pd.Series(dtype=str)).astype(str) == "official_release_date"].copy()
    if df.empty:
        return _empty_qra_event_shock_summary_publish_frame()
    df = _augment_qra_elasticity_frame(df)
    df = _apply_event_claim_scope(df)
    keep = [column for column in _qra_event_shock_summary_publish_columns() if column in df.columns]
    sort_columns = [column for column in ("event_date_requested", "event_id") if column in keep]
    if not sort_columns:
        sort_columns = [column for column in ("quarter", "event_id") if column in keep]
    return (
        df[keep]
        .drop_duplicates(subset=["event_id", "event_date_type"], keep="first")
        .sort_values(sort_columns, kind="stable")
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


def build_pricing_regression_summary_publish_table() -> pd.DataFrame:
    return _read_optional_source_csv(
        "pricing_regression_summary",
        columns=list(PRICING_REGRESSION_SUMMARY_COLUMNS),
    )


def build_pricing_spec_registry_publish_table() -> pd.DataFrame:
    return _read_optional_source_csv(
        "pricing_spec_registry",
        columns=list(PRICING_SPEC_REGISTRY_COLUMNS),
    )


def build_pricing_subsample_grid_publish_table() -> pd.DataFrame:
    return _read_optional_source_csv(
        "pricing_subsample_grid",
        columns=list(PRICING_SUBSAMPLE_GRID_COLUMNS),
    )


def build_pricing_regression_robustness_publish_table() -> pd.DataFrame:
    return _read_optional_source_csv(
        "pricing_regression_robustness",
        columns=list(PRICING_REGRESSION_ROBUSTNESS_COLUMNS),
    )


def build_pricing_scenario_translation_publish_table() -> pd.DataFrame:
    return _read_optional_source_csv(
        "pricing_scenario_translation",
        columns=list(PRICING_SCENARIO_TRANSLATION_COLUMNS),
    )


def build_pricing_release_flow_panel_publish_table() -> pd.DataFrame:
    return _read_processed_csv(
        PROCESSED_DIR / "pricing_release_flow_panel.csv",
        columns=list(RELEASE_FLOW_PANEL_COLUMNS),
    )


def build_pricing_release_flow_leave_one_out_publish_table() -> pd.DataFrame:
    return _read_optional_source_csv(
        "pricing_release_flow_leave_one_out",
        columns=list(PRICING_RELEASE_FLOW_LEAVE_ONE_OUT_COLUMNS),
    )


def build_pricing_tau_sensitivity_grid_publish_table() -> pd.DataFrame:
    return _read_optional_source_csv(
        "pricing_tau_sensitivity_grid",
        columns=list(PRICING_TAU_SENSITIVITY_GRID_COLUMNS),
    )


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
    try:
        panel = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=columns)
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
    try:
        panel = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=columns)
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
            "dataset": "pricing",
            "series_id": "ati_baseline_bn",
            "frequency": "monthly (release-carried)",
            "value_units": "USD billions",
            "sign_convention": "Positive values imply a more bill-heavy maturity tilt relative to the 18% bill-share baseline.",
            "source_quality": "derived_pricing_reduced_form",
            "series_role": "supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "pricing",
            "series_id": "stock_excess_bills_bn",
            "frequency": "monthly",
            "value_units": "USD billions",
            "sign_convention": "Positive values imply a larger stock of bills relative to the 18% marketable bill-share baseline.",
            "source_quality": "derived_pricing_reduced_form",
            "series_role": "supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "pricing",
            "series_id": "headline_public_duration_supply",
            "frequency": "weekly (W-WED)",
            "value_units": "USD billions",
            "sign_convention": "Positive values imply more duration supply reaching the public in that week.",
            "source_quality": "derived_pricing_reduced_form",
            "series_role": "supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "pricing",
            "series_id": "pricing_release_flow_panel",
            "frequency": "release-event",
            "value_units": "release rows",
            "sign_convention": "One row per unique market-pricing marker with fixed-horizon release deltas and pre-release placebo windows.",
            "source_quality": "derived_pricing_reduced_form",
            "series_role": "supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "pricing",
            "series_id": "pricing_release_flow_leave_one_out",
            "frequency": "artifact",
            "value_units": "diagnostic rows",
            "sign_convention": "One row per omitted release and outcome for the +63 business-day release-level flow anchor.",
            "source_quality": "derived_pricing_reduced_form",
            "series_role": "supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "pricing",
            "series_id": "pricing_tau_sensitivity_grid",
            "frequency": "artifact",
            "value_units": "regression rows",
            "sign_convention": "Rows report stock-only coefficients under alternate target bill-share anchors.",
            "source_quality": "derived_pricing_reduced_form",
            "series_role": "supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "pricing",
            "series_id": "THREEFYTP10",
            "frequency": "monthly / weekly",
            "value_units": "basis points",
            "sign_convention": "Higher values imply a higher 10-year term-premium proxy.",
            "source_quality": "derived_pricing_reduced_form",
            "series_role": "supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "pricing",
            "series_id": "DGS10",
            "frequency": "monthly / weekly",
            "value_units": "basis points",
            "sign_convention": "Higher values imply a higher 10-year Treasury yield.",
            "source_quality": "derived_pricing_reduced_form",
            "series_role": "supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "pricing_scenario_translation",
            "series_id": "implied_bp_change_threefytp10",
            "frequency": "scenario",
            "value_units": "basis points",
            "sign_convention": "Positive values imply a higher 10-year term-premium proxy under the translated scenario.",
            "source_quality": "derived_pricing_reduced_form",
            "series_role": "supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "pricing_spec_registry",
            "series_id": "pricing_spec_registry",
            "frequency": "artifact",
            "value_units": "registry rows",
            "sign_convention": "One row per locked pricing specification and headline outcome.",
            "source_quality": "derived_pricing_reduced_form",
            "series_role": "supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "pricing_subsample_grid",
            "series_id": "pricing_subsample_grid",
            "frequency": "artifact",
            "value_units": "regression rows",
            "sign_convention": "Subsample rows report primary-predictor coefficients under the named sample restriction.",
            "source_quality": "derived_pricing_reduced_form",
            "series_role": "supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "pricing_scenario_translation",
            "series_id": "implied_bp_change_dgs10",
            "frequency": "scenario",
            "value_units": "basis points",
            "sign_convention": "Positive values imply a higher 10-year Treasury yield under the translated scenario.",
            "source_quality": "derived_pricing_reduced_form",
            "series_role": "supporting",
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
    duplicate_subset = ["event_id", "event_date_type", "series", "window"]
    if set(duplicate_subset).issubset(df.columns):
        if "treatment_variant" in df.columns:
            duplicate_subset.append("treatment_variant")
        if df.duplicated(subset=duplicate_subset).any():
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


def _qra_review_surface_integrity(
    qra_event_registry: pd.DataFrame,
    qra_shock_crosswalk: pd.DataFrame,
    event_usability: pd.DataFrame,
    qra_shock_summary: pd.DataFrame,
    qra_event_robustness: pd.DataFrame,
) -> dict[str, object]:
    issues: list[str] = []
    if qra_event_registry.empty or qra_shock_crosswalk.empty or event_usability.empty or qra_shock_summary.empty:
        issues.append("missing_review_surface")
        return {"ready": False, "issues": issues}

    registry = qra_event_registry.copy()
    registry = registry.drop_duplicates(subset=["event_id"], keep="first").copy()
    registry["headline_eligibility_reason"] = registry.get(
        "headline_eligibility_reason",
        pd.Series(index=registry.index, dtype=object),
    ).fillna("")

    crosswalk = qra_shock_crosswalk.copy()
    if "treatment_variant" in crosswalk.columns:
        canonical = crosswalk.loc[crosswalk["treatment_variant"].astype(str) == "canonical_shock_bn"].copy()
        if not canonical.empty:
            crosswalk = canonical
    crosswalk = crosswalk.loc[crosswalk.get("event_date_type", pd.Series(dtype=object)).astype(str) == "official_release_date"].copy()
    crosswalk = crosswalk.drop_duplicates(subset=["event_id"], keep="first").copy()

    shock = qra_shock_summary.copy()
    if "treatment_variant" in shock.columns:
        canonical = shock.loc[shock["treatment_variant"].astype(str) == "canonical_shock_bn"].copy()
        if not canonical.empty:
            shock = canonical
    shock = shock.loc[shock.get("event_date_type", pd.Series(dtype=object)).astype(str) == "official_release_date"].copy()
    shock = shock.drop_duplicates(subset=["event_id"], keep="first").copy()
    if "overlap_severity" not in shock.columns:
        shock["overlap_severity"] = "none"
    if "usable_for_headline_reason" not in shock.columns:
        shock["usable_for_headline_reason"] = "missing_shock_summary"
    if "usable_for_headline" not in shock.columns:
        shock["usable_for_headline"] = False

    registry_check = registry.merge(
        crosswalk[["event_id", "usable_for_headline_reason", "spec_id", "treatment_variant"]],
        on="event_id",
        how="inner",
        suffixes=("_registry", "_crosswalk"),
    )
    if registry_check.empty:
        issues.append("missing_registry_crosswalk_overlap")
    else:
        reason_mismatch = registry_check.loc[
            registry_check["headline_eligibility_reason"].astype(str).str.strip()
            != registry_check["usable_for_headline_reason"].astype(str).str.strip()
        ]
        if not reason_mismatch.empty:
            issues.append("registry_crosswalk_reason_mismatch")

    summary_check = shock.merge(
        crosswalk[["event_id", "usable_for_headline_reason", "spec_id", "treatment_variant"]],
        on="event_id",
        how="inner",
        suffixes=("_summary", "_crosswalk"),
    )
    if summary_check.empty:
        issues.append("missing_summary_crosswalk_overlap")
    else:
        reason_mismatch = summary_check.loc[
            summary_check["usable_for_headline_reason_summary"].astype(str).str.strip()
            != summary_check["usable_for_headline_reason_crosswalk"].astype(str).str.strip()
        ]
        if not reason_mismatch.empty:
            issues.append("summary_crosswalk_reason_mismatch")
        spec_mismatch = summary_check.loc[
            summary_check["spec_id_summary"].astype(str).str.strip()
            != summary_check["spec_id_crosswalk"].astype(str).str.strip()
        ]
        if not spec_mismatch.empty:
            issues.append("summary_crosswalk_spec_mismatch")
        variant_mismatch = summary_check.loc[
            summary_check["treatment_variant_summary"].astype(str).str.strip()
            != summary_check["treatment_variant_crosswalk"].astype(str).str.strip()
        ]
        if not variant_mismatch.empty:
            issues.append("summary_crosswalk_variant_mismatch")

    summary_group = (
        shock.groupby(
            [
                "event_date_type",
                "headline_bucket",
                "classification_review_status",
                "shock_review_status",
                "overlap_severity",
                "usable_for_headline",
                "usable_for_headline_reason",
            ],
            dropna=False,
        )["event_id"]
        .nunique()
        .reset_index(name="event_count_summary")
    )
    usability_group = event_usability.copy()
    if "treatment_variant" in usability_group.columns:
        canonical = usability_group.loc[
            usability_group["treatment_variant"].astype(str) == "canonical_shock_bn"
        ].copy()
        if not canonical.empty:
            usability_group = canonical
    usability_group = usability_group.loc[
        usability_group.get("event_date_type", pd.Series(dtype=object)).astype(str)
        == "official_release_date"
    ].copy()
    merged_group = usability_group.merge(
        summary_group,
        on=[
            "event_date_type",
            "headline_bucket",
            "classification_review_status",
            "shock_review_status",
            "overlap_severity",
            "usable_for_headline",
            "usable_for_headline_reason",
        ],
        how="outer",
    )
    if merged_group["event_count"].fillna(-1).astype(float).ne(merged_group["event_count_summary"].fillna(-1).astype(float)).any():
        issues.append("usability_summary_count_mismatch")

    if qra_event_robustness.empty:
        issues.append("missing_qra_event_robustness")
    else:
        variants = set(qra_event_robustness.get("sample_variant", pd.Series(dtype=object)).astype(str))
        if not {"all_events", "overlap_excluded"}.issubset(variants):
            issues.append("missing_overlap_variants")
    return {"ready": not issues, "issues": issues}


def build_dataset_status_table() -> pd.DataFrame:
    official_capture = build_official_capture_readiness_table()
    official_capture_history_status = build_official_capture_history_status_table()
    official_capture_backfill_queue = build_official_capture_backfill_queue_table()
    extension_status = build_extension_status_table()
    official_ati_headline = _official_ati_headline_table()
    official_ati_headline_ready = not official_ati_headline.empty
    qra_elasticity_readiness = _qra_elasticity_readiness(TABLES_DIR / "qra_event_elasticity.csv")
    qra_elasticity_tier = str(qra_elasticity_readiness["readiness_tier"])
    if qra_elasticity_tier == "supporting_ready":
        qra_elasticity_tier = "supporting_provisional"
    qra_event_registry = build_qra_event_registry_publish_table()
    qra_release_component_registry = build_qra_release_component_registry_publish_table()
    qra_benchmark_evidence_registry = build_qra_benchmark_evidence_registry_table()
    qra_causal_qa = build_qra_causal_qa_publish_table()
    causal_claims_status = build_causal_claims_status_table()
    event_design_status = build_event_design_status_publish_table()
    qra_benchmark_coverage = build_qra_benchmark_coverage_table()
    qra_benchmark_blockers = build_qra_benchmark_blockers_by_event_publish_table()
    causal_design_ready = _causal_design_supporting_ready(event_design_status)
    causal_design_missing = _causal_design_missing_fields(event_design_status)
    qra_shock_crosswalk = build_qra_shock_crosswalk_publish_table()
    qra_shock_summary = build_qra_event_shock_summary_publish_table()
    qra_event_robustness = build_qra_robustness_publish_table()
    treatment_comparison = build_treatment_comparison_publish_table()
    qra_long_rate_translation = build_qra_long_rate_translation_panel()
    event_usability = build_event_usability_publish_table()
    leave_one_out = build_leave_one_event_out_publish_table()
    auction_absorption = build_auction_absorption_publish_table()
    pricing_spec_registry = build_pricing_spec_registry_publish_table()
    pricing_regression_summary = build_pricing_regression_summary_publish_table()
    pricing_subsample_grid = build_pricing_subsample_grid_publish_table()
    pricing_regression_robustness = build_pricing_regression_robustness_publish_table()
    pricing_scenario_translation = build_pricing_scenario_translation_publish_table()
    pricing_release_flow_panel = build_pricing_release_flow_panel_publish_table()
    pricing_release_flow_leave_one_out = build_pricing_release_flow_leave_one_out_publish_table()
    pricing_tau_sensitivity_grid = build_pricing_tau_sensitivity_grid_publish_table()
    qra_review_surface = _qra_review_surface_integrity(
        qra_event_registry=qra_event_registry,
        qra_shock_crosswalk=qra_shock_crosswalk,
        event_usability=event_usability,
        qra_shock_summary=qra_shock_summary,
        qra_event_robustness=qra_event_robustness,
    )
    pricing_missing: list[str] = []
    if pricing_spec_registry.empty:
        pricing_missing.append("pricing_spec_registry_missing")
    if pricing_regression_summary.empty:
        pricing_missing.append("pricing_regression_summary_missing")
    if pricing_subsample_grid.empty:
        pricing_missing.append("pricing_subsample_grid_missing")
    if pricing_regression_robustness.empty:
        pricing_missing.append("pricing_regression_robustness_missing")
    if pricing_scenario_translation.empty:
        pricing_missing.append("pricing_scenario_translation_missing")
    if pricing_release_flow_panel.empty:
        pricing_missing.append("pricing_release_flow_panel_missing")
    if pricing_release_flow_leave_one_out.empty:
        pricing_missing.append("pricing_release_flow_leave_one_out_missing")
    if pricing_tau_sensitivity_grid.empty:
        pricing_missing.append("pricing_tau_sensitivity_grid_missing")
    rows = [
        {
            "dataset": "official_capture",
            "readiness_tier": (
                "headline_ready"
                if not official_capture.empty and bool(official_capture["headline_ready"].any())
                else "fallback_only"
            ),
            "source_quality": (
                "exact_official"
                if not official_capture.empty and set(official_capture["source_quality"]) == {"exact_official"}
                else (
                    "exact_official_window_plus_seed_history"
                    if not official_capture.empty and bool(official_capture["headline_ready"].any())
                    else "mixed"
                )
            ),
            "headline_ready": bool(not official_capture.empty and official_capture["headline_ready"].any()),
            "fallback_only": bool(official_capture.empty or not official_capture["headline_ready"].any()),
            "missing_critical_fields": (
                ""
                if official_capture.empty or bool(official_capture["headline_ready"].any())
                else "|".join(sorted({value for value in official_capture["missing_critical_fields"] if str(value).strip()}))
            ),
            "last_regenerated_utc": _artifact_mtime(PROCESSED_DIR / "official_quarterly_refunding_capture.csv"),
            "review_maturity": "headline_ready" if not official_capture.empty and bool(official_capture["headline_ready"].any()) else "provisional_supporting",
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
            "review_maturity": "headline_ready" if official_ati_headline_ready else "provisional_supporting",
            "public_role": "headline",
        },
        {
            "dataset": "qra_event_elasticity",
            "readiness_tier": (
                qra_elasticity_tier
                if (
                    qra_review_surface["ready"]
                    or qra_elasticity_tier in {"not_started", "schema_incomplete"}
                )
                else "supporting_provisional"
            ),
            "source_quality": "manual_qra_shock_template_plus_event_panel",
            "headline_ready": False,
            "fallback_only": (
                True
                if qra_elasticity_tier == "supporting_provisional"
                else bool(qra_elasticity_readiness["fallback_only"]) or not qra_review_surface["ready"]
            ),
            "missing_critical_fields": "|".join(
                part
                for part in (
                    str(qra_elasticity_readiness["missing_critical_fields"]),
                    "|".join(qra_review_surface["issues"]),
                )
                if part
            ),
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "qra_event_elasticity.csv"),
            "review_maturity": "provisional_supporting"
            if not _read_optional_source_csv("qra_event_elasticity").empty
            else "not_started",
            "public_role": "supporting",
        },
        {
            "dataset": "qra_event_registry_v2",
            "readiness_tier": (
                "supporting_ready"
                if (not qra_event_registry.empty and qra_review_surface["ready"] and causal_design_ready)
                else "supporting_provisional"
            ),
            "source_quality": "derived_event_ledger",
            "headline_ready": False,
            "fallback_only": qra_event_registry.empty or not qra_review_surface["ready"] or not causal_design_ready,
            "missing_critical_fields": "|".join(
                part
                for part in (
                    "|".join(qra_review_surface["issues"]),
                    causal_design_missing,
                )
                if part
            ),
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "qra_event_registry_v2.csv"),
            "review_maturity": "provisional_supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "qra_release_component_registry",
            "readiness_tier": (
                "supporting_ready"
                if (not qra_release_component_registry.empty and causal_design_ready)
                else ("supporting_provisional" if not qra_release_component_registry.empty else "not_started")
            ),
            "source_quality": "derived_component_registry",
            "headline_ready": False,
            "fallback_only": qra_release_component_registry.empty or not causal_design_ready,
            "missing_critical_fields": causal_design_missing,
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "qra_release_component_registry.csv"),
            "review_maturity": "provisional_supporting" if not qra_release_component_registry.empty else "not_started",
            "public_role": "supporting",
        },
        {
            "dataset": "qra_benchmark_coverage",
            "readiness_tier": "supporting_ready" if not qra_benchmark_coverage.empty else "not_started",
            "source_quality": "derived_benchmark_coverage",
            "headline_ready": False,
            "fallback_only": qra_benchmark_coverage.empty,
            "missing_critical_fields": "" if not qra_benchmark_coverage.empty else "benchmark_coverage_missing",
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "qra_release_component_registry.csv"),
            "review_maturity": "supporting_ready" if not qra_benchmark_coverage.empty else "not_started",
            "public_role": "supporting",
        },
        {
            "dataset": "qra_benchmark_blockers_by_event",
            "readiness_tier": "supporting_ready" if not qra_benchmark_blockers.empty else "not_started",
            "source_quality": "derived_benchmark_blockers",
            "headline_ready": False,
            "fallback_only": qra_benchmark_blockers.empty,
            "missing_critical_fields": "" if not qra_benchmark_blockers.empty else "benchmark_blockers_missing",
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "qra_benchmark_blockers_by_event.csv"),
            "review_maturity": "supporting_ready" if not qra_benchmark_blockers.empty else "not_started",
            "public_role": "supporting",
        },
        {
            "dataset": "qra_benchmark_evidence_registry",
            "readiness_tier": "supporting_ready" if not qra_benchmark_evidence_registry.empty else "not_started",
            "source_quality": "derived_benchmark_evidence_registry",
            "headline_ready": False,
            "fallback_only": qra_benchmark_evidence_registry.empty,
            "missing_critical_fields": "" if not qra_benchmark_evidence_registry.empty else "benchmark_evidence_registry_missing",
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "qra_release_component_registry.csv"),
            "review_maturity": "supporting_ready" if not qra_benchmark_evidence_registry.empty else "not_started",
            "public_role": "supporting",
        },
        {
            "dataset": "qra_causal_qa_ledger",
            "readiness_tier": (
                "supporting_ready"
                if (not qra_causal_qa.empty and causal_design_ready)
                else ("supporting_provisional" if not qra_causal_qa.empty else "not_started")
            ),
            "source_quality": "derived_causal_qa",
            "headline_ready": False,
            "fallback_only": qra_causal_qa.empty or not causal_design_ready,
            "missing_critical_fields": causal_design_missing,
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "qra_causal_qa_ledger.csv"),
            "review_maturity": "provisional_supporting" if not qra_causal_qa.empty else "not_started",
            "public_role": "supporting",
        },
        {
            "dataset": "causal_claims_status",
            "readiness_tier": "supporting_ready" if not causal_claims_status.empty else "not_started",
            "source_quality": "derived_causal_claims_status",
            "headline_ready": False,
            "fallback_only": causal_claims_status.empty,
            "missing_critical_fields": "" if not causal_claims_status.empty else "causal_claims_status_missing",
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "event_design_status.csv"),
            "review_maturity": "supporting_ready" if not causal_claims_status.empty else "not_started",
            "public_role": "supporting",
        },
        {
            "dataset": "official_capture_history_status",
            "readiness_tier": "supporting_ready" if not official_capture_history_status.empty else "not_started",
            "source_quality": (
                "exact_official_window_plus_archive_scaffold_history"
                if not official_capture_history_status.empty
                else "missing"
            ),
            "headline_ready": False,
            "fallback_only": official_capture_history_status.empty,
            "missing_critical_fields": "" if not official_capture_history_status.empty else "official_capture_history_missing",
            "last_regenerated_utc": _artifact_mtime(PROCESSED_DIR / "official_quarterly_refunding_capture.csv"),
            "review_maturity": "supporting_ready" if not official_capture_history_status.empty else "not_started",
            "public_role": "supporting",
        },
        {
            "dataset": "official_capture_backfill_queue",
            "readiness_tier": "supporting_ready" if not official_capture_backfill_queue.empty else "not_started",
            "source_quality": "derived_backfill_queue",
            "headline_ready": False,
            "fallback_only": official_capture_backfill_queue.empty,
            "missing_critical_fields": "" if not official_capture_backfill_queue.empty else "official_capture_backfill_queue_missing",
            "last_regenerated_utc": _artifact_mtime(PROCESSED_DIR / "official_quarterly_refunding_capture.csv"),
            "review_maturity": "supporting_ready" if not official_capture_backfill_queue.empty else "not_started",
            "public_role": "supporting",
        },
        {
            "dataset": "event_design_status",
            "readiness_tier": (
                "supporting_ready"
                if (not event_design_status.empty and causal_design_ready)
                else ("supporting_provisional" if not event_design_status.empty else "not_started")
            ),
            "source_quality": "derived_event_design_status",
            "headline_ready": False,
            "fallback_only": event_design_status.empty or not causal_design_ready,
            "missing_critical_fields": causal_design_missing,
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "event_design_status.csv"),
            "review_maturity": "provisional_supporting" if not event_design_status.empty else "not_started",
            "public_role": "supporting",
        },
        {
            "dataset": "qra_shock_crosswalk_v1",
            "readiness_tier": "supporting_ready" if (not qra_shock_crosswalk.empty and qra_review_surface["ready"]) else "supporting_provisional",
            "source_quality": "derived_shock_crosswalk",
            "headline_ready": False,
            "fallback_only": qra_shock_crosswalk.empty or not qra_review_surface["ready"],
            "missing_critical_fields": "|".join(qra_review_surface["issues"]),
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "qra_shock_crosswalk_v1.csv"),
            "review_maturity": "provisional_supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "treatment_comparison_table",
            "readiness_tier": "supporting_ready" if not treatment_comparison.empty else "fallback_only",
            "source_quality": "derived_treatment_comparison",
            "headline_ready": False,
            "fallback_only": treatment_comparison.empty,
            "missing_critical_fields": "",
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "treatment_comparison_table.csv"),
            "review_maturity": "supporting_ready" if not treatment_comparison.empty else "provisional_supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "qra_long_rate_translation_panel",
            "readiness_tier": "supporting_provisional" if not qra_long_rate_translation.empty else "not_started",
            "source_quality": "derived_long_rate_translation",
            "headline_ready": False,
            "fallback_only": True,
            "missing_critical_fields": "" if not qra_long_rate_translation.empty else "qra_long_rate_translation_missing",
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "qra_shock_crosswalk_v1.csv"),
            "review_maturity": "provisional_supporting" if not qra_long_rate_translation.empty else "not_started",
            "public_role": "supporting",
        },
        {
            "dataset": "event_usability_table",
            "readiness_tier": "supporting_ready" if (not event_usability.empty and qra_review_surface["ready"]) else "supporting_provisional",
            "source_quality": "derived_qra_usability",
            "headline_ready": False,
            "fallback_only": event_usability.empty or not qra_review_surface["ready"],
            "missing_critical_fields": "|".join(qra_review_surface["issues"]),
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "event_usability_table.csv"),
            "review_maturity": "supporting_ready" if qra_review_surface["ready"] else "provisional_supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "leave_one_event_out_table",
            "readiness_tier": "supporting_ready" if (not leave_one_out.empty and qra_review_surface["ready"]) else "supporting_provisional",
            "source_quality": "derived_qra_robustness",
            "headline_ready": False,
            "fallback_only": leave_one_out.empty or not qra_review_surface["ready"],
            "missing_critical_fields": "|".join(qra_review_surface["issues"]),
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "leave_one_event_out_table.csv"),
            "review_maturity": "provisional_supporting",
            "public_role": "supporting",
        },
        {
            "dataset": "auction_absorption_table",
            "readiness_tier": "supporting_ready" if not auction_absorption.empty else "fallback_only",
            "source_quality": "derived_absorption_bridge",
            "headline_ready": False,
            "fallback_only": auction_absorption.empty,
            "missing_critical_fields": "",
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "auction_absorption_table.csv"),
            "review_maturity": "supporting_ready",
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
            "review_maturity": "headline_ready",
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
            "review_maturity": "headline_ready",
            "public_role": "headline",
        },
        {
            "dataset": "pricing",
            "readiness_tier": "supporting_provisional" if not pricing_regression_summary.empty else "not_started",
            "source_quality": "derived_pricing_reduced_form",
            "headline_ready": False,
            "fallback_only": True,
            "missing_critical_fields": "|".join(pricing_missing),
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "pricing_regression_summary.csv"),
            "review_maturity": "provisional_supporting" if not pricing_regression_summary.empty else "not_started",
            "public_role": "supporting",
        },
        {
            "dataset": "pricing_spec_registry",
            "readiness_tier": "supporting_provisional" if not pricing_spec_registry.empty else "not_started",
            "source_quality": "derived_pricing_reduced_form",
            "headline_ready": False,
            "fallback_only": True,
            "missing_critical_fields": "pricing_spec_registry_missing" if pricing_spec_registry.empty else "",
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "pricing_spec_registry.csv"),
            "review_maturity": "provisional_supporting" if not pricing_spec_registry.empty else "not_started",
            "public_role": "supporting",
        },
        {
            "dataset": "pricing_subsample_grid",
            "readiness_tier": "supporting_provisional" if not pricing_subsample_grid.empty else "not_started",
            "source_quality": "derived_pricing_reduced_form",
            "headline_ready": False,
            "fallback_only": True,
            "missing_critical_fields": "pricing_subsample_grid_missing" if pricing_subsample_grid.empty else "",
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "pricing_subsample_grid.csv"),
            "review_maturity": "provisional_supporting" if not pricing_subsample_grid.empty else "not_started",
            "public_role": "supporting",
        },
        {
            "dataset": "pricing_release_flow_panel",
            "readiness_tier": "supporting_provisional" if not pricing_release_flow_panel.empty else "not_started",
            "source_quality": "derived_pricing_reduced_form",
            "headline_ready": False,
            "fallback_only": True,
            "missing_critical_fields": "pricing_release_flow_panel_missing" if pricing_release_flow_panel.empty else "",
            "last_regenerated_utc": _artifact_mtime(PROCESSED_DIR / "pricing_release_flow_panel.csv"),
            "review_maturity": "provisional_supporting" if not pricing_release_flow_panel.empty else "not_started",
            "public_role": "supporting",
        },
        {
            "dataset": "pricing_release_flow_leave_one_out",
            "readiness_tier": "supporting_provisional" if not pricing_release_flow_leave_one_out.empty else "not_started",
            "source_quality": "derived_pricing_reduced_form",
            "headline_ready": False,
            "fallback_only": True,
            "missing_critical_fields": "pricing_release_flow_leave_one_out_missing" if pricing_release_flow_leave_one_out.empty else "",
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "pricing_release_flow_leave_one_out.csv"),
            "review_maturity": "provisional_supporting" if not pricing_release_flow_leave_one_out.empty else "not_started",
            "public_role": "supporting",
        },
        {
            "dataset": "pricing_tau_sensitivity_grid",
            "readiness_tier": "supporting_provisional" if not pricing_tau_sensitivity_grid.empty else "not_started",
            "source_quality": "derived_pricing_reduced_form",
            "headline_ready": False,
            "fallback_only": True,
            "missing_critical_fields": "pricing_tau_sensitivity_grid_missing" if pricing_tau_sensitivity_grid.empty else "",
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "pricing_tau_sensitivity_grid.csv"),
            "review_maturity": "provisional_supporting" if not pricing_tau_sensitivity_grid.empty else "not_started",
            "public_role": "supporting",
        },
        {
            "dataset": "pricing_scenario_translation",
            "readiness_tier": "supporting_provisional" if not pricing_scenario_translation.empty else "not_started",
            "source_quality": "derived_pricing_reduced_form",
            "headline_ready": False,
            "fallback_only": True,
            "missing_critical_fields": "pricing_scenario_translation_missing" if pricing_scenario_translation.empty else "",
            "last_regenerated_utc": _artifact_mtime(TABLES_DIR / "pricing_scenario_translation.csv"),
            "review_maturity": "provisional_supporting" if not pricing_scenario_translation.empty else "not_started",
            "public_role": "supporting",
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
                "review_maturity": "summary_ready" if row["backend_status"] == "summary_ready" else "provisional_supporting",
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
    publish_table("official_capture_history_status", "Official Capture History Status", build_official_capture_history_status_table())
    publish_table("official_capture_backfill_queue", "Official Capture Backfill Queue", build_official_capture_backfill_queue_table())
    publish_table("ati_seed_vs_official", "ATI Seed vs Official Comparison", build_ati_seed_vs_official_comparison())
    publish_table("qra_event_table", "QRA Event Table", build_qra_event_publish_table())
    publish_table("qra_event_summary", "QRA Event Summary", build_qra_summary_publish_table())
    publish_table("qra_event_robustness", "QRA Event Robustness", build_qra_robustness_publish_table())
    publish_table("qra_event_registry_v2", "QRA Event Registry V2", build_qra_event_registry_publish_table())
    publish_table("qra_release_component_registry", "QRA Release Component Registry", build_qra_release_component_registry_publish_table())
    publish_table("qra_benchmark_coverage", "QRA Benchmark Coverage", build_qra_benchmark_coverage_table())
    publish_table("qra_benchmark_blockers_by_event", "QRA Benchmark Blockers By Event", build_qra_benchmark_blockers_by_event_publish_table())
    publish_table("qra_benchmark_evidence_registry", "QRA Benchmark Evidence Registry", build_qra_benchmark_evidence_registry_table())
    publish_table("qra_causal_qa_ledger", "QRA Causal QA Ledger", build_qra_causal_qa_publish_table())
    publish_table("causal_claims_status", "Causal Claims Status", build_causal_claims_status_table())
    publish_table("event_design_status", "Event Design Status", build_event_design_status_publish_table())
    publish_table("qra_shock_crosswalk_v1", "QRA Shock Crosswalk V1", build_qra_shock_crosswalk_publish_table())
    publish_table("treatment_comparison_table", "Treatment Comparison Table", build_treatment_comparison_publish_table())
    publish_table("qra_long_rate_translation_panel", "QRA Long-Rate Translation Panel", build_qra_long_rate_translation_panel())
    publish_table("event_usability_table", "Event Usability Table", build_event_usability_publish_table())
    publish_table("leave_one_event_out_table", "Leave One Event Out Table", build_leave_one_event_out_publish_table())
    publish_table("auction_absorption_table", "Auction Absorption Table", build_auction_absorption_publish_table())
    if (TABLES_DIR / "qra_event_elasticity.csv").exists():
        publish_table("qra_event_elasticity", "QRA Event Elasticity", build_qra_event_elasticity_publish_table())
        publish_table(
            "qra_event_elasticity_diagnostic",
            "QRA Event Elasticity Diagnostic",
            build_qra_event_elasticity_diagnostic_publish_table(),
        )
    if _first_existing_path(TABLES_DIR / "qra_event_shock_summary.csv", PROCESSED_DIR / "qra_event_shock_summary.csv", TABLES_DIR / "qra_event_elasticity.csv") is not None:
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
    publish_table("pricing_spec_registry", "Pricing Spec Registry", build_pricing_spec_registry_publish_table())
    publish_table("pricing_regression_summary", "Pricing Regression Summary", build_pricing_regression_summary_publish_table())
    publish_table("pricing_subsample_grid", "Pricing Subsample Grid", build_pricing_subsample_grid_publish_table())
    publish_table("pricing_regression_robustness", "Pricing Regression Robustness", build_pricing_regression_robustness_publish_table())
    publish_table("pricing_release_flow_panel", "Pricing Release Flow Panel", build_pricing_release_flow_panel_publish_table())
    publish_table(
        "pricing_release_flow_leave_one_out",
        "Pricing Release Flow Leave One Out",
        build_pricing_release_flow_leave_one_out_publish_table(),
    )
    publish_table(
        "pricing_tau_sensitivity_grid",
        "Pricing Tau Sensitivity Grid",
        build_pricing_tau_sensitivity_grid_publish_table(),
    )
    publish_table("pricing_scenario_translation", "Pricing Scenario Translation", build_pricing_scenario_translation_publish_table())
    publish_table("data_sources_summary", "Data Sources Summary", build_data_sources_publish_table())
    publish_table("investor_allotments_summary", "Investor Allotments Summary", build_investor_allotments_summary_table())
    publish_table("primary_dealer_summary", "Primary Dealer Summary", build_primary_dealer_summary_table())
    publish_table("sec_nmfp_summary", "SEC N-MFP Summary", build_sec_nmfp_summary_table())
    publish_table("extension_status", "Extension Backend Status", build_extension_status_table())
    publish_table("dataset_status", "Dataset Status", build_dataset_status_table())
    publish_table("series_metadata_catalog", "Series Metadata Catalog", build_series_metadata_catalog())
    write_json(build_index_metadata(), publish_dir / "index.json")
