from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ati_shadow_policy.io_utils import ensure_dir, write_df
from ati_shadow_policy.paths import OUTPUT_DIR

AUDIT_COLUMNS = [
    "lane",
    "paper_role",
    "status",
    "evidence",
    "primary_artifacts",
    "allowed_claim",
    "forbidden_upgrade",
]


@dataclass(frozen=True)
class AuditInputs:
    publish_dir: Path = OUTPUT_DIR / "publish"

    def read(self, stem: str) -> pd.DataFrame:
        path = self.publish_dir / f"{stem}.csv"
        if not path.exists():
            raise ValueError(f"Missing required audit evidence: {path.name}")
        frame = pd.read_csv(path)
        if frame.empty:
            raise ValueError(f"Empty required audit evidence: {path.name}")
        return frame


def _count_value(values: pd.Series, label: str) -> int:
    if len(values) != 1:
        raise ValueError(f"Expected one nonnegative integer count for {label}")
    value = pd.to_numeric(values, errors="coerce").iloc[0]
    if pd.isna(value) or not math.isfinite(value) or value < 0 or value % 1:
        raise ValueError(f"Expected one nonnegative integer count for {label}")
    return int(value)


def _metric_value(frame: pd.DataFrame, scope: str, metric: str) -> int:
    match = frame[(frame["scope"] == scope) & (frame["metric"] == metric)]
    return _count_value(match["value"], f"{scope}/{metric}")


def _validate_pricing(rows: pd.DataFrame) -> None:
    if rows.empty:
        return
    expected = {
        "public_readiness": {"supporting_provisional"},
        "public_claim_role": {"supporting", "supporting_anchor", "supporting_context"},
        "term_units": {"USD 100bn"},
        "outcome_units": {"basis points"},
    }
    for column, allowed in expected.items():
        if not rows[column].isin(allowed).all():
            raise ValueError(f"Pricing evidence has contradictory {column}")
    for column in ["coef", "p_value"]:
        values = pd.to_numeric(rows[column], errors="coerce")
        if not values.map(lambda value: pd.notna(value) and math.isfinite(value)).all():
            raise ValueError(f"Pricing evidence has invalid {column}")
        if column == "p_value" and not values.between(0, 1).all():
            raise ValueError("Pricing evidence has invalid p_value")


def _primary_predictor(frame: pd.DataFrame, model_id: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    required = {"model_id", "term_role"}
    if not required.issubset(frame.columns):
        return pd.DataFrame()
    return frame[(frame["model_id"] == model_id) & (frame["term_role"] == "primary_predictor")].copy()


def _coef_phrase(rows: pd.DataFrame) -> str:
    if rows.empty:
        return "missing"
    bits: list[str] = []
    for row in rows.itertuples(index=False):
        dep = getattr(row, "dependent_variable", "")
        coef = getattr(row, "coef", pd.NA)
        p_value = getattr(row, "p_value", pd.NA)
        if pd.isna(coef) or pd.isna(p_value):
            continue
        bits.append(f"{dep} {float(coef):+.3f} bp per $100bn, p={float(p_value):.3f}")
    return "; ".join(bits) if bits else "missing"


def build_qra_promotion_audit(publish_dir: Path = OUTPUT_DIR / "publish") -> pd.DataFrame:
    """Build the bounded evidence audit for qrawatch."""
    inputs = AuditInputs(publish_dir)
    capture = inputs.read("official_capture_completion")
    benchmarks = inputs.read("qra_benchmark_coverage")
    causal = inputs.read("causal_claims_status")
    usability = inputs.read("event_usability_table")
    pricing = inputs.read("pricing_regression_summary")
    robustness = inputs.read("pricing_regression_robustness")
    diagnostic = inputs.read("qra_event_elasticity_diagnostic")

    schemas = [
        (capture, {"is_headline_ready"}),
        (benchmarks, {"scope", "metric", "value"}),
        (causal, {"claim_id", "claim_scope", "headline_ready", "source_family_exhausted_count"}),
        (usability, {"event_date_type", "usable_for_headline", "claim_scope"}),
        (pricing, {"model_id", "term_role", "coef", "p_value", "public_readiness", "public_claim_role", "term_units", "outcome_units"}),
        (robustness, {"variant_family", "coef", "p_value", "public_readiness", "public_claim_role", "term_units", "outcome_units"}),
        (diagnostic, {"usable_for_headline_reason"}),
    ]
    for frame, required in schemas:
        if not required.issubset(frame.columns):
            raise ValueError(f"Audit evidence missing columns: {sorted(required - set(frame.columns))}")
    for frame, column in [(capture, "is_headline_ready"), (usability, "usable_for_headline")]:
        if not frame[column].isin([True, False]).all():
            raise ValueError(f"Audit evidence {column} must contain booleans")

    capture_rows = len(capture)
    capture_ready = int(capture.get("is_headline_ready", pd.Series(dtype=bool)).fillna(False).sum())
    financing_scope = "current_sample_financing_estimates"
    causal_eligible = _metric_value(benchmarks, financing_scope, "causal_eligible_count")
    benchmark_ready = _metric_value(benchmarks, financing_scope, "external_benchmark_ready_count")
    post_release_invalid = _metric_value(benchmarks, financing_scope, "post_release_invalid_count")
    pilot = causal[causal["claim_id"] == "current_sample_financing_pilot"]
    source_exhausted = _count_value(
        pilot["source_family_exhausted_count"], "current_sample_financing_pilot/source_family_exhausted_count"
    )
    if not pilot["claim_scope"].eq("causal_pilot_only").all() or not pilot["headline_ready"].eq(False).all():
        raise ValueError("Financing evidence has contradictory claim scope or headline readiness")

    official = usability[usability["event_date_type"] == "official_release_date"]
    if not official["claim_scope"].eq("descriptive_only").all():
        raise ValueError("Official-event evidence has contradictory claim_scope")
    official_usable = int(official["usable_for_headline"].sum())

    diagnostic_non_official = 0
    if "usable_for_headline_reason" in diagnostic.columns:
        diagnostic_non_official = int(
            (diagnostic["usable_for_headline_reason"] == "non_official_event_date_type").sum()
        )

    release_anchor = _primary_predictor(pricing, "release_flow_baseline_63bd")
    monthly_flow = _primary_predictor(pricing, "monthly_flow_baseline")
    monthly_stock = _primary_predictor(pricing, "monthly_stock_baseline")
    weekly_duration = _primary_predictor(pricing, "weekly_duration_baseline")
    placebos = robustness[
        robustness.get("variant_family", pd.Series(dtype=str)).astype(str) == "release_flow_placebo"
    ].copy() if not robustness.empty else pd.DataFrame()

    for selected in [release_anchor, monthly_flow, monthly_stock, weekly_duration, placebos]:
        _validate_pricing(selected)

    rows = [
        {
            "lane": "official_qra_measurement",
            "paper_role": "appendix_headline_measurement",
            "status": "promote_for_measurement",
            "evidence": f"{capture_ready}/{capture_rows} official quarters headline-ready.",
            "primary_artifacts": "output/publish/official_capture_completion.csv;output/publish/ati_quarter_table.csv",
            "allowed_claim": "QRA Watch supports official maturity-composition measurement and timing documentation.",
            "forbidden_upgrade": "Do not turn measurement coverage into a causal long-rate elasticity.",
        },
        {
            "lane": "current_sample_financing_pilot",
            "paper_role": "credibility_appendix",
            "status": "bounded_supporting",
            "evidence": (
                f"{benchmark_ready} benchmark-ready financing rows; {causal_eligible} pass all causal gates; "
                f"{post_release_invalid} post-release-invalid rows; {source_exhausted} source-family-exhausted rows."
            ),
            "primary_artifacts": "output/publish/causal_claims_status.csv;output/publish/qra_benchmark_coverage.csv",
            "allowed_claim": "A narrow post-2022Q3 financing-estimates pilot supports observability and timing discipline.",
            "forbidden_upgrade": "Do not call this a settled or full-sample causal estimate.",
        },
        {
            "lane": "official_release_event_windows",
            "paper_role": "timing_appendix",
            "status": "descriptive_only",
            "evidence": f"{official_usable} official-release rows are marked usable; published claim_scope remains descriptive_only.",
            "primary_artifacts": "output/publish/event_usability_table.csv;output/publish/qra_event_summary.csv",
            "allowed_claim": "Official QRA event windows are useful timing and sign diagnostics.",
            "forbidden_upgrade": "Do not treat bucket means as causal announcement effects.",
        },
        {
            "lane": "monday_wednesday_subevents",
            "paper_role": "blocked_until_new_design",
            "status": "not_yet_separated",
            "evidence": (
                "Existing artifacts separate official release dates from T-1 pricing markers, "
                f"but {diagnostic_non_official} diagnostic rows are blocked as non-official event-date type."
            ),
            "primary_artifacts": "output/publish/qra_event_elasticity_diagnostic.csv",
            "allowed_claim": "Current artifacts document the need for subevent separation.",
            "forbidden_upgrade": "Do not claim distinct Monday financing-estimate versus Wednesday refunding-detail effects yet.",
        },
        {
            "lane": "release_flow_pricing_anchor",
            "paper_role": "supporting_pricing_context",
            "status": "supporting_provisional",
            "evidence": _coef_phrase(release_anchor),
            "primary_artifacts": "output/publish/pricing_regression_summary.csv;output/publish/pricing_release_flow_leave_one_out.csv",
            "allowed_claim": "The +63bd release-flow coefficients provide provisional release-window pricing context.",
            "forbidden_upgrade": "Do not use the release-flow coefficient as the headline elasticity.",
        },
        {
            "lane": "monthly_maturity_tilt_flow",
            "paper_role": "supporting_pricing_context",
            "status": "supporting_provisional",
            "evidence": _coef_phrase(monthly_flow),
            "primary_artifacts": "output/publish/pricing_regression_summary.csv;output/publish/pricing_regression_robustness.csv",
            "allowed_claim": "Monthly Maturity-Tilt Flow provides provisional reduced-form pricing context.",
            "forbidden_upgrade": "Do not present it as clean announcement-time identification.",
        },
        {
            "lane": "weekly_public_duration_supply",
            "paper_role": "mechanism_context",
            "status": "supporting_provisional",
            "evidence": _coef_phrase(weekly_duration),
            "primary_artifacts": "output/publish/pricing_regression_summary.csv;docs/PRICING_RESULTS_MEMO.md",
            "allowed_claim": "Public duration supply provides provisional mechanism context.",
            "forbidden_upgrade": "Do not treat the weekly coefficient as stable across regimes or structural.",
        },
        {
            "lane": "excess_bills_stock",
            "paper_role": "blocked_pricing_claim",
            "status": "not_promoted",
            "evidence": _coef_phrase(monthly_stock),
            "primary_artifacts": "output/publish/pricing_regression_summary.csv",
            "allowed_claim": "Excess Bills Stock is a diagnostic comparison variable.",
            "forbidden_upgrade": "Do not promote stock-based term-out scenario arithmetic as headline-ready.",
        },
        {
            "lane": "pre_release_placebo",
            "paper_role": "boundary_check",
            "status": "boundary_check",
            "evidence": _coef_phrase(placebos),
            "primary_artifacts": "output/publish/pricing_regression_robustness.csv",
            "allowed_claim": "Pre-release placebo checks are part of the boundary discipline.",
            "forbidden_upgrade": "Do not ignore pre-release drift/leakage risk in QRA language.",
        },
    ]
    availability = {
        "official_qra_measurement": capture_ready > 0,
        "current_sample_financing_pilot": benchmark_ready > 0 and causal_eligible > 0,
        "official_release_event_windows": official_usable > 0,
        "monday_wednesday_subevents": diagnostic_non_official > 0,
        "release_flow_pricing_anchor": not release_anchor.empty,
        "monthly_maturity_tilt_flow": not monthly_flow.empty,
        "weekly_public_duration_supply": not weekly_duration.empty,
        "excess_bills_stock": not monthly_stock.empty,
        "pre_release_placebo": not placebos.empty,
    }
    for row in rows:
        if not availability.get(row["lane"], False) or row["evidence"] == "missing":
            row["status"] = "evidence_unavailable"
            row["allowed_claim"] = "No empirical claim: required lane evidence is unavailable."
    return pd.DataFrame(rows, columns=AUDIT_COLUMNS)


def write_qra_promotion_audit(
    table_path: Path = OUTPUT_DIR / "tables" / "qra_promotion_audit.csv",
    report_path: Path = OUTPUT_DIR / "tables" / "qra_promotion_audit.md",
    *,
    publish_dir: Path = OUTPUT_DIR / "publish",
) -> tuple[Path, Path]:
    audit = build_qra_promotion_audit(publish_dir)
    write_df(audit, table_path)

    lines = [
        "# QRA Watch Promotion Audit",
        "",
        (
            "This audit is descriptive evidence governance, not causal identification. "
            "It summarizes QRA measurement and pricing artifacts. Pricing labels describe "
            "provisional research roles, not empirical strength or statistical gates."
        ),
        "",
        "## Decision",
        "",
        (
            "Use the lane availability and claim limits below. Measurement coverage does "
            "not establish a causal effect; pricing interpretations remain provisional."
        ),
        "",
        "## Audit Table",
        "",
        audit.to_markdown(index=False),
        "",
        "## Writing Rule",
        "",
        (
            "Allowed: QRA evidence supports measurement, timing discipline, and bounded "
            "announcement-surprise auditing. Forbidden: a settled causal long-rate "
            "elasticity, distinct Monday/Wednesday subevent effects, or structural "
            "maturity-composition pricing claims."
        ),
        "",
    ]
    ensure_dir(report_path.parent)
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return table_path, report_path
