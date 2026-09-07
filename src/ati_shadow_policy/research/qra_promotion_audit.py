from __future__ import annotations

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


def _metric_value(frame: pd.DataFrame, scope: str, metric: str) -> int | None:
    if frame.empty or not {"scope", "metric", "value"}.issubset(frame.columns):
        return None
    match = frame[(frame["scope"] == scope) & (frame["metric"] == metric)]
    if match.empty:
        return None
    value = pd.to_numeric(match["value"], errors="coerce").dropna()
    if value.empty:
        return None
    if len(value) != 1 or value.iloc[0] < 0 or value.iloc[0] % 1:
        raise ValueError(f"Expected one nonnegative integer count for {scope}/{metric}")
    return int(value.iloc[0])


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
        (causal, {"source_family_exhausted_count"}),
        (usability, {"event_date_type", "usable_for_headline"}),
        (pricing, {"model_id", "term_role", "coef", "p_value"}),
        (robustness, {"variant_family", "coef", "p_value"}),
        (diagnostic, {"usable_for_headline_reason"}),
    ]
    for frame, required in schemas:
        if not required.issubset(frame.columns):
            raise ValueError(f"Audit evidence missing columns: {sorted(required - set(frame.columns))}")
    for frame, column in [(capture, "is_headline_ready"), (usability, "usable_for_headline")]:
        if not frame[column].dropna().isin([True, False]).all():
            raise ValueError(f"Audit evidence {column} must contain booleans")

    capture_rows = len(capture)
    capture_ready = int(capture.get("is_headline_ready", pd.Series(dtype=bool)).fillna(False).sum())
    financing_scope = "current_sample_financing_estimates"
    causal_eligible = _metric_value(benchmarks, financing_scope, "causal_eligible_count")
    benchmark_ready = _metric_value(benchmarks, financing_scope, "external_benchmark_ready_count")
    post_release_invalid = _metric_value(benchmarks, financing_scope, "post_release_invalid_count")
    if any(value is None for value in [causal_eligible, benchmark_ready, post_release_invalid]):
        raise ValueError("Required financing benchmark counts are missing")
    source_exhausted = None
    if not causal.empty and "source_family_exhausted_count" in causal.columns:
        value = pd.to_numeric(causal["source_family_exhausted_count"], errors="coerce").dropna()
        source_exhausted = int(value.iloc[0]) if not value.empty else None

    official_usable = 0
    if not usability.empty and {"event_date_type", "usable_for_headline"}.issubset(usability.columns):
        official = usability[usability["event_date_type"] == "official_release_date"]
        official_usable = int(official["usable_for_headline"].fillna(False).astype(bool).sum())

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
            "status": "weak_imprecise",
            "evidence": _coef_phrase(release_anchor),
            "primary_artifacts": "output/publish/pricing_regression_summary.csv;output/publish/pricing_release_flow_leave_one_out.csv",
            "allowed_claim": "The +63bd release-flow design is cleaner than monthly timing but remains imprecise.",
            "forbidden_upgrade": "Do not use the release-flow coefficient as the headline elasticity.",
        },
        {
            "lane": "monthly_maturity_tilt_flow",
            "paper_role": "supporting_pricing_context",
            "status": "strongest_reduced_form_signal",
            "evidence": _coef_phrase(monthly_flow),
            "primary_artifacts": "output/publish/pricing_regression_summary.csv;output/publish/pricing_regression_robustness.csv",
            "allowed_claim": "Monthly Maturity-Tilt Flow is the strongest reduced-form pricing relationship.",
            "forbidden_upgrade": "Do not present it as clean announcement-time identification.",
        },
        {
            "lane": "weekly_public_duration_supply",
            "paper_role": "mechanism_context",
            "status": "strong_but_regime_sensitive",
            "evidence": _coef_phrase(weekly_duration),
            "primary_artifacts": "output/publish/pricing_regression_summary.csv;docs/PRICING_RESULTS_MEMO.md",
            "allowed_claim": "Public duration supply is a mechanism-context series with large reduced-form associations.",
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
            "status": "mixed_but_not_dominant",
            "evidence": _coef_phrase(placebos),
            "primary_artifacts": "output/publish/pricing_regression_robustness.csv",
            "allowed_claim": "Pre-release placebo checks are part of the boundary discipline.",
            "forbidden_upgrade": "Do not ignore pre-release drift/leakage risk in QRA language.",
        },
    ]
    availability = {
        "official_qra_measurement": capture_ready > 0,
        "release_flow_pricing_anchor": not release_anchor.empty,
        "monthly_maturity_tilt_flow": not monthly_flow.empty,
        "weekly_public_duration_supply": not weekly_duration.empty,
        "excess_bills_stock": not monthly_stock.empty,
        "pre_release_placebo": not placebos.empty,
    }
    for row in rows:
        if availability.get(row["lane"], True) is False or row["evidence"] == "missing":
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
            "It summarizes QRA measurement and pricing artifacts. Fixed qualitative "
            "assessments require review against these inputs; they are not statistical gates."
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
