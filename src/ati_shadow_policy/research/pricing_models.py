from __future__ import annotations

from collections.abc import Mapping, Sequence

import pandas as pd
import statsmodels.api as sm

from .pricing_panels import (
    RELEASE_FLOW_HORIZONS_BD,
    RELEASE_FLOW_PLACEBO_WINDOWS_BD,
    release_flow_control_column,
    release_flow_delta_column,
    release_flow_placebo_label,
    release_flow_window_label,
)


HEADLINE_OUTCOMES = ("THREEFYTP10", "DGS10")
SUPPORTING_OUTCOMES = ("DGS30", "slope_10y_2y", "slope_30y_2y")
OUTCOME_VARIABLES = (*HEADLINE_OUTCOMES, *SUPPORTING_OUTCOMES)
SUPPORTING_PRICING_OUTCOME = "DGS30"

PANEL_KEYS = (
    "official_ati_price_panel",
    "mspd_stock_excess_bills_panel",
    "weekly_supply_price_panel",
    "pricing_release_flow_panel",
)

OFFICIAL_ATI_PRICE_PANEL = PANEL_KEYS[0]
MSPD_STOCK_PANEL = PANEL_KEYS[1]
WEEKLY_SUPPLY_PANEL = PANEL_KEYS[2]
RELEASE_FLOW_PANEL = PANEL_KEYS[3]

OUTCOME_LABELS = {
    "THREEFYTP10": "10-year term premium proxy (THREEFYTP10)",
    "DGS10": "10-year Treasury constant maturity yield",
    "DGS30": "30-year Treasury constant maturity yield",
    "slope_10y_2y": "10Y minus 2Y yield spread",
    "slope_30y_2y": "30Y minus 2Y yield spread",
}

PREDICTOR_LABELS = {
    "ati_baseline_bn": "Maturity-Tilt Flow (internal `ati_baseline_bn`, USD bn)",
    "stock_excess_bills_bn": "Excess Bills Stock (internal `stock_excess_bills_bn`, USD bn)",
    "stock_excess_bills_share": "Excess Bills Share Gap (internal `stock_excess_bills_share`)",
    "cumulative_ati_baseline_bn": "Cumulative Maturity-Tilt Flow (internal `cumulative_ati_baseline_bn`, USD bn)",
    "headline_public_duration_supply": "Public Duration Supply (USD bn)",
    "qt_proxy": "QT proxy (USD bn)",
    "buybacks_accepted": "Buybacks accepted (USD bn)",
    "delta_wdtgal": "Change in WDTGAL (USD bn)",
    "DFF": "Effective federal funds rate",
    "debt_limit_dummy": "Debt-limit period dummy",
}

PREDICTOR_UNITS = {
    "ati_baseline_bn": "USD bn",
    "stock_excess_bills_bn": "USD bn",
    "stock_excess_bills_share": "Fraction",
    "cumulative_ati_baseline_bn": "USD bn",
    "headline_public_duration_supply": "USD bn",
    "qt_proxy": "USD bn",
    "buybacks_accepted": "USD bn",
    "delta_wdtgal": "USD bn",
    "DFF": "Percent",
    "debt_limit_dummy": "Indicator",
}

for horizon_bd in RELEASE_FLOW_HORIZONS_BD:
    window_label = release_flow_window_label(horizon_bd)
    control_term = release_flow_control_column(window_label)
    PREDICTOR_LABELS[control_term] = f"Change in DFF from release marker to +{horizon_bd} business days"
    PREDICTOR_UNITS[control_term] = "Percent"

for start_bd, end_bd in RELEASE_FLOW_PLACEBO_WINDOWS_BD:
    window_label = release_flow_placebo_label(start_bd, end_bd)
    control_term = release_flow_control_column(window_label)
    PREDICTOR_LABELS[control_term] = (
        f"Change in DFF over placebo window [{start_bd}, {end_bd}] business days relative to release"
    )
    PREDICTOR_UNITS[control_term] = "Percent"

NEWEY_WEST_MAXLAGS = 4
SCENARIO_SCALE_BN = 100.0
SCALED_100BN_TERMS = {"ati_baseline_bn", "stock_excess_bills_bn", "headline_public_duration_supply"}
TAU_GRID = (0.15, 0.18, 0.20)
DATE_CANDIDATES = ("date", "qra_release_date", "market_pricing_marker_minus_1d")

PRICING_SPEC_REGISTRY_COLUMNS = (
    "spec_id",
    "spec_family",
    "headline_flag",
    "anchor_role",
    "window_definition",
    "sample_start",
    "sample_end",
    "outcome",
    "predictor_set",
    "control_set",
    "frequency",
    "notes",
)

PRICING_REGRESSION_SUMMARY_COLUMNS = (
    "model_id",
    "model_family",
    "model_mode",
    "panel_key",
    "panel_frequency",
    "window_definition",
    "anchor_role",
    "dependent_variable",
    "dependent_label",
    "outcome_role",
    "term",
    "coef",
    "std_err",
    "t_stat",
    "p_value",
    "nobs",
    "effective_shock_count",
    "rsquared",
    "term_role",
    "term_label",
    "term_units",
    "outcome_units",
    "cov_type",
    "cov_maxlags",
    "term_mode",
    "sample_start",
    "sample_end",
    "notes",
)

PRICING_REGRESSION_ROBUSTNESS_COLUMNS = (
    "dependent_variable",
    "dependent_label",
    "model_id",
    "model_family",
    "variant_id",
    "variant_family",
    "panel_frequency",
    "window_definition",
    "term",
    "coef",
    "std_err",
    "t_stat",
    "p_value",
    "nobs",
    "effective_shock_count",
    "rsquared",
    "term_role",
    "term_label",
    "term_units",
    "outcome_units",
    "cov_type",
    "cov_maxlags",
    "term_mode",
    "model_mode",
    "sample_start",
    "sample_end",
    "notes",
)

PRICING_SUBSAMPLE_GRID_COLUMNS = (
    "spec_id",
    "spec_family",
    "variant_id",
    "variant_family",
    "frequency",
    "window_definition",
    "dependent_variable",
    "dependent_label",
    "outcome_role",
    "term",
    "term_label",
    "coef",
    "std_err",
    "t_stat",
    "p_value",
    "nobs",
    "effective_shock_count",
    "rsquared",
    "cov_type",
    "cov_maxlags",
    "sample_start",
    "sample_end",
    "notes",
)

SCENARIO_TRANSLATION_COLUMNS = (
    "scenario_id",
    "scenario_label",
    "scenario_role",
    "scenario_shock_bn",
    "scenario_shock_scale_bn",
    "model_id",
    "model_family",
    "dependent_variable",
    "dependent_label",
    "outcome_role",
    "term",
    "term_label",
    "coef_bp_per_100bn",
    "implied_bp_change",
    "nobs",
    "effective_shock_count",
    "p_value",
    "notes",
)

PRICING_RELEASE_FLOW_LEAVE_ONE_OUT_COLUMNS = (
    "spec_id",
    "window_definition",
    "dependent_variable",
    "dependent_label",
    "omitted_release_id",
    "coef",
    "std_err",
    "t_stat",
    "p_value",
    "nobs",
    "effective_shock_count",
    "sample_start",
    "sample_end",
    "notes",
)

PRICING_TAU_SENSITIVITY_GRID_COLUMNS = (
    "tau",
    "model_id",
    "model_family",
    "dependent_variable",
    "dependent_label",
    "term",
    "term_label",
    "coef",
    "std_err",
    "t_stat",
    "p_value",
    "nobs",
    "effective_shock_count",
    "rsquared",
    "sample_start",
    "sample_end",
    "notes",
)

def _release_flow_baseline_specs() -> tuple[dict[str, object], ...]:
    specs: list[dict[str, object]] = []
    primary_horizon = max(RELEASE_FLOW_HORIZONS_BD)
    for horizon_bd in RELEASE_FLOW_HORIZONS_BD:
        window_label = release_flow_window_label(horizon_bd)
        specs.append(
            {
                "spec_id": (
                    f"release_flow_baseline_{horizon_bd}bd"
                    if horizon_bd == primary_horizon
                    else f"release_flow_horizon_{horizon_bd}bd"
                ),
                "spec_family": "release_flow",
                "headline_flag": True,
                "anchor_role": "credibility_anchor" if horizon_bd == primary_horizon else "supporting",
                "window_definition": window_label,
                "panel_key": RELEASE_FLOW_PANEL,
                "panel_frequency": "release-event",
                "predictor_terms": ("ati_baseline_bn",),
                "control_terms": (release_flow_control_column(window_label), "debt_limit_dummy"),
                "dependent_by_outcome": {
                    "THREEFYTP10": release_flow_delta_column("THREEFYTP10", window_label),
                    "DGS10": release_flow_delta_column("DGS10", window_label),
                    "DGS30": release_flow_delta_column("DGS30", window_label),
                },
                "sample_start": "2009-01-01",
                "notes": (
                    f"Unique-release fixed-horizon Maturity-Tilt Flow specification from the pre-release pricing marker "
                    f"to +{horizon_bd} business days after the official release."
                ),
            }
        )
    return tuple(specs)


BASELINE_SPECS = (
    *_release_flow_baseline_specs(),
    {
        "spec_id": "monthly_flow_baseline",
        "spec_family": "monthly_flow",
        "headline_flag": True,
        "anchor_role": "headline_context",
        "window_definition": "carry_forward_monthly",
        "panel_key": OFFICIAL_ATI_PRICE_PANEL,
        "panel_frequency": "monthly",
        "predictor_terms": ("ati_baseline_bn",),
        "control_terms": ("DFF", "debt_limit_dummy"),
        "sample_start": "2009-01-01",
        "notes": "Monthly carry-forward Maturity-Tilt Flow specification retained as side-by-side headline context, not the main credibility anchor.",
    },
    {
        "spec_id": "monthly_stock_baseline",
        "spec_family": "monthly_stock",
        "headline_flag": True,
        "anchor_role": "supporting",
        "window_definition": "carry_forward_monthly",
        "panel_key": OFFICIAL_ATI_PRICE_PANEL,
        "panel_frequency": "monthly",
        "predictor_terms": ("stock_excess_bills_bn",),
        "control_terms": ("DFF", "debt_limit_dummy"),
        "sample_start": "2009-01-01",
        "notes": "Monthly stock specification using Excess Bills Stock after policy-rate and debt-limit controls.",
    },
    {
        "spec_id": "weekly_duration_baseline",
        "spec_family": "weekly_duration",
        "headline_flag": True,
        "anchor_role": "supporting",
        "window_definition": "weekly_duration_window",
        "panel_key": WEEKLY_SUPPLY_PANEL,
        "panel_frequency": "weekly (W-WED)",
        "predictor_terms": ("headline_public_duration_supply",),
        "control_terms": ("qt_proxy", "buybacks_accepted", "delta_wdtgal", "DFF"),
        "sample_start": None,
        "notes": "Weekly reduced-form specification using Public Duration Supply with QT, buybacks, TGA, and policy-rate controls.",
    },
)


def _release_flow_placebo_specs() -> tuple[dict[str, object], ...]:
    specs: list[dict[str, object]] = []
    for start_bd, end_bd in RELEASE_FLOW_PLACEBO_WINDOWS_BD:
        window_label = release_flow_placebo_label(start_bd, end_bd)
        specs.append(
            {
                "variant_id": window_label,
                "variant_family": "release_flow_placebo",
                "spec_id": f"release_flow_placebo_{abs(start_bd)}bd_to_{abs(end_bd)}bd",
                "spec_family": "release_flow_placebo",
                "panel_key": RELEASE_FLOW_PANEL,
                "panel_frequency": "release-event",
                "window_definition": window_label,
                "predictor_terms": ("ati_baseline_bn",),
                "control_terms": (release_flow_control_column(window_label), "debt_limit_dummy"),
                "dependent_by_outcome": {
                    "THREEFYTP10": release_flow_delta_column("THREEFYTP10", window_label),
                    "DGS10": release_flow_delta_column("DGS10", window_label),
                    "DGS30": release_flow_delta_column("DGS30", window_label),
                },
                "sample_start": "2009-01-01",
                "notes": (
                    f"Pre-release placebo specification over window [{start_bd}, {end_bd}] business days relative to the official release."
                ),
            }
        )
    return tuple(specs)

ROBUSTNESS_SPECS = (
    {
        "variant_id": "flow_vs_stock_horse_race",
        "variant_family": "flow_vs_stock_horse_race",
        "spec_id": "monthly_flow_vs_stock_horse_race",
        "spec_family": "monthly_flow_vs_stock",
        "panel_key": OFFICIAL_ATI_PRICE_PANEL,
        "panel_frequency": "monthly",
        "window_definition": "carry_forward_monthly",
        "predictor_terms": ("ati_baseline_bn", "stock_excess_bills_bn"),
        "control_terms": ("DFF", "debt_limit_dummy"),
        "sample_start": "2009-01-01",
        "notes": "Monthly horse-race specification with Maturity-Tilt Flow and Excess Bills Stock together.",
    },
    {
        "variant_id": "standardized_predictors",
        "variant_family": "standardized_predictors",
        "spec_id": "monthly_flow_baseline_standardized",
        "spec_family": "monthly_flow",
        "panel_key": OFFICIAL_ATI_PRICE_PANEL,
        "panel_frequency": "monthly",
        "window_definition": "carry_forward_monthly",
        "predictor_terms": ("ati_baseline_bn",),
        "control_terms": ("DFF", "debt_limit_dummy"),
        "sample_start": "2009-01-01",
        "standardize_predictors": True,
        "notes": "Monthly flow baseline with the primary predictor standardized to one standard deviation.",
    },
    {
        "variant_id": "standardized_predictors",
        "variant_family": "standardized_predictors",
        "spec_id": "monthly_stock_baseline_standardized",
        "spec_family": "monthly_stock",
        "panel_key": OFFICIAL_ATI_PRICE_PANEL,
        "panel_frequency": "monthly",
        "window_definition": "carry_forward_monthly",
        "predictor_terms": ("stock_excess_bills_bn",),
        "control_terms": ("DFF", "debt_limit_dummy"),
        "sample_start": "2009-01-01",
        "standardize_predictors": True,
        "notes": "Monthly stock baseline with the primary predictor standardized to one standard deviation.",
    },
    {
        "variant_id": "standardized_predictors",
        "variant_family": "standardized_predictors",
        "spec_id": "weekly_duration_baseline_standardized",
        "spec_family": "weekly_duration",
        "panel_key": WEEKLY_SUPPLY_PANEL,
        "panel_frequency": "weekly (W-WED)",
        "window_definition": "weekly_duration_window",
        "predictor_terms": ("headline_public_duration_supply",),
        "control_terms": ("qt_proxy", "buybacks_accepted", "delta_wdtgal", "DFF"),
        "sample_start": None,
        "standardize_predictors": True,
        "notes": "Weekly duration baseline with Public Duration Supply standardized to one standard deviation.",
    },
    *_release_flow_placebo_specs(),
)

SUBSAMPLE_VARIANTS = (
    {"variant_id": "post_2009", "variant_family": "post_2009", "sample_start": "2009-01-01", "exclude_debt_limit": False},
    {"variant_id": "post_2014", "variant_family": "post_2014", "sample_start": "2014-01-01", "exclude_debt_limit": False},
    {"variant_id": "post_2020", "variant_family": "post_2020", "sample_start": "2020-01-01", "exclude_debt_limit": False},
    {"variant_id": "exclude_debt_limit", "variant_family": "exclude_debt_limit", "sample_start": None, "exclude_debt_limit": True},
)

SCENARIO_DEFINITIONS = (
    {
        "scenario_id": "plus_100bn_duration_supply",
        "scenario_label": "Plus $100bn Public Duration Supply shock",
        "scenario_role": "supporting",
        "scenario_shock_bn": 100.0,
        "model_id": "weekly_duration_baseline",
        "term": "headline_public_duration_supply",
    },
    {
        "scenario_id": "plus_500bn_term_out",
        "scenario_label": "Plus $500bn term-out translation via Excess Bills Stock",
        "scenario_role": "illustrative_only",
        "scenario_shock_bn": 500.0,
        "model_id": "monthly_stock_baseline",
        "term": "stock_excess_bills_bn",
    },
    {
        "scenario_id": "plus_1000bn_term_out",
        "scenario_label": "Plus $1000bn term-out translation via Excess Bills Stock",
        "scenario_role": "illustrative_only",
        "scenario_shock_bn": 1000.0,
        "model_id": "monthly_stock_baseline",
        "term": "stock_excess_bills_bn",
    },
)


def _as_float_series(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    out = frame.copy()
    for column in out.columns:
        if column in DATE_CANDIDATES or column.endswith("_date"):
            out[column] = pd.to_datetime(out[column], errors="coerce")
    for column in columns:
        if column in out.columns and column not in DATE_CANDIDATES and not column.endswith("_date"):
            out[column] = pd.to_numeric(out[column], errors="coerce")
    return out


def _ensure_required_columns(
    panel: pd.DataFrame,
    columns: Sequence[str],
    optional_defaults: Mapping[str, float] | None = None,
) -> pd.DataFrame:
    optional_defaults = optional_defaults or {}
    out = panel.copy()
    for column in columns:
        if column in out.columns:
            continue
        if column in optional_defaults:
            out[column] = float(optional_defaults[column])
        else:
            return pd.DataFrame(columns=panel.columns.tolist())
    return out


def _panel_date_column(panel: pd.DataFrame) -> str | None:
    for column in DATE_CANDIDATES:
        if column in panel.columns:
            return column
    return None


def _build_sample_bounds(panel: pd.DataFrame) -> tuple[str | None, str | None]:
    date_col = _panel_date_column(panel)
    if not date_col:
        return None, None
    dates = pd.to_datetime(panel[date_col], errors="coerce")
    if dates.empty or dates.notna().sum() == 0:
        return None, None
    return dates.min().strftime("%Y-%m-%d"), dates.max().strftime("%Y-%m-%d")


def _effective_shock_count(panel: pd.DataFrame) -> int:
    if "release_id" in panel.columns:
        release_count = int(panel["release_id"].dropna().astype(str).nunique())
        if "market_pricing_marker_minus_1d" in panel.columns:
            marker_count = int(pd.to_datetime(panel["market_pricing_marker_minus_1d"], errors="coerce").dropna().nunique())
            if marker_count != release_count:
                raise ValueError(
                    "Release-level pricing panels require effective_shock_count to equal unique "
                    f"market-pricing markers. release_ids={release_count} markers={marker_count}"
                )
            return marker_count
        return release_count
    date_col = _panel_date_column(panel)
    if date_col:
        return int(pd.to_datetime(panel[date_col], errors="coerce").dropna().nunique())
    return int(len(panel))


def implied_bp_change(coef: float, shock_bn: float, shock_scale_bn: float = SCENARIO_SCALE_BN) -> float:
    return float(coef) * (float(shock_bn) / float(shock_scale_bn))


def _dependent_column(spec: Mapping[str, object], outcome: str) -> str:
    dependent_map = spec.get("dependent_by_outcome")
    if isinstance(dependent_map, Mapping):
        return str(dependent_map.get(outcome, outcome))
    return outcome


def _regression_sample(panel: pd.DataFrame, dependent_variable: str, x_vars: Sequence[str]) -> pd.DataFrame:
    keep = [dependent_variable, *x_vars]
    date_col = _panel_date_column(panel)
    if date_col:
        keep = [date_col, *keep]
    if "release_id" in panel.columns:
        keep = [*keep, "release_id"]
    keep = [column for column in keep if column in panel.columns]
    reg_data = panel.loc[:, keep].dropna().copy()
    if dependent_variable not in reg_data.columns:
        return pd.DataFrame(columns=keep)
    return reg_data


def run_hac_regression(
    panel: pd.DataFrame,
    dependent_variable: str,
    x_vars: Sequence[str],
    *,
    maxlags: int = NEWEY_WEST_MAXLAGS,
) -> tuple[pd.DataFrame, str | None, str | None, int]:
    reg_data = _regression_sample(panel, dependent_variable, x_vars)
    if reg_data.empty or len(reg_data) < max(8, len(x_vars) + 3):
        return (
            pd.DataFrame(
                columns=["dependent_variable", "term", "coef", "std_err", "t_stat", "p_value", "nobs", "rsquared", "cov_type", "cov_maxlags"]
            ),
            None,
            None,
            0,
        )

    y = reg_data[dependent_variable]
    X = sm.add_constant(reg_data[list(x_vars)], prepend=True)
    result = sm.OLS(y, X).fit(cov_type="HAC", cov_kwds={"maxlags": int(maxlags)})
    out = pd.DataFrame(
        {
            "dependent_variable": dependent_variable,
            "term": result.params.index,
            "coef": result.params.values,
            "std_err": result.bse.values,
            "t_stat": result.tvalues.values,
            "p_value": result.pvalues.values,
            "nobs": result.nobs,
            "rsquared": result.rsquared,
            "cov_type": [result.cov_type] * len(result.params),
            "cov_maxlags": [int(maxlags)] * len(result.params),
        }
    )
    sample_start, sample_end = _build_sample_bounds(reg_data)
    return out, sample_start, sample_end, _effective_shock_count(reg_data)


def _resolve_sample_start(spec: Mapping[str, object], variant_start: str | None = None) -> str | None:
    values = [value for value in (spec.get("sample_start"), variant_start) if value]
    if not values:
        return None
    return max(values)


def _prepare_regression_panel(
    panel: pd.DataFrame,
    spec: Mapping[str, object],
    outcome: str,
    *,
    variant_start: str | None = None,
    exclude_debt_limit: bool = False,
    standardize_predictors: bool = False,
) -> tuple[pd.DataFrame, str]:
    dependent_variable = _dependent_column(spec, outcome)
    predictors = list(spec["predictor_terms"])
    controls = list(spec["control_terms"])
    required = [dependent_variable, *predictors, *controls]
    filtered = _ensure_required_columns(
        _as_float_series(panel, required),
        required,
        optional_defaults={"debt_limit_dummy": 0.0},
    )
    if filtered.empty:
        return filtered, dependent_variable

    date_col = _panel_date_column(filtered)
    if date_col:
        filtered = filtered.dropna(subset=[date_col]).sort_values(date_col).reset_index(drop=True)
        sample_start = _resolve_sample_start(spec, variant_start)
        if sample_start is not None:
            filtered = filtered.loc[pd.to_datetime(filtered[date_col], errors="coerce") >= pd.Timestamp(sample_start)].copy()
    if exclude_debt_limit and "debt_limit_dummy" in filtered.columns:
        filtered = filtered.loc[filtered["debt_limit_dummy"].fillna(0) != 1].copy()
    if filtered.empty:
        return filtered, dependent_variable

    for term in predictors:
        series = pd.to_numeric(filtered[term], errors="coerce")
        if standardize_predictors:
            std = float(series.std(ddof=0))
            filtered[term] = 0.0 if std <= 0 else (series - float(series.mean())) / std
        elif term in SCALED_100BN_TERMS:
            filtered[term] = series / SCENARIO_SCALE_BN
        else:
            filtered[term] = series
    return filtered, dependent_variable


def _term_role(term: str, predictors: Sequence[str], controls: Sequence[str]) -> str:
    if term == "const":
        return "intercept"
    if term in predictors:
        return "primary_predictor"
    if term in controls:
        return "control"
    return "other"


def _term_label(term: str, *, standardized_predictors: bool, predictors: Sequence[str]) -> str:
    label = PREDICTOR_LABELS.get(term, term)
    if standardized_predictors and term in predictors:
        return f"{label} (1 SD)"
    return label


def _term_units(term: str, *, standardized_predictors: bool, predictors: Sequence[str]) -> str:
    if standardized_predictors and term in predictors:
        return "standard deviations"
    if term in predictors and term in SCALED_100BN_TERMS:
        return "USD 100bn"
    return PREDICTOR_UNITS.get(term, "")


def _run_spec_rows(
    spec: Mapping[str, object],
    pricing_panels: Mapping[str, pd.DataFrame],
    outcomes: Sequence[str],
    *,
    model_mode: str,
    term_mode: str,
    variant_start: str | None = None,
    exclude_debt_limit: bool = False,
    standardize_predictors: bool = False,
) -> pd.DataFrame:
    panel = pricing_panels.get(str(spec["panel_key"]))
    if panel is None or panel.empty:
        return pd.DataFrame(columns=PRICING_REGRESSION_SUMMARY_COLUMNS)

    rows: list[pd.DataFrame] = []
    predictors = list(spec["predictor_terms"])
    controls = list(spec["control_terms"])
    for outcome in outcomes:
        prepared, dependent_variable = _prepare_regression_panel(
            panel,
            spec,
            outcome,
            variant_start=variant_start,
            exclude_debt_limit=exclude_debt_limit,
            standardize_predictors=standardize_predictors,
        )
        if prepared.empty:
            continue
        reg, sample_start, sample_end, effective_shock_count = run_hac_regression(
            prepared,
            dependent_variable,
            [*predictors, *controls],
        )
        if reg.empty:
            continue
        reg = reg.copy()
        reg["model_id"] = spec["spec_id"]
        reg["model_family"] = spec["spec_family"]
        reg["model_mode"] = model_mode
        reg["panel_key"] = spec["panel_key"]
        reg["panel_frequency"] = spec["panel_frequency"]
        reg["window_definition"] = spec["window_definition"]
        reg["anchor_role"] = spec.get("anchor_role", "supporting")
        reg["dependent_variable"] = outcome
        reg["dependent_label"] = OUTCOME_LABELS.get(outcome, outcome)
        reg["outcome_role"] = "headline" if outcome in HEADLINE_OUTCOMES else "supporting"
        reg["term_role"] = reg["term"].map(lambda term: _term_role(str(term), predictors, controls))
        reg["term_label"] = reg["term"].map(
            lambda term: _term_label(str(term), standardized_predictors=standardize_predictors, predictors=predictors)
        )
        reg["term_units"] = reg["term"].map(
            lambda term: _term_units(str(term), standardized_predictors=standardize_predictors, predictors=predictors)
        )
        reg["outcome_units"] = "basis points"
        reg["term_mode"] = term_mode
        reg["sample_start"] = sample_start
        reg["sample_end"] = sample_end
        reg["effective_shock_count"] = effective_shock_count
        reg["notes"] = spec["notes"]
        rows.append(reg[list(PRICING_REGRESSION_SUMMARY_COLUMNS)])

    if not rows:
        return pd.DataFrame(columns=PRICING_REGRESSION_SUMMARY_COLUMNS)
    return pd.concat(rows, ignore_index=True)


def _baseline_spec(spec_id: str) -> Mapping[str, object]:
    for spec in BASELINE_SPECS:
        if spec["spec_id"] == spec_id:
            return spec
    raise KeyError(spec_id)


def build_pricing_spec_registry(pricing_panels: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for spec in BASELINE_SPECS:
        panel = pricing_panels.get(str(spec["panel_key"]))
        if panel is None or panel.empty:
            continue
        for outcome in HEADLINE_OUTCOMES:
            prepared, dependent_variable = _prepare_regression_panel(panel, spec, outcome)
            if prepared.empty:
                continue
            sample = _regression_sample(prepared, dependent_variable, [*spec["predictor_terms"], *spec["control_terms"]])
            sample_start, sample_end = _build_sample_bounds(sample)
            rows.append(
                {
                    "spec_id": spec["spec_id"],
                    "spec_family": spec["spec_family"],
                    "headline_flag": bool(spec["headline_flag"]),
                    "anchor_role": spec["anchor_role"],
                    "window_definition": spec["window_definition"],
                    "sample_start": sample_start,
                    "sample_end": sample_end,
                    "outcome": outcome,
                    "predictor_set": "|".join(spec["predictor_terms"]),
                    "control_set": "|".join(spec["control_terms"]),
                    "frequency": spec["panel_frequency"],
                    "notes": spec["notes"],
                }
            )
    return pd.DataFrame(rows, columns=PRICING_SPEC_REGISTRY_COLUMNS)


def build_pricing_regression_summary(pricing_panels: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    rows = [
        _run_spec_rows(
            spec,
            pricing_panels,
            HEADLINE_OUTCOMES,
            model_mode="headline_baseline",
            term_mode="baseline",
        )
        for spec in BASELINE_SPECS
    ]
    rows = [row for row in rows if not row.empty]
    if not rows:
        return pd.DataFrame(columns=PRICING_REGRESSION_SUMMARY_COLUMNS)
    return pd.concat(rows, ignore_index=True)


def build_pricing_regression_robustness(pricing_panels: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []

    for spec in BASELINE_SPECS:
        support_rows = _run_spec_rows(
            spec,
            pricing_panels,
            (SUPPORTING_PRICING_OUTCOME,),
            model_mode="supporting_outcome",
            term_mode="supporting_outcome_dgs30",
        )
        if not support_rows.empty:
            support_rows = support_rows.loc[support_rows["term_role"] == "primary_predictor"].copy()
            if not support_rows.empty:
                support_rows["variant_id"] = "supporting_outcome_dgs30"
                support_rows["variant_family"] = "supporting_outcome_dgs30"
                rows.append(support_rows)

    for spec in ROBUSTNESS_SPECS:
        robust_rows = _run_spec_rows(
            spec,
            pricing_panels,
            (*HEADLINE_OUTCOMES, SUPPORTING_PRICING_OUTCOME),
            model_mode="robustness",
            term_mode=str(spec["variant_family"]),
            standardize_predictors=bool(spec.get("standardize_predictors", False)),
        )
        if robust_rows.empty:
            continue
        robust_rows = robust_rows.loc[robust_rows["term_role"] == "primary_predictor"].copy()
        if robust_rows.empty:
            continue
        robust_rows["variant_id"] = spec["variant_id"]
        robust_rows["variant_family"] = spec["variant_family"]
        rows.append(robust_rows)

    if not rows:
        return pd.DataFrame(columns=PRICING_REGRESSION_ROBUSTNESS_COLUMNS)
    combined = pd.concat(rows, ignore_index=True)
    return combined[list(PRICING_REGRESSION_ROBUSTNESS_COLUMNS)].copy()


def build_pricing_subsample_grid(pricing_panels: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for spec in BASELINE_SPECS:
        for variant in SUBSAMPLE_VARIANTS:
            sample_rows = _run_spec_rows(
                spec,
                pricing_panels,
                (*HEADLINE_OUTCOMES, SUPPORTING_PRICING_OUTCOME),
                model_mode="subsample",
                term_mode=str(variant["variant_family"]),
                variant_start=variant["sample_start"],
                exclude_debt_limit=bool(variant["exclude_debt_limit"]),
            )
            if sample_rows.empty:
                continue
            sample_rows = sample_rows.loc[sample_rows["term_role"] == "primary_predictor"].copy()
            if sample_rows.empty:
                continue
            sample_rows["spec_id"] = spec["spec_id"]
            sample_rows["spec_family"] = spec["spec_family"]
            sample_rows["variant_id"] = variant["variant_id"]
            sample_rows["variant_family"] = variant["variant_family"]
            sample_rows["frequency"] = spec["panel_frequency"]
            rows.append(sample_rows)

    if not rows:
        return pd.DataFrame(columns=PRICING_SUBSAMPLE_GRID_COLUMNS)
    combined = pd.concat(rows, ignore_index=True)
    return combined[list(PRICING_SUBSAMPLE_GRID_COLUMNS)].copy()


def build_pricing_release_flow_leave_one_out(pricing_panels: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    panel = pricing_panels.get(RELEASE_FLOW_PANEL)
    spec = _baseline_spec(f"release_flow_baseline_{max(RELEASE_FLOW_HORIZONS_BD)}bd")
    if panel is None or panel.empty or "release_id" not in panel.columns:
        return pd.DataFrame(columns=PRICING_RELEASE_FLOW_LEAVE_ONE_OUT_COLUMNS)

    rows: list[dict[str, object]] = []
    for outcome in HEADLINE_OUTCOMES:
        prepared, dependent_variable = _prepare_regression_panel(panel, spec, outcome)
        if prepared.empty or "release_id" not in prepared.columns:
            continue
        release_ids = [str(value) for value in prepared["release_id"].dropna().astype(str).unique()]
        for omitted_release_id in release_ids:
            reduced = prepared.loc[prepared["release_id"].astype(str) != omitted_release_id].copy()
            reg, sample_start, sample_end, effective_shock_count = run_hac_regression(
                reduced,
                dependent_variable,
                [*spec["predictor_terms"], *spec["control_terms"]],
            )
            if reg.empty:
                continue
            primary = reg.loc[reg["term"] == "ati_baseline_bn"]
            if primary.empty:
                continue
            row = primary.iloc[0]
            rows.append(
                {
                    "spec_id": spec["spec_id"],
                    "window_definition": spec["window_definition"],
                    "dependent_variable": outcome,
                    "dependent_label": OUTCOME_LABELS.get(outcome, outcome),
                    "omitted_release_id": omitted_release_id,
                    "coef": float(row["coef"]),
                    "std_err": float(row["std_err"]),
                    "t_stat": float(row["t_stat"]),
                    "p_value": float(row["p_value"]),
                    "nobs": int(row["nobs"]),
                    "effective_shock_count": effective_shock_count,
                    "sample_start": sample_start,
                    "sample_end": sample_end,
                    "notes": f"Leave-one-release-out diagnostic for the +{max(RELEASE_FLOW_HORIZONS_BD)} business-day Maturity-Tilt Flow anchor.",
                }
            )
    return pd.DataFrame(rows, columns=PRICING_RELEASE_FLOW_LEAVE_ONE_OUT_COLUMNS)


def build_pricing_tau_sensitivity_grid(pricing_panels: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    panel = pricing_panels.get(OFFICIAL_ATI_PRICE_PANEL)
    if panel is None or panel.empty:
        return pd.DataFrame(columns=PRICING_TAU_SENSITIVITY_GRID_COLUMNS)
    if "marketable_bill_share" not in panel.columns or "marketable_outstanding_bn" not in panel.columns:
        return pd.DataFrame(columns=PRICING_TAU_SENSITIVITY_GRID_COLUMNS)

    rows: list[dict[str, object]] = []
    spec = _baseline_spec("monthly_stock_baseline")
    for tau in TAU_GRID:
        tau_panel = panel.copy()
        tau_panel["stock_excess_bills_share"] = pd.to_numeric(tau_panel["marketable_bill_share"], errors="coerce") - float(tau)
        tau_panel["stock_excess_bills_bn"] = tau_panel["stock_excess_bills_share"] * pd.to_numeric(
            tau_panel["marketable_outstanding_bn"],
            errors="coerce",
        )
        for outcome in (*HEADLINE_OUTCOMES, SUPPORTING_PRICING_OUTCOME):
            prepared, dependent_variable = _prepare_regression_panel(tau_panel, spec, outcome)
            if prepared.empty:
                continue
            reg, sample_start, sample_end, effective_shock_count = run_hac_regression(
                prepared,
                dependent_variable,
                [*spec["predictor_terms"], *spec["control_terms"]],
            )
            if reg.empty:
                continue
            primary = reg.loc[reg["term"] == "stock_excess_bills_bn"]
            if primary.empty:
                continue
            row = primary.iloc[0]
            rows.append(
                {
                    "tau": tau,
                    "model_id": f"monthly_stock_tau_{int(round(tau * 100)):02d}",
                    "model_family": "monthly_stock_tau_sensitivity",
                    "dependent_variable": outcome,
                    "dependent_label": OUTCOME_LABELS.get(outcome, outcome),
                    "term": "stock_excess_bills_bn",
                    "term_label": PREDICTOR_LABELS["stock_excess_bills_bn"],
                    "coef": float(row["coef"]),
                    "std_err": float(row["std_err"]),
                    "t_stat": float(row["t_stat"]),
                    "p_value": float(row["p_value"]),
                    "nobs": int(row["nobs"]),
                    "effective_shock_count": effective_shock_count,
                    "rsquared": float(row["rsquared"]),
                    "sample_start": sample_start,
                    "sample_end": sample_end,
                    "notes": f"Monthly stock-only pricing sensitivity using target tau={tau:.2f}.",
                }
            )
    return pd.DataFrame(rows, columns=PRICING_TAU_SENSITIVITY_GRID_COLUMNS)


def build_pricing_scenario_translation(
    regression_summary: pd.DataFrame,
    scenario_definitions: Sequence[Mapping[str, object]] = SCENARIO_DEFINITIONS,
    shock_scale_bn: float = SCENARIO_SCALE_BN,
) -> pd.DataFrame:
    if regression_summary.empty:
        return pd.DataFrame(columns=SCENARIO_TRANSLATION_COLUMNS)

    rows: list[dict[str, object]] = []
    for scenario in scenario_definitions:
        model_id = str(scenario["model_id"])
        scenario_term = str(scenario["term"])
        scenario_shock = float(scenario["scenario_shock_bn"])
        for outcome in HEADLINE_OUTCOMES:
            rows_candidates = regression_summary.loc[
                (regression_summary["model_id"] == model_id)
                & (regression_summary["term"] == scenario_term)
                & (regression_summary["dependent_variable"] == outcome)
            ]
            if rows_candidates.empty:
                continue
            row = rows_candidates.iloc[0]
            coef = float(row["coef"])
            rows.append(
                {
                    "scenario_id": scenario["scenario_id"],
                    "scenario_label": scenario["scenario_label"],
                    "scenario_role": scenario.get("scenario_role", "supporting"),
                    "scenario_shock_bn": scenario_shock,
                    "scenario_shock_scale_bn": float(shock_scale_bn),
                    "model_id": model_id,
                    "model_family": row["model_family"],
                    "dependent_variable": outcome,
                    "dependent_label": row["dependent_label"],
                    "outcome_role": row["outcome_role"],
                    "term": scenario_term,
                    "term_label": PREDICTOR_LABELS.get(scenario_term, scenario_term),
                    "coef_bp_per_100bn": coef,
                    "implied_bp_change": implied_bp_change(coef, scenario_shock, shock_scale_bn),
                    "nobs": int(row["nobs"]),
                    "effective_shock_count": int(row.get("effective_shock_count", row["nobs"])),
                    "p_value": float(row["p_value"]),
                    "notes": row["notes"],
                }
            )

    return pd.DataFrame(rows, columns=SCENARIO_TRANSLATION_COLUMNS)
