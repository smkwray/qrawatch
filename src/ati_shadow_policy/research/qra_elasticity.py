from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd
import re

from .qra_classification import (
    SUMMARY_HEADLINE_BUCKETS,
    coerce_shock_sign,
    normalize_lower,
    normalize_text,
)
from .qra_schedule_diff import build_qra_schedule_shock_summary

DEFAULT_METRICS = (
    "DGS10_d1",
    "DGS10_d3",
    "DGS30_d1",
    "DGS30_d3",
    "THREEFYTP10_d1",
    "THREEFYTP10_d3",
    "slope_10y_2y_d1",
    "slope_10y_2y_d3",
)
SHOCK_TEMPLATE_KEYS = ("event_id", "event_date_type")
SPEC_DURATION_TREATMENT_V1 = "spec_duration_treatment_v1"
SPEC_QRA_EVENT_V2 = "spec_qra_event_v2"
CANONICAL_TREATMENT_VARIANT = "canonical_shock_bn"
FIXED_10Y_EQ_TREATMENT_VARIANT = "fixed_10y_eq_bn"
DYNAMIC_10Y_EQ_TREATMENT_VARIANT = "dynamic_10y_eq_bn"
DV01_TREATMENT_VARIANT = "dv01_usd"
DV01_SCALE_USD = 1_000_000.0
_TREATMENT_VARIANTS = (
    (CANONICAL_TREATMENT_VARIANT, "shock_bn", "USD billions", "bp_per_100bn", 100.0),
    (FIXED_10Y_EQ_TREATMENT_VARIANT, "schedule_diff_10y_eq_bn", "USD billions", "bp_per_100bn", 100.0),
    (DYNAMIC_10Y_EQ_TREATMENT_VARIANT, "schedule_diff_dynamic_10y_eq_bn", "USD billions", "bp_per_100bn", 100.0),
    (DV01_TREATMENT_VARIANT, "schedule_diff_dv01_usd", "USD", "bp_per_1mm_dv01", DV01_SCALE_USD),
)
_TREATMENT_VARIANT_ORDER = {variant[0]: idx for idx, variant in enumerate(_TREATMENT_VARIANTS)}
SHOCK_TEMPLATE_MANUAL_COLUMNS = (
    "shock_bn",
    "shock_source",
    "shock_notes",
    "shock_review_status",
)
SHOCK_TEMPLATE_DERIVED_COLUMNS = (
    "previous_event_id",
    "previous_quarter",
    "gross_notional_delta_bn",
    "schedule_diff_10y_eq_bn",
    "schedule_diff_dynamic_10y_eq_bn",
    "schedule_diff_dv01_usd",
    "shock_construction",
)
_MONTH_NAME_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\b",
    flags=re.IGNORECASE,
)
_NO_CHANGE_PATTERNS = (
    "does not anticipate needing to increase nominal coupon or frn auction sizes",
    "does not anticipate needing to make any further increases in nominal coupon or frn auction sizes",
    "anticipates maintaining nominal coupon and frn auction sizes",
    "anticipates maintaining nominal coupon and frn auction sizes for at least the next several quarters",
    "believes its current auction sizes leave it well positioned",
    "did not anticipate needing to increase nominal coupon or frn auction sizes",
    "keep nominal coupon and frn auction sizes unchanged",
    "keep auction sizes unchanged",
    "keep its nominal coupon and frn auction sizes unchanged",
    "keep nominal coupon auction sizes unchanged",
    "frn sizes unchanged",
)
_TENOR_10Y_EQ_WEIGHTS = {
    "2Y": 0.2,
    "3Y": 0.3,
    "5Y": 0.5,
    "7Y": 0.7,
    "10Y": 1.0,
    "20Y": 2.0,
    "30Y": 3.0,
    "2Y_FRN": 0.05,
}

_NEGATIVE_CHANGE_VERBS = {"reduce", "decrease", "cut"}
_POSITIVE_CHANGE_VERBS = {"increase", "raise"}
_REVIEW_DATE_RE = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _last_non_missing(values: pd.Series) -> object:
    for value in reversed(values.tolist()):
        if not _is_missing(value):
            return value
    return np.nan


def _dedupe_on_keys(
    frame: pd.DataFrame,
    key_columns: Iterable[str] = SHOCK_TEMPLATE_KEYS,
) -> pd.DataFrame:
    keys = list(key_columns)
    missing = [column for column in keys if column not in frame.columns]
    if missing:
        raise KeyError(f"Missing key column(s): {missing}")
    if frame.empty:
        return frame.copy()

    rows = []
    for _, group in frame.groupby(keys, sort=False, dropna=False):
        row: dict[str, object] = {}
        first = group.iloc[0]
        for column in group.columns:
            if column in keys:
                row[column] = first[column]
            else:
                row[column] = _last_non_missing(group[column])
        rows.append(row)
    return pd.DataFrame(rows)


def _first_non_empty_value(row: pd.Series, columns: Iterable[str]) -> object:
    for column in columns:
        if column not in row.index:
            continue
        value = row[column]
        if not _is_missing(value):
            return value
    return pd.NA


def _comparison_family_for_variant(treatment_variant: object) -> str:
    if normalize_lower(treatment_variant) == DV01_TREATMENT_VARIANT:
        return "dv01_equivalent"
    return "bp_per_100bn"


def _comparison_family_reference_variant(comparison_family: str) -> str:
    if comparison_family == "dv01_equivalent":
        return DV01_TREATMENT_VARIANT
    return CANONICAL_TREATMENT_VARIANT


def _comparison_family_label(comparison_family: str) -> str:
    if comparison_family == "dv01_equivalent":
        return "DV01-equivalent family"
    return "bp-per-100bn family"


def _headline_eligibility_reason(
    event_date_type: object,
    treatment_variant: str,
    shock_missing: bool,
    small_denominator: bool,
    headline_bucket: str,
    classification_review_status: str,
    shock_review_status: str,
) -> str:
    if treatment_variant != CANONICAL_TREATMENT_VARIANT:
        return "non_canonical_treatment_variant"
    if str(event_date_type) != "official_release_date":
        return "non_official_event_date_type"
    if shock_missing:
        return "shock_missing"
    if small_denominator:
        return "small_denominator"
    if headline_bucket not in SUMMARY_HEADLINE_BUCKETS:
        return "non_headline_bucket"
    if classification_review_status != "reviewed":
        return "classification_not_reviewed"
    if shock_review_status != "reviewed":
        return "shock_not_reviewed"
    return "usable"


def _small_denominator_flag(
    treatment_variant: str,
    shock_value: float,
    small_denominator_threshold_bn: float,
) -> bool:
    if treatment_variant == DV01_TREATMENT_VARIANT:
        threshold_dv01 = float(small_denominator_threshold_bn) * 100000.0
        return abs(float(shock_value)) < threshold_dv01
    return abs(float(shock_value)) < float(small_denominator_threshold_bn)


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _overlap_severity_from_row(row: pd.Series) -> str:
    direct = normalize_text(row.get("overlap_severity"))
    if direct:
        return direct
    overlap_flag = _as_bool(row.get("overlap_flag"))
    if not overlap_flag:
        return "none"
    overlap_label = normalize_text(row.get("overlap_label")).lower()
    if overlap_label:
        return "high"
    return "low"


def _reviewer_from_status(value: object) -> str:
    status = normalize_lower(value)
    if status == "reviewed":
        return "manual_review"
    if status == "provisional":
        return "provisional_review"
    return ""


def _review_date_from_text(value: object) -> object:
    text = normalize_text(value)
    match = _REVIEW_DATE_RE.search(text)
    if match is None:
        return pd.NA
    return match.group(1)


def _review_date_from_row(row: pd.Series) -> object:
    for column in ("review_date", "classification_review_date", "shock_review_date"):
        value = row.get(column, pd.NA)
        if not _is_missing(value):
            return value
    for column in ("notes", "review_notes", "shock_notes"):
        value = _review_date_from_text(row.get(column, pd.NA))
        if not _is_missing(value):
            return value
    return pd.NA


def _merge_overlap_overrides(
    working: pd.DataFrame,
    overlap_annotations: pd.DataFrame | None,
) -> pd.DataFrame:
    if overlap_annotations is None or overlap_annotations.empty or "event_id" not in overlap_annotations.columns:
        return working

    overlap = overlap_annotations.copy()
    keep = [
        column
        for column in ("event_id", "overlap_flag", "overlap_label", "overlap_note", "overlap_severity")
        if column in overlap.columns
    ]
    overlap = overlap[keep].drop_duplicates(subset=["event_id"], keep="last").copy()
    merged = working.merge(overlap, on="event_id", how="left", suffixes=("", "_override"))
    for column in ("overlap_flag", "overlap_label", "overlap_note", "overlap_severity"):
        override = f"{column}_override"
        if override not in merged.columns:
            continue
        merged[column] = merged[override].where(~merged[override].isna(), merged.get(column, pd.Series(index=merged.index, dtype=object)))
        merged = merged.drop(columns=[override])
    return merged


def build_qra_review_ledger(
    elasticity: pd.DataFrame,
    overlap_annotations: pd.DataFrame | None = None,
) -> pd.DataFrame:
    columns = [
        "spec_id",
        "event_spec_id",
        "treatment_version_id",
        "treatment_variant",
        "canonical_shock_id",
        "event_id",
        "quarter",
        "event_label",
        "event_date_requested",
        "event_date_aligned",
        "event_date_type",
        "policy_statement_url",
        "financing_estimates_url",
        "timing_quality",
        "headline_bucket",
        "classification_review_status",
        "shock_review_status",
        "shock_missing_flag",
        "small_denominator_flag",
        "usable_for_headline",
        "usable_for_headline_reason",
        "overlap_flag",
        "overlap_label",
        "overlap_note",
        "overlap_severity",
        "reviewer",
        "review_date",
        "shock_bn",
        "schedule_diff_10y_eq_bn",
        "schedule_diff_dynamic_10y_eq_bn",
        "schedule_diff_dv01_usd",
        "gross_notional_delta_bn",
        "shock_source",
        "shock_notes",
        "shock_construction",
        "alternative_treatment_complete",
        "alternative_treatment_missing_fields",
        "alternative_treatment_missing_reason",
    ]
    if elasticity.empty:
        return pd.DataFrame(columns=columns)

    working = elasticity.copy()
    if "treatment_variant" in working.columns:
        canonical = working.loc[working["treatment_variant"] == CANONICAL_TREATMENT_VARIANT].copy()
        if not canonical.empty:
            working = canonical
    working = _merge_overlap_overrides(working, overlap_annotations)

    keep = [
        column
        for column in (
            "event_id",
            "quarter",
            "event_label",
            "event_date_requested",
            "event_date_aligned",
            "event_date_type",
            "policy_statement_url",
            "financing_estimates_url",
            "timing_quality",
            "headline_bucket",
            "classification_review_status",
            "shock_review_status",
            "shock_missing_flag",
            "small_denominator_flag",
            "usable_for_headline",
            "usable_for_headline_reason",
            "overlap_flag",
            "overlap_label",
            "overlap_note",
            "overlap_severity",
            "reviewer",
            "review_date",
            "classification_reviewer",
            "classification_review_date",
            "shock_reviewer",
            "shock_review_date",
            "notes",
            "review_notes",
            "shock_bn",
            "schedule_diff_10y_eq_bn",
            "schedule_diff_dynamic_10y_eq_bn",
            "schedule_diff_dv01_usd",
            "gross_notional_delta_bn",
            "shock_source",
            "shock_notes",
            "shock_construction",
            "event_spec_id",
            "spec_id",
        )
        if column in working.columns
    ]
    ledger = _dedupe_on_keys(working[keep].copy(), key_columns=SHOCK_TEMPLATE_KEYS)
    if ledger.empty:
        return pd.DataFrame(columns=columns)

    for column in (
        "shock_bn",
        "schedule_diff_10y_eq_bn",
        "schedule_diff_dynamic_10y_eq_bn",
        "schedule_diff_dv01_usd",
        "gross_notional_delta_bn",
        "shock_source",
        "shock_notes",
        "shock_construction",
        "overlap_flag",
        "overlap_label",
        "overlap_note",
        "overlap_severity",
    ):
        if column not in ledger.columns:
            ledger[column] = pd.NA
    if "shock_missing_flag" not in ledger.columns:
        ledger["shock_missing_flag"] = ledger.get(
            "usable_for_headline_reason",
            pd.Series(index=ledger.index, dtype=object),
        ).astype(str).eq("shock_missing")
    else:
        ledger["shock_missing_flag"] = ledger["shock_missing_flag"].map(_as_bool)
    if "small_denominator_flag" not in ledger.columns:
        ledger["small_denominator_flag"] = False
    else:
        ledger["small_denominator_flag"] = ledger["small_denominator_flag"].map(_as_bool)

    ledger["classification_review_status"] = ledger.get(
        "classification_review_status",
        pd.Series(index=ledger.index, dtype=object),
    ).fillna("")
    ledger["shock_review_status"] = ledger.get(
        "shock_review_status",
        pd.Series(index=ledger.index, dtype=object),
    ).fillna("")
    ledger["headline_bucket"] = ledger.get("headline_bucket", pd.Series(index=ledger.index, dtype=object)).fillna("")
    ledger["usable_for_headline_reason"] = ledger.apply(
        lambda row: (
            normalize_text(row.get("usable_for_headline_reason"))
            or _headline_eligibility_reason(
                event_date_type=row.get("event_date_type", ""),
                treatment_variant=CANONICAL_TREATMENT_VARIANT,
                shock_missing=bool(row.get("shock_missing_flag", False)),
                small_denominator=bool(row.get("small_denominator_flag", False)),
                headline_bucket=normalize_lower(row.get("headline_bucket")),
                classification_review_status=normalize_lower(row.get("classification_review_status")),
                shock_review_status=normalize_lower(row.get("shock_review_status")),
            )
        ),
        axis=1,
    )
    ledger["usable_for_headline"] = ledger["usable_for_headline_reason"].eq("usable")
    ledger["overlap_severity"] = ledger.apply(_overlap_severity_from_row, axis=1)
    ledger["reviewer"] = ledger.apply(
        lambda row: (
            normalize_text(
                _first_non_empty_value(
                    row,
                    ("reviewer", "classification_reviewer", "shock_reviewer"),
                )
            )
            or _reviewer_from_status(_first_non_empty_value(row, ("classification_review_status", "shock_review_status")))
        ),
        axis=1,
    )
    ledger["review_date"] = ledger.apply(_review_date_from_row, axis=1)
    alt_status = ledger.apply(_alternative_treatment_status, axis=1, result_type="expand")
    alt_status.columns = [
        "alternative_treatment_complete",
        "alternative_treatment_missing_fields",
        "alternative_treatment_missing_reason",
    ]
    ledger = pd.concat([ledger, alt_status], axis=1)
    ledger["canonical_shock_id"] = CANONICAL_TREATMENT_VARIANT
    ledger["spec_id"] = SPEC_QRA_EVENT_V2
    ledger["event_spec_id"] = SPEC_QRA_EVENT_V2
    ledger["treatment_version_id"] = SPEC_DURATION_TREATMENT_V1
    ledger["treatment_variant"] = CANONICAL_TREATMENT_VARIANT
    for column in columns:
        if column not in ledger.columns:
            ledger[column] = pd.NA
    return ledger[columns].sort_values(["event_id", "event_date_type"], kind="stable").reset_index(drop=True)


def _contains_no_change_signal(text: str) -> bool:
    normalized = text.lower()
    return any(pattern in normalized for pattern in _NO_CHANGE_PATTERNS)


def _change_sign_from_verb(verb: str) -> float:
    normalized = normalize_lower(verb)
    if normalized in _NEGATIVE_CHANGE_VERBS:
        return -1.0
    return 1.0


def _context_seed_columns(panel: pd.DataFrame) -> list[str]:
    columns: list[str] = []
    seen: set[str] = set()
    for column in panel.columns:
        if column in SHOCK_TEMPLATE_KEYS:
            continue
        if column.endswith("_level_t") or column.endswith("_d1") or column.endswith("_d3"):
            continue
        if column in seen:
            continue
        seen.add(column)
        columns.append(column)
    return columns


def _append_delta(delta_map: dict[str, float], tenor: str, amount: float, occurrences: float) -> None:
    delta_map[tenor] = delta_map.get(tenor, 0.0) + float(amount) * float(occurrences)


def _parse_tenor_delta_totals(guidance_nominal: object, guidance_frns: object) -> dict[str, float]:
    nominal_text = normalize_text(guidance_nominal)
    frn_text = normalize_text(guidance_frns)
    combined = " ".join(part for part in (nominal_text, frn_text) if part)
    deltas: dict[str, float] = {}
    if not combined:
        return deltas

    short_group = re.search(
        r"(?P<verb>increase|raise|reduce|decrease|cut)(?: the)? auction(?:s)? sizes? of the 2-\s*and\s*5-year by \$?(?P<twofive>\d+(?:\.\d+)?) billion per month,\s*"
        r"the 3-year by \$?(?P<three>\d+(?:\.\d+)?) billion per month,\s*"
        r"and the 7-year by \$?(?P<seven>\d+(?:\.\d+)?) billion per month",
        nominal_text,
        flags=re.IGNORECASE,
    )
    if short_group is not None:
        sign = _change_sign_from_verb(short_group.group("verb"))
        twofive = sign * float(short_group.group("twofive"))
        _append_delta(deltas, "2Y", twofive, 3.0)
        _append_delta(deltas, "5Y", twofive, 3.0)
        _append_delta(deltas, "3Y", sign * float(short_group.group("three")), 3.0)
        _append_delta(deltas, "7Y", sign * float(short_group.group("seven")), 3.0)

    long_group = re.search(
        r"(?P<verb>increase|raise|reduce|decrease|cut)(?: both the new issue and the reopening auction size of the| the)? 10-year note by \$?(?P<ten>\d+(?:\.\d+)?) billion(?:,\s*and\s+|,\s*|\s+and\s+)"
        r"the 30-year bond by \$?(?P<thirty>\d+(?:\.\d+)?) billion(?:,\s*and\s+|,\s*|\s+and\s+)"
        r"the \$?20-year bond by \$?(?P<twenty>\d+(?:\.\d+)?) billion",
        nominal_text,
        flags=re.IGNORECASE,
    )
    if long_group is not None:
        sign = _change_sign_from_verb(long_group.group("verb"))
        _append_delta(deltas, "10Y", sign * float(long_group.group("ten")), 3.0)
        _append_delta(deltas, "30Y", sign * float(long_group.group("thirty")), 3.0)
        _append_delta(deltas, "20Y", sign * float(long_group.group("twenty")), 3.0)
    else:
        long_partial = re.search(
            r"(?P<verb>increase|raise|reduce|decrease|cut)(?: both the new issue and the reopening auction size of the| the)? 10-year note by \$?(?P<ten>\d+(?:\.\d+)?) billion(?:,\s*and\s+|,\s*|\s+and\s+)"
            r"the 30-year bond by \$?(?P<thirty>\d+(?:\.\d+)?) billion",
            nominal_text,
            flags=re.IGNORECASE,
        )
        if long_partial is not None:
            sign = _change_sign_from_verb(long_partial.group("verb"))
            _append_delta(deltas, "10Y", sign * float(long_partial.group("ten")), 3.0)
            _append_delta(deltas, "30Y", sign * float(long_partial.group("thirty")), 3.0)
        if re.search(r"(?:maintain|keep)(?: the)? 20-year bond", nominal_text, flags=re.IGNORECASE):
            deltas.setdefault("20Y", 0.0)

    frn_match = re.search(
        r"(?P<verb>increase|raise|reduce|decrease|cut)(?: the [A-Za-z]+ and [A-Za-z]+ reopening auction size of the| the [A-Za-z]+ reopening auction size of the| the)? 2-year FRN by \$?(?P<delta>\d+(?:\.\d+)?) billion",
        frn_text,
        flags=re.IGNORECASE,
    )
    if frn_match is not None:
        month_count = len(_MONTH_NAME_RE.findall(frn_text))
        if month_count == 0:
            month_count = 1
        sign = _change_sign_from_verb(frn_match.group("verb"))
        _append_delta(deltas, "2Y_FRN", sign * float(frn_match.group("delta")), float(month_count))

    return deltas


def _tenor_delta_10y_eq(delta_map: dict[str, float]) -> float:
    return float(sum(delta_map.get(tenor, 0.0) * weight for tenor, weight in _TENOR_10Y_EQ_WEIGHTS.items()))


def _format_parser_note(delta_map: dict[str, float], unsigned_shock_bn: float, shock_sign: float | None) -> str:
    pieces: list[str] = []
    if delta_map:
        detail = ", ".join(f"{tenor}={amount:g}" for tenor, amount in sorted(delta_map.items()))
        pieces.append(f"Auto tenor parser quarterly totals: {detail}.")
    else:
        pieces.append("Auto tenor parser inferred no coupon/FRN size change from hold language.")
    pieces.append(f"Unsigned 10y-equivalent shock={unsigned_shock_bn:g}bn.")
    if shock_sign is None:
        pieces.append("No curated sign available; nonzero shocks require manual review.")
    elif shock_sign > 0:
        pieces.append("Signed positive from shock_sign_curated=1.")
    elif shock_sign < 0:
        pieces.append("Signed negative from shock_sign_curated=-1.")
    else:
        pieces.append("Signed zero from shock_sign_curated=0.")
    return " ".join(pieces)


def _format_schedule_diff_note(component_rows: pd.DataFrame, total_10y_eq_bn: float) -> str:
    pieces: list[str] = []
    if not component_rows.empty:
        detail = ", ".join(
            f"{row['tenor']}={float(row['delta_bn']):g}"
            for _, row in component_rows.sort_values(["issue_type", "tenor"], kind="stable").iterrows()
        )
        pieces.append(f"Schedule diff quarterly totals: {detail}.")
    pieces.append(f"Schedule-diff 10y-equivalent shock={float(total_10y_eq_bn):g}bn.")
    pieces.append("Derived from quarter-over-quarter monthly schedule differences.")
    return " ".join(pieces)


def _alternative_treatment_status(row: pd.Series) -> tuple[bool, str, str]:
    fields = (
        "schedule_diff_10y_eq_bn",
        "schedule_diff_dynamic_10y_eq_bn",
        "schedule_diff_dv01_usd",
        "gross_notional_delta_bn",
    )
    missing_fields = [
        field
        for field in fields
        if pd.isna(pd.to_numeric(pd.Series([row.get(field)]), errors="coerce").iloc[0])
    ]
    if not missing_fields:
        return True, "", ""

    review_status = normalize_lower(row.get("shock_review_status"))
    shock_source = normalize_lower(row.get("shock_source"))
    construction = normalize_lower(row.get("shock_construction"))
    if "manual_statement_review" in shock_source:
        reason = "manual_statement_primary_only_pending_alt_treatments"
    elif "manual_schedule_diff" in shock_source:
        reason = "manual_schedule_diff_missing_alternative_fields"
    elif construction.startswith("manual_override"):
        reason = "manual_override_missing_alternative_fields"
    elif review_status == "reviewed":
        reason = "reviewed_event_missing_alternative_treatment_fields"
    else:
        reason = "alternative_treatment_fields_not_populated"
    return False, "|".join(missing_fields), reason


def autofill_qra_shock_template_from_schedule_components(
    template: pd.DataFrame,
    schedule_components: pd.DataFrame,
) -> pd.DataFrame:
    if schedule_components is None or schedule_components.empty:
        return template.copy()

    required = {"event_id", "tenor", "delta_bn", "contribution_10y_eq_bn"}
    missing = sorted(required - set(schedule_components.columns))
    if missing:
        raise KeyError(f"Schedule components missing required autofill column(s): {missing}")

    output = template.copy()
    summary = build_qra_schedule_shock_summary(schedule_components)
    summary_map = summary.set_index("event_id").to_dict(orient="index")

    for idx, row in output.iterrows():
        event_id = str(row.get("event_id", ""))
        summary_row = summary_map.get(event_id)
        if summary_row is None:
            continue
        if _is_missing(row.get("previous_event_id")):
            output.at[idx, "previous_event_id"] = summary_row.get("previous_event_id", pd.NA)
        if _is_missing(row.get("previous_quarter")):
            output.at[idx, "previous_quarter"] = summary_row.get("previous_quarter", pd.NA)
        if pd.isna(pd.to_numeric(pd.Series([row.get("gross_notional_delta_bn")]), errors="coerce").iloc[0]):
            output.at[idx, "gross_notional_delta_bn"] = summary_row.get("gross_notional_delta_bn", pd.NA)
        if pd.isna(pd.to_numeric(pd.Series([row.get("schedule_diff_10y_eq_bn")]), errors="coerce").iloc[0]):
            output.at[idx, "schedule_diff_10y_eq_bn"] = summary_row.get("schedule_diff_10y_eq_bn", pd.NA)
        if pd.isna(pd.to_numeric(pd.Series([row.get("schedule_diff_dynamic_10y_eq_bn")]), errors="coerce").iloc[0]):
            output.at[idx, "schedule_diff_dynamic_10y_eq_bn"] = summary_row.get("schedule_diff_dynamic_10y_eq_bn", pd.NA)
        if pd.isna(pd.to_numeric(pd.Series([row.get("schedule_diff_dv01_usd")]), errors="coerce").iloc[0]):
            output.at[idx, "schedule_diff_dv01_usd"] = summary_row.get("schedule_diff_dv01_usd", pd.NA)
        if _is_missing(row.get("shock_construction")):
            output.at[idx, "shock_construction"] = (
                "schedule_diff_primary"
                if _is_missing(row.get("shock_source")) or row.get("shock_source") == "schedule_diff_10y_eq_v1"
                else "manual_override_with_schedule_context"
            )

        existing_shock = pd.to_numeric(pd.Series([row.get("shock_bn")]), errors="coerce").iloc[0]
        if not pd.isna(existing_shock):
            continue
        total_10y_eq_bn = float(summary_row["schedule_diff_10y_eq_bn"])
        output.at[idx, "shock_bn"] = total_10y_eq_bn
        output.at[idx, "shock_construction"] = "schedule_diff_primary"
        if _is_missing(row.get("shock_source")):
            output.at[idx, "shock_source"] = "schedule_diff_10y_eq_v1"
        if _is_missing(row.get("shock_notes")):
            component_rows = schedule_components.loc[schedule_components["event_id"].astype(str) == event_id].copy()
            output.at[idx, "shock_notes"] = _format_schedule_diff_note(component_rows, total_10y_eq_bn)
        if _is_missing(row.get("shock_review_status")):
            output.at[idx, "shock_review_status"] = "provisional"
    return output


def autofill_qra_shock_template_from_capture(
    template: pd.DataFrame,
    capture_template: pd.DataFrame,
) -> pd.DataFrame:
    required_template = [
        *SHOCK_TEMPLATE_KEYS,
        "event_date_type",
        "event_date_requested",
        "shock_sign_curated",
        "classification_review_status",
    ]
    missing_template = [column for column in required_template if column not in template.columns]
    if missing_template:
        raise KeyError(f"Shock template missing required autofill column(s): {missing_template}")

    required_capture = ["qra_release_date", "guidance_nominal_coupons", "guidance_frns"]
    missing_capture = [column for column in required_capture if column not in capture_template.columns]
    if missing_capture:
        raise KeyError(f"Capture template missing required autofill column(s): {missing_capture}")

    output = template.copy()
    output["event_date_requested"] = pd.to_datetime(output["event_date_requested"], errors="coerce")
    for column in ("shock_source", "shock_notes", "shock_review_status"):
        if column in output.columns:
            output[column] = output[column].astype(object)

    capture = capture_template.copy()
    capture["qra_release_date"] = pd.to_datetime(capture["qra_release_date"], errors="coerce")
    capture = capture.dropna(subset=["qra_release_date"]).copy()

    parser_rows: list[dict[str, object]] = []
    for _, capture_row in capture.iterrows():
        delta_map = _parse_tenor_delta_totals(
            capture_row.get("guidance_nominal_coupons"),
            capture_row.get("guidance_frns"),
        )
        combined_text = " ".join(
            part for part in (
                normalize_text(capture_row.get("guidance_nominal_coupons")),
                normalize_text(capture_row.get("guidance_frns")),
            )
            if part
        )
        unsigned_shock_bn = abs(_tenor_delta_10y_eq(delta_map))
        if unsigned_shock_bn == 0.0 and not _contains_no_change_signal(combined_text):
            continue
        parser_rows.append(
            {
                "qra_release_date": capture_row["qra_release_date"],
                "unsigned_shock_bn": unsigned_shock_bn,
                "parser_detail": delta_map,
            }
        )

    if not parser_rows:
        return output

    parser_map = pd.DataFrame(parser_rows)
    official_rows = output.loc[output["event_date_type"] == "official_release_date", ["event_id", "event_date_requested"]].copy()
    official_rows = official_rows.merge(
        parser_map,
        left_on="event_date_requested",
        right_on="qra_release_date",
        how="left",
    )
    event_fill_map = official_rows.set_index("event_id")[["unsigned_shock_bn", "parser_detail"]].to_dict(orient="index")

    for idx, row in output.iterrows():
        existing_shock = pd.to_numeric(pd.Series([row.get("shock_bn")]), errors="coerce").iloc[0]
        if not pd.isna(existing_shock):
            continue
        fill = event_fill_map.get(str(row["event_id"]))
        if not fill:
            continue
        unsigned_shock_bn = fill.get("unsigned_shock_bn")
        if pd.isna(unsigned_shock_bn):
            continue
        unsigned_shock_bn = float(unsigned_shock_bn)
        shock_sign = coerce_shock_sign(row.get("shock_sign_curated"))
        if unsigned_shock_bn == 0.0:
            signed_shock_bn = 0.0
        elif shock_sign is None:
            continue
        else:
            signed_shock_bn = unsigned_shock_bn * float(shock_sign)
        output.at[idx, "shock_bn"] = signed_shock_bn
        if _is_missing(row.get("shock_source")):
            output.at[idx, "shock_source"] = "auto_tenor_parser_v1"
        if _is_missing(row.get("shock_notes")):
            output.at[idx, "shock_notes"] = _format_parser_note(fill.get("parser_detail") or {}, unsigned_shock_bn, shock_sign)
        if _is_missing(row.get("shock_review_status")):
            output.at[idx, "shock_review_status"] = "provisional"

    return output


def build_qra_shock_template(
    panel: pd.DataFrame,
    existing_template: pd.DataFrame | None = None,
    capture_template: pd.DataFrame | None = None,
    schedule_components: pd.DataFrame | None = None,
) -> pd.DataFrame:
    required = list(SHOCK_TEMPLATE_KEYS)
    missing = [column for column in required if column not in panel.columns]
    if missing:
        raise KeyError(f"Event panel missing required column(s): {missing}")

    base_columns = _context_seed_columns(panel)
    seeded = panel[[*required, *base_columns]].copy()
    seeded = _dedupe_on_keys(seeded, key_columns=required)
    seeded["shock_bn"] = np.nan
    for column in ("shock_source", "shock_notes", "shock_review_status"):
        seeded[column] = pd.Series([np.nan] * len(seeded), dtype=object)
    for column in SHOCK_TEMPLATE_DERIVED_COLUMNS:
        seeded[column] = pd.Series([np.nan] * len(seeded), dtype=object)

    if existing_template is None or existing_template.empty:
        output = seeded
    else:
        existing = _dedupe_on_keys(existing_template.copy(), key_columns=required)
        output = seeded.merge(existing, on=required, how="left", suffixes=("", "_existing"))

        for column in SHOCK_TEMPLATE_MANUAL_COLUMNS:
            existing_column = f"{column}_existing"
            if existing_column in output.columns:
                output[column] = output[existing_column].where(~output[existing_column].isna(), output[column])
        for column in SHOCK_TEMPLATE_DERIVED_COLUMNS:
            existing_column = f"{column}_existing"
            if existing_column in output.columns:
                output[column] = output[existing_column].where(~output[existing_column].isna(), output[column])

        for column in base_columns:
            existing_column = f"{column}_existing"
            if existing_column in output.columns:
                output[column] = output[column].where(~output[column].isna(), output[existing_column])

        passthrough_columns = [column for column in existing.columns if column not in seeded.columns]
        for column in passthrough_columns:
            existing_column = f"{column}_existing"
            if existing_column in output.columns:
                output[column] = output[existing_column]

        drop_existing_columns = [column for column in output.columns if column.endswith("_existing")]
        output = output.drop(columns=drop_existing_columns)

    for column in SHOCK_TEMPLATE_KEYS:
        output[column] = output[column].astype(str)
    if schedule_components is not None:
        output = autofill_qra_shock_template_from_schedule_components(output, schedule_components)
    if capture_template is not None:
        output = autofill_qra_shock_template_from_capture(output, capture_template)
    output = output.sort_values(list(SHOCK_TEMPLATE_KEYS), kind="stable").reset_index(drop=True)
    return output


def build_qra_event_elasticity(
    panel: pd.DataFrame,
    shock_template: pd.DataFrame,
    shock_column: str = "shock_bn",
    small_denominator_threshold_bn: float = 10.0,
    metrics: Iterable[str] = DEFAULT_METRICS,
) -> pd.DataFrame:
    required_panel = list(SHOCK_TEMPLATE_KEYS)
    missing_panel = [column for column in required_panel if column not in panel.columns]
    if missing_panel:
        raise KeyError(f"Event panel missing required column(s): {missing_panel}")

    required_template = [*SHOCK_TEMPLATE_KEYS, shock_column]
    missing_template = [column for column in required_template if column not in shock_template.columns]
    if missing_template:
        raise KeyError(f"Shock template missing required column(s): {missing_template}")

    panel_deduped = _dedupe_on_keys(panel.copy(), key_columns=SHOCK_TEMPLATE_KEYS)
    if shock_column != "shock_bn":
        treatment_variants = tuple(
            item
            for item in _TREATMENT_VARIANTS
            if item[0] == CANONICAL_TREATMENT_VARIANT
        )
    else:
        treatment_variants = _TREATMENT_VARIANTS

    shock_context_columns = [
        column
        for column in (
            shock_column,
            "shock_source",
            "shock_notes",
            "shock_review_status",
            "previous_event_id",
            "previous_quarter",
            "gross_notional_delta_bn",
            "schedule_diff_10y_eq_bn",
            "schedule_diff_dynamic_10y_eq_bn",
            "schedule_diff_dv01_usd",
            "shock_construction",
        )
        if column in shock_template.columns
    ]
    shocks = _dedupe_on_keys(
        shock_template[[*SHOCK_TEMPLATE_KEYS, *shock_context_columns]].copy(),
        key_columns=SHOCK_TEMPLATE_KEYS,
    )
    merged = panel_deduped.merge(shocks, on=list(SHOCK_TEMPLATE_KEYS), how="left")
    for col in {
        shock_column,
        "schedule_diff_10y_eq_bn",
        "schedule_diff_dynamic_10y_eq_bn",
        "schedule_diff_dv01_usd",
    }:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce")

    metric_columns = [column for column in metrics if column in merged.columns]
    records: list[dict[str, object]] = []

    for _, row in merged.iterrows():
        headline_bucket = normalize_lower(row.get("headline_bucket"))
        classification_review_status = normalize_lower(row.get("classification_review_status"))
        shock_review_status = normalize_lower(row.get("shock_review_status"))
        expected_direction = normalize_text(row.get("expected_direction"))

        for delta_column in metric_columns:
            delta_pp = pd.to_numeric(pd.Series([row[delta_column]]), errors="coerce").iloc[0]
            delta_bp = np.nan if pd.isna(delta_pp) else float(delta_pp) * 100.0
            series_name, window = delta_column.rsplit("_", 1)
            for treatment_variant, treatment_column, treatment_shock_units, elasticity_units, scale_denominator in treatment_variants:
                treatment_value_raw = row.get(treatment_column, pd.NA)
                treatment_value = pd.to_numeric(pd.Series([treatment_value_raw]), errors="coerce").iloc[0]
                shock_missing = bool(pd.isna(treatment_value))
                small_denominator = False
                if not shock_missing:
                    small_denominator = _small_denominator_flag(
                        treatment_variant=treatment_variant,
                        shock_value=float(treatment_value),
                        small_denominator_threshold_bn=float(small_denominator_threshold_bn),
                    )

                shock_scale = np.nan
                if not shock_missing and float(treatment_value) != 0.0:
                    shock_scale = float(treatment_value) / float(scale_denominator)

                elasticity_value = np.nan
                if not pd.isna(delta_bp) and not pd.isna(shock_scale):
                    elasticity_value = float(delta_bp) / float(shock_scale)

                usable_for_headline_reason = _headline_eligibility_reason(
                    event_date_type=row["event_date_type"],
                    treatment_variant=treatment_variant,
                    shock_missing=shock_missing,
                    small_denominator=small_denominator,
                    headline_bucket=headline_bucket,
                    classification_review_status=classification_review_status,
                    shock_review_status=shock_review_status,
                )
                usable_for_headline = usable_for_headline_reason == "usable"
                elasticity_bp_per_100bn = elasticity_value if treatment_shock_units == "USD billions" else np.nan
                records.append(
                    {
                        "spec_id": SPEC_DURATION_TREATMENT_V1,
                        "event_spec_id": SPEC_QRA_EVENT_V2,
                        "treatment_variant": treatment_variant,
                        "canonical_shock_id": shock_column,
                        "treatment_shock_column": treatment_column,
                        "treatment_shock_units": treatment_shock_units,
                        "elasticity_units": elasticity_units,
                        "quarter": row.get("quarter", pd.NA),
                        "event_id": row["event_id"],
                        "event_label": row.get("event_label", pd.NA),
                        "event_date_requested": row.get("event_date_requested", pd.NA),
                        "event_date_aligned": row.get("event_date_aligned", pd.NA),
                        "event_date_type": row["event_date_type"],
                        "policy_statement_url": row.get("policy_statement_url", pd.NA),
                        "financing_estimates_url": row.get("financing_estimates_url", pd.NA),
                        "timing_quality": row.get("timing_quality", pd.NA),
                        "overlap_flag": _as_bool(row.get("overlap_flag", False)),
                        "overlap_label": row.get("overlap_label", pd.NA),
                        "overlap_note": row.get("overlap_note", pd.NA),
                        "overlap_severity": _overlap_severity_from_row(row),
                        "expected_direction": expected_direction,
                        "current_quarter_action": row.get("current_quarter_action", pd.NA),
                        "forward_guidance_bias": row.get("forward_guidance_bias", pd.NA),
                        "headline_bucket": row.get("headline_bucket", pd.NA),
                        "shock_sign_curated": row.get("shock_sign_curated", pd.NA),
                        "classification_confidence": row.get("classification_confidence", pd.NA),
                        "classification_review_status": row.get("classification_review_status", pd.NA),
                        "series": series_name,
                        "window": window,
                        "delta_pp": delta_pp,
                        "delta_bp": delta_bp,
                        shock_column: row.get(shock_column, pd.NA),
                        "treatment_shock_value": treatment_value,
                        "shock_source": row.get("shock_source", pd.NA),
                        "shock_notes": row.get("shock_notes", pd.NA),
                        "shock_review_status": row.get("shock_review_status", pd.NA),
                        "previous_event_id": row.get("previous_event_id", pd.NA),
                        "previous_quarter": row.get("previous_quarter", pd.NA),
                        "gross_notional_delta_bn": row.get("gross_notional_delta_bn", pd.NA),
                        "schedule_diff_10y_eq_bn": row.get("schedule_diff_10y_eq_bn", pd.NA),
                        "schedule_diff_dynamic_10y_eq_bn": row.get("schedule_diff_dynamic_10y_eq_bn", pd.NA),
                        "schedule_diff_dv01_usd": row.get("schedule_diff_dv01_usd", pd.NA),
                        "shock_construction": row.get("shock_construction", pd.NA),
                        "shock_missing_flag": shock_missing,
                        "small_denominator_flag": small_denominator,
                        "elasticity_value": elasticity_value,
                        "elasticity_bp_per_100bn": elasticity_bp_per_100bn,
                        "usable_for_headline": usable_for_headline,
                        "usable_for_headline_reason": usable_for_headline_reason,
                    }
                )

    output = pd.DataFrame(records)
    if output.empty:
        return output
    output["sign_flip_flag"] = False
    for _, group in output.groupby(["event_id", "series", "window", "treatment_variant"], sort=False, dropna=False):
        if len(group) < 2:
            continue
        signs = {
            int(np.sign(value))
            for value in group["delta_bp"]
            if pd.notna(value) and float(value) != 0.0
        }
        if len(signs) > 1:
            output.loc[group.index, "sign_flip_flag"] = True
    output = output.sort_values(
        ["event_id", "event_date_type", "series", "window", "treatment_variant"],
        kind="stable",
    ).reset_index(drop=True)
    return output


def build_qra_shock_crosswalk_v1(
    shock_template: pd.DataFrame,
    canonical_shock_id: str = "shock_bn",
) -> pd.DataFrame:
    ledger = build_qra_review_ledger(shock_template)
    if ledger.empty:
        return pd.DataFrame(
            columns=[
                "spec_id",
                "treatment_version_id",
                "treatment_variant",
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
                "usable_for_headline_reason",
            ]
        )

    out = ledger.copy()
    out["manual_override_reason"] = out.apply(
        lambda row: (
            ("" if _is_missing(row.get("shock_notes", pd.NA)) else normalize_text(row.get("shock_notes")))
            or (
                "manual_override_with_schedule_context"
                if normalize_lower(row.get("shock_construction")).startswith("manual_override")
                else ("manual_source_override" if "manual" in normalize_lower(row.get("shock_source")) else "")
            )
        ),
        axis=1,
    )
    keep = [
        "spec_id",
        "treatment_version_id",
        "treatment_variant",
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
        "usable_for_headline_reason",
    ]
    return out[keep].sort_values(["event_id", "event_date_type"], kind="stable").reset_index(drop=True)


def build_event_usability_table(
    elasticity: pd.DataFrame,
    overlap_annotations: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if elasticity.empty:
        return pd.DataFrame(
            columns=[
                "spec_id",
                "treatment_version_id",
                "treatment_variant",
                "headline_bucket",
                "classification_review_status",
                "shock_review_status",
                "event_date_type",
                "overlap_severity",
                "usable_for_headline",
                "usable_for_headline_reason",
                "event_count",
                "n_rows",
                "n_events",
            ]
        )

    working = build_qra_review_ledger(elasticity, overlap_annotations=overlap_annotations)
    if working.empty:
        return pd.DataFrame()
    grouped = (
        working.groupby(
            [
                "spec_id",
                "treatment_version_id",
                "treatment_variant",
                "headline_bucket",
                "classification_review_status",
                "shock_review_status",
                "event_date_type",
                "overlap_severity",
                "usable_for_headline",
                "usable_for_headline_reason",
            ],
            dropna=False,
            observed=True,
        )
        .agg(n_rows=("event_id", "size"), n_events=("event_id", "nunique"))
        .reset_index()
    )
    grouped["event_count"] = grouped["n_events"]
    return grouped.sort_values(
        [
            "event_date_type",
            "headline_bucket",
            "classification_review_status",
            "shock_review_status",
            "usable_for_headline",
            "usable_for_headline_reason",
        ],
        kind="stable",
    ).reset_index(drop=True)


def build_leave_one_event_out_table(elasticity: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "spec_id",
        "treatment_variant",
        "series",
        "window",
        "dropped_event_id",
        "left_out_event_id",
        "n_events",
        "mean_elasticity",
        "estimate",
        "elasticity_units",
    ]
    if elasticity.empty:
        return pd.DataFrame(columns=columns)

    required = {"event_id", "series", "window", "treatment_variant", "elasticity_value"}
    missing = sorted(required - set(elasticity.columns))
    if missing:
        raise KeyError(f"Elasticity frame missing required leave-one-out column(s): {missing}")

    working = elasticity.copy()
    if "usable_for_headline" in working.columns:
        working = working.loc[working["usable_for_headline"]].copy()
    if working.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, object]] = []
    for (spec_id, treatment_variant, series, window), group in working.groupby(
        ["spec_id", "treatment_variant", "series", "window"],
        sort=True,
        dropna=False,
    ):
        all_values = pd.to_numeric(group["elasticity_value"], errors="coerce")
        rows.append(
            {
                "spec_id": spec_id,
                "treatment_variant": treatment_variant,
                "series": series,
                "window": window,
                "dropped_event_id": "__none__",
                "left_out_event_id": "__none__",
                "n_events": int(all_values.notna().sum()),
                "mean_elasticity": all_values.mean(),
                "estimate": all_values.mean(),
                "elasticity_units": _first_non_empty_value(group.iloc[0], ("elasticity_units",)),
            }
        )
        for event_id in sorted(group["event_id"].dropna().astype(str).unique()):
            subset = group.loc[group["event_id"].astype(str) != event_id]
            subset_values = pd.to_numeric(subset["elasticity_value"], errors="coerce")
            rows.append(
                {
                    "spec_id": spec_id,
                    "treatment_variant": treatment_variant,
                "series": series,
                "window": window,
                "dropped_event_id": event_id,
                "left_out_event_id": event_id,
                "n_events": int(subset_values.notna().sum()),
                "mean_elasticity": subset_values.mean(),
                "estimate": subset_values.mean(),
                "elasticity_units": _first_non_empty_value(group.iloc[0], ("elasticity_units",)),
            }
        )
    return pd.DataFrame(rows, columns=columns).sort_values(
        ["treatment_variant", "series", "window", "dropped_event_id"],
        kind="stable",
    ).reset_index(drop=True)


def build_treatment_comparison_table(elasticity: pd.DataFrame) -> pd.DataFrame:
    columns = [
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
    ]
    if elasticity.empty:
        return pd.DataFrame(columns=columns)

    working = elasticity.copy()
    if "event_date_type" in working.columns:
        working = working.loc[working["event_date_type"].astype(str) == "official_release_date"].copy()
    if working.empty:
        return pd.DataFrame(columns=columns)
    if "spec_id" not in working.columns:
        working["spec_id"] = SPEC_DURATION_TREATMENT_V1
    if "usable_for_headline" not in working.columns:
        working["usable_for_headline"] = working.apply(_headline_eligibility_reason, axis=1) == "usable"
    if "comparison_family" not in working.columns:
        working["comparison_family"] = working.get("treatment_variant", pd.Series(index=working.index, dtype=object)).map(
            _comparison_family_for_variant
        )
    if "elasticity_units" not in working.columns:
        working["elasticity_units"] = pd.NA
    if "elasticity_value" not in working.columns:
        if "elasticity_bp_per_100bn" in working.columns:
            working["elasticity_value"] = working["elasticity_bp_per_100bn"]
            if working["elasticity_units"].isna().all():
                working["elasticity_units"] = "bp_per_100bn"
        elif "elasticity" in working.columns:
            working["elasticity_value"] = working["elasticity"]
        else:
            return pd.DataFrame(columns=columns)

    rows: list[dict[str, object]] = []
    group_columns = ["spec_id", "event_date_type", "series", "window", "treatment_variant"]
    for (spec_id, event_date_type, series, window, treatment_variant), group in working.groupby(
        group_columns,
        sort=False,
        dropna=False,
    ):
        values = pd.to_numeric(group["elasticity_value"], errors="coerce")
        headline_flags = group.get("usable_for_headline", pd.Series(False, index=group.index)).fillna(False).astype(bool)
        comparison_family = _comparison_family_for_variant(treatment_variant)
        rows.append(
            {
                "spec_id": spec_id,
                "event_date_type": event_date_type,
                "series": series,
                "window": window,
                "treatment_variant": treatment_variant,
                "comparison_family": comparison_family,
                "comparison_family_label": _comparison_family_label(comparison_family),
                "elasticity_units": _first_non_empty_value(group.iloc[0], ("elasticity_units",)),
                "n_rows": int(len(group)),
                "n_events": int(group["event_id"].dropna().astype(str).nunique()),
                "n_headline_eligible_events": int(group.loc[headline_flags, "event_id"].dropna().astype(str).nunique()),
                "headline_eligible_share": float(
                    group.loc[headline_flags, "event_id"].dropna().astype(str).nunique()
                )
                / float(group["event_id"].dropna().astype(str).nunique())
                if group["event_id"].dropna().astype(str).nunique()
                else np.nan,
                "mean_elasticity_value": values.mean(),
                "median_elasticity_value": values.median(),
                "std_elasticity_value": values.std(),
                "min_elasticity_value": values.min(),
                "max_elasticity_value": values.max(),
                "mean_abs_elasticity_value": values.abs().mean(),
                "family_reference_variant": _comparison_family_reference_variant(comparison_family),
            }
        )

    summary = pd.DataFrame(rows)
    if summary.empty:
        return pd.DataFrame(columns=columns)

    family_reference_rows = (
        summary.loc[
            summary["treatment_variant"].astype(str)
            == summary["family_reference_variant"].astype(str)
        ]
        .rename(columns={"mean_elasticity_value": "family_reference_mean_elasticity_value"})
        [
            [
                "spec_id",
                "event_date_type",
                "series",
                "window",
                "comparison_family",
                "family_reference_variant",
                "family_reference_mean_elasticity_value",
            ]
        ]
        .drop_duplicates(
            subset=["spec_id", "event_date_type", "series", "window", "comparison_family", "family_reference_variant"],
            keep="first",
        )
    )
    summary = summary.merge(
        family_reference_rows,
        on=[
            "spec_id",
            "event_date_type",
            "series",
            "window",
            "comparison_family",
            "family_reference_variant",
        ],
        how="left",
    )
    summary["delta_vs_family_reference_mean_elasticity_value"] = (
        summary["mean_elasticity_value"] - summary["family_reference_mean_elasticity_value"]
    )

    headline_rows: list[dict[str, object]] = []
    for (spec_id, event_date_type, series, window), group in summary.groupby(
        ["spec_id", "event_date_type", "series", "window"],
        dropna=False,
        sort=False,
    ):
        bp_group = group.loc[group["comparison_family"].astype(str) == "bp_per_100bn"].copy()
        canonical = bp_group.loc[bp_group["treatment_variant"].astype(str) == CANONICAL_TREATMENT_VARIANT].copy()
        canonical_eligible = int(canonical["n_headline_eligible_events"].fillna(0).sum()) if not canonical.empty else 0
        bp_spread = (
            float(bp_group["mean_elasticity_value"].max() - bp_group["mean_elasticity_value"].min())
            if not bp_group.empty and bp_group["mean_elasticity_value"].notna().any()
            else np.nan
        )
        bp_spread_text = f"{bp_spread:.3f}" if pd.notna(bp_spread) else "nan"
        if canonical_eligible > 0:
            status = "retain_canonical_contract"
            reason = (
                "canonical_headline_contract_has_eligible_rows;"
                f"bp_family_spread={bp_spread_text};"
                "comparison_families_published_for_audit"
            )
        else:
            status = "review_canonical_contract"
            reason = "canonical_headline_contract_has_no_eligible_rows;comparison_families_published_for_audit"
        headline_rows.append(
            {
                "spec_id": spec_id,
                "event_date_type": event_date_type,
                "series": series,
                "window": window,
                "headline_recommendation_status": status,
                "headline_recommendation_reason": reason,
                "primary_treatment_variant": CANONICAL_TREATMENT_VARIANT,
                "primary_treatment_reason": "canonical_shock_bn remains the headline contract; fixed, dynamic, and DV01 variants are comparison diagnostics.",
                "bp_family_spread_elasticity_value": bp_spread,
            }
        )

    headline = pd.DataFrame(headline_rows)
    summary = summary.merge(
        headline,
        on=["spec_id", "event_date_type", "series", "window"],
        how="left",
    )

    for column in (
        "headline_recommendation_status",
        "headline_recommendation_reason",
        "primary_treatment_variant",
        "primary_treatment_reason",
    ):
        if column not in summary.columns:
            summary[column] = pd.NA

    return summary[columns].sort_values(
        [
            "series",
            "window",
            "comparison_family",
            "treatment_variant",
        ],
        key=lambda s: s.map(lambda value: _TREATMENT_VARIANT_ORDER.get(str(value), len(_TREATMENT_VARIANT_ORDER)))
        if s.name == "treatment_variant"
        else s,
        kind="stable",
    ).reset_index(drop=True)
