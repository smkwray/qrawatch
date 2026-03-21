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
    merged[shock_column] = pd.to_numeric(merged[shock_column], errors="coerce")

    metric_columns = [column for column in metrics if column in merged.columns]
    records: list[dict[str, object]] = []

    for _, row in merged.iterrows():
        shock_bn = row[shock_column]
        shock_missing = bool(pd.isna(shock_bn))
        small_denominator = bool((not shock_missing) and (abs(float(shock_bn)) < small_denominator_threshold_bn))
        shock_scale = np.nan
        if not shock_missing and float(shock_bn) != 0.0:
            shock_scale = float(shock_bn) / 100.0

        headline_bucket = normalize_lower(row.get("headline_bucket"))
        classification_review_status = normalize_lower(row.get("classification_review_status"))
        shock_review_status = normalize_lower(row.get("shock_review_status"))
        expected_direction = normalize_text(row.get("expected_direction"))
        usable_for_headline = (
            row["event_date_type"] == "official_release_date"
            and
            (not shock_missing)
            and (not small_denominator)
            and headline_bucket in SUMMARY_HEADLINE_BUCKETS
            and classification_review_status == "reviewed"
            and shock_review_status == "reviewed"
        )

        for delta_column in metric_columns:
            delta_pp = pd.to_numeric(pd.Series([row[delta_column]]), errors="coerce").iloc[0]
            delta_bp = np.nan if pd.isna(delta_pp) else float(delta_pp) * 100.0
            elasticity_bp_per_100bn = np.nan
            if not pd.isna(delta_bp) and not pd.isna(shock_scale):
                elasticity_bp_per_100bn = float(delta_bp) / float(shock_scale)

            series_name, window = delta_column.rsplit("_", 1)
            records.append(
                {
                    "quarter": row.get("quarter", pd.NA),
                    "event_id": row["event_id"],
                    "event_label": row.get("event_label", pd.NA),
                    "event_date_requested": row.get("event_date_requested", pd.NA),
                    "event_date_aligned": row.get("event_date_aligned", pd.NA),
                    "event_date_type": row["event_date_type"],
                    "policy_statement_url": row.get("policy_statement_url", pd.NA),
                    "financing_estimates_url": row.get("financing_estimates_url", pd.NA),
                    "timing_quality": row.get("timing_quality", pd.NA),
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
                    shock_column: shock_bn,
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
                    "elasticity_bp_per_100bn": elasticity_bp_per_100bn,
                    "usable_for_headline": usable_for_headline,
                }
            )

    output = pd.DataFrame(records)
    if output.empty:
        return output
    output["sign_flip_flag"] = False
    for _, group in output.groupby(["event_id", "series", "window"], sort=False, dropna=False):
        if len(group) < 2:
            continue
        signs = {
            int(np.sign(value))
            for value in group["delta_bp"]
            if pd.notna(value) and float(value) != 0.0
        }
        if len(signs) > 1:
            output.loc[group.index, "sign_flip_flag"] = True
    output = output.sort_values(["event_id", "event_date_type", "series", "window"], kind="stable").reset_index(drop=True)
    return output
