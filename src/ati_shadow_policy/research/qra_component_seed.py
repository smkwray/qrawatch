from __future__ import annotations

from collections.abc import Iterable

import pandas as pd


RELEASE_COMPONENT_REGISTRY_COLUMNS = [
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
]

EXPECTATION_TEMPLATE_COLUMNS = [
    "release_component_id",
    "event_id",
    "component_type",
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
]

CONTAMINATION_TEMPLATE_COLUMNS = [
    "release_component_id",
    "event_id",
    "component_type",
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
]


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except TypeError:
        pass
    return str(value).strip() == ""


def _normalize_text(value: object) -> str:
    if _is_missing(value):
        return ""
    return " ".join(str(value).split()).strip()


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    out = df.copy()
    for column in columns:
        if column not in out.columns:
            out[column] = pd.NA
    return out[list(columns)]


def _merge_existing_seeded(
    seeded: pd.DataFrame,
    existing: pd.DataFrame | None,
    key: str,
    columns: Iterable[str],
    preserve_orphans: bool = False,
) -> pd.DataFrame:
    target_columns = list(columns)
    seeded = _ensure_columns(seeded, target_columns)
    if existing is None or existing.empty:
        return seeded.sort_values(key, kind="stable").reset_index(drop=True)

    existing = _ensure_columns(existing, target_columns)
    if key not in existing.columns:
        return seeded.sort_values(key, kind="stable").reset_index(drop=True)

    merge_how = "outer" if preserve_orphans else "left"
    merged = seeded.merge(existing, on=key, how=merge_how, suffixes=("_seeded", "_existing"), indicator=True)
    rows: list[dict[str, object]] = []
    for _, row in merged.iterrows():
        out_row: dict[str, object] = {key: row[key]}
        for column in target_columns:
            if column == key:
                continue
            existing_value = row.get(f"{column}_existing", pd.NA)
            seeded_value = row.get(f"{column}_seeded", pd.NA)
            out_row[column] = existing_value if not _is_missing(existing_value) else seeded_value
        rows.append(out_row)
    out = pd.DataFrame(rows)
    return _ensure_columns(out, target_columns).sort_values(key, kind="stable").reset_index(drop=True)


def seed_release_component_registry(
    component_registry: pd.DataFrame,
    existing: pd.DataFrame | None = None,
    preserve_orphans: bool = False,
) -> pd.DataFrame:
    seeded = _ensure_columns(component_registry, RELEASE_COMPONENT_REGISTRY_COLUMNS)
    if not seeded.empty:
        notes: list[object] = []
        timezone_asserted: list[object] = []
        for _, row in seeded.iterrows():
            base_note = _normalize_text(row.get("review_notes"))
            precision = _normalize_text(row.get("timestamp_precision"))
            component_type = _normalize_text(row.get("component_type"))
            if precision == "exact_time" and component_type == "financing_estimates":
                prompt = "Record release timestamp evidence (source method + URL/note) for this reviewed exact-time financing component."
                if prompt not in base_note:
                    base_note = " ".join(part for part in (base_note, prompt) if part)
            notes.append(base_note or pd.NA)
            timezone_asserted.append("America/New_York" if precision == "exact_time" else pd.NA)
        seeded["review_notes"] = notes
        seeded["release_timezone_asserted"] = timezone_asserted
    return _merge_existing_seeded(
        seeded,
        existing,
        key="release_component_id",
        columns=RELEASE_COMPONENT_REGISTRY_COLUMNS,
        preserve_orphans=preserve_orphans,
    )


def _shock_context_map(shock_summary: pd.DataFrame | None) -> dict[str, dict[str, object]]:
    if shock_summary is None or shock_summary.empty:
        return {}
    working = shock_summary.copy()
    if "event_date_type" in working.columns:
        working = working.loc[working["event_date_type"].astype(str) == "official_release_date"].copy()
    if working.empty:
        return {}
    keep = [column for column in ("event_id", "shock_bn", "shock_review_status", "shock_notes") if column in working.columns]
    return working[keep].drop_duplicates(subset=["event_id"], keep="first").set_index("event_id").to_dict(orient="index")


def seed_expectation_template(
    component_registry: pd.DataFrame,
    shock_summary: pd.DataFrame | None = None,
    existing: pd.DataFrame | None = None,
    preserve_orphans: bool = False,
) -> pd.DataFrame:
    shock_context = _shock_context_map(shock_summary)
    rows: list[dict[str, object]] = []
    for _, row in component_registry.iterrows():
        event_id = str(row.get("event_id") or "")
        component_type = str(row.get("component_type") or "")
        timestamp_precision = _normalize_text(row.get("timestamp_precision"))
        seeded_family = _normalize_text(row.get("benchmark_source_family")).lower()
        context = shock_context.get(event_id, {})
        shock_bn = context.get("shock_bn", pd.NA)
        shock_review_status = context.get("shock_review_status", "")
        note_parts = ["Seeded review row. Fill benchmark timestamp/source and component-level expected/realized composition by hand."]
        benchmark_timing_status = "same_release_placeholder"
        if component_type == "financing_estimates":
            note_parts.append(
                "Verify benchmark evidence that was observable before the release-component timestamp (document, timestamp source, and pre-release verification flags)."
            )
            if seeded_family and seeded_family != "treasury_release":
                benchmark_timing_status = "external_timing_unverified"
        if timestamp_precision == "exact_time" and component_type == "financing_estimates":
            note_parts.append("Record timestamp-evidence source method and supporting URL/note for this exact-time component.")
        if not _is_missing(shock_bn):
            note_parts.append(f"Event-level descriptive shock_bn={shock_bn}.")
        if not _is_missing(shock_review_status):
            note_parts.append(f"Event-level shock_review_status={shock_review_status}.")
        rows.append(
            {
                "release_component_id": row.get("release_component_id"),
                "event_id": event_id,
                "component_type": component_type,
                "benchmark_timestamp_et": pd.NA,
                "benchmark_source": pd.NA,
                "benchmark_source_family": pd.NA,
                "benchmark_document_url": pd.NA,
                "benchmark_document_local": pd.NA,
                "benchmark_release_timestamp_et": pd.NA,
                "benchmark_release_timestamp_precision": pd.NA,
                "benchmark_timestamp_source_method": pd.NA,
                "benchmark_pre_release_verified_flag": False,
                "benchmark_observed_before_component_flag": False,
                "benchmark_timing_status": benchmark_timing_status,
                "external_benchmark_ready": False,
                "expected_composition_bn": pd.NA,
                "realized_composition_bn": pd.NA,
                "composition_surprise_bn": pd.NA,
                "surprise_construction_method": pd.NA,
                "surprise_units": pd.NA,
                "benchmark_stale_flag": False,
                "expectation_review_status": "pending",
                "expectation_notes": " ".join(note_parts),
            }
        )
    seeded = pd.DataFrame(rows)
    merged = _merge_existing_seeded(
        seeded,
        existing,
        key="release_component_id",
        columns=EXPECTATION_TEMPLATE_COLUMNS,
        preserve_orphans=preserve_orphans,
    )
    if merged.empty:
        return merged

    seeded_note_map = (
        seeded.set_index("release_component_id")["expectation_notes"].to_dict()
        if "release_component_id" in seeded.columns
        else {}
    )
    refreshed_notes: list[object] = []
    for _, row in merged.iterrows():
        current_note = _normalize_text(row.get("expectation_notes"))
        seeded_note = _normalize_text(seeded_note_map.get(row.get("release_component_id"), pd.NA))
        should_refresh = (
            current_note.startswith("Seeded review row.")
            and "shock_bn=<NA>" in current_note
            and seeded_note.startswith("Seeded review row.")
            and "shock_bn=<NA>" not in seeded_note
        )
        refreshed_notes.append(seeded_note if should_refresh else row.get("expectation_notes"))
    merged["expectation_notes"] = refreshed_notes
    return merged


def _overlap_map(overlap_annotations: pd.DataFrame | None) -> dict[str, dict[str, object]]:
    if overlap_annotations is None or overlap_annotations.empty:
        return {}
    working = overlap_annotations.copy()
    keep = [column for column in ("event_id", "overlap_flag", "overlap_label", "overlap_note", "overlap_severity") if column in working.columns]
    return working[keep].drop_duplicates(subset=["event_id"], keep="last").set_index("event_id").to_dict(orient="index")


def seed_contamination_reviews(
    component_registry: pd.DataFrame,
    overlap_annotations: pd.DataFrame | None = None,
    existing: pd.DataFrame | None = None,
    preserve_orphans: bool = False,
) -> pd.DataFrame:
    overlap_map = _overlap_map(overlap_annotations)
    rows: list[dict[str, object]] = []
    for _, row in component_registry.iterrows():
        event_id = str(row.get("event_id") or "")
        component_type = str(row.get("component_type") or "")
        overlap = overlap_map.get(event_id, {})
        overlap_flag = _coerce_bool(overlap.get("overlap_flag"))
        overlap_label = overlap.get("overlap_label", pd.NA)
        overlap_note = overlap.get("overlap_note", pd.NA)
        decision_rule = pd.NA
        if not _is_missing(overlap_note):
            decision_rule = f"Review overlap annotation: {_normalize_text(overlap_note)}"
        note = overlap_note
        if _is_missing(note):
            note = "Seeded review row. Confirm macro/policy contamination by hand."
        rows.append(
            {
                "release_component_id": row.get("release_component_id"),
                "event_id": event_id,
                "component_type": component_type,
                "contamination_flag": overlap_flag,
                "contamination_status": "pending_review",
                "contamination_review_status": "pending",
                "contamination_label": overlap_label,
                "contamination_window_start_et": pd.NA,
                "contamination_window_end_et": pd.NA,
                "confound_release_type": pd.NA,
                "confound_release_timestamp_et": pd.NA,
                "decision_rule": decision_rule,
                "exclude_from_causal_pool": pd.NA,
                "decision_confidence": pd.NA,
                "contamination_notes": note,
            }
        )
    seeded = pd.DataFrame(rows)
    return _merge_existing_seeded(
        seeded,
        existing,
        key="release_component_id",
        columns=CONTAMINATION_TEMPLATE_COLUMNS,
        preserve_orphans=preserve_orphans,
    )
