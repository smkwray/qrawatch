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
    "benchmark_timing_status",
    "external_benchmark_ready",
    "expected_composition_bn",
    "realized_composition_bn",
    "composition_surprise_bn",
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
    "contamination_notes",
]


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
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
) -> pd.DataFrame:
    target_columns = list(columns)
    seeded = _ensure_columns(seeded, target_columns)
    if existing is None or existing.empty:
        return seeded.sort_values(key, kind="stable").reset_index(drop=True)

    existing = _ensure_columns(existing, target_columns)
    if key not in existing.columns:
        return seeded.sort_values(key, kind="stable").reset_index(drop=True)

    merged = seeded.merge(existing, on=key, how="outer", suffixes=("_seeded", "_existing"), indicator=True)
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
) -> pd.DataFrame:
    seeded = _ensure_columns(component_registry, RELEASE_COMPONENT_REGISTRY_COLUMNS)
    return _merge_existing_seeded(
        seeded,
        existing,
        key="release_component_id",
        columns=RELEASE_COMPONENT_REGISTRY_COLUMNS,
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
) -> pd.DataFrame:
    shock_context = _shock_context_map(shock_summary)
    rows: list[dict[str, object]] = []
    for _, row in component_registry.iterrows():
        event_id = str(row.get("event_id") or "")
        component_type = str(row.get("component_type") or "")
        context = shock_context.get(event_id, {})
        shock_bn = context.get("shock_bn", pd.NA)
        shock_review_status = context.get("shock_review_status", "")
        note_parts = ["Seeded review row. Fill benchmark timestamp/source and component-level expected/realized composition by hand."]
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
                "benchmark_timing_status": "same_release_placeholder",
                "external_benchmark_ready": False,
                "expected_composition_bn": pd.NA,
                "realized_composition_bn": pd.NA,
                "composition_surprise_bn": pd.NA,
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
                "contamination_notes": note,
            }
        )
    seeded = pd.DataFrame(rows)
    return _merge_existing_seeded(
        seeded,
        existing,
        key="release_component_id",
        columns=CONTAMINATION_TEMPLATE_COLUMNS,
    )
