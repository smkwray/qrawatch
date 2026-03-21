from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from ati_shadow_policy.qra_capture import CAPTURE_COLUMNS, OFFICIAL_QA_STATUSES, seed_capture_rows_from_local_sources


_STRING_MISSING = {"", "nan", "none"}


@dataclass(frozen=True)
class SeedSyncResult:
    dataframe: pd.DataFrame
    rows_added: int
    cells_enriched: int
    conflicting_cells_skipped: int


def build_seed_rows(
    *,
    direction: str,
    historical_seed_df: pd.DataFrame | None = None,
    qra_event_seed_df: pd.DataFrame | None = None,
    quarterly_refunding_seed_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    selected = direction.lower().strip()
    if selected not in {"forward", "backward", "both"}:
        raise ValueError("direction must be one of: forward, backward, both")

    if selected in {"backward", "both"}:
        if historical_seed_df is None:
            raise ValueError("historical_seed_df is required for backward/both directions")
        rows.append(_coerce_to_capture_contract(historical_seed_df))

    if selected in {"forward", "both"}:
        if qra_event_seed_df is None or quarterly_refunding_seed_df is None:
            raise ValueError(
                "qra_event_seed_df and quarterly_refunding_seed_df are required for forward/both directions"
            )
        rows.append(
            _coerce_to_capture_contract(
                seed_capture_rows_from_local_sources(
                    qra_event_seed=qra_event_seed_df,
                    quarterly_refunding_seed=quarterly_refunding_seed_df,
                )
            )
        )

    if not rows:
        return pd.DataFrame(columns=CAPTURE_COLUMNS)
    combined = pd.concat(rows, ignore_index=True)
    return _dedupe_by_quarter_richness(_normalize_capture_strings(combined))


def sync_capture_template(
    template_df: pd.DataFrame,
    seed_rows_df: pd.DataFrame,
) -> SeedSyncResult:
    _require_capture_contract(template_df)

    template = _normalize_capture_strings(template_df)
    seed_rows = _normalize_capture_strings(_coerce_to_capture_contract(seed_rows_df))

    if seed_rows.empty:
        return SeedSyncResult(
            dataframe=_sort_capture_rows(template),
            rows_added=0,
            cells_enriched=0,
            conflicting_cells_skipped=0,
        )

    quarter_to_index: dict[str, int] = {}
    for idx, quarter in enumerate(template["quarter"].tolist()):
        quarter_key = _clean(quarter)
        if quarter_key:
            quarter_to_index[quarter_key] = idx

    rows_added = 0
    cells_enriched = 0
    conflicting_cells_skipped = 0

    for _, seed_row in seed_rows.iterrows():
        quarter = _clean(seed_row.get("quarter", ""))
        if not quarter:
            continue

        if quarter not in quarter_to_index:
            template = pd.concat(
                [template, pd.DataFrame([seed_row], columns=list(CAPTURE_COLUMNS))],
                ignore_index=True,
            )
            quarter_to_index[quarter] = len(template) - 1
            rows_added += 1
            continue

        idx = quarter_to_index[quarter]
        for col in CAPTURE_COLUMNS:
            if col == "quarter":
                continue
            incoming = _clean(seed_row.get(col, ""))
            existing = _clean(template.at[idx, col])
            if existing or not incoming:
                if existing and incoming and existing != incoming:
                    conflicting_cells_skipped += 1
                continue
            template.at[idx, col] = incoming
            cells_enriched += 1

    return SeedSyncResult(
        dataframe=_sort_capture_rows(template),
        rows_added=rows_added,
        cells_enriched=cells_enriched,
        conflicting_cells_skipped=conflicting_cells_skipped,
    )


def _coerce_to_capture_contract(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(columns=list(CAPTURE_COLUMNS))
    for col in CAPTURE_COLUMNS:
        if col in df.columns:
            out[col] = df[col]
        else:
            out[col] = ""
    return out


def _require_capture_contract(df: pd.DataFrame) -> None:
    actual = list(df.columns)
    expected = list(CAPTURE_COLUMNS)
    if actual != expected:
        missing = [col for col in expected if col not in actual]
        extra = [col for col in actual if col not in expected]
        raise ValueError(
            "Capture template contract mismatch. "
            f"Missing={missing} Extra={extra} ExpectedOrder={expected}"
        )


def _normalize_capture_strings(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in CAPTURE_COLUMNS:
        if col not in out.columns:
            out[col] = ""
        out[col] = (
            out[col]
            .fillna("")
            .astype(str)
            .str.strip()
            .replace({"nan": "", "None": "", "none": ""})
        )
    return out[list(CAPTURE_COLUMNS)]


def _dedupe_by_quarter_richness(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    deduped_rows: list[pd.Series] = []
    for _, group in df.groupby("quarter", sort=False, dropna=False):
        if len(group) == 1:
            deduped_rows.append(group.iloc[0])
            continue
        selected = max(group.iterrows(), key=lambda item: (_source_priority(item[1]), _richness_score(item[1])))[1]
        deduped_rows.append(selected)
    return _sort_capture_rows(pd.DataFrame(deduped_rows, columns=list(CAPTURE_COLUMNS)))


def _richness_score(row: pd.Series) -> int:
    score = 0
    for col in CAPTURE_COLUMNS:
        value = _clean(row.get(col, ""))
        if value:
            score += 1
    return score


def _has_seed_dependency(row: pd.Series) -> bool:
    text = "|".join(
        [
            _clean(row.get("source_doc_type", "")),
            _clean(row.get("source_doc_local", "")),
        ]
    ).lower()
    return "seed_csv" in text


def _source_priority(row: pd.Series) -> int:
    qa_status = _clean(row.get("qa_status", ""))
    if qa_status in OFFICIAL_QA_STATUSES and not _has_seed_dependency(row):
        return 3
    if qa_status == "semi_automated_capture":
        return 2
    if qa_status == "seed_only":
        return 1
    return 0


def _sort_capture_rows(df: pd.DataFrame) -> pd.DataFrame:
    sortable = df.copy()
    sortable["_qra_release_date_sort"] = pd.to_datetime(sortable["qra_release_date"], errors="coerce")
    sortable["_quarter_sort"] = sortable["quarter"].astype(str)
    sortable = sortable.sort_values(
        by=["_qra_release_date_sort", "_quarter_sort"],
        ascending=[True, True],
        kind="stable",
        na_position="last",
    ).drop(columns=["_qra_release_date_sort", "_quarter_sort"])
    return sortable.reset_index(drop=True)


def _clean(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in _STRING_MISSING:
        return ""
    return text
