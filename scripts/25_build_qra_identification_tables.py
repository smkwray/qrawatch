from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import MANUAL_DIR, PROCESSED_DIR, TABLES_DIR, ensure_project_dirs
from ati_shadow_policy.research.identification import (
    build_event_design_status,
    build_event_usability_table,
    build_leave_one_event_out_table,
    build_qra_benchmark_blockers_by_event,
    build_qra_event_registry_v2,
    build_qra_release_component_registry,
    build_qra_shock_crosswalk_v1,
    summarize_qra_causal_qa,
)
from ati_shadow_policy.research.qra_elasticity import build_qra_review_ledger
from ati_shadow_policy.specs import spec_registry_frame


def _read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def main() -> None:
    ensure_project_dirs()

    panel = _read_csv_if_exists(PROCESSED_DIR / "qra_event_panel.csv")
    release_calendar = _read_csv_if_exists(MANUAL_DIR / "qra_release_calendar_seed.csv")
    release_components = _read_csv_if_exists(MANUAL_DIR / "qra_release_component_registry.csv")
    expectation_template = _read_csv_if_exists(MANUAL_DIR / "qra_component_expectation_template.csv")
    overlap_annotations = _read_csv_if_exists(MANUAL_DIR / "qra_event_overlap_annotations.csv")
    contamination_reviews = _read_csv_if_exists(MANUAL_DIR / "qra_event_contamination_reviews.csv")
    elasticity = _read_csv_if_exists(TABLES_DIR / "qra_event_elasticity.csv")
    shock_summary = build_qra_review_ledger(elasticity, overlap_annotations=overlap_annotations)
    write_df(shock_summary, PROCESSED_DIR / "qra_event_shock_summary.csv")
    write_df(shock_summary, TABLES_DIR / "qra_event_shock_summary.csv")

    spec_registry = spec_registry_frame()
    write_df(spec_registry, PROCESSED_DIR / "spec_registry.csv")

    event_registry = build_qra_event_registry_v2(
        panel=panel,
        release_calendar=release_calendar,
        overlap_annotations=overlap_annotations,
        shock_summary=shock_summary,
        release_components=release_components,
        expectation_template=expectation_template,
        contamination_reviews=contamination_reviews,
        release_calendar_source=str(MANUAL_DIR / "qra_release_calendar_seed.csv"),
        overlap_annotations_source=str(MANUAL_DIR / "qra_event_overlap_annotations.csv"),
        shock_summary_source=str(TABLES_DIR / "qra_event_shock_summary.csv"),
    )
    write_df(event_registry, PROCESSED_DIR / "qra_event_registry_v2.csv")
    write_df(event_registry, TABLES_DIR / "qra_event_registry_v2.csv")

    component_registry = build_qra_release_component_registry(
        event_registry,
        release_components=release_components,
        expectation_template=expectation_template,
        contamination_reviews=contamination_reviews,
    )
    write_df(component_registry, PROCESSED_DIR / "qra_release_component_registry.csv")
    write_df(component_registry, TABLES_DIR / "qra_release_component_registry.csv")

    causal_qa = summarize_qra_causal_qa(component_registry)
    write_df(causal_qa, PROCESSED_DIR / "qra_causal_qa_ledger.csv")
    write_df(causal_qa, TABLES_DIR / "qra_causal_qa_ledger.csv")

    event_design_status = build_event_design_status(component_registry)
    write_df(event_design_status, TABLES_DIR / "event_design_status.csv")

    benchmark_blockers = build_qra_benchmark_blockers_by_event(component_registry)
    write_df(benchmark_blockers, PROCESSED_DIR / "qra_benchmark_blockers_by_event.csv")
    write_df(benchmark_blockers, TABLES_DIR / "qra_benchmark_blockers_by_event.csv")

    shock_crosswalk = build_qra_shock_crosswalk_v1(elasticity)
    write_df(shock_crosswalk, PROCESSED_DIR / "qra_shock_crosswalk_v1.csv")
    write_df(shock_crosswalk, TABLES_DIR / "qra_shock_crosswalk_v1.csv")
    write_df(shock_crosswalk, TABLES_DIR / "shock_crosswalk_table.csv")

    event_usability = build_event_usability_table(elasticity, overlap_annotations=overlap_annotations)
    write_df(event_usability, TABLES_DIR / "event_usability_table.csv")

    leave_one_out = build_leave_one_event_out_table(elasticity)
    write_df(leave_one_out, TABLES_DIR / "leave_one_event_out_table.csv")

    print(
        "Saved identification artifacts: "
        f"spec_registry={len(spec_registry):,}, "
        f"event_registry={len(event_registry):,}, "
        f"component_registry={len(component_registry):,}, "
        f"causal_qa={len(causal_qa):,}, "
        f"benchmark_blockers={len(benchmark_blockers):,}, "
        f"shock_summary={len(shock_summary):,}, "
        f"shock_crosswalk={len(shock_crosswalk):,}, "
        f"event_usability={len(event_usability):,}, "
        f"leave_one_out={len(leave_one_out):,}"
    )


if __name__ == "__main__":
    main()
