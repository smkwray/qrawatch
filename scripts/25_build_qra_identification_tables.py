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
    build_event_usability_table,
    build_leave_one_event_out_table,
    build_qra_event_registry_v2,
    build_qra_shock_crosswalk_v1,
)
from ati_shadow_policy.specs import spec_registry_frame


def _read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def main() -> None:
    ensure_project_dirs()

    panel = _read_csv_if_exists(PROCESSED_DIR / "qra_event_panel.csv")
    release_calendar = _read_csv_if_exists(MANUAL_DIR / "qra_release_calendar_seed.csv")
    overlap_annotations = _read_csv_if_exists(MANUAL_DIR / "qra_event_overlap_annotations.csv")
    elasticity = _read_csv_if_exists(TABLES_DIR / "qra_event_elasticity.csv")
    shock_summary = _read_csv_if_exists(TABLES_DIR / "qra_event_elasticity.csv")

    spec_registry = spec_registry_frame()
    write_df(spec_registry, PROCESSED_DIR / "spec_registry.csv")

    event_registry = build_qra_event_registry_v2(
        panel=panel,
        release_calendar=release_calendar,
        overlap_annotations=overlap_annotations,
        shock_summary=shock_summary,
        release_calendar_source=str(MANUAL_DIR / "qra_release_calendar_seed.csv"),
        overlap_annotations_source=str(MANUAL_DIR / "qra_event_overlap_annotations.csv"),
        shock_summary_source=str(TABLES_DIR / "qra_event_elasticity.csv"),
    )
    write_df(event_registry, PROCESSED_DIR / "qra_event_registry_v2.csv")
    write_df(event_registry, TABLES_DIR / "qra_event_registry_v2.csv")

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
        f"shock_crosswalk={len(shock_crosswalk):,}, "
        f"event_usability={len(event_usability):,}, "
        f"leave_one_out={len(leave_one_out):,}"
    )


if __name__ == "__main__":
    main()
