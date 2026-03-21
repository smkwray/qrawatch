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
from ati_shadow_policy.research.event_study import (
    build_overlap_exclusion_audit_note,
    summarize_event_panel,
    summarize_event_panel_robustness,
)
from ati_shadow_policy.research.qra_elasticity import (
    build_event_usability_table,
    build_leave_one_event_out_table,
    build_treatment_comparison_table,
    build_qra_shock_crosswalk_v1,
)

def main() -> None:
    ensure_project_dirs()
    panel = pd.read_csv(PROCESSED_DIR / "qra_event_panel.csv")
    overlap_annotations_path = MANUAL_DIR / "qra_event_overlap_annotations.csv"
    if "overlap_flag" not in panel.columns and overlap_annotations_path.exists():
        overlap_annotations = pd.read_csv(overlap_annotations_path)
        panel = panel.merge(overlap_annotations, on="event_id", how="left")
    if "overlap_flag" in panel.columns:
        panel["overlap_flag"] = panel["overlap_flag"].map(
            lambda value: str(value).strip().lower() in {"1", "true", "t", "yes", "y"}
        )

    summary = summarize_event_panel(panel)
    write_df(summary, TABLES_DIR / "qra_event_summary.csv")
    md = "# QRA Event Summary\n\n" + summary.to_markdown(index=False)
    (TABLES_DIR / "qra_event_summary.md").write_text(md + "\n", encoding="utf-8")

    robustness = summarize_event_panel_robustness(panel)
    write_df(robustness, TABLES_DIR / "qra_event_summary_robustness.csv")
    audit_note = build_overlap_exclusion_audit_note(panel, robustness)
    robustness_lines = ["# QRA Event Summary Robustness", ""]
    robustness_lines.append(audit_note)
    robustness_lines.append("")
    robustness_lines.append(robustness.to_markdown(index=False))
    (TABLES_DIR / "qra_event_summary_robustness.md").write_text("\n".join(robustness_lines) + "\n", encoding="utf-8")

    shock_template_path = MANUAL_DIR / "qra_event_shock_template.csv"
    if shock_template_path.exists():
        shock_template = pd.read_csv(shock_template_path)
        crosswalk = build_qra_shock_crosswalk_v1(shock_template)
        write_df(crosswalk, TABLES_DIR / "qra_shock_crosswalk_v1.csv")

    elasticity_path = TABLES_DIR / "qra_event_elasticity.csv"
    if elasticity_path.exists():
        elasticity = pd.read_csv(elasticity_path)
        treatment_comparison = build_treatment_comparison_table(elasticity)
        write_df(treatment_comparison, TABLES_DIR / "treatment_comparison_table.csv")
        usability = build_event_usability_table(elasticity)
        leave_one_out = build_leave_one_event_out_table(elasticity)
        write_df(usability, TABLES_DIR / "event_usability_table.csv")
        write_df(leave_one_out, TABLES_DIR / "leave_one_event_out_table.csv")
    print(f"Saved event summary with {len(summary):,} rows")

if __name__ == "__main__":
    main()
