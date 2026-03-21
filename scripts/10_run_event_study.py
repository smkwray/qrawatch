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
from ati_shadow_policy.research.event_study import summarize_event_panel, summarize_event_panel_robustness

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
    robustness_lines = ["# QRA Event Summary Robustness", ""]
    if "overlap_flag" in panel.columns and bool(panel["overlap_flag"].any()):
        overlap_events = int(panel["overlap_flag"].sum())
        robustness_lines.append(f"Overlap-annotated events excluded in the sensitivity check: {overlap_events}.")
        robustness_lines.append("")
    else:
        robustness_lines.append("No overlap annotations are currently marked for exclusion.")
        robustness_lines.append("")
    robustness_lines.append(robustness.to_markdown(index=False))
    (TABLES_DIR / "qra_event_summary_robustness.md").write_text("\n".join(robustness_lines) + "\n", encoding="utf-8")
    print(f"Saved event summary with {len(summary):,} rows")

if __name__ == "__main__":
    main()
