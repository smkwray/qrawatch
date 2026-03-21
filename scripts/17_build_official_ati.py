from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import PROCESSED_DIR, TABLES_DIR, ensure_project_dirs
from ati_shadow_policy.qra_capture import build_ati_input_from_official_capture
from ati_shadow_policy.research.ati_index import build_ati_index


def main() -> None:
    ensure_project_dirs()
    capture = pd.read_csv(PROCESSED_DIR / "official_quarterly_refunding_capture.csv")
    ati_input = build_ati_input_from_official_capture(capture)
    ati = build_ati_index(ati_input)
    out_path = PROCESSED_DIR / "ati_index_official_capture.csv"
    write_df(ati, out_path)

    summary_cols = [
        "quarter",
        "financing_need_bn",
        "net_bills_bn",
        "bill_share",
        "missing_coupons_18_bn",
        "qa_status",
    ]
    summary = ati[summary_cols].copy()
    write_df(summary, TABLES_DIR / "ati_official_summary.csv")
    md = "# ATI Official Summary\n\n" + summary.to_markdown(index=False)
    (TABLES_DIR / "ati_official_summary.md").write_text(md + "\n", encoding="utf-8")
    print(f"Saved official ATI index rows={len(ati):,} path={out_path}")


if __name__ == "__main__":
    main()
