from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pathlib import Path

import pandas as pd

from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import MANUAL_DIR, PROCESSED_DIR, TABLES_DIR, ensure_project_dirs
from ati_shadow_policy.research.ati_index import build_ati_index

def main() -> None:
    ensure_project_dirs()
    seed_path = MANUAL_DIR / "quarterly_refunding_seed.csv"
    df = pd.read_csv(seed_path)
    ati = build_ati_index(df)
    out_path = PROCESSED_DIR / "ati_index_seed.csv"
    write_df(ati, out_path)

    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    summary_cols = [
        "quarter",
        "financing_need_bn",
        "net_bills_bn",
        "bill_share",
        "missing_coupons_15_bn",
        "missing_coupons_18_bn",
        "missing_coupons_20_bn",
    ]
    summary = ati[summary_cols].copy()
    write_df(summary, TABLES_DIR / "ati_seed_summary.csv")
    md = "# ATI Seed Summary\n\n" + summary.to_markdown(index=False)
    (TABLES_DIR / "ati_seed_summary.md").write_text(md + "\n", encoding="utf-8")
    print(f"Saved ATI seed index to {out_path}")

if __name__ == "__main__":
    main()
