from __future__ import annotations

from pathlib import Path
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ati_shadow_policy.paths import OUTPUT_DIR
from ati_shadow_policy.research.qra_promotion_audit import write_qra_promotion_audit


def main() -> None:
    table_path, report_path = write_qra_promotion_audit()
    publish_dir = OUTPUT_DIR / "publish"
    publish_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(table_path, publish_dir / table_path.name)
    shutil.copy2(report_path, publish_dir / report_path.name)
    print(f"Saved {table_path}")
    print(f"Saved {report_path}")


if __name__ == "__main__":
    main()
