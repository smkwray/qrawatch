from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ati_shadow_policy.publish import PUBLISH_DIR, build_publish_artifacts


def main() -> None:
    build_publish_artifacts()
    print(f"Saved publish artifacts to {PUBLISH_DIR}")


if __name__ == "__main__":
    main()
