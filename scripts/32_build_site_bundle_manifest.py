from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ati_shadow_policy.publish import write_site_bundle_manifest


def main() -> None:
    manifest_path = write_site_bundle_manifest()
    print(f"Saved site bundle manifest to {manifest_path}")


if __name__ == "__main__":
    main()
