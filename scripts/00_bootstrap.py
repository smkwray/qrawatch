from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ati_shadow_policy.paths import ensure_project_dirs, RAW_DIR, INTERIM_DIR, PROCESSED_DIR, OUTPUT_DIR, TABLES_DIR, FIGURES_DIR

def main() -> None:
    ensure_project_dirs()
    print("Project directories are ready.")
    print(f"RAW_DIR={RAW_DIR}")
    print(f"INTERIM_DIR={INTERIM_DIR}")
    print(f"PROCESSED_DIR={PROCESSED_DIR}")
    print(f"OUTPUT_DIR={OUTPUT_DIR}")
    print(f"TABLES_DIR={TABLES_DIR}")
    print(f"FIGURES_DIR={FIGURES_DIR}")

if __name__ == "__main__":
    main()
