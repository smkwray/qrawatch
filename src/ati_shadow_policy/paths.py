from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
SRC_DIR = PACKAGE_DIR.parent
PROJECT_ROOT = SRC_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"
MANUAL_DIR = DATA_DIR / "manual"
OUTPUT_DIR = PROJECT_ROOT / "output"
FIGURES_DIR = OUTPUT_DIR / "figures"
TABLES_DIR = OUTPUT_DIR / "tables"
LOGS_DIR = PROJECT_ROOT / "logs"
CONFIG_DIR = PROJECT_ROOT / "configs"
REFERENCES_DIR = PROJECT_ROOT / "references"

ALL_DIRS = [
    DATA_DIR,
    RAW_DIR,
    INTERIM_DIR,
    PROCESSED_DIR,
    MANUAL_DIR,
    OUTPUT_DIR,
    FIGURES_DIR,
    TABLES_DIR,
    LOGS_DIR,
    CONFIG_DIR,
    REFERENCES_DIR,
]

def ensure_project_dirs() -> None:
    for path in ALL_DIRS:
        path.mkdir(parents=True, exist_ok=True)
