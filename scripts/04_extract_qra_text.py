from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pathlib import Path

from bs4 import BeautifulSoup
import pdfplumber

from ati_shadow_policy.io_utils import write_text
from ati_shadow_policy.paths import INTERIM_DIR, RAW_DIR, ensure_project_dirs


def infer_extract_type(path: Path) -> str | None:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        return "pdf"
    if suffix in {".html", ".htm"}:
        return "html"
    try:
        sniff = path.read_bytes()[:4096].lower()
    except OSError:
        return None
    if sniff.startswith(b"%pdf-"):
        return "pdf"
    if b"<html" in sniff or b"<!doctype html" in sniff:
        return "html"
    return None


def extract_pdf_text(path: Path) -> str:
    chunks = []
    with pdfplumber.open(path) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            chunks.append(f"\n\n===== PAGE {i} =====\n{text}")
    return "".join(chunks).strip()

def extract_html_text(path: Path) -> str:
    soup = BeautifulSoup(path.read_text(encoding="utf-8", errors="ignore"), "lxml")
    return soup.get_text("\n", strip=True)

def main() -> None:
    ensure_project_dirs()
    files_dir = RAW_DIR / "qra" / "files"
    out_dir = INTERIM_DIR / "qra_text"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not files_dir.exists():
        raise SystemExit(f"No downloaded QRA files found at {files_dir}. Run scripts/03_download_qra_materials.py first.")

    count = 0
    for path in files_dir.glob("*"):
        kind = infer_extract_type(path)
        if kind == "pdf":
            text = extract_pdf_text(path)
        elif kind == "html":
            text = extract_html_text(path)
        else:
            continue
        out_path = out_dir / f"{path.stem}.txt"
        write_text(text, out_path)
        count += 1
    print(f"Extracted text files: {count}")

if __name__ == "__main__":
    main()
