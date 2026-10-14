"""Rebuild the official quarter table on Treasury's realized privately held net marketable borrowing.

capture: download the financing-estimates archive page and every quarterly financing release
from the quarter before --first through the quarter after --last into OUT/sources, logging
the URL, retrieval time and SHA-256 of each file. Existing files are never replaced.

build: read each quarter's realized borrowing sentence from the release published in the
following quarter (releases listed in --release-log, by default OUT/sources/release_downloads.csv),
label the financing value the template held (own-quarter estimate, prior-release estimate or
following-quarter estimate) from the same releases, write the realized capture to --realized
(tracked) and run script 34, with its MSPD reconciliation, on a template whose financing fields
are the realized values. Writes OUT/publish/financing_convention_audit.csv and adds a
realized_financing block to OUT/receipt.json.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from urllib.parse import unquote, urljoin

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from ati_shadow_policy.io_utils import download_binary_with_metadata, write_df
from ati_shadow_policy.paths import MANUAL_DIR
from ati_shadow_policy.qra_capture import (
    _extract_html_text,
    _parse_html_document,
    _strip_financing_release_note_fragments,
    financing_sentences,
)

ARCHIVE_URL = (
    "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/"
    "quarterly-refunding-archives/quarterly-refunding-financing-estimates-by-calendar-year"
)
FINANCING_DOC_TYPE = "quarterly_refunding_press_release"
DEFAULT_REALIZED = MANUAL_DIR / "realized_privately_held_borrowing.csv"
REALIZED_COLUMNS = [
    "quarter",
    "realized_financing_bn",
    "status",
    "release_quarter",
    "release_date",
    "release_url",
    "quoted_sentence",
    "wording",
    "retrieved_utc",
    "doc_sha256",
    "doc_local",
]
LABELS = {
    "own": "own_quarter_estimate",
    "prior": "prior_release_estimate",
    "following": "misread_following_quarter_estimate",
}

spec = importlib.util.spec_from_file_location("official_quarter_refresh", ROOT / "scripts" / "34_build_official_quarter_refresh.py")
assert spec is not None and spec.loader is not None
refresh = importlib.util.module_from_spec(spec)
spec.loader.exec_module(refresh)


def qshift(quarter: str, step: int) -> str:
    period = pd.Period(quarter, freq="Q") + step
    return f"{period.year}Q{period.quarter}"


def quarter_range(first: str, last: str) -> list[str]:
    return [f"{p.year}Q{p.quarter}" for p in pd.period_range(first, last, freq="Q")]


def archive_release_links(path: Path) -> dict[str, str]:
    """Map each archive quarter to its release URL using the year column header, not aria-label.

    The archive labels the February 2025 release "2024 1st Quarter"; the header is reliable.
    """
    soup = _parse_html_document(path.read_text(encoding="utf-8", errors="ignore"))
    links: dict[str, str] = {}
    for anchor in soup.select("th[headers] a[href], td[headers] a[href]"):
        year = anchor.find_parent(["th", "td"]).get("headers", "")
        year = year[0] if isinstance(year, list) else year
        text = re.sub(r"[\u200b\ufeff]", "", " ".join(anchor.get_text(" ", strip=True).split()))
        match = re.fullmatch(r"([1-4])(?:st|nd|rd|th) Quarter", text)
        if re.fullmatch(r"\d{4}", str(year)) and match:
            links.setdefault(f"{year}Q{match.group(1)}", urljoin("https://home.treasury.gov", anchor["href"]))
    return links


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _local_name(url: str) -> str:
    name = unquote(url.rstrip("/").rsplit("/", 1)[-1])
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    return name if "." in name else f"{name}.html"


def capture(out_dir: Path, first: str, last: str) -> int:
    sources = out_dir / "sources"
    log_path = sources / "release_downloads.csv"
    archive = sources / "quarterly-refunding-financing-estimates-by-calendar-year.html"
    if log_path.exists() or archive.exists():
        raise SystemExit(f"Refusing to replace existing capture in {sources}")
    meta = download_binary_with_metadata(ARCHIVE_URL, archive)
    if meta["download_status"] != "ok":
        raise SystemExit(f"Archive download failed: {meta}")
    links = archive_release_links(archive)
    rows = [{"release_quarter": "archive", "url": ARCHIVE_URL, "local_path": refresh._rel(archive), **meta, "sha256": _sha256(archive)}]
    for quarter in quarter_range(qshift(first, -1), qshift(last, 1)):
        url = links.get(quarter)
        if url is None:
            rows.append({"release_quarter": quarter, "url": None, "download_status": "missing_from_archive"})
            continue
        path = sources / "releases" / _local_name(url)
        if path.exists():
            raise SystemExit(f"Refusing to replace {path}")
        meta = download_binary_with_metadata(url, path)
        rows.append(
            {
                "release_quarter": quarter,
                "url": url,
                "local_path": refresh._rel(path) if path.exists() else None,
                **meta,
                "sha256": _sha256(path) if path.exists() else None,
            }
        )
    log = pd.DataFrame(rows)
    write_df(log, log_path)
    failed = log.loc[log["download_status"].ne("ok"), ["release_quarter", "url", "download_status"]]
    print(f"Downloaded {int(log['download_status'].eq('ok').sum())} files to {sources}")
    if not failed.empty:
        print("Not downloaded:\n" + failed.to_string(index=False))
        return 1
    return 0


def release_text_and_date(path: Path) -> tuple[str, str | None]:
    if path.suffix.lower() == ".pdf":
        import pdfplumber

        with pdfplumber.open(path) as pdf:
            text = " ".join(" ".join((page.extract_text() or "").split()) for page in pdf.pages)
        match = re.search(r"([A-Z][a-z]+ \d{1,2}, \d{4})", text)
        return text, pd.Timestamp(match.group(1)).strftime("%Y-%m-%d") if match else None
    markup = path.read_text(encoding="utf-8", errors="ignore")
    match = re.search(r"<time[^>]*datetime=\"?(\d{4}-\d{2}-\d{2})", markup)
    return _extract_html_text(path), match.group(1) if match else None


def wording(sentence: str) -> str:
    lowered = sentence.lower()
    if "privately-held" in lowered or "privately held" in lowered:
        return "privately_held_net_marketable"
    if "net marketable" in lowered:
        return "net_marketable"
    return "marketable"


def read_releases(log: pd.DataFrame) -> dict[str, dict[str, object]]:
    releases: dict[str, dict[str, object]] = {}
    for row in log[log["release_quarter"].ne("archive") & log["download_status"].eq("ok")].itertuples():
        path = ROOT / str(row.local_path)
        if _sha256(path) != row.sha256:
            raise SystemExit(f"{path} no longer matches its logged SHA-256")
        text, date = release_text_and_date(path)
        releases[str(row.release_quarter)] = {
            "url": row.url,
            "date": date,
            "retrieved_utc": row.downloaded_at_utc,
            "sha256": row.sha256,
            "local": str(row.local_path),
            "sentences": financing_sentences(text, date),
        }
    return releases


def _single(sentences: list[dict[str, object]], quarter: str, kind: str) -> dict[str, object] | None:
    hits = [s for s in sentences if s["quarter"] == quarter and s["kind"] == kind]
    if len({s["amount_bn"] for s in hits}) > 1:
        raise SystemExit(f"Conflicting {kind} values for {quarter}: {hits}")
    return hits[0] if hits else None


def realized_rows(releases: dict[str, dict[str, object]], quarters: list[str]) -> pd.DataFrame:
    rows = []
    for quarter in quarters:
        release_quarter = qshift(quarter, 1)
        release = releases.get(release_quarter)
        hit = _single(release["sentences"], quarter, "realized") if release else None
        rows.append(
            {
                "quarter": quarter,
                "realized_financing_bn": hit["amount_bn"] if hit else None,
                "status": "captured" if hit else ("no_realized_sentence" if release else "release_not_downloaded"),
                "release_quarter": release_quarter,
                "release_date": release["date"] if release else None,
                "release_url": release["url"] if release else None,
                "quoted_sentence": hit["sentence"] if hit else None,
                "wording": wording(str(hit["sentence"])) if hit else None,
                "retrieved_utc": release["retrieved_utc"] if release else None,
                "doc_sha256": release["sha256"] if release else None,
                "doc_local": release["local"] if release else None,
            }
        )
    return pd.DataFrame(rows, columns=REALIZED_COLUMNS)


def label_old_financing(template: pd.DataFrame, releases: dict[str, dict[str, object]]) -> pd.DataFrame:
    """Name the estimate each template value is: the release in q, in q-1, or q's release for q+1."""
    rows = []
    for row in template.itertuples():
        quarter, old = str(row.quarter), float(row.total_financing_need_bn)
        own_release, prior_release = releases.get(quarter), releases.get(qshift(quarter, -1))
        candidates = {
            "own": _single(own_release["sentences"], quarter, "estimate") if own_release else None,
            "prior": _single(prior_release["sentences"], quarter, "estimate") if prior_release else None,
            "following": _single(own_release["sentences"], qshift(quarter, 1), "estimate") if own_release else None,
        }
        values = {key: (hit["amount_bn"] if hit else None) for key, hit in candidates.items()}
        matched = [key for key, value in values.items() if value is not None and abs(value - old) < 1e-9]
        # The release the template cites first breaks an own/prior tie.
        if len(matched) > 1:
            cited = str(row.financing_source_url).split("|")[0]
            matched = [key for key in matched if (releases.get(quarter if key != "prior" else qshift(quarter, -1)) or {}).get("url") == cited] or matched
        label = "|".join(LABELS[key] for key in matched) if matched else "unmatched"
        source = {"own": own_release, "following": own_release, "prior": prior_release}.get(matched[0]) if len(matched) == 1 else None
        rows.append(
            {
                "quarter": quarter,
                "old_financing_bn": old,
                "old_convention": label,
                "old_source_release_date": source["date"] if source else None,
                "old_source_url": source["url"] if source else str(row.financing_source_url),
                "own_quarter_estimate_bn": values["own"],
                "prior_release_estimate_bn": values["prior"],
                "following_quarter_estimate_in_own_release_bn": values["following"],
            }
        )
    return pd.DataFrame(rows)


def realized_template(template: pd.DataFrame, realized: pd.DataFrame) -> pd.DataFrame:
    out = template.copy()
    by_quarter = realized.set_index("quarter")
    for idx, row in out.iterrows():
        hit = by_quarter.loc[row["quarter"]]
        statement_urls = [u for u in str(row["refunding_statement_source_url"]).split("|") if u]
        statement_docs = [d for d in str(row["refunding_statement_source_doc_local"]).split("|") if d]
        out.at[idx, "total_financing_need_bn"] = f"{float(hit['realized_financing_bn']):g}"
        out.at[idx, "financing_source_url"] = hit["release_url"]
        out.at[idx, "financing_source_doc_local"] = hit["doc_local"]
        out.at[idx, "financing_source_doc_type"] = FINANCING_DOC_TYPE
        out.at[idx, "source_url"] = "|".join([*statement_urls, hit["release_url"], row["auction_reconstruction_source_url"]])
        out.at[idx, "source_doc_local"] = "|".join([*statement_docs, hit["doc_local"], row["auction_reconstruction_source_doc_local"]])
        out.at[idx, "source_doc_type"] = "|".join(
            ["official_quarterly_refunding_statement"] * len(statement_urls) + [FINANCING_DOC_TYPE, row["auction_reconstruction_source_doc_type"]]
        )
        note = _strip_financing_release_note_fragments(str(row["notes"]) if pd.notna(row["notes"]) else "")
        out.at[idx, "notes"] = (
            f"{note} Financing is the realized privately held net marketable borrowing reported in the "
            f"{hit['release_date']} Treasury borrowing-estimate release."
        ).strip()
    return out


def build(
    out_dir: Path,
    template_path: Path,
    auctions: Path,
    mspd: Path,
    log_path: Path,
    realized_path: Path,
    argv: list[str],
) -> int:
    template = pd.read_csv(template_path, dtype=str, keep_default_na=False)
    quarters = template["quarter"].tolist()
    if quarters != quarter_range(quarters[0], quarters[-1]):
        raise SystemExit("Template quarters are not one contiguous run")
    inputs = out_dir / "inputs" / "capture_template_realized.csv"
    audit_path = out_dir / "publish" / "financing_convention_audit.csv"
    for path in (inputs, audit_path, out_dir / "receipt.json"):
        if path.exists():
            raise SystemExit(f"Refusing to replace existing outputs: {path}")

    releases = read_releases(pd.read_csv(log_path, dtype=str))
    realized = realized_rows(releases, quarters)
    write_df(realized, realized_path)
    gaps = realized.loc[realized["status"].ne("captured"), ["quarter", "status"]]
    if not gaps.empty:
        print("Realized borrowing unavailable; no table built:\n" + gaps.to_string(index=False))
        return 1

    write_df(realized_template(template, realized), inputs)
    script_34_args = ["--template", str(inputs), "--auctions", str(auctions), "--mspd", str(mspd), "--out-dir", str(out_dir)]
    status = refresh.main(script_34_args)
    if status:
        return status

    table = pd.read_csv(out_dir / "publish" / "ati_quarter_table.csv")
    audit = (
        label_old_financing(template, releases)
        .merge(realized[["quarter", "realized_financing_bn", "release_date", "release_url", "quoted_sentence", "wording"]], on="quarter")
        .merge(table[["quarter", "net_bills_bn", "ati_baseline_bn"]], on="quarter", validate="one_to_one")
    )
    audit["old_ati_baseline_bn"] = audit["net_bills_bn"] - 0.18 * audit["old_financing_bn"]
    audit["financing_change_bn"] = audit["realized_financing_bn"] - audit["old_financing_bn"]
    write_df(audit, audit_path)

    receipt_path = out_dir / "receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["argv"] = argv
    receipt["realized_financing"] = {
        "definition": "Realized privately held net marketable borrowing for quarter q, quoted from the Treasury "
        "financing release published in quarter q+1; replaces total_financing_need_bn.",
        "script_34_args": script_34_args,
        "template_estimates": refresh._file(template_path),
        "release_download_log": refresh._file(log_path),
        "realized_capture": refresh._file(realized_path),
        "financing_convention_audit": refresh._file(audit_path),
        "validation": {
            "quarters": len(realized),
            "captured": int(realized["status"].eq("captured").sum()),
            "wording_counts": realized["wording"].value_counts().to_dict(),
            "old_convention_counts": audit["old_convention"].value_counts().to_dict(),
            "max_financing_residual_usd_billions": float(
                (table.set_index("quarter")["financing_need_bn"] - realized.set_index("quarter")["realized_financing_bn"]).abs().max()
            ),
        },
    }
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {audit_path} and the realized_financing receipt block")
    return 0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    cap = sub.add_parser("capture", help="Download the archive page and financing releases.")
    cap.add_argument("--out-dir", required=True)
    cap.add_argument("--first", default="2009Q1")
    cap.add_argument("--last", default="2026Q2")
    bld = sub.add_parser("build", help="Extract realized borrowing and rebuild the quarter table.")
    bld.add_argument("--out-dir", required=True)
    bld.add_argument("--template", default=str(MANUAL_DIR / "official_quarterly_refunding_capture_template.csv"))
    bld.add_argument("--auctions", required=True)
    bld.add_argument("--mspd", required=True, help="FiscalData mspd_table_1 CSV extract for the net-bill reconciliation.")
    bld.add_argument("--release-log", default=None, help="Release download log (default: OUT/sources/release_downloads.csv).")
    bld.add_argument("--realized", default=str(DEFAULT_REALIZED), help="Realized capture CSV to write (default: %(default)s).")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "capture":
        return capture(Path(args.out_dir), args.first, args.last)
    argv_record = [refresh._rel(Path(__file__)), *(argv if argv is not None else sys.argv[1:])]
    out_dir = Path(args.out_dir)
    log_path = Path(args.release_log) if args.release_log else out_dir / "sources" / "release_downloads.csv"
    return build(out_dir, Path(args.template), Path(args.auctions), Path(args.mspd), log_path, Path(args.realized), argv_record)


if __name__ == "__main__":
    raise SystemExit(main())
