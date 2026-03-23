from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from urllib.parse import urljoin

import pandas as pd

from .io_utils import coerce_numeric
from .paths import PROJECT_ROOT


CAPTURE_COLUMNS: tuple[str, ...] = (
    "quarter",
    "qra_release_date",
    "market_pricing_marker_minus_1d",
    "total_financing_need_bn",
    "net_bill_issuance_bn",
    "gross_coupon_schedule_bn",
    "net_coupon_issuance_bn",
    "frn_issuance_bn",
    "guidance_nominal_coupons",
    "guidance_frns",
    "guidance_buybacks",
    "financing_source_url",
    "financing_source_doc_local",
    "financing_source_doc_type",
    "refunding_statement_source_url",
    "refunding_statement_source_doc_local",
    "refunding_statement_source_doc_type",
    "auction_reconstruction_source_url",
    "auction_reconstruction_source_doc_local",
    "auction_reconstruction_source_doc_type",
    "source_url",
    "source_doc_local",
    "source_doc_type",
    "qa_status",
    "notes",
)
LEGACY_CAPTURE_COLUMNS: tuple[str, ...] = (
    "quarter",
    "qra_release_date",
    "market_pricing_marker_minus_1d",
    "total_financing_need_bn",
    "net_bill_issuance_bn",
    "gross_coupon_schedule_bn",
    "net_coupon_issuance_bn",
    "frn_issuance_bn",
    "guidance_nominal_coupons",
    "guidance_frns",
    "guidance_buybacks",
    "source_url",
    "source_doc_local",
    "source_doc_type",
    "qa_status",
    "notes",
)

ALLOWED_QA_STATUSES: tuple[str, ...] = (
    "seed_only",
    "manual_official_capture",
    "semi_automated_capture",
    "parser_verified",
)
OFFICIAL_QA_STATUSES = {"manual_official_capture", "parser_verified"}
NUMERIC_COLUMNS: tuple[str, ...] = (
    "total_financing_need_bn",
    "net_bill_issuance_bn",
    "gross_coupon_schedule_bn",
    "net_coupon_issuance_bn",
    "frn_issuance_bn",
)
DATE_COLUMNS: tuple[str, ...] = (
    "qra_release_date",
    "market_pricing_marker_minus_1d",
)
BASE_REQUIRED_COLUMNS: tuple[str, ...] = (
    "quarter",
    "qra_release_date",
    "market_pricing_marker_minus_1d",
    "qa_status",
)
OFFICIAL_REQUIRED_COLUMNS: tuple[str, ...] = (
    "total_financing_need_bn",
    "net_bill_issuance_bn",
    "source_url",
    "source_doc_local",
    "source_doc_type",
)
ROLE_REQUIRED_PREFIXES: tuple[str, ...] = (
    "financing",
    "refunding_statement",
    "auction_reconstruction",
)

_QUARTER_RE = re.compile(r"^\d{4}Q[1-4]$")
_BORROWING_SENTENCE_RE = re.compile(
    r"During the (?P<period>.+?)\s+quarter, Treasury expects to (?:borrow|issue) \$"
    r"(?P<amount>[0-9.]+)\s+(?P<unit>trillion|billion)",
    flags=re.IGNORECASE,
)
_BORROWING_RELEASE_TITLE = "Treasury Announces Marketable Borrowing Estimates"
_QUARTER_LABEL_RE = re.compile(
    r"(?P<year>\d{4})\s+(?P<quarter>[1-4])(?:st|nd|rd|th)\s+Quarter",
    flags=re.IGNORECASE,
)
_QUARTER_ONLY_LABEL_RE = re.compile(
    r"(?P<quarter>[1-4])(?:st|nd|rd|th)\s+Quarter",
    flags=re.IGNORECASE,
)
_REFUNDING_RANGE_RE = re.compile(
    r"anticipated auction sizes for the (?P<range>[A-Za-z]+\s+to\s+[A-Za-z]+\s+\d{4}) quarter",
    flags=re.IGNORECASE,
)
_REFUNDING_RANGE_WITH_YEARS_RE = re.compile(
    r"anticipated auction sizes(?: in billions of dollars)? for the "
    r"(?P<range>[A-Za-z]+\s+\d{4}\s*[–-]\s*[A-Za-z]+\s+\d{4}) quarter",
    flags=re.IGNORECASE,
)
_MONTH_ROW_RE = re.compile(
    r"(?P<month>(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{2})\s+"
    r"(?P<two>\d+)\s+(?P<three>\d+)\s+(?P<five>\d+)\s+(?P<seven>\d+)\s+"
    r"(?P<ten>\d+)\s+(?P<twenty>\d+)\s+(?P<thirty>\d+)\s+(?P<frn>\d+)",
    flags=re.IGNORECASE,
)
_SENTENCE_RE = re.compile(r"[^.?!]+[.?!]")
_FISCALDATA_AUCTIONS_URL = (
    "https://fiscaldata.treasury.gov/datasets/treasury-securities-auctions-data/auctions-query"
)
_AUCTION_RECON_SOURCE_DOC_TYPE = "official_auction_reconstruction"
_AUCTION_COMPLETION_EXACT = "exact_official_numeric"
_AUCTION_COMPLETION_PROMOTED = "promoted_official_numeric"
_AUCTION_COMPLETION_SEMI = "semi_automated_guidance_only"
_AUCTION_COMPLETION_SEED = "seed_assisted"
_AUCTION_COMPLETION_OTHER = "other"

AUCTION_REQUIRED_COLUMNS: tuple[str, ...] = (
    "issue_date",
    "security_type",
    "offering_amt",
    "est_pub_held_mat_by_type_amt",
)


@dataclass(frozen=True)
class CaptureBuildResult:
    dataframe: pd.DataFrame
    seeded_rows_added: int


def read_capture_template(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Capture template not found: {path}")
    template = pd.read_csv(path, dtype=str).fillna("")
    return _coerce_capture_contract(template)


def read_qra_event_seed(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"QRA event seed not found: {path}")
    return pd.read_csv(path, dtype=str).fillna("")


def read_quarterly_refunding_seed(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Quarterly refunding seed not found: {path}")
    return pd.read_csv(path, dtype=str).fillna("")


def quarter_from_event_seed_row(event: pd.Series) -> str:
    explicit_quarter = _string_or_empty(event.get("quarter"))
    if explicit_quarter and _QUARTER_RE.fullmatch(explicit_quarter):
        return explicit_quarter

    release_date = _string_or_empty(event.get("official_release_date"))
    if not release_date:
        return ""
    return next_quarter_from_release_date(release_date)


def next_quarter_from_release_date(release_date: str) -> str:
    timestamp = pd.to_datetime(release_date, errors="raise")
    current_quarter = (int(timestamp.month) - 1) // 3 + 1
    if current_quarter == 4:
        return f"{int(timestamp.year) + 1}Q1"
    return f"{int(timestamp.year)}Q{current_quarter + 1}"


def seed_capture_rows_from_local_sources(
    qra_event_seed: pd.DataFrame,
    quarterly_refunding_seed: pd.DataFrame,
) -> pd.DataFrame:
    if qra_event_seed.empty:
        return pd.DataFrame(columns=CAPTURE_COLUMNS)

    quarterly_cols = {"quarter", "financing_need_bn", "net_bills_bn"}
    missing_q_cols = sorted(quarterly_cols - set(quarterly_refunding_seed.columns))
    if missing_q_cols:
        raise ValueError(
            "Quarterly refunding seed is missing required columns: "
            + ", ".join(missing_q_cols)
        )

    qra_cols = {"official_release_date", "market_pricing_marker_minus_1d"}
    missing_e_cols = sorted(qra_cols - set(qra_event_seed.columns))
    if missing_e_cols:
        raise ValueError(
            "QRA event seed is missing required columns: " + ", ".join(missing_e_cols)
        )

    financing_by_quarter = (
        quarterly_refunding_seed[["quarter", "financing_need_bn", "net_bills_bn"]]
        .copy()
        .set_index("quarter")
    )

    rows: list[dict[str, str]] = []
    for _, event in qra_event_seed.iterrows():
        release_date = str(event["official_release_date"]).strip()
        t_minus_1 = str(event["market_pricing_marker_minus_1d"]).strip()
        if not release_date or not t_minus_1:
            continue

        quarter = quarter_from_event_seed_row(event)
        if not quarter:
            continue
        financing_need = ""
        net_bills = ""
        seed_comments = "Seeded from local seed CSVs; replace with official quarter capture."
        if quarter in financing_by_quarter.index:
            financing_need = str(financing_by_quarter.loc[quarter, "financing_need_bn"]).strip()
            net_bills = str(financing_by_quarter.loc[quarter, "net_bills_bn"]).strip()
        else:
            seed_comments = (
                "Seeded from qra_event_seed only; quarterly_refunding_seed currently has no matching quarter."
            )

        row = {col: "" for col in CAPTURE_COLUMNS}
        row["quarter"] = quarter
        row["qra_release_date"] = release_date
        row["market_pricing_marker_minus_1d"] = t_minus_1
        row["total_financing_need_bn"] = financing_need
        row["net_bill_issuance_bn"] = net_bills
        row["source_doc_local"] = "data/manual/qra_event_seed.csv|data/manual/quarterly_refunding_seed.csv"
        row["source_doc_type"] = "seed_csv"
        row["qa_status"] = "seed_only"
        row["notes"] = seed_comments
        rows.append(row)

    seeded = pd.DataFrame(rows, columns=CAPTURE_COLUMNS)
    seeded = seeded.sort_values(["quarter", "qra_release_date"], kind="stable").drop_duplicates(
        subset=["quarter"], keep="last"
    )
    return seeded.reset_index(drop=True)


def build_official_capture(
    template_df: pd.DataFrame,
    *,
    qra_event_seed_df: pd.DataFrame | None = None,
    quarterly_refunding_seed_df: pd.DataFrame | None = None,
    seed_missing_quarters: bool = False,
) -> CaptureBuildResult:
    capture = _coerce_capture_contract(template_df.copy())
    _validate_columns(capture)
    capture = _normalize_capture(capture)

    seeded_rows_added = 0
    if seed_missing_quarters:
        if qra_event_seed_df is None or quarterly_refunding_seed_df is None:
            raise ValueError(
                "seed_missing_quarters=True requires qra_event_seed_df and quarterly_refunding_seed_df."
            )
        seeded = seed_capture_rows_from_local_sources(
            qra_event_seed=qra_event_seed_df,
            quarterly_refunding_seed=quarterly_refunding_seed_df,
        )
        if not seeded.empty:
            existing_quarters = set(capture["quarter"].dropna().astype(str))
            to_add = seeded[~seeded["quarter"].isin(existing_quarters)].copy()
            seeded_rows_added = len(to_add)
            if seeded_rows_added:
                capture = pd.concat([capture, to_add], ignore_index=True)
                capture = _normalize_capture(capture)

    _validate_rows(capture)
    capture = _sort_capture(capture)
    return CaptureBuildResult(dataframe=capture, seeded_rows_added=seeded_rows_added)


def build_ati_input_from_official_capture(df: pd.DataFrame) -> pd.DataFrame:
    required = ["quarter", "total_financing_need_bn", "net_bill_issuance_bn", "qa_status", "source_doc_local"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Official capture is missing columns required for ATI rebuild: {missing}")

    out = df.copy()
    out["completion_tier"] = out.apply(_completion_tier_for_row, axis=1)
    out = out.rename(
        columns={
            "total_financing_need_bn": "financing_need_bn",
            "net_bill_issuance_bn": "net_bills_bn",
        }
    )
    keep = [
        "quarter",
        "financing_need_bn",
        "net_bills_bn",
        "qa_status",
        "source_doc_local",
        "source_url",
        "source_doc_type",
        "notes",
        "completion_tier",
    ]
    for col in keep:
        if col not in out.columns:
            out[col] = pd.NA
    out = out[keep].copy()
    out = out[
        out["completion_tier"].isin({_AUCTION_COMPLETION_EXACT, _AUCTION_COMPLETION_PROMOTED})
    ].copy()
    for col in ["quarter", "financing_need_bn", "net_bills_bn"]:
        out[col] = out[col].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    out = out.dropna(subset=["quarter", "financing_need_bn", "net_bills_bn"])
    out["capture_quality"] = out["qa_status"]
    out["capture_source"] = out["source_doc_local"]
    return out.reset_index(drop=True)


def build_financing_release_source_map(
    qra_event_seed_df: pd.DataFrame,
    qra_downloads_df: pd.DataFrame,
    text_dir: Path,
) -> pd.DataFrame:
    required_event_cols = {"official_release_date"}
    missing_event_cols = sorted(required_event_cols - set(qra_event_seed_df.columns))
    if missing_event_cols:
        raise ValueError(f"QRA event seed is missing required columns: {missing_event_cols}")

    required_download_cols = {"href", "local_path"}
    missing_download_cols = sorted(required_download_cols - set(qra_downloads_df.columns))
    if missing_download_cols:
        raise ValueError(f"QRA downloads are missing required columns: {missing_download_cols}")

    rows: list[dict[str, object]] = []
    for _, event in qra_event_seed_df.iterrows():
        release_date = _string_or_empty(event.get("official_release_date"))
        if not release_date:
            continue

        release_ts = pd.to_datetime(release_date, errors="raise")
        quarter = quarter_from_event_seed_row(event)
        if not quarter:
            continue
        release_date_phrase = _human_date(release_ts)
        expected_period = _quarter_period_label(quarter)
        matched_row: dict[str, object] | None = None

        for _, download in qra_downloads_df.iterrows():
            raw_local_path = _string_or_empty(download.get("local_path"))
            if not raw_local_path:
                continue

            local_path = _normalize_repo_local_reference(raw_local_path)
            download_quarter = _string_or_empty(download.get("quarter"))
            text_path = text_dir / f"{Path(raw_local_path).stem}.txt"
            if text_path.exists():
                text = text_path.read_text(encoding="utf-8", errors="ignore")
            else:
                resolved_local_path = _resolve_repo_local_path(raw_local_path)
                if not resolved_local_path.exists():
                    continue
                text = _extract_html_text(resolved_local_path)
            if not text:
                continue
            if _BORROWING_RELEASE_TITLE not in text:
                continue
            if download_quarter != quarter and release_date_phrase not in text:
                continue

            matched_row = {
                "quarter": quarter,
                "qra_release_date": release_date,
                "official_release_date_phrase": release_date_phrase,
                "expected_period": expected_period,
                "financing_source_url": _string_or_empty(download.get("href")),
                "financing_source_doc_local": local_path,
                "financing_source_doc_type": _string_or_empty(download.get("doc_type")) or "quarterly_refunding_press_release",
                "source_url": _string_or_empty(download.get("href")),
                "source_doc_local": local_path,
                "source_doc_type": _string_or_empty(download.get("doc_type")) or "quarterly_refunding_press_release",
                "announced_borrowing_bn": _extract_borrowing_amount_for_quarter(text, quarter),
                "match_status": "matched",
            }
            break

        if matched_row is None:
            matched_row = {
                "quarter": quarter,
                "qra_release_date": release_date,
                "official_release_date_phrase": release_date_phrase,
                "expected_period": expected_period,
                "financing_source_url": pd.NA,
                "financing_source_doc_local": pd.NA,
                "financing_source_doc_type": pd.NA,
                "source_url": pd.NA,
                "source_doc_local": pd.NA,
                "source_doc_type": pd.NA,
                "announced_borrowing_bn": pd.NA,
                "match_status": "missing",
            }

        rows.append(matched_row)

    columns = [
        "quarter",
        "qra_release_date",
        "official_release_date_phrase",
        "expected_period",
        "financing_source_url",
        "financing_source_doc_local",
        "financing_source_doc_type",
        "source_url",
        "source_doc_local",
        "source_doc_type",
        "announced_borrowing_bn",
        "match_status",
    ]
    return pd.DataFrame(rows, columns=columns)


def build_refunding_statement_manifest(
    capture_df: pd.DataFrame,
    archive_html_paths: list[Path],
) -> pd.DataFrame:
    required_cols = {"quarter"}
    missing = sorted(required_cols - set(capture_df.columns))
    if missing:
        raise ValueError(f"Official capture is missing required columns for statement manifest: {missing}")

    wanted_quarters = {
        _string_or_empty(value)
        for value in capture_df["quarter"].tolist()
        if _string_or_empty(value)
    }
    rows: list[dict[str, object]] = []
    seen_quarters: set[str] = set()
    source_page = (
        "https://home.treasury.gov/policy-issues/financing-the-government/"
        "quarterly-refunding/official-remarks-on-quarterly-refunding-by-calendar-year"
    )

    for archive_html_path in archive_html_paths:
        if not archive_html_path.exists():
            continue
        for quarter, href, label in _parse_quarter_archive_links(archive_html_path).values():
            if quarter not in wanted_quarters or quarter in seen_quarters:
                continue
            rows.append(
                {
                    "quarter": quarter,
                    "text": label,
                    "href": href,
                    "doc_type": "official_quarterly_refunding_statement",
                    "source_page": source_page,
                    "archive_html_local": str(archive_html_path),
                }
            )
            seen_quarters.add(quarter)

    columns = ["quarter", "text", "href", "doc_type", "source_page", "archive_html_local"]
    return pd.DataFrame(rows, columns=columns).sort_values("quarter", kind="stable").reset_index(drop=True)


def build_refunding_statement_source_map(statement_downloads_df: pd.DataFrame) -> pd.DataFrame:
    required_cols = {"quarter", "href", "local_path"}
    missing = sorted(required_cols - set(statement_downloads_df.columns))
    if missing:
        raise ValueError(f"Refunding statement downloads are missing required columns: {missing}")

    rows: list[dict[str, object]] = []
    for _, download in statement_downloads_df.iterrows():
        quarter = _string_or_empty(download.get("quarter"))
        raw_local_path = _string_or_empty(download.get("local_path"))
        if not quarter or not raw_local_path:
            continue
        resolved_local_path = _resolve_repo_local_path(raw_local_path)
        if not resolved_local_path.exists():
            continue
        local_path = _normalize_repo_local_reference(raw_local_path)

        text = _extract_html_text(resolved_local_path)
        projected_financing_section = _extract_projected_financing_section(text)
        nominal_section = _extract_named_section(
            text,
            "NOMINAL COUPON AND FRN FINANCING",
            ("TIPS FINANCING", "BILL ISSUANCE", "BUYBACKS"),
        )
        if not nominal_section:
            nominal_section = projected_financing_section
        bill_section = _extract_named_section(
            text,
            "BILL ISSUANCE",
            ("6-WEEK BILL BENCHMARK STATUS", "INTRODUCTION OF THE 6-WEEK BILL BENCHMARK", "BUYBACKS"),
        )
        six_week_section = _extract_named_section(
            text,
            "6-WEEK BILL BENCHMARK STATUS",
            ("BUYBACKS",),
        ) or _extract_named_section(
            text,
            "INTRODUCTION OF THE 6-WEEK BILL BENCHMARK",
            ("BUYBACKS",),
        )
        buybacks_section = _extract_named_section(
            text,
            "BUYBACKS",
            ("LARGE POSITION REPORT (LPR) CALL", "Please send comments or suggestions"),
        ) or _extract_named_section(
            text,
            "BUYBACK OUTREACH",
            ("ADDITIONAL PUBLIC TRANSPARENCY", "SMALL-VALUE BUYBACK OPERATION", "Please send comments or suggestions"),
        )
        bill_guidance = _merge_note(
            _clean_section_text(bill_section),
            _clean_section_text(six_week_section),
        )
        if not bill_guidance and projected_financing_section:
            bill_guidance = _extract_bill_guidance(projected_financing_section)
        if "regular bill auction sizes" not in bill_guidance.lower():
            bill_guidance = _merge_note(bill_guidance, _extract_bill_guidance(text))

        rows.append(
            {
                "quarter": quarter,
                "refunding_statement_source_url": _string_or_empty(download.get("href")),
                "refunding_statement_source_doc_local": local_path,
                "refunding_statement_source_doc_type": _string_or_empty(download.get("doc_type"))
                or "official_quarterly_refunding_statement",
                "source_url": _string_or_empty(download.get("href")),
                "source_doc_local": local_path,
                "source_doc_type": _string_or_empty(download.get("doc_type"))
                or "official_quarterly_refunding_statement",
                "guidance_nominal_coupons": _extract_nominal_coupon_guidance(nominal_section),
                "guidance_frns": _extract_frn_guidance(nominal_section),
                "guidance_buybacks": _clean_section_text(buybacks_section),
                "bill_guidance": bill_guidance,
                "match_status": "matched",
            }
        )

    columns = [
        "quarter",
        "refunding_statement_source_url",
        "refunding_statement_source_doc_local",
        "refunding_statement_source_doc_type",
        "source_url",
        "source_doc_local",
        "source_doc_type",
        "guidance_nominal_coupons",
        "guidance_frns",
        "guidance_buybacks",
        "bill_guidance",
        "match_status",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)
    return (
        pd.DataFrame(rows, columns=columns)
        .sort_values("quarter", kind="stable")
        .drop_duplicates(subset=["quarter"], keep="first")
        .reset_index(drop=True)
    )


def build_quarter_net_issuance_from_auctions(
    auctions_df: pd.DataFrame,
    *,
    quarters: list[str] | None = None,
) -> pd.DataFrame:
    missing = sorted(set(AUCTION_REQUIRED_COLUMNS) - set(auctions_df.columns))
    if missing:
        raise ValueError(
            "Auctions reconstruction is missing required columns: "
            + ", ".join(missing)
        )

    out = auctions_df.copy()
    out["issue_date"] = pd.to_datetime(out["issue_date"], errors="coerce")
    out["offering_amt"] = coerce_numeric(out["offering_amt"])
    out["est_pub_held_mat_by_type_amt"] = coerce_numeric(out["est_pub_held_mat_by_type_amt"])
    out["bucket"] = out.apply(_classify_auction_bucket, axis=1)
    out = out.dropna(subset=["issue_date", "offering_amt"])
    out["quarter"] = out["issue_date"].dt.to_period("Q").astype(str)
    if quarters:
        wanted = {_string_or_empty(value) for value in quarters if _string_or_empty(value)}
        out = out[out["quarter"].isin(wanted)]

    by_date = (
        out.groupby(["quarter", "bucket", "issue_date"], as_index=False, dropna=False)
        .agg(
            offering_amt=("offering_amt", "sum"),
            est_pub_held_mat_by_type_amt=("est_pub_held_mat_by_type_amt", "min"),
            cmb_only_issue_date=("cash_management_bill_cmb", lambda series: bool(series.fillna("").eq("Yes").all())),
        )
        .sort_values(["quarter", "bucket", "issue_date"], kind="stable")
    )
    by_date["issue_date_has_maturity_estimate"] = by_date["est_pub_held_mat_by_type_amt"].notna()
    by_date["net_issuance_amt"] = (
        by_date["offering_amt"] - by_date["est_pub_held_mat_by_type_amt"]
    )

    quarter_bucket = (
        by_date.groupby(["quarter", "bucket"], as_index=False)
        .agg(
            gross_offering_amt=("offering_amt", "sum"),
            maturing_estimate_amt=("est_pub_held_mat_by_type_amt", "sum"),
            net_issuance_amt=("net_issuance_amt", "sum"),
            issue_dates=("issue_date", "nunique"),
            issue_dates_missing_maturing_estimate=(
                "issue_date_has_maturity_estimate",
                lambda series: int((~series).sum()),
            ),
        )
        .sort_values(["quarter", "bucket"], kind="stable")
    )
    cmb_only_missing = (
        by_date.loc[~by_date["issue_date_has_maturity_estimate"]]
        .groupby(["quarter", "bucket"], as_index=False)
        .agg(
            issue_dates_cmb_only_missing_maturity=(
                "cmb_only_issue_date",
                lambda series: int(series.fillna(False).sum()),
            )
        )
    )
    quarter_bucket = quarter_bucket.merge(cmb_only_missing, on=["quarter", "bucket"], how="left")
    quarter_bucket["issue_dates_cmb_only_missing_maturity"] = (
        quarter_bucket["issue_dates_cmb_only_missing_maturity"].fillna(0).astype(int)
    )
    quarter_bucket["gross_offering_bn"] = quarter_bucket["gross_offering_amt"] / 1e9
    quarter_bucket["maturing_estimate_bn"] = quarter_bucket["maturing_estimate_amt"] / 1e9
    quarter_bucket["net_issuance_bn"] = quarter_bucket["net_issuance_amt"] / 1e9
    quarter_bucket["reconstruction_status"] = quarter_bucket.apply(
        lambda row: (
            "complete"
            if int(row["issue_dates_missing_maturing_estimate"])
            == int(row["issue_dates_cmb_only_missing_maturity"])
            else "partial"
        ),
        axis=1,
    )

    return quarter_bucket[
        [
            "quarter",
            "bucket",
            "gross_offering_bn",
            "maturing_estimate_bn",
            "net_issuance_bn",
            "issue_dates",
            "issue_dates_missing_maturing_estimate",
            "issue_dates_cmb_only_missing_maturity",
            "reconstruction_status",
        ]
    ].reset_index(drop=True)


def enrich_capture_with_auction_reconstruction(
    capture_df: pd.DataFrame,
    quarter_net_df: pd.DataFrame,
) -> pd.DataFrame:
    capture = _coerce_capture_contract(capture_df.copy().astype(object))
    _validate_columns(capture)
    if quarter_net_df.empty:
        return capture

    required = {"quarter", "bucket", "net_issuance_bn", "gross_offering_bn", "reconstruction_status"}
    missing = sorted(required - set(quarter_net_df.columns))
    if missing:
        raise ValueError(
            "Quarter-net reconstruction is missing required columns: " + ", ".join(missing)
        )

    pivot = quarter_net_df.copy()
    if "issue_dates_cmb_only_missing_maturity" not in pivot.columns:
        pivot["issue_dates_cmb_only_missing_maturity"] = 0
    pivot["quarter"] = pivot["quarter"].astype(str)
    pivot = pivot.pivot_table(
        index="quarter",
        columns="bucket",
        values=[
            "net_issuance_bn",
            "gross_offering_bn",
            "reconstruction_status",
            "issue_dates_cmb_only_missing_maturity",
        ],
        aggfunc="first",
    )
    pivot.columns = ["__".join(str(part) for part in col) for col in pivot.columns]
    pivot = pivot.reset_index().set_index("quarter")

    for idx, row in capture.iterrows():
        quarter = _string_or_empty(row.get("quarter"))
        if not quarter or quarter not in pivot.index:
            continue
        source = pivot.loc[quarter]
        bill_net = source.get("net_issuance_bn__bill_like")
        bill_status = _string_or_empty(source.get("reconstruction_status__bill_like"))
        has_complete_bill_reconstruction = bill_status == "complete" and pd.notna(bill_net)
        if pd.notna(bill_net):
            capture.at[idx, "net_bill_issuance_bn"] = f"{float(bill_net):g}"

        existing_url = _string_or_empty(capture.at[idx, "source_url"])
        capture.at[idx, "source_url"] = _merge_pipe_values(_FISCALDATA_AUCTIONS_URL, existing_url)
        existing_local = _string_or_empty(capture.at[idx, "source_doc_local"])
        capture.at[idx, "source_doc_local"] = _merge_pipe_values(
            "data/raw/fiscaldata/auctions_query.csv", existing_local
        )
        existing_type = _string_or_empty(capture.at[idx, "source_doc_type"])
        capture.at[idx, "source_doc_type"] = _merge_pipe_values(
            _AUCTION_RECON_SOURCE_DOC_TYPE, existing_type
        )
        existing_auction_url = _string_or_empty(capture.at[idx, "auction_reconstruction_source_url"])
        capture.at[idx, "auction_reconstruction_source_url"] = _merge_pipe_values(
            _FISCALDATA_AUCTIONS_URL, existing_auction_url
        )
        existing_auction_local = _string_or_empty(
            capture.at[idx, "auction_reconstruction_source_doc_local"]
        )
        capture.at[idx, "auction_reconstruction_source_doc_local"] = _merge_pipe_values(
            "data/raw/fiscaldata/auctions_query.csv", existing_auction_local
        )
        existing_auction_type = _string_or_empty(
            capture.at[idx, "auction_reconstruction_source_doc_type"]
        )
        capture.at[idx, "auction_reconstruction_source_doc_type"] = _merge_pipe_values(
            _AUCTION_RECON_SOURCE_DOC_TYPE, existing_auction_type
        )

        if (
            has_complete_bill_reconstruction
            and _has_official_financing_provenance(capture.loc[idx])
            and _has_official_refunding_statement_provenance(capture.loc[idx])
            and _has_official_auction_reconstruction_provenance(capture.loc[idx])
        ):
            capture.at[idx, "source_doc_local"] = _drop_seed_provenance(
                _string_or_empty(capture.at[idx, "source_doc_local"])
            )
            capture.at[idx, "source_doc_type"] = _drop_seed_provenance(
                _string_or_empty(capture.at[idx, "source_doc_type"])
            )
            capture.at[idx, "qa_status"] = "manual_official_capture"

        note = (
            "Exact net bill issuance reconstructed from official auctions feed "
            f"(bill={_format_optional_float(bill_net)} bn). "
            "Non-bill auction buckets remain support-only because maturity estimates are not "
            "bucket-separable at the quarter level."
        )
        cmb_only_missing = pd.to_numeric(
            pd.Series([source.get("issue_dates_cmb_only_missing_maturity__bill_like")]),
            errors="coerce",
        ).iloc[0]
        if pd.notna(cmb_only_missing) and int(cmb_only_missing) > 0:
            note = _merge_note(
                note,
                "CMB-only issue dates without same-day maturity estimates in the auctions feed are "
                "treated as non-blocking for quarter-level bill reconstruction.",
            )
        capture.at[idx, "notes"] = _merge_note(_string_or_empty(capture.at[idx, "notes"]), note)
    return capture


def build_capture_completion_status(
    capture_df: pd.DataFrame,
    quarter_net_df: pd.DataFrame,
) -> pd.DataFrame:
    capture = _coerce_capture_contract(capture_df.copy())
    _validate_columns(capture)
    recon = quarter_net_df.copy()
    if recon.empty:
        recon = pd.DataFrame(
            columns=[
                "quarter",
                "bucket",
                "gross_offering_bn",
                "maturing_estimate_bn",
                "net_issuance_bn",
                "issue_dates_missing_maturing_estimate",
                "issue_dates_cmb_only_missing_maturity",
                "reconstruction_status",
            ]
        )
    if "bucket" in recon.columns:
        recon_bill = recon.loc[recon["bucket"] == "bill_like"].copy()
    else:
        recon_bill = pd.DataFrame(columns=recon.columns)
    if "issue_dates_cmb_only_missing_maturity" not in recon_bill.columns:
        recon_bill["issue_dates_cmb_only_missing_maturity"] = 0
    recon_bill = recon_bill.drop_duplicates(subset=["quarter"], keep="first")
    recon_bill = recon_bill.rename(
        columns={
            "net_issuance_bn": "reconstructed_net_bill_issuance_bn",
            "reconstruction_status": "reconstruction_status_bill",
            "issue_dates_missing_maturing_estimate": "issue_dates_missing_maturity_bill",
            "issue_dates_cmb_only_missing_maturity": "issue_dates_cmb_only_missing_maturity_bill",
        }
    )
    merged = capture.merge(
        recon_bill[
            [
                "quarter",
                "reconstructed_net_bill_issuance_bn",
                "reconstruction_status_bill",
                "issue_dates_missing_maturity_bill",
                "issue_dates_cmb_only_missing_maturity_bill",
            ]
        ],
        on="quarter",
        how="left",
    )

    rows: list[dict[str, object]] = []
    for _, row in merged.iterrows():
        completion_tier = _completion_tier_for_row(row)
        net_bill = _float_or_na(row.get("net_bill_issuance_bn"))
        reconstructed = _float_or_na(row.get("reconstructed_net_bill_issuance_bn"))
        rows.append(
            {
                "quarter": _string_or_empty(row.get("quarter")),
                "completion_tier": completion_tier,
                "qa_status": _string_or_empty(row.get("qa_status")),
                "source_doc_type": _string_or_empty(row.get("source_doc_type")),
                "uses_seed_source": _uses_seed_source(row),
                "net_bill_issuance_bn": net_bill if pd.notna(net_bill) else pd.NA,
                "reconstructed_net_bill_issuance_bn": reconstructed if pd.notna(reconstructed) else pd.NA,
                "net_bill_diff_vs_reconstruction_bn": (
                    net_bill - reconstructed if pd.notna(net_bill) and pd.notna(reconstructed) else pd.NA
                ),
                "reconstruction_status_bill": _string_or_empty(row.get("reconstruction_status_bill")),
                "issue_dates_missing_maturity_bill": row.get("issue_dates_missing_maturity_bill", pd.NA),
                "issue_dates_cmb_only_missing_maturity_bill": row.get(
                    "issue_dates_cmb_only_missing_maturity_bill", pd.NA
                ),
                "is_headline_ready": completion_tier in {
                    _AUCTION_COMPLETION_EXACT,
                    _AUCTION_COMPLETION_PROMOTED,
                },
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values("quarter", kind="stable").reset_index(drop=True)


def enrich_capture_with_financing_release_map(
    capture_df: pd.DataFrame,
    financing_release_map_df: pd.DataFrame,
) -> pd.DataFrame:
    capture = _coerce_capture_contract(capture_df.copy().astype(object))
    _validate_columns(capture)
    if financing_release_map_df.empty:
        return capture

    release_map = financing_release_map_df.copy()
    release_map["quarter"] = release_map["quarter"].astype(str)
    release_map = release_map.drop_duplicates(subset=["quarter"], keep="first").set_index("quarter")

    for idx, row in capture.iterrows():
        quarter = _string_or_empty(row.get("quarter"))
        if not quarter or quarter not in release_map.index:
            continue

        source = release_map.loc[quarter]
        if _string_or_empty(source.get("match_status")) != "matched":
            continue

        source_url = (
            _string_or_empty(source.get("financing_source_url"))
            or _string_or_empty(source.get("source_url"))
        )
        source_doc_local = (
            _string_or_empty(source.get("financing_source_doc_local"))
            or _string_or_empty(source.get("source_doc_local"))
        )
        source_doc_type = (
            _string_or_empty(source.get("financing_source_doc_type"))
            or _string_or_empty(source.get("source_doc_type"))
        )
        announced_borrowing_bn = source.get("announced_borrowing_bn")

        if source_url:
            existing_url = _string_or_empty(capture.at[idx, "financing_source_url"])
            capture.at[idx, "financing_source_url"] = _merge_pipe_values(source_url, existing_url)
        if source_doc_local:
            existing_local = _string_or_empty(capture.at[idx, "financing_source_doc_local"])
            capture.at[idx, "financing_source_doc_local"] = _merge_pipe_values(source_doc_local, existing_local)
        if source_doc_type:
            existing_type = _string_or_empty(capture.at[idx, "financing_source_doc_type"])
            capture.at[idx, "financing_source_doc_type"] = _merge_pipe_values(source_doc_type, existing_type)

        # Preserve legacy aggregated provenance fields for one transition round.
        if source_url:
            existing_url = _string_or_empty(capture.at[idx, "source_url"])
            capture.at[idx, "source_url"] = _merge_pipe_values(source_url, existing_url)
        if source_doc_local:
            existing_local = _string_or_empty(capture.at[idx, "source_doc_local"])
            capture.at[idx, "source_doc_local"] = _merge_pipe_values(source_doc_local, existing_local)
        if source_doc_type:
            existing_type = _string_or_empty(capture.at[idx, "source_doc_type"])
            capture.at[idx, "source_doc_type"] = _merge_pipe_values(source_doc_type, existing_type)
        if pd.notna(announced_borrowing_bn):
            capture.at[idx, "total_financing_need_bn"] = f"{float(announced_borrowing_bn):g}"

        if _string_or_empty(capture.at[idx, "qa_status"]) == "seed_only":
            capture.at[idx, "qa_status"] = "semi_automated_capture"

        note = (
            f"Official borrowing estimate matched to "
            f"{source.get('official_release_date_phrase')} Treasury borrowing-estimate release."
        )
        existing_notes = _strip_financing_release_note_fragments(
            _string_or_empty(capture.at[idx, "notes"])
        )
        capture.at[idx, "notes"] = _merge_note(existing_notes, note)

    return capture


def enrich_capture_with_refunding_statement_map(
    capture_df: pd.DataFrame,
    statement_map_df: pd.DataFrame,
) -> pd.DataFrame:
    capture = _coerce_capture_contract(capture_df.copy().astype(object))
    _validate_columns(capture)
    if statement_map_df.empty:
        return capture

    statement_map = statement_map_df.copy()
    statement_map["quarter"] = statement_map["quarter"].astype(str)
    statement_map = statement_map.drop_duplicates(subset=["quarter"], keep="first").set_index("quarter")

    for idx, row in capture.iterrows():
        quarter = _string_or_empty(row.get("quarter"))
        if not quarter or quarter not in statement_map.index:
            continue

        source = statement_map.loc[quarter]
        if _string_or_empty(source.get("match_status")) != "matched":
            continue

        for source_col, target_col in (
            ("refunding_statement_source_url", "refunding_statement_source_url"),
            ("source_url", "refunding_statement_source_url"),
            ("refunding_statement_source_doc_local", "refunding_statement_source_doc_local"),
            ("source_doc_local", "refunding_statement_source_doc_local"),
            ("refunding_statement_source_doc_type", "refunding_statement_source_doc_type"),
            ("source_doc_type", "refunding_statement_source_doc_type"),
        ):
            incoming = _string_or_empty(source.get(source_col))
            if not incoming:
                continue
            existing = _string_or_empty(capture.at[idx, target_col])
            capture.at[idx, target_col] = _merge_pipe_values(incoming, existing)

        # Preserve legacy aggregated provenance fields for one transition round.
        for col in ("source_url", "source_doc_local", "source_doc_type"):
            incoming = _string_or_empty(source.get(col))
            if not incoming:
                continue
            existing = _string_or_empty(capture.at[idx, col])
            capture.at[idx, col] = _merge_pipe_values(incoming, existing)

        for col in ("guidance_nominal_coupons", "guidance_frns", "guidance_buybacks"):
            incoming = _string_or_empty(source.get(col))
            if incoming:
                capture.at[idx, col] = incoming

        if _string_or_empty(capture.at[idx, "qa_status"]) == "seed_only":
            capture.at[idx, "qa_status"] = "semi_automated_capture"

        note_parts = ["Official quarterly refunding statement matched for issuance guidance."]
        bill_guidance = _string_or_empty(source.get("bill_guidance"))
        if bill_guidance:
            note_parts.append(f"Bill guidance: {bill_guidance}")
        capture.at[idx, "notes"] = _merge_note(_string_or_empty(capture.at[idx, "notes"]), " ".join(note_parts))

    return capture


def _coerce_capture_contract(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy().astype(object)
    actual = list(out.columns)
    expected = list(CAPTURE_COLUMNS)
    legacy_expected = list(LEGACY_CAPTURE_COLUMNS)
    if actual == expected:
        return _hydrate_role_provenance_from_legacy(out[expected].copy())
    if actual == legacy_expected:
        for col in CAPTURE_COLUMNS:
            if col not in out.columns:
                out[col] = pd.NA
        return _hydrate_role_provenance_from_legacy(out[expected].copy())

    missing = [col for col in expected if col not in actual]
    extra = [col for col in actual if col not in expected]
    raise ValueError(
        "Official QRA capture columns do not match required contract. "
        f"Missing={missing} Extra={extra} ExpectedOrder={expected}"
    )


def _hydrate_role_provenance_from_legacy(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy().astype(object)
    for idx in out.index:
        out = _hydrate_role_from_legacy(
            out,
            idx,
            prefix="financing",
            matcher=_is_financing_provenance_entry,
        )
        out = _hydrate_role_from_legacy(
            out,
            idx,
            prefix="refunding_statement",
            matcher=_is_refunding_statement_provenance_entry,
        )
        out = _hydrate_role_from_legacy(
            out,
            idx,
            prefix="auction_reconstruction",
            matcher=_is_auction_reconstruction_provenance_entry,
        )
    return out


def _hydrate_role_from_legacy(
    df: pd.DataFrame,
    row_idx: int,
    *,
    prefix: str,
    matcher,
) -> pd.DataFrame:
    url_col = f"{prefix}_source_url"
    local_col = f"{prefix}_source_doc_local"
    type_col = f"{prefix}_source_doc_type"
    existing_url = _string_or_empty(df.at[row_idx, url_col])
    existing_local = _string_or_empty(df.at[row_idx, local_col])
    existing_type = _string_or_empty(df.at[row_idx, type_col])
    if existing_url and existing_local and existing_type:
        return df

    matched_url = ""
    matched_local = ""
    matched_type = ""
    for candidate_url, candidate_local, candidate_type in _iter_legacy_provenance_entries(df.loc[row_idx]):
        if matcher(candidate_url, candidate_local, candidate_type):
            matched_url = candidate_url
            matched_local = candidate_local
            matched_type = candidate_type
            break

    if not existing_url and matched_url:
        df.at[row_idx, url_col] = matched_url
    if not existing_local and matched_local:
        df.at[row_idx, local_col] = matched_local
    if not existing_type and matched_type:
        df.at[row_idx, type_col] = matched_type
    return df


def _iter_legacy_provenance_entries(row: pd.Series) -> list[tuple[str, str, str]]:
    urls = _split_pipe_values(row.get("source_url"))
    locals_ = _split_pipe_values(row.get("source_doc_local"))
    types = _split_pipe_values(row.get("source_doc_type"))
    max_len = max(len(urls), len(locals_), len(types), 0)
    entries: list[tuple[str, str, str]] = []
    for idx in range(max_len):
        entries.append(
            (
                urls[idx] if idx < len(urls) else "",
                locals_[idx] if idx < len(locals_) else "",
                types[idx] if idx < len(types) else "",
            )
        )
    return entries


def _split_pipe_values(value: object) -> list[str]:
    raw = _string_or_empty(value)
    if not raw:
        return []
    values: list[str] = []
    for part in raw.split("|"):
        cleaned = part.strip()
        if cleaned:
            values.append(cleaned)
    return values


def _is_financing_provenance_entry(url: str, local: str, doc_type: str) -> bool:
    combined = " ".join((url, local, doc_type)).lower()
    return (
        "quarterly_refunding_press_release" in combined
        or "borrowing-estimates" in combined
        or "treasury announces marketable borrowing estimates" in combined
    )


def _is_refunding_statement_provenance_entry(url: str, local: str, doc_type: str) -> bool:
    combined = " ".join((url, local, doc_type)).lower()
    return "official_quarterly_refunding_statement" in combined or (
        "refunding" in combined and "statement" in combined
    )


def _is_auction_reconstruction_provenance_entry(url: str, local: str, doc_type: str) -> bool:
    combined = " ".join((url, local, doc_type)).lower()
    return _AUCTION_RECON_SOURCE_DOC_TYPE in combined or "auctions_query" in combined


def _normalize_capture(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    for col in CAPTURE_COLUMNS:
        normalized[col] = (
            normalized[col]
            .fillna("")
            .astype(str)
            .str.strip()
            .replace({"nan": "", "None": ""})
        )

    for col in (name for name in CAPTURE_COLUMNS if name.endswith("source_doc_local")):
        mask = normalized[col] != ""
        normalized.loc[mask, col] = normalized.loc[mask, col].map(
            _normalize_repo_local_reference_list
        )

    for col in DATE_COLUMNS:
        mask = normalized[col] != ""
        normalized.loc[mask, col] = pd.to_datetime(
            normalized.loc[mask, col], errors="raise"
        ).dt.strftime("%Y-%m-%d")

    for col in NUMERIC_COLUMNS:
        mask = normalized[col] != ""
        normalized.loc[mask, col] = pd.to_numeric(
            normalized.loc[mask, col], errors="raise"
        ).astype(float).map(lambda value: f"{value:g}")

    normalized = normalized.replace({"": pd.NA})
    for idx, row in normalized.iterrows():
        qa_status = _string_or_empty(row.get("qa_status"))
        if qa_status not in {"seed_only", "semi_automated_capture"}:
            continue
        if _uses_seed_source(row):
            continue
        if _is_missing(row.get("total_financing_need_bn")) or _is_missing(row.get("net_bill_issuance_bn")):
            continue
        if not (
            _has_official_financing_provenance(row)
            and _has_official_refunding_statement_provenance(row)
            and _has_official_auction_reconstruction_provenance(row)
        ):
            continue
        normalized.at[idx, "qa_status"] = "manual_official_capture"
    return normalized


def _validate_columns(df: pd.DataFrame) -> None:
    actual = list(df.columns)
    expected = list(CAPTURE_COLUMNS)
    if actual == expected:
        return

    missing = [col for col in expected if col not in actual]
    extra = [col for col in actual if col not in expected]
    raise ValueError(
        "Official QRA capture columns do not match required contract. "
        f"Missing={missing} Extra={extra} ExpectedOrder={expected}"
    )


def _validate_rows(df: pd.DataFrame) -> None:
    errors: list[str] = []
    quarter_values = df["quarter"].dropna().astype(str)
    duplicated_quarters = sorted(quarter_values[quarter_values.duplicated()].unique().tolist())
    if duplicated_quarters:
        errors.append(
            "duplicate quarter rows are not allowed: " + ", ".join(duplicated_quarters)
        )

    for idx, row in df.iterrows():
        row_num = idx + 2
        qa_status = _string_or_empty(row["qa_status"])
        if qa_status and qa_status not in ALLOWED_QA_STATUSES:
            errors.append(
                f"row {row_num}: qa_status='{qa_status}' is invalid. "
                f"Allowed={list(ALLOWED_QA_STATUSES)}"
            )

        for col in BASE_REQUIRED_COLUMNS:
            if _is_missing(row[col]):
                errors.append(f"row {row_num}: required field '{col}' is missing")

        quarter = _string_or_empty(row["quarter"])
        if quarter and _QUARTER_RE.match(quarter) is None:
            errors.append(f"row {row_num}: quarter='{quarter}' must match YYYYQ[1-4]")

        release = _string_or_empty(row["qra_release_date"])
        marker = _string_or_empty(row["market_pricing_marker_minus_1d"])
        if release and marker:
            release_ts = pd.to_datetime(release, errors="raise")
            marker_ts = pd.to_datetime(marker, errors="raise")
            if marker_ts != (release_ts - pd.Timedelta(days=1)):
                errors.append(
                    f"row {row_num}: market_pricing_marker_minus_1d must equal qra_release_date - 1 day"
                )

        if qa_status in OFFICIAL_QA_STATUSES:
            for col in OFFICIAL_REQUIRED_COLUMNS:
                if _is_missing(row[col]):
                    errors.append(
                        f"row {row_num}: qa_status='{qa_status}' requires non-empty '{col}'"
                    )
            if qa_status == "manual_official_capture":
                for prefix in ROLE_REQUIRED_PREFIXES:
                    role_type = _string_or_empty(row.get(f"{prefix}_source_doc_type"))
                    role_url = _string_or_empty(row.get(f"{prefix}_source_url"))
                    role_local = _string_or_empty(row.get(f"{prefix}_source_doc_local"))
                    if not role_type:
                        errors.append(
                            f"row {row_num}: qa_status='manual_official_capture' requires "
                            f"non-empty '{prefix}_source_doc_type'"
                        )
                    if not role_url and not role_local:
                        errors.append(
                            f"row {row_num}: qa_status='manual_official_capture' requires "
                            f"'{prefix}_source_url' or '{prefix}_source_doc_local'"
                        )
        elif qa_status == "seed_only":
            if _is_missing(row["source_doc_local"]):
                errors.append(
                    f"row {row_num}: qa_status='seed_only' requires source_doc_local"
                )

    if errors:
        raise ValueError("Official QRA capture validation failed:\n- " + "\n- ".join(errors))


def _sort_capture(df: pd.DataFrame) -> pd.DataFrame:
    sortable = df.copy()
    sortable["_qra_release_date_sort"] = pd.to_datetime(
        sortable["qra_release_date"], errors="coerce"
    )
    sortable["_quarter_sort"] = sortable["quarter"].astype(str)
    sortable = sortable.sort_values(
        by=["_qra_release_date_sort", "_quarter_sort"],
        ascending=[True, True],
        kind="stable",
        na_position="last",
    ).drop(columns=["_qra_release_date_sort", "_quarter_sort"])
    return sortable.reset_index(drop=True)


def _is_missing(value: object) -> bool:
    if value is pd.NA:
        return True
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    return str(value).strip() == ""


def _string_or_empty(value: object) -> str:
    if _is_missing(value):
        return ""
    return str(value).strip()


def _resolve_repo_local_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _normalize_repo_local_reference(value: str) -> str:
    path = Path(value)
    if not path.is_absolute():
        return str(path)
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def _normalize_repo_local_reference_list(value: object) -> str:
    values: list[str] = []
    for raw in _split_pipe_values(value):
        normalized = _normalize_repo_local_reference(raw)
        if normalized and normalized not in values:
            values.append(normalized)
    return "|".join(values)


def _quarter_period_label(quarter: str) -> str:
    year = int(quarter[:4])
    qnum = int(quarter[-1])
    labels = {
        1: f"January - March {year}",
        2: f"April - June {year}",
        3: f"July - September {year}",
        4: f"October - December {year}",
    }
    return labels[qnum]


def _normalize_period_text(value: str) -> str:
    normalized = value.lower()
    normalized = normalized.replace("–", "-").replace("—", "-").replace("‑", "-")
    normalized = normalized.replace("\xa0", " ")
    normalized = re.sub(r"[^a-z0-9-]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def _human_date(timestamp: pd.Timestamp) -> str:
    return f"{timestamp.strftime('%B')} {timestamp.day}, {timestamp.year}"


def _parse_quarter_archive_links(archive_html_path: Path) -> dict[str, tuple[str, str, str]]:
    soup = _parse_html_document(
        archive_html_path.read_text(encoding="utf-8", errors="ignore")
    )
    links: dict[str, tuple[str, str, str]] = {}
    table = soup.find(
        "table",
        attrs={
            "aria-label": re.compile(
                r"Official Remarks on Quarterly Refunding", flags=re.IGNORECASE
            )
        },
    )
    rows = table.find_all("tr") if table is not None else soup.find_all("tr")
    current_year = ""
    for row in rows:
        for header in row.find_all(["th", "td"]):
            header_id = _string_or_empty(header.get("id"))
            if re.fullmatch(r"\d{4}", header_id):
                current_year = header_id
                break
            header_text = _string_or_empty(header.get_text(" ", strip=True))
            if re.fullmatch(r"\d{4}", header_text):
                current_year = header_text
                break

        for anchor in row.find_all("a", href=True):
            label = _string_or_empty(anchor.get("aria-label")) or " ".join(
                anchor.get_text(" ", strip=True).split()
            )
            quarter = _quarter_from_label(label)
            if not quarter:
                quarter = _quarter_from_quarter_only_label(label, current_year)
            if not quarter or quarter in links:
                continue
            href = urljoin("https://home.treasury.gov", anchor["href"])
            links[quarter] = (quarter, href, label)

    if links:
        return links

    for anchor in soup.find_all("a", href=True):
        label = _string_or_empty(anchor.get("aria-label")) or " ".join(
            anchor.get_text(" ", strip=True).split()
        )
        quarter = _quarter_from_label(label)
        if not quarter or quarter in links:
            continue
        href = urljoin("https://home.treasury.gov", anchor["href"])
        links[quarter] = (quarter, href, label)
    return links


def _quarter_from_label(label: str) -> str:
    match = _QUARTER_LABEL_RE.search(label)
    if match is None:
        return ""
    return f"{match.group('year')}Q{match.group('quarter')}"


def _quarter_from_quarter_only_label(label: str, year: str) -> str:
    if not year or re.fullmatch(r"\d{4}", year) is None:
        return ""
    normalized = label.replace("\u200b", "").replace("\ufeff", "")
    match = _QUARTER_ONLY_LABEL_RE.search(normalized)
    if match is None:
        return ""
    return f"{year}Q{match.group('quarter')}"


def _extract_html_text(path: Path) -> str:
    soup = _parse_html_document(path.read_text(encoding="utf-8", errors="ignore"))
    text = " ".join(soup.get_text(" ", strip=True).split())
    return text.replace("\xa0", " ")


def _parse_html_document(markup: str):
    from bs4 import BeautifulSoup
    from bs4 import FeatureNotFound

    try:
        return BeautifulSoup(markup, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(markup, "html.parser")


def _extract_named_section(text: str, heading: str, end_headings: tuple[str, ...]) -> str:
    if not text:
        return ""
    pattern = re.escape(heading)
    if end_headings:
        end_pattern = "|".join(re.escape(item) for item in end_headings)
        regex = re.compile(
            rf"{pattern}\s+(?P<section>.*?)(?=(?:{end_pattern})\s+|$)",
            flags=re.IGNORECASE | re.DOTALL,
        )
    else:
        regex = re.compile(rf"{pattern}\s+(?P<section>.*)$", flags=re.IGNORECASE | re.DOTALL)
    match = regex.search(text)
    if match is None:
        return ""
    return _clean_section_text(match.group("section"))


def _extract_projected_financing_section(text: str) -> str:
    return _extract_named_section(
        text,
        "PROJECTED FINANCING NEEDS AND ISSUANCE PLANS",
        (
            "TIPS FINANCING",
            "DEBT LIMIT",
            "BUYBACK OUTREACH",
            "BUYBACKS",
            "ADDITIONAL PUBLIC TRANSPARENCY",
            "LARGE POSITION REPORT (LPR) CALL",
            "SMALL-VALUE BUYBACK OPERATION",
            "Please send comments or suggestions",
        ),
    )


def _clean_section_text(text: str) -> str:
    cleaned = " ".join(str(text).replace("\xa0", " ").split())
    cleaned = re.sub(r"\s+([,;:.])", r"\1", cleaned)
    return cleaned.strip()


def _extract_nominal_coupon_guidance(section: str) -> str:
    section = _clean_section_text(section)
    if not section:
        return ""

    pieces: list[str] = []
    sentence_summary = _join_matching_sentences(
        section,
        include_keywords=(
            "Treasury plans to increase",
            "Treasury plans to maintain",
            "Treasury believes its current auction sizes",
            "Treasury intends to keep nominal coupon and FRN new issue and reopening auction sizes unchanged",
            "Treasury does not anticipate making any changes to nominal coupon and FRN new issue or reopening auction sizes",
            "Treasury does not anticipate needing to increase nominal coupon",
            "Treasury will continue to evaluate whether additional relative adjustments",
        ),
        exclude_keywords=("FRN",),
    )
    if sentence_summary:
        pieces.append(sentence_summary)

    monthly_schedule = _format_monthly_nominal_schedule(section)
    if monthly_schedule:
        pieces.append(monthly_schedule)

    return " ".join(part for part in pieces if part).strip()


def _extract_frn_guidance(section: str) -> str:
    section = _clean_section_text(section)
    if not section:
        return ""

    pieces: list[str] = []
    sentence_summary = _join_matching_sentences(
        section,
        include_keywords=("2-year FRN", "FRN auction size", "FRN by"),
        exclude_keywords=(),
    )
    if sentence_summary:
        pieces.append(sentence_summary)

    monthly_schedule = _format_monthly_frn_schedule(section)
    if monthly_schedule:
        pieces.append(monthly_schedule)

    return " ".join(part for part in pieces if part).strip()


def _extract_bill_guidance(section: str) -> str:
    section = _clean_section_text(section)
    if not section:
        return ""
    return _join_matching_sentences(
        section,
        include_keywords=(
            "regular bill auction sizes",
            "bill auction sizes",
            "benchmark bill issuance",
            "CMB",
            "cash management bill",
        ),
        exclude_keywords=(),
    )


def _join_matching_sentences(
    text: str,
    include_keywords: tuple[str, ...],
    exclude_keywords: tuple[str, ...],
) -> str:
    sentences = [match.group(0).strip() for match in _SENTENCE_RE.finditer(text)]
    if not sentences:
        return ""

    selected: list[str] = []
    for sentence in sentences:
        if include_keywords and not any(token.lower() in sentence.lower() for token in include_keywords):
            continue
        if exclude_keywords and any(token.lower() in sentence.lower() for token in exclude_keywords):
            continue
        if sentence not in selected:
            selected.append(sentence)
    return " ".join(selected).strip()


def _format_monthly_nominal_schedule(section: str) -> str:
    rows = _extract_target_schedule_rows(section)
    if not rows:
        return ""
    formatted = [
        (
            f"{row['month']}: 2Y={row['two']}, 3Y={row['three']}, 5Y={row['five']}, "
            f"7Y={row['seven']}, 10Y={row['ten']}, 20Y={row['twenty']}, 30Y={row['thirty']}"
        )
        for row in rows
    ]
    return "Monthly nominal schedule: " + "; ".join(formatted) + "."


def _format_monthly_frn_schedule(section: str) -> str:
    rows = _extract_target_schedule_rows(section)
    if not rows:
        return ""
    formatted = [f"{row['month']}: 2Y FRN={row['frn']}" for row in rows]
    return "Monthly FRN schedule: " + "; ".join(formatted) + "."


def _extract_target_schedule_rows(section: str) -> list[dict[str, str]]:
    matches = [
        {
            key: value
            for key, value in match.groupdict().items()
        }
        for match in _MONTH_ROW_RE.finditer(section)
    ]
    if not matches:
        return []

    target_range = _extract_target_refunding_range(section)
    if not target_range:
        return matches

    wanted = set(_month_labels_for_range(target_range))
    filtered = [row for row in matches if row["month"] in wanted]
    return filtered or matches


def _extract_target_refunding_range(section: str) -> str:
    for regex in (_REFUNDING_RANGE_RE, _REFUNDING_RANGE_WITH_YEARS_RE):
        match = regex.search(section)
        if match is not None:
            return _clean_section_text(match.group("range"))
    return ""


def _month_labels_for_range(range_text: str) -> list[str]:
    month_map = {
        "January": 1,
        "February": 2,
        "March": 3,
        "April": 4,
        "May": 5,
        "June": 6,
        "July": 7,
        "August": 8,
        "September": 9,
        "October": 10,
        "November": 11,
        "December": 12,
    }
    short_map = {
        1: "Jan",
        2: "Feb",
        3: "Mar",
        4: "Apr",
        5: "May",
        6: "Jun",
        7: "Jul",
        8: "Aug",
        9: "Sep",
        10: "Oct",
        11: "Nov",
        12: "Dec",
    }
    normalized = re.sub(r"\s*[–-]\s*", " to ", range_text.strip())
    match = re.match(
        r"(?P<start>[A-Za-z]+)\s+(?:(?P<start_year>\d{4})\s+)?to\s+"
        r"(?P<end>[A-Za-z]+)\s+(?P<end_year>\d{4})",
        normalized,
        flags=re.IGNORECASE,
    )
    if match is None:
        return []
    start_year = int(match.group("start_year") or match.group("end_year"))
    end_year = int(match.group("end_year"))
    start_month = month_map.get(match.group("start").capitalize())
    end_month = month_map.get(match.group("end").capitalize())
    if start_month is None or end_month is None:
        return []
    labels: list[str] = []
    current_year = start_year
    current_month = start_month
    while (current_year, current_month) <= (end_year, end_month):
        labels.append(f"{short_map[current_month]}-{str(current_year)[2:]}")
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1
    return labels


def _extract_borrowing_amount_for_quarter(text: str, quarter: str) -> float | object:
    expected_period = _normalize_period_text(_quarter_period_label(quarter))
    for match in _BORROWING_SENTENCE_RE.finditer(text):
        period = _normalize_period_text(match.group("period"))
        if period != expected_period:
            continue
        amount = float(match.group("amount"))
        unit = match.group("unit").lower()
        return amount * 1000.0 if unit == "trillion" else amount
    return pd.NA


def _merge_pipe_values(primary: str, secondary: str) -> str:
    values: list[str] = []
    for raw in (primary, secondary):
        for part in str(raw).split("|"):
            cleaned = part.strip()
            if cleaned and cleaned not in values:
                values.append(cleaned)
    return "|".join(values)


def _merge_note(existing: str, extra: str) -> str:
    existing = existing.strip()
    extra = extra.strip()
    if not existing:
        return extra
    if extra in existing:
        return existing
    return f"{existing} {extra}"


def _strip_financing_release_note_fragments(note: str) -> str:
    cleaned = re.sub(
        r"\s*Official borrowing estimate matched to [A-Za-z]+ \d{1,2}, \d{4} Treasury borrowing-estimate release\.",
        "",
        note,
    )
    cleaned = cleaned.replace(
        " net_bill_issuance_bn remains seed-derived pending official quarter capture.",
        "",
    )
    cleaned = cleaned.replace(
        " net_bill_issuance_bn is still missing pending official quarter capture.",
        "",
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _classify_auction_bucket(row: pd.Series) -> str:
    security_type = _string_or_empty(row.get("security_type")).lower()
    if "bill" in security_type:
        return "bill_like"
    if _string_or_empty(row.get("cash_management_bill_cmb")) == "Yes":
        return "bill_like"
    if _string_or_empty(row.get("floating_rate")) == "Yes":
        return "frn"
    if _string_or_empty(row.get("inflation_index_security")) == "Yes":
        return "tips"
    return "nominal_coupon"


def _float_or_na(value: object) -> float | object:
    if _is_missing(value):
        return pd.NA
    converted = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(converted):
        return pd.NA
    return float(converted)


def _format_optional_float(value: object) -> str:
    numeric = _float_or_na(value)
    if pd.isna(numeric):
        return "NA"
    return f"{float(numeric):g}"


def _uses_seed_source(row: pd.Series) -> bool:
    values = (
        _string_or_empty(row.get("source_doc_type")).lower(),
        _string_or_empty(row.get("source_doc_local")).lower(),
        _string_or_empty(row.get("financing_source_doc_type")).lower(),
        _string_or_empty(row.get("financing_source_doc_local")).lower(),
        _string_or_empty(row.get("refunding_statement_source_doc_type")).lower(),
        _string_or_empty(row.get("refunding_statement_source_doc_local")).lower(),
        _string_or_empty(row.get("auction_reconstruction_source_doc_type")).lower(),
        _string_or_empty(row.get("auction_reconstruction_source_doc_local")).lower(),
    )
    return any("seed_csv" in value or "quarterly_refunding_seed.csv" in value for value in values)


def _completion_tier_for_row(row: pd.Series) -> str:
    qa_status = _string_or_empty(row.get("qa_status"))
    has_net_bill = not _is_missing(row.get("net_bill_issuance_bn"))
    has_financing = not _is_missing(row.get("total_financing_need_bn"))
    uses_seed = _uses_seed_source(row)
    has_auction_reconstruction = _has_official_auction_reconstruction_provenance(row)
    has_official_financing = _has_official_financing_provenance(row)
    has_official_refunding_statement = _has_official_refunding_statement_provenance(row)
    if (
        qa_status in OFFICIAL_QA_STATUSES
        and has_financing
        and has_net_bill
        and has_auction_reconstruction
        and has_official_financing
        and has_official_refunding_statement
        and not uses_seed
    ):
        return _AUCTION_COMPLETION_EXACT
    if (
        qa_status in OFFICIAL_QA_STATUSES
        and has_financing
        and has_net_bill
        and has_official_financing
        and has_official_refunding_statement
        and not uses_seed
    ):
        return _AUCTION_COMPLETION_PROMOTED
    if qa_status == "semi_automated_capture":
        return _AUCTION_COMPLETION_SEMI
    if qa_status == "seed_only" or uses_seed:
        return _AUCTION_COMPLETION_SEED
    return _AUCTION_COMPLETION_OTHER


def _has_official_financing_provenance(row: pd.Series) -> bool:
    financing_source_doc_type = _string_or_empty(row.get("financing_source_doc_type")).lower()
    if financing_source_doc_type:
        return "quarterly_refunding_press_release" in financing_source_doc_type
    source_doc_type = _string_or_empty(row.get("source_doc_type")).lower()
    return "quarterly_refunding_press_release" in source_doc_type


def _has_official_refunding_statement_provenance(row: pd.Series) -> bool:
    refunding_source_doc_type = _string_or_empty(
        row.get("refunding_statement_source_doc_type")
    ).lower()
    if refunding_source_doc_type:
        return "official_quarterly_refunding_statement" in refunding_source_doc_type
    source_doc_type = _string_or_empty(row.get("source_doc_type")).lower()
    return "official_quarterly_refunding_statement" in source_doc_type


def _has_official_auction_reconstruction_provenance(row: pd.Series) -> bool:
    auction_source_doc_type = _string_or_empty(
        row.get("auction_reconstruction_source_doc_type")
    ).lower()
    if auction_source_doc_type:
        return _AUCTION_RECON_SOURCE_DOC_TYPE in auction_source_doc_type
    source_doc_type = _string_or_empty(row.get("source_doc_type")).lower()
    return _AUCTION_RECON_SOURCE_DOC_TYPE in source_doc_type


def _drop_seed_provenance(value: str) -> str:
    kept: list[str] = []
    for part in value.split("|"):
        cleaned = part.strip()
        lowered = cleaned.lower()
        if not cleaned:
            continue
        if "seed_csv" in lowered or "qra_event_seed.csv" in lowered or "quarterly_refunding_seed.csv" in lowered:
            continue
        if cleaned not in kept:
            kept.append(cleaned)
    return "|".join(kept)
