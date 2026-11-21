# QRA Watch

QRA Watch measures the bill-versus-coupon mix of US Treasury financing from Quarterly Refunding Announcements (QRAs) and FiscalData auction records, and estimates reduced-form regressions linking maturity mix and public duration supply to Treasury yields, term premia, and money-market conditions.

## Data

All inputs come from public US government sources (see [DATA_SOURCES.md](DATA_SOURCES.md)):

- US Treasury Quarterly Refunding pages and archives: refunding statements, financing estimates, and Treasury Borrowing Advisory Committee (TBAC) materials.
- FiscalData API: Treasury security auctions, Monthly Statement of the Public Debt (MSPD), Treasury buyback operations, average interest rates, debt outstanding, Daily Treasury Statement cash balances, and Monthly Treasury Statement receipts and outlays.
- FRED: Treasury yields and 10-year term premia, federal funds rate, S&P 500, VIX, Federal Reserve balance-sheet items, and ON RRP balances.
- Treasury Investor Class Auction Allotments, New York Fed Primary Dealer Statistics, and SEC Form N-MFP monthly and quarterly archives.

Tracked CSV files under `data/manual/` hold hand-verified QRA release dates and financing figures.

## Main measures

All three measures are reported in USD billions (see [the data dictionary](docs/DATA_DICTIONARY.md) for CSV column names):

- Quarterly maturity-tilt flow: quarterly net bill issuance minus 18% of quarterly financing need (with 15% and 20% comparisons). Positive values indicate heavier bill issuance than an 18% bill share.
- Monthly excess bills stock: monthly MSPD marketable bill share minus 18%, multiplied by total marketable debt outstanding. A separate cumulative series sums quarterly maturity-tilt flow over time.
- Weekly public duration supply: weekly exact non-bill net Treasury issuance plus a quantitative-tightening proxy from Federal Reserve Treasury holdings minus accepted Treasury buybacks.

## Install

Requires Python 3.10 or later. From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[test]"
```

## Configuration

Set credentials in the environment rather than tracked files:

- `FRED_API_KEY` (optional) — selects the FRED JSON API (`api.stlouisfed.org`); when unset, downloads use the public FRED CSV endpoint.
- `SEC_USER_AGENT` — contact-bearing User-Agent header for SEC Form N-MFP downloads (`scripts/06_download_sec_nmfp.py`).

## Usage

Create the local `data/` and `output/` directory tree without downloading data:

```bash
make bootstrap PYTHON=python
```

Run the full public-data download, panel construction, regression, chart, and validation pipeline:

```bash
make regenerate PYTHON=python
```

Individual pipeline steps are available as `Makefile` targets (`download-core`, `qra`, `official-capture`, `official-ati`, `investor`, `primary`, `sec`, `event`, `plumbing`, `duration`, `pricing-panels`, `pricing-regressions`, `pricing-figures`, `publish`, `qra-quality`, `backend-validate`).

Two standalone scripts rebuild the quarterly financing table into a separate output directory without overwriting `data/processed/` or `output/publish/`:

```bash
python scripts/34_build_official_quarter_refresh.py \
  --auctions data/raw/fiscaldata/auctions_query.csv \
  --mspd data/raw/fiscaldata/mspd_table_1.csv \
  --out-dir output/refresh_official

python scripts/35_build_realized_financing_refresh.py capture --out-dir output/refresh_realized
python scripts/35_build_realized_financing_refresh.py build \
  --auctions data/raw/fiscaldata/auctions_query.csv \
  --mspd data/raw/fiscaldata/mspd_table_1.csv \
  --out-dir output/refresh_realized
```

`scripts/34_build_official_quarter_refresh.py` reconciles settlement-date net bill issuance against MSPD Table 1 (default tolerance `$1.0B` via `--max-mspd-residual-bn`). `scripts/35_build_realized_financing_refresh.py` captures Treasury borrowing-estimate releases and replaces planned financing estimates with realized privately held net marketable borrowing reported in the following quarter's release.

## Output

Generated files are gitignored:

- `data/raw/`, `data/interim/`, and `data/processed/` — downloaded source files and constructed weekly, monthly, and quarterly panels.
- `output/publish/` — JSON and CSV summary tables for maturity mix, public duration supply, and pricing regressions.
- `output/tables/` and `output/figures/` — intermediate analytical tables and generated charts.

## Limits

- Pricing regressions (release-level at horizons up to 63 business days, monthly carry-forward, and weekly duration-supply models) and money-market regressions are reduced-form associations; they do not establish structural interest-rate elasticities, Treasury policy motives, or forecasts.
- Quarterly financing estimates and realized privately held net marketable borrowing have different publication dates; realized borrowing cannot stand in for pre-announcement expectations in an event study.
- Full-pipeline builds require official QRA capture inputs beyond the synthetic test fixtures, and rebuilding financing tables via scripts 34 or 35 does not automatically rerun the pricing regressions.

## Documentation

- [Data sources](DATA_SOURCES.md)
- [Data dictionary](docs/DATA_DICTIONARY.md)
- [Pricing methods](docs/PRICING_METHODS.md)
- [Regression specifications](docs/REGRESSION_SPECS.md)
- [Official capture protocol](docs/QRA_CAPTURE_PROTOCOL.md)
- [Status glossary](docs/STATUS_GLOSSARY.md)

## Tests

The test suite uses synthetic fixtures and requires no network access or API keys:

```bash
make test PYTHON=python
ruff check src tests scripts
```
