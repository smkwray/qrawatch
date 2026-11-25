# Pipeline targets and refresh scripts

## Makefile targets

`make regenerate PYTHON=python` runs the full pipeline. Each step is also a target of its own:

| Target | What it does |
|---|---|
| `download-core` | Downloads FiscalData tables and the core FRED series |
| `qra` | Downloads Quarterly Refunding materials and extracts their text |
| `official-capture` | Builds the official QRA capture table |
| `official-ati` | Builds the official quarterly Maturity Tilt table |
| `investor` | Downloads Treasury investor class auction allotments and builds their inventory |
| `primary` | Downloads New York Fed primary dealer statistics and builds their inventory |
| `sec` | Downloads SEC Form N-MFP archives and builds their inventory (needs `SEC_USER_AGENT`) |
| `event` | Builds the QRA event panel and runs the event study |
| `plumbing` | Runs the money-market regressions |
| `duration` | Builds the weekly public duration supply series |
| `pricing-panels` | Builds the pricing panels |
| `pricing-regressions` | Runs the pricing regressions |
| `pricing-figures` | Draws the pricing charts |
| `publish` | Writes the JSON and CSV summary tables under `output/publish/` |
| `qra-quality` | Writes a quality report on the QRA capture |
| `backend-validate` | Checks the published tables for schema and consistency errors |

`regenerate` also runs intermediate QRA enrichment, identification, and auction-absorption steps, in the order listed in `Makefile`.

## Standalone financing refresh scripts

Two scripts rebuild the quarterly financing table into a new output directory. They do not overwrite `data/processed/` or `output/publish/`, and they do not rerun the pricing regressions.

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

`34_build_official_quarter_refresh.py` reconciles settlement-date net bill issuance against Table 1 of the Monthly Statement of the Public Debt (MSPD); `--max-mspd-residual-bn` sets the largest allowed gap (default $1.0 billion). `35_build_realized_financing_refresh.py` downloads Treasury's borrowing-estimate releases (`capture`) and rebuilds the table with realized privately held net marketable borrowing, as reported in the following quarter's release, in place of the planned estimates (`build`). `build` also writes the realized borrowing figures to the tracked file `data/manual/realized_privately_held_borrowing.csv` unless `--realized` names another file.
