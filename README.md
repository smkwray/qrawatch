# Treasury Maturity Composition as Shadow Balance-Sheet Policy

This repo is a reproducible research and data product for the project:

**Do shifts in Treasury's bill-versus-coupon mix operate like a shadow form of balance-sheet policy by changing public duration supply, term premia, and market plumbing?**

The design is intentionally **neutral about motive**. The repo treats the “activist Treasury issuance” framing (Miran) as a source of testable claims, then maps those claims into public-data measurement, event-study, and plumbing exercises. The measure itself is presented as Treasury maturity composition — a neutral measurement construct.

## What is already seeded

- a clear project framing and research brief
- manual seed files for the core quarter-level “missing coupons” arithmetic
- key QRA event dates drawn from the note
- download scripts for official Treasury, FiscalData, FRED, SEC, and New York Fed sources
- initial Python modules for:
  - coupon-shortfall / missing-coupons construction
  - auction classification
  - generic event-window work
  - generic download helpers
- tests for the core arithmetic

## Current status

The repo now has a reproducible backend product and public site, with a deliberately narrow first-release scope:

- the official quarter capture is exact and non-seed for the current quarters in scope
- the official coupon-shortfall rebuild is derived from that capture path
- the plumbing baseline uses exact net bill and non-bill series, with fallbacks labeled separately
- the duration headline is a hybrid exact-plus-proxy construction with explicit fallback comparisons
- the publish layer under `output/publish/` is the frontend-facing API
- investor allotments, primary dealer, and SEC N-MFP are now summary-ready extension modules

Exact official quarter coverage currently spans `2023Q4` through `2024Q3`.
This public release should be read as an in-progress research/data product, not as a finished long-history dataset.

## Quickstart

```bash
python3 -m venv ~/venvs/qrawatch
source .env
"$HOME/venvs/qrawatch/bin/python" -m ensurepip --upgrade
"$HOME/venvs/qrawatch/bin/python" -m pip install -r requirements.txt
"$HOME/venvs/qrawatch/bin/python" -m pip install -e .

"$HOME/venvs/qrawatch/bin/python" scripts/00_bootstrap.py
"$HOME/venvs/qrawatch/bin/python" scripts/08_build_ati_index.py
"$HOME/venvs/qrawatch/bin/python" -B -m pytest -q
```

When network access is available, then run:

```bash
source .env
"$HOME/venvs/qrawatch/bin/python" scripts/01_download_fiscaldata.py --all
"$HOME/venvs/qrawatch/bin/python" scripts/02_download_fred.py --preset core
"$HOME/venvs/qrawatch/bin/python" scripts/03_download_qra_materials.py --download-files
"$HOME/venvs/qrawatch/bin/python" scripts/04_extract_qra_text.py
"$HOME/venvs/qrawatch/bin/python" scripts/20_enrich_official_qra_capture.py
"$HOME/venvs/qrawatch/bin/python" scripts/13_build_official_qra_capture.py
"$HOME/venvs/qrawatch/bin/python" scripts/17_build_official_ati.py
"$HOME/venvs/qrawatch/bin/python" scripts/09_build_qra_event_panel.py
"$HOME/venvs/qrawatch/bin/python" scripts/10_run_event_study.py
"$HOME/venvs/qrawatch/bin/python" scripts/11_run_plumbing_regressions.py
"$HOME/venvs/qrawatch/bin/python" scripts/12_build_public_duration_supply.py
"$HOME/venvs/qrawatch/bin/python" scripts/16_build_investor_allotments_inventory.py
"$HOME/venvs/qrawatch/bin/python" scripts/18_build_primary_dealer_inventory.py
"$HOME/venvs/qrawatch/bin/python" scripts/19_build_sec_nmfp_inventory.py
"$HOME/venvs/qrawatch/bin/python" scripts/15_build_publish_artifacts.py
"$HOME/venvs/qrawatch/bin/python" scripts/21_validate_backend.py
```

Or use:

```bash
make bootstrap
make seed
make test
make regenerate
```

## Repository map

- `PROJECT_BRIEF.md` — the paper question, scope, hypotheses, and contribution
- `RESEARCH_DESIGN.md` — equations, identification, outcomes, and robustness
- `DATA_SOURCES.md` — source registry and why each dataset matters
- `VALIDATION_CHECKLIST.md` — QA before trusting any headline result
- `data/manual/` — seed quarter inputs and event dates
- `src/ati_shadow_policy/` — reusable download and research modules
- `scripts/` — task-oriented entry points
- `tests/` — unit tests for the core logic
- `references/source_note/` — the uploaded note and a distilled testable-claims memo

## Sign conventions used here

The repo uses the following baseline sign convention:

- **positive coupon shortfall / missing coupons** = more bills and fewer coupons than the chosen target share
- **positive public duration supply** = more duration pushed into private hands
- **positive QT contribution** = Fed Treasury holdings falling
- **positive buyback contribution** = duration removed from public hands is recorded with a minus sign in the duration-supply construction

## Baseline targets

The baseline bill-share target is **18%**, with robustness at **15%** and **20%**.

## What is intentionally provisional

Some pieces are seeded rather than fully automated:

- the seed shortfall path still exists for comparison, but the headline official coupon-shortfall path is now exact and non-seed
- the QRA download and parsing scripts are designed to collect official documents first, then support later automation
- the QRA enrichment step now pairs each capture quarter with both the Treasury borrowing-estimate release and the matching official quarterly refunding statement, writing intermediate maps under `data/processed/`
- the duration headline still combines exact non-bill net supply with a QT proxy, and publish artifacts keep fallback constructions explicit
- the SEC N-MFP backend stops at summary analytics depth rather than security-level research depth
- TIC remains out of scope for the current public release
- QRA downloads now use deterministic filenames of the form `<slug>_<sha1>.pdf|.html` and record provenance in `data/raw/qra/downloads.csv`

## Current limitations

- the exact official quarter history is still short and currently covers only `2023Q4` through `2024Q3`
- the event-study layer is informative but still small-sample
- extension modules are supporting context, not the headline result
- the long-history version of the project still needs additional official quarter capture

## Backend artifacts

The backend now emits a publish-ready layer under `output/publish/`, derived from processed outputs rather than raw files. Core artifacts include:

- coupon-shortfall quarter tables
- official QRA quarter capture tables
- seed-vs-official shortfall comparisons
- QRA event tables, baseline summaries, and robustness summaries
- plumbing baseline summaries and robustness summaries
- duration-supply summaries and construction comparisons
- data-source and extension-status inventories

Useful backend commands:

```bash
source .env
"$HOME/venvs/qrawatch/bin/python" scripts/13_build_official_qra_capture.py
"$HOME/venvs/qrawatch/bin/python" scripts/17_build_official_ati.py
"$HOME/venvs/qrawatch/bin/python" scripts/14_qra_quality_report.py
"$HOME/venvs/qrawatch/bin/python" scripts/15_build_publish_artifacts.py
"$HOME/venvs/qrawatch/bin/python" scripts/20_enrich_official_qra_capture.py
"$HOME/venvs/qrawatch/bin/python" scripts/16_build_investor_allotments_inventory.py
"$HOME/venvs/qrawatch/bin/python" scripts/18_build_primary_dealer_inventory.py
"$HOME/venvs/qrawatch/bin/python" scripts/19_build_sec_nmfp_inventory.py
"$HOME/venvs/qrawatch/bin/python" scripts/21_validate_backend.py
```

Public site consumers should read only from `output/publish/`. The key site-facing status tables are:

- `output/publish/dataset_status.csv`
- `output/publish/extension_status.csv`
- `output/publish/series_metadata_catalog.csv`
- `output/publish/investor_allotments_summary.{csv,json,md}`
- `output/publish/primary_dealer_summary.{csv,json,md}`
- `output/publish/sec_nmfp_summary.{csv,json,md}`

If `FRED_API_KEY` is set in `.env`, the FRED downloader uses the official API to avoid flaky public CSV fetches for larger series.

## What comes next

- extend exact official quarter capture into 2025
- extend the official quarter history backward after the 2025 forward pass
- deepen extensions after the core quarter history is longer

The current workflow emphasizes reproducibility:

- start from the cleanest observable objects,
- get the first pricing and plumbing results,
- then automate the brittle parts.
