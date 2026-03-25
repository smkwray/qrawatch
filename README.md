# QRA Watch

**Treasury Maturity Composition Research**

[View the public site](https://smkwray.github.io/qrawatch/)

QRA Watch is an open, reproducible research and data product measuring whether Treasury's bill-versus-coupon mix operates like a shadow form of balance-sheet policy — changing public duration supply, term premia, and market plumbing. All data is sourced from public APIs and official filings: FiscalData, FRED, Treasury QRA documents, and SEC EDGAR.

The project stays neutral about motive. It treats the underlying proposition as a testable empirical question, then maps it into official measurement, duration/plumbing mechanisms, and reduced-form pricing.

## Claim boundary

The project maintains an explicit hierarchy between what the current evidence can and cannot support:

**What the evidence supports:** Official maturity-composition measurement across the current public history window; headline plumbing and duration supply regressions at headline readiness; a narrow post-2022Q3 financing-estimates pilot with reviewed benchmark evidence and Tier A components.

**What the evidence does not establish:** A settled or full-sample causal estimate of Treasury issuance effects on long rates. The pricing layer remains supporting/provisional reduced-form evidence. Scenario translations are illustrative only.

## Evidence hierarchy

| Lane | Description | Status |
|------|-------------|--------|
| **Headline measurement & mechanism** | Official maturity composition, plumbing regressions, public duration supply | Headline ready |
| **Strongest reduced-form signal** | Monthly carry-forward Maturity-Tilt Flow specification | Supporting/provisional |
| **Release-level credibility lane** | Unique-release fixed-horizon +63bd flow design | Supporting/provisional |
| **Bounded causal pilot** | Post-2022Q3 financing-estimates event design | Supporting, bounded |
| **Supporting extensions** | Investor allotments, primary dealer, SEC N-MFP | Summary ready |

The monthly flow spec remains the strongest current reduced-form signal. The release-level +63bd design is the cleaner credibility lane. The causal pilot is narrower and supporting — not the main pricing coefficient source in this round.

## Key terminology

- **Maturity Tilt** — the quarter-level bill-versus-coupon composition object from official Treasury financing arithmetic
- **Maturity-Tilt Flow** — the signed quarterly flow object (internal field: `ati_baseline_bn`)
- **Excess Bills Stock** — the stock object (internal field: `stock_excess_bills_bn`)
- **Public Duration Supply** — the weekly duration-supply construction used in the duration and pricing layers

## Site architecture

The public site at `site/index.html` is a static frontend (HTML + CSS + vanilla JS) that reads only from published backend artifacts:

- **`site/data/`** — JSON and CSV artifacts mirrored from `output/publish/`
- **`site/figures/`** — SVG figures mirrored from `output/figures/`
- **`site/data/index.json`** — the artifact manifest used for conditional rendering

The frontend never accesses raw or interim backend data. All numeric values, status text, coverage windows, and claim boundaries displayed on the site are loaded from backend-generated artifacts at runtime. `make site` mirrors the current `output/publish/` artifacts into the site bundle.

## Quickstart

```bash
python3 -m venv ~/venvs/qrawatch
source .env
"$HOME/venvs/qrawatch/bin/python" -m ensurepip --upgrade
"$HOME/venvs/qrawatch/bin/python" -m pip install -r requirements.txt
"$HOME/venvs/qrawatch/bin/python" -m pip install -e .
```

With network access:

```bash
make bootstrap
make test
make regenerate
make pricing-figures
make site
```

`make regenerate` runs the full backend pipeline. `make site` mirrors publish artifacts into the frontend bundle.

## Repository map

- `README.md` — project overview, claim boundary, and evidence hierarchy
- `DATA_SOURCES.md` — source registry
- `docs/PRICING_METHODS.md` — pricing estimands, panel design, interpretation boundaries
- `docs/PRICING_RESULTS_MEMO.md` — current coefficients and claim boundary
- `docs/BENCHMARK_SEARCH_CLOSURE.md` — bounded benchmark-search closure memo
- `docs/STATUS_GLOSSARY.md` — readiness/source-quality labels
- `docs/DATA_DICTIONARY.md` — field-level definitions
- `docs/REGRESSION_SPECS.md` — regression equations and layouts
- `docs/QRA_CAPTURE_PROTOCOL.md` — official quarter-capture workflow
- `src/ati_shadow_policy/` — reusable modules
- `scripts/` — pipeline entry points
- `tests/` — unit tests

## Sign conventions

- Positive `ati_baseline_bn` = more bill-heavy maturity tilt relative to the 18% baseline
- Positive Public Duration Supply = more duration pushed into private hands
- Positive QT contribution = Fed Treasury holdings falling
- Buybacks enter the duration construction with opposite sign (they remove duration from public hands)

## What is intentionally provisional

- The pricing layer is reduced-form and published as supporting/provisional
- Extension modules remain supporting context, not headline evidence
- The duration headline combines exact non-bill net supply with a QT proxy
- The QRA event and causal layers remain supporting audit surfaces
- Pre-2009Q1 historical extension remains out of scope

## What comes next

- Keep the QRA causal lane bounded rather than reopening broad benchmark hunts
- Freeze the pricing claim boundary after the completed-month monthly lock
- Shift effort toward frontend communication if no remaining backend task moves the claim boundary materially
