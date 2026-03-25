# Treasury Maturity Composition as Shadow Balance-Sheet Policy

[Project website](https://smkwray.github.io/qrawatch/)

This repo is a reproducible research and data product for a neutral question:

**Do shifts in Treasury’s bill-versus-coupon mix operate like a shadow form of balance-sheet policy by changing public duration supply, term premia, and market plumbing?**

Public-facing terminology in this round:

- `Maturity Tilt` = the quarter-level bill-versus-coupon composition object built from official Treasury financing arithmetic
- `Maturity-Tilt Flow` = the signed quarterly flow object published through internal field `ati_baseline_bn`
- `Excess Bills Stock` = the stock object published through internal field `stock_excess_bills_bn`
- `Public Duration Supply` = the weekly duration-supply construction used in the duration and pricing layers

The design stays neutral about motive. The repo treats the Miran-style claim as a testable empirical proposition, then maps it into official measurement, duration/plumbing mechanisms, and reduced-form pricing. The QRA event layer remains a supporting audit surface rather than the headline engine.

## Current research lanes

- official Maturity Tilt measurement (`missing_coupons_*`, bill share, financing composition by quarter)
- public duration-supply measurement (Treasury non-bill supply + QT proxy - buybacks)
- plumbing mechanism tests (bills vs coupons, ON RRP, reserves, TGA controls)
- pricing regressions (10-year yield and term premium proxy vs Maturity-Tilt Flow, Excess Bills Stock, and Public Duration Supply)
- supporting QRA event and causal-pilot audit surfaces (non-headline)

## Current status

The repo now has a reproducible backend product and public site, with headline focus on official Maturity Tilt measurement, duration supply, plumbing, and pricing:

- exact official quarter capture is live for the current public history window
- quarter-level Maturity Tilt arithmetic is rebuilt from that official capture path
- the plumbing baseline uses exact net bill and non-bill series, with fallbacks labeled separately
- the duration headline is a hybrid exact-plus-proxy construction with explicit fallback comparisons
- the pricing layer now publishes a locked `pricing_spec_registry`, a `pricing_subsample_grid`, scenario translations, and five paper-style figures
- `claim_scope` separates descriptive-only rows, causal-pilot-only rows, and headline rows so the public boundary is machine-readable
- the publish layer under `output/publish/` is the frontend-facing API
- investor allotments, primary dealer, and SEC N-MFP remain summary-ready supporting extensions
- QRA event, shock-crosswalk, usability, leave-one-out, and absorption bridge tables are published when their source files exist, but they are supporting audit surfaces rather than referee-grade causal estimates

Exact official quarter coverage currently spans `2009Q1` through `2025Q4`.
This public release should be read as an in-progress research/data product, not as a settled full-sample event-causal design.

## Pricing credibility pack

The pricing layer now centers on a unique-release fixed-horizon flow profile:

- `release_flow_baseline_63bd` is the primary release-level scalar, using one row per unique market-pricing marker and cumulative changes from the pre-release marker to `+63` business days
- `release_flow_horizon_{1,5,10,21,42}bd` publish the same release-flow design at shorter fixed horizons so the sign profile is visible rather than hidden behind one endpoint
- pre-release placebo windows `[-21,-1]` and `[-5,-1]` business days are published as supporting robustness checks
- `monthly_flow_baseline` is still published side-by-side as a carry-forward context spec and remains the strongest current reduced-form signal
- `monthly_stock_baseline` and `weekly_duration_baseline` remain supporting reduced-form context rather than the main release-level credibility object

Headline quantity coefficients are published in **basis points per `$100bn`** on the named input.

The pricing credibility pack now includes:

- `output/publish/pricing_spec_registry.{csv,json,md}`
- `output/publish/pricing_regression_summary.{csv,json,md}`
- `output/publish/pricing_subsample_grid.{csv,json,md}`
- `output/publish/pricing_regression_robustness.{csv,json,md}`
- `output/publish/pricing_release_flow_panel.{csv,json,md}`
- `output/publish/pricing_release_flow_leave_one_out.{csv,json,md}`
- `output/publish/pricing_tau_sensitivity_grid.{csv,json,md}`
- `output/publish/pricing_scenario_translation.{csv,json,md}`
- `output/figures/maturity_tilt_flow_vs_dgs10.svg`
- `output/figures/excess_bills_stock_vs_threefytp10.svg`
- `output/figures/pricing_headline_coefficients.svg`
- `output/figures/pricing_release_flow_horizon_profile.svg`
- `output/figures/pricing_scenario_translation.svg`

See `docs/PRICING_METHODS.md` for the estimand and spec details and `docs/PRICING_RESULTS_MEMO.md` for the current coefficients.

## QRA causal boundary

The event-causal layer stays explicitly bounded. The causal surface is currently a small post-`2022Q3` current-sample financing pilot with `14` current-sample financing components, `6` verified pre-release external benchmarks, `5` Tier A components, `8` source-family-exhausted blocked rows, and `0` open benchmark candidates rather than a full-sample design.

The benchmark-search closure memo is in `docs/BENCHMARK_SEARCH_CLOSURE.md`. The repo does **not** treat that event lane as the headline pricing coefficient source in this round.

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
"$HOME/venvs/qrawatch/bin/python" scripts/11_run_plumbing_regressions.py
"$HOME/venvs/qrawatch/bin/python" scripts/12_build_public_duration_supply.py
"$HOME/venvs/qrawatch/bin/python" scripts/29_build_pricing_panels.py
"$HOME/venvs/qrawatch/bin/python" scripts/30_run_pricing_regressions.py
"$HOME/venvs/qrawatch/bin/python" scripts/31_build_pricing_figures.py
"$HOME/venvs/qrawatch/bin/python" scripts/15_build_publish_artifacts.py
"$HOME/venvs/qrawatch/bin/python" scripts/21_validate_backend.py
```

Or use:

```bash
make bootstrap
make test
make regenerate
make pricing-figures
make site
```

`make site` mirrors the current `output/publish/` CSV/JSON artifacts and `output/figures/` SVGs into `site/data/` and `site/figures/`, overwriting stale site copies so the frontend bundle matches the regenerated backend outputs.

## Repository map

- `README.md` — project question, current status, claim boundary, and reproducibility workflow
- `DATA_SOURCES.md` — source registry and why each dataset matters
- `docs/REGRESSION_SPECS.md` — compact equations and baseline regression layouts
- `docs/QRA_CAPTURE_PROTOCOL.md` — official quarter-capture workflow and provenance rules
- `docs/DATA_DICTIONARY.md` — field-level definitions for publish artifacts and source tables
- `docs/STATUS_GLOSSARY.md` — readiness/source-quality labels and public terminology
- `docs/PRICING_METHODS.md` — pricing estimands, panel design, and reduced-form interpretation boundaries
- `docs/PRICING_RESULTS_MEMO.md` — current pricing coefficients, robustness, and claim boundary
- `docs/BENCHMARK_SEARCH_CLOSURE.md` — bounded benchmark-search closure memo for the current QRA causal pilot
- `src/ati_shadow_policy/` — reusable download and research modules
- `scripts/` — task-oriented entry points
- `tests/` — unit tests for the core logic

## Sign conventions

- positive `missing_coupons_*` / `ati_baseline_bn` means a more bill-heavy maturity tilt relative to the chosen target share
- positive Public Duration Supply means more duration pushed into private hands
- positive QT contribution means Fed Treasury holdings falling
- buybacks enter the duration construction with the opposite sign because they remove duration from public hands

## What is intentionally provisional

- the pricing layer is still reduced-form and published as `supporting_provisional`
- extension modules remain supporting context, not headline evidence
- the duration headline still combines exact non-bill net supply with a QT proxy, and publish artifacts keep fallback constructions explicit
- the QRA event and elasticity layers remain supporting/provisional infrastructure; they are not promoted into headline pricing evidence here
- the longer historical extension before `2009Q1` remains out of scope for the current public release

## What comes next

- keep the QRA causal lane bounded and documented as a narrow audited pilot rather than reopening broad benchmark hunts
- decide whether one more bounded backend design round is justified or whether the current pricing claim boundary should stand as the stopping point for this public release
- if no remaining backend task is likely to move that claim boundary, shift effort to frontend and public communication rather than reopening open-ended robustness work
- only if a bounded backend round is justified, choose between richer release controls, a stronger event bridge, or a broader design pivot
