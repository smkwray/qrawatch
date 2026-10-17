# QRA Watch

[Results site](https://smkwray.github.io/qrawatch/) · [Data sources](DATA_SOURCES.md) · [Pricing methods](docs/PRICING_METHODS.md)

QRA Watch measures the bill-versus-coupon composition of US Treasury financing and studies its associations with public duration supply, money-market conditions and interest rates. **QRA** means Quarterly Refunding Announcement. The project combines Treasury releases and auction data with Federal Reserve and public regulatory sources; it does not infer Treasury's motives from financing composition.

## Measurement and claim boundaries

| Evidence layer | Interpretation |
|---|---|
| Official quarterly financing and issuance | Descriptive measurement of Treasury maturity composition |
| Plumbing and duration-supply regressions | Supporting, provisional mechanism context |
| Monthly pricing regressions | Reduced-form associations with a longer sample and weaker identification |
| Release-level pricing at a fixed 63-business-day horizon | Supporting, provisional pricing design with one observation per release |
| Financing-announcement event pilot | Bounded evidence requiring independently timed pre-release benchmarks |

The retained site evidence covers official quarters from 2009Q1 through 2025Q4 and pricing results from the March 2026 snapshot. It does not establish fresh upstream data, a full-sample causal effect on long rates, a structural elasticity, or a stable regime coefficient. Illustrative scenario translations are not forecasts. A versioned measurement refresh is separate from the site snapshot and does not automatically update its pricing evidence.

Financing estimates and realized financing are distinct objects with different information dates. The quarterly refresh supports realized privately held net borrowing, settlement-date bill issues and maturities, and reconciliation with the Monthly Statement of the Public Debt (MSPD). Realized borrowing cannot substitute for a pre-announcement expectation in an event design.

The weekly plumbing/duration flow builder uses the same bill-settlement reconstruction as the quarterly path, including cash-management bills and maturity-only dates within the observed issue-date span. Missing amounts remain unavailable. The retained site snapshot has not been rebuilt with this arithmetic correction; its generating inputs and pricing conclusions remain unchanged.

## Main measures

- **Maturity Tilt:** bill-versus-coupon composition relative to a stated financing baseline.
- **Maturity-Tilt Flow:** signed quarterly coupon shortfall relative to the 18% baseline bill share; positive values mean more bill-heavy financing.
- **Excess Bills Stock:** the accumulated flow measure, distinct from a quarterly financing flow.
- **Public Duration Supply:** duration placed with private holders. Federal Reserve Treasury runoff adds supply; buybacks remove it. The headline combines non-bill net supply with a quantitative-tightening proxy.

See the [data dictionary](docs/DATA_DICTIONARY.md) for units, fields and construction details.

## Install and validate

Requires Python 3.10 or later. In an activated environment:

```bash
python -m pip install -e ".[test]"
make test PYTHON=python
make bootstrap PYTHON=python
```

Tests use synthetic fixtures. `make bootstrap` only creates local data/output directories; it does not download data. No private configuration file or API key is needed for these commands.

## Build with public data

```bash
make regenerate PYTHON=python
```

This runs the network-dependent acquisition and analytical pipeline and writes local generated outputs. An exported `FRED_API_KEY` selects the FRED API; otherwise downloads use the public graph endpoint. Set `SEC_USER_AGENT` to an appropriate contact-bearing identifier for SEC acquisition. Keep credentials in the environment, outside tracked files.

The pipeline needs official capture inputs and source coverage beyond what unit tests prove. Missing benchmarks, uncertain event timing and failed identification checks remain explicit. Successful execution alone does not make a causal result admissible.

The separate refresh entry points are `scripts/34_build_official_quarter_refresh.py` and `scripts/35_build_realized_financing_refresh.py`; use their `--help` output for required inputs and versioned output destinations. Their receipts keep source hashes and financing conventions separate from the default site bundle.

## Outputs and website

| Path | Contents |
|---|---|
| `data/raw/`, `data/interim/`, `data/processed/` | Downloaded inputs and locally constructed panels |
| `output/publish/` | Backend JSON/CSV summaries and evidence-status tables |
| `output/figures/` | Backend-generated figures |
| `site/data/`, `site/figures/` | Curated tracked snapshots consumed by the static frontend |
| `site/data/index.json` | Manifest controlling which artifacts the frontend renders |

Raw and processed data, output reports, logs, environments and caches are ignored. The site snapshots and documented manual capture inputs are intentional tracked release artifacts.

To rebuild the local site snapshot after a backend build:

```bash
make pricing-figures PYTHON=python
make site PYTHON=python
```

`make site` rebuilds publication summaries, validates and mirrors them into the tracked site bundle; it does not deploy the website. To view the existing snapshot without rebuilding it:

```bash
python -m http.server 8000 --directory site
```

The frontend reads the published JSON/CSV artifacts and figures, not raw or interim data. Numeric results, coverage, status counts and evidence roles come from backend artifacts.

## Documentation

- [Data sources](DATA_SOURCES.md)
- [Pricing methods](docs/PRICING_METHODS.md) and [retained pricing results](docs/PRICING_RESULTS_MEMO.md)
- [Regression specifications](docs/REGRESSION_SPECS.md)
- [Official capture protocol](docs/QRA_CAPTURE_PROTOCOL.md)
- [Evidence-status glossary](docs/STATUS_GLOSSARY.md)
- [Data dictionary](docs/DATA_DICTIONARY.md)

Reusable modules are under `src/ati_shadow_policy/`, command entry points under `scripts/`, and synthetic test fixtures under `tests/`.
