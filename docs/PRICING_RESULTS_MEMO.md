# Pricing Results Memo

Date: March 23, 2026

## Objective

This memo summarizes the current reduced-form pricing signal using neutral public labels:

- `Maturity-Tilt Flow` = internal field `ati_baseline_bn`
- `Excess Bills Stock` = internal field `stock_excess_bills_bn`
- `Public Duration Supply` = internal field `headline_public_duration_supply`

Headline estimand:

**basis-point change in `DGS10` and `THREEFYTP10` per `$100bn` shock on the named quantity input.**

## Locked baseline specs

- `monthly_flow_baseline`
  - outcomes: `THREEFYTP10`, `DGS10`
  - controls: `DFF`, `debt_limit_dummy`
  - sample: `2009-02-28` through `2026-03-31`
- `monthly_stock_baseline`
  - outcomes: `THREEFYTP10`, `DGS10`
  - controls: `DFF`, `debt_limit_dummy`
  - sample: `2009-01-31` through `2026-03-31`
- `weekly_duration_baseline`
  - outcomes: `THREEFYTP10`, `DGS10`
  - controls: `qt_proxy`, `buybacks_accepted`, `delta_wdtgal`, `DFF`
  - samples:
    - `THREEFYTP10`: `2003-01-08` through `2026-03-11`
    - `DGS10`: `2003-01-08` through `2026-03-18`

## Headline coefficients

Current baseline coefficients in bp per `$100bn`:

- `monthly_flow_baseline`
  - `THREEFYTP10`: `-2.813` bp, `p = 0.023`
  - `DGS10`: `-5.215` bp, `p = 0.027`
- `monthly_stock_baseline`
  - `THREEFYTP10`: `0.835` bp, `p = 0.473`
  - `DGS10`: `1.495` bp, `p = 0.479`
- `weekly_duration_baseline`
  - `THREEFYTP10`: `-26.084` bp, `p = 3.33e-09`
  - `DGS10`: `-44.054` bp, `p = 4.92e-08`

## What survives robustness

The most stable current signal is the **monthly Maturity-Tilt Flow** result:

- `post_2014`
  - `THREEFYTP10`: `-2.110` bp, `p = 0.037`
  - `DGS10`: `-3.892` bp, `p = 0.031`
- `post_2020`
  - `THREEFYTP10`: `-2.040` bp, `p = 0.001`
  - `DGS10`: `-3.757` bp, `p = 0.001`
- `exclude_debt_limit`
  - `THREEFYTP10`: `-3.716` bp, `p = 0.005`
  - `DGS10`: `-7.041` bp, `p = 0.004`
- supporting `DGS30` checks
  - baseline: `-5.387` bp, `p = 0.042`
  - `post_2014`: `-3.806` bp, `p = 0.026`
  - `post_2020`: `-3.486` bp, `p = 0.001`

The **weekly Public Duration Supply** result is numerically large, but less stable across sample windows:

- full sample baseline is strongly negative
- `post_2014` remains strongly negative
  - `THREEFYTP10`: `-9.912` bp, `p = 0.00017`
  - `DGS10`: `-16.822` bp, `p = 0.00037`
- `exclude_debt_limit` also remains strongly negative
  - `THREEFYTP10`: `-22.794` bp, `p = 0.00030`
  - `DGS10`: `-35.891` bp, `p = 0.00192`
- but the signal weakens sharply in `post_2009` and effectively disappears in `post_2020`

The **Excess Bills Stock** result does not currently survive as a persuasive standalone headline coefficient:

- baseline `THREEFYTP10`: `0.835` bp, `p = 0.473`
- baseline `DGS10`: `1.495` bp, `p = 0.479`
- sign and magnitude move across subsamples without achieving strong statistical support

## Horse race and standardized checks

Flow-vs-stock horse race:

- `Maturity-Tilt Flow` stays negative in both headline outcomes
  - `THREEFYTP10`: `-2.761` bp, `p = 0.074`
  - `DGS10`: `-5.121` bp, `p = 0.081`
- `Excess Bills Stock` stays weak
  - `THREEFYTP10`: `0.750` bp, `p = 0.486`
  - `DGS10`: `1.372` bp, `p = 0.482`

Standardized primary-predictor checks:

- standardized `monthly_flow_baseline`
  - `THREEFYTP10`: `-8.644` bp per `1 SD`, `p = 0.023`
  - `DGS10`: `-16.025` bp per `1 SD`, `p = 0.027`
- standardized `weekly_duration_baseline`
  - `THREEFYTP10`: `-6.575` bp per `1 SD`, `p = 3.33e-09`
  - `DGS10`: `-11.105` bp per `1 SD`, `p = 4.92e-08`
- standardized `monthly_stock_baseline`
  - still weak and statistically unpersuasive

## Scenario translation

Published scenario arithmetic:

- `plus_100bn_duration_supply`
  - `THREEFYTP10`: `-26.084` bp
  - `DGS10`: `-44.054` bp
- `plus_500bn_term_out`
  - `THREEFYTP10`: `4.174` bp
  - `DGS10`: `7.477` bp
- `plus_1000bn_term_out`
  - `THREEFYTP10`: `8.348` bp
  - `DGS10`: `14.954` bp

Interpretation note:

- the duration-supply scenario inherits the instability of the weekly-duration baseline across sample windows
- the term-out scenarios inherit the weakness of the standalone Excess Bills Stock coefficient

## Current claim boundary

The strongest current reduced-form claim is:

- the repo now has a defensible **monthly Maturity-Tilt Flow** relationship in which a more bill-heavy quarter-level maturity tilt is associated with lower long rates and a lower 10-year term premium proxy, with the sign surviving several key monthly subsamples

What remains out of bounds:

- a settled structural causal estimate of maturity composition on rates
- a claim that the weekly duration coefficient is already stable enough to anchor the project by itself
- a claim that Excess Bills Stock is already a strong standalone pricing result
- a claim that the supporting QRA event lane by itself identifies the headline coefficient
