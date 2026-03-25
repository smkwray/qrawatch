# Pricing Results Memo

Date: March 24, 2026

## Objective

This memo summarizes the current reduced-form pricing signal using neutral public labels for the repo's `Maturity Tilt` pricing layer:

- `Maturity-Tilt Flow` = internal field `ati_baseline_bn`
- `Excess Bills Stock` = internal field `stock_excess_bills_bn`
- `Public Duration Supply` = internal field `headline_public_duration_supply`

Headline estimand:

**basis-point change in `DGS10` and `THREEFYTP10` per `$100bn` shock on the named quantity input.**

## Locked baseline specs

- `release_flow_baseline_63bd`
  - outcomes: `THREEFYTP10`, `DGS10`
  - controls: matching-horizon `delta_DFF`, `debt_limit_dummy`
  - sample: `2010-02-03` through `2025-07-30`
  - effective shocks: `60`
  - construction: one row per unique market-pricing marker; same-day duplicate release rows are collapsed before regression
- `release_flow_horizon_{1,5,10,21,42}bd`
  - outcomes: `THREEFYTP10`, `DGS10`
  - controls: matching-horizon `delta_DFF`, `debt_limit_dummy`
  - sample end varies with horizon and data availability
  - role: supporting horizon-profile checkpoints around the `+63bd` scalar
- `monthly_flow_baseline`
  - outcomes: `THREEFYTP10`, `DGS10`
  - controls: `DFF`, `debt_limit_dummy`
  - sample: `2009-02-28` through `2026-03-31`
  - effective shocks shown in the current table equal monthly rows, so this spec remains context rather than the main credibility anchor
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

- release-flow horizon profile
  - `+1bd`
    - `THREEFYTP10`: `-0.040` bp, `p = 0.652`
    - `DGS10`: `0.058` bp, `p = 0.728`
  - `+5bd`
    - `THREEFYTP10`: `-0.241` bp, `p = 0.138`
    - `DGS10`: `-0.497` bp, `p = 0.128`
  - `+10bd`
    - `THREEFYTP10`: `0.080` bp, `p = 0.750`
    - `DGS10`: `0.179` bp, `p = 0.728`
  - `+21bd`
    - `THREEFYTP10`: `0.164` bp, `p = 0.626`
    - `DGS10`: `0.418` bp, `p = 0.496`
  - `+42bd`
    - `THREEFYTP10`: `-0.404` bp, `p = 0.699`
    - `DGS10`: `-0.906` bp, `p = 0.647`
  - `+63bd` anchor
    - `THREEFYTP10`: `-0.309` bp, `p = 0.585`
    - `DGS10`: `-0.749` bp, `p = 0.485`
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

The fixed-horizon redesign cleaned up the release panel, but it did **not** yet produce a persuasive release-level elasticity.

- the `+63bd` anchor keeps the expected negative sign
  - `post_2014`
    - `THREEFYTP10`: `-0.408` bp, `p = 0.473`
    - `DGS10`: `-0.912` bp, `p = 0.402`
  - `post_2020`
    - `THREEFYTP10`: `-0.806` bp, `p = 0.134`
    - `DGS10`: `-1.752` bp, `p = 0.114`
  - `exclude_debt_limit`
    - `THREEFYTP10`: `-0.721` bp, `p = 0.246`
    - `DGS10`: `-1.575` bp, `p = 0.224`
  - supporting `DGS30` check
    - baseline: `-0.603` bp, `p = 0.508`

The horizon profile still shows endpoint instability rather than a settled curve:

- `+5bd` is negative on both outcomes
- `+10bd` and `+21bd` turn positive on both outcomes
- `+42bd` and `+63bd` return negative, but remain imprecise

The pre-release placebo checks are mixed rather than cleanly zero:

- `[-21,-1]`
  - `THREEFYTP10`: `-0.215` bp, `p = 0.218`
  - `DGS10`: `-0.019` bp, `p = 0.967`
- `[-5,-1]`
  - `THREEFYTP10`: `0.066` bp, `p = 0.657`
  - `DGS10`: `0.210` bp, `p = 0.490`

The strongest current reduced-form result is still the **monthly Maturity-Tilt Flow** relationship:

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

The **weekly Public Duration Supply** result is still numerically large but regime-sensitive:

- full sample baseline remains strongly negative
- `post_2014` remains strongly negative
  - `THREEFYTP10`: `-9.912` bp, `p = 0.00017`
  - `DGS10`: `-16.822` bp, `p = 0.00037`
- `exclude_debt_limit` remains strongly negative
  - `THREEFYTP10`: `-22.794` bp, `p = 0.00030`
  - `DGS10`: `-35.891` bp, `p = 0.00192`
- but `post_2009` is weak and `post_2020` is effectively absent

The **Excess Bills Stock** result is still weak:

- baseline `THREEFYTP10`: `0.835` bp, `p = 0.473`
- baseline `DGS10`: `1.495` bp, `p = 0.479`
- tau sensitivity does not fix it
  - `tau = 0.15`
    - `THREEFYTP10`: `0.407` bp, `p = 0.702`
    - `DGS10`: `0.695` bp, `p = 0.721`
  - `tau = 0.20`
    - `THREEFYTP10`: `1.143` bp, `p = 0.331`
    - `DGS10`: `2.076` bp, `p = 0.329`

## Leave-one-release-out

The `+63bd` release-level anchor does not appear to be driven by one single release, but it also does not tighten into a persuasive estimate when any one release is removed.

- `release_flow_baseline_63bd`, `THREEFYTP10`
  - coefficient range: `-0.625` to `0.062` bp
  - p-value range: `0.255` to `0.917`
- `release_flow_baseline_63bd`, `DGS10`
  - coefficient range: `-1.414` to `0.055` bp
  - p-value range: `0.265` to `0.959`

So the fixed-horizon release-level flow design is cleaner than the prior variable-window implementation, but still too imprecise to displace the monthly flow result as the strongest published signal.

## Horse race, standardized checks, and scenarios

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

Published scenario arithmetic:

- `plus_100bn_duration_supply`
  - `THREEFYTP10`: `-26.084` bp
  - `DGS10`: `-44.054` bp
  - role: `supporting`
- `plus_500bn_term_out`
  - `THREEFYTP10`: `4.174` bp
  - `DGS10`: `7.477` bp
  - role: `illustrative_only`
- `plus_1000bn_term_out`
  - `THREEFYTP10`: `8.348` bp
  - `DGS10`: `14.954` bp
  - role: `illustrative_only`

## Current claim boundary

The strongest current reduced-form claim is:

- the repo now has a cleaner unique-release fixed-horizon pricing panel, with same-day duplicate release rows collapsed into one pricing shock and a published horizon profile around the `+63bd` scalar
- the strongest current negative Maturity-Tilt signal still remains the **monthly carry-forward Maturity-Tilt Flow** relationship rather than the release-level anchor
- the `+63bd` release-flow coefficient is directionally plausible, but still too small and imprecise to anchor the project by itself

What remains out of bounds:

- a settled structural causal estimate of maturity composition on rates
- a claim that the `+63bd` release-flow coefficient is already strong enough to be the sole headline elasticity
- a claim that the release-flow horizon profile is already stable across natural endpoint choices
- a claim that the weekly duration coefficient is already stable across regimes
- a claim that Excess Bills Stock is already a persuasive standalone pricing result
- a claim that stock-based term-out scenario arithmetic is headline-ready rather than illustrative only
- a claim that the supporting QRA event lane by itself identifies the headline coefficient
