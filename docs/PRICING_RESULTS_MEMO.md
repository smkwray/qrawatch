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

- `release_flow_baseline_next_release`
  - outcomes: `THREEFYTP10`, `DGS10`
  - controls: matching-horizon `delta_DFF`, `debt_limit_dummy`
  - sample: `2010-02-03` through `2025-04-30`
  - effective shocks: `58`
- `release_flow_baseline_21bd`
  - outcomes: `THREEFYTP10`, `DGS10`
  - controls: matching-horizon `delta_DFF`, `debt_limit_dummy`
  - sample: `2010-02-03` through `2025-07-30`
  - effective shocks: `59`
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

- `release_flow_baseline_next_release`
  - `THREEFYTP10`: `-0.295` bp, `p = 0.630`
  - `DGS10`: `-0.759` bp, `p = 0.511`
- `release_flow_baseline_21bd`
  - `THREEFYTP10`: `0.168` bp, `p = 0.615`
  - `DGS10`: `0.429` bp, `p = 0.488`
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

The key result of this round is negative but still not strong enough:

- the primary release-level flow anchor keeps the expected negative sign
  - `post_2014`
    - `THREEFYTP10`: `-0.422` bp, `p = 0.492`
    - `DGS10`: `-0.998` bp, `p = 0.393`
  - `post_2020`
    - `THREEFYTP10`: `-0.832` bp, `p = 0.147`
    - `DGS10`: `-1.876` bp, `p = 0.097`
  - `exclude_debt_limit`
    - `THREEFYTP10`: `-0.758` bp, `p = 0.273`
    - `DGS10`: `-1.677` bp, `p = 0.248`
  - supporting `DGS30` check
    - baseline: `-0.550` bp, `p = 0.579`

The supporting `+21bd` release window does not reinforce the anchor:

- baseline sign turns positive
  - `THREEFYTP10`: `0.168` bp, `p = 0.615`
  - `DGS10`: `0.429` bp, `p = 0.488`
- `post_2020` remains small and positive
  - `THREEFYTP10`: `0.034` bp, `p = 0.892`
  - `DGS10`: `0.192` bp, `p = 0.727`

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

The release-level anchor does not appear to be driven by one single release, but it also does not tighten into a persuasive estimate when any one release is removed.

- `release_flow_baseline_next_release`, `THREEFYTP10`
  - coefficient range: `-0.674` to `0.094` bp
  - p-value range: `0.243` to `0.901`
- `release_flow_baseline_next_release`, `DGS10`
  - coefficient range: `-1.430` to `0.055` bp
  - p-value range: `0.229` to `0.963`

So the release-level flow design is directionally plausible, but still too imprecise to displace the monthly flow result as the strongest published signal.

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

- the repo still has one meaningful negative Maturity-Tilt signal, but it remains the **monthly carry-forward Maturity-Tilt Flow** relationship rather than the new release-level anchor
- the new release-level flow design is conceptually cleaner and directionally plausible, but its current coefficients are too small and imprecise to anchor the project by themselves

What remains out of bounds:

- a settled structural causal estimate of maturity composition on rates
- a claim that the release-level flow coefficient is already strong enough to be the sole headline elasticity
- a claim that the weekly duration coefficient is already stable across regimes
- a claim that Excess Bills Stock is already a persuasive standalone pricing result
- a claim that stock-based term-out scenario arithmetic is headline-ready rather than illustrative only
- a claim that the supporting QRA event lane by itself identifies the headline coefficient
