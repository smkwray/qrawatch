# Pricing Methods

## Objective

The pricing layer estimates how Treasury maturity composition and public duration supply relate to long rates in reduced form.

Public labels used in this round:

- `Maturity Tilt` = quarter-level financing composition object built from official quarter capture
- `Maturity-Tilt Flow` = public label for internal field `ati_baseline_bn`
- `Excess Bills Stock` = public label for internal field `stock_excess_bills_bn`
- `Public Duration Supply` = weekly duration-supply object used in the duration and pricing layers

Primary headline outcomes:

- `THREEFYTP10` (10-year term premium proxy)
- `DGS10` (10-year nominal Treasury yield)

Supporting pricing outcome:

- `DGS30`

## Headline estimands

The locked public estimand in this round is:

**basis-point change in `DGS10` and `THREEFYTP10` per `$100bn` shock on the named quantity input.**

That is implemented three ways:

- Maturity-Tilt Flow baseline: bp change in rates per `$100bn` change in `ati_baseline_bn`
- Excess Bills Stock baseline: bp change in rates per `$100bn` change in `stock_excess_bills_bn`
- Public Duration Supply baseline: bp change in rates per `$100bn` change in `headline_public_duration_supply`

## Locked baseline specifications

`monthly_flow_baseline`

`Y_t = a + b1 * ati_baseline_bn_t + b2 * DFF_t + b3 * debt_limit_dummy_t + e_t`

- outcomes: `DGS10`, `THREEFYTP10`
- sample start: `2009Q1`
- interpretation: Maturity-Tilt Flow relationship with long rates after policy-rate and debt-limit controls

`monthly_stock_baseline`

`Y_t = a + b1 * stock_excess_bills_bn_t + b2 * DFF_t + b3 * debt_limit_dummy_t + e_t`

- outcomes: `DGS10`, `THREEFYTP10`
- sample start: `2009Q1`
- interpretation: stock relationship between excess bills and long rates after policy-rate and debt-limit controls

`weekly_duration_baseline`

`Y_t = a + b1 * headline_public_duration_supply_t + b2 * qt_proxy_t + b3 * buybacks_accepted_t + b4 * delta_wdtgal_t + b5 * DFF_t + e_t`

- outcomes: `DGS10`, `THREEFYTP10`
- sample: non-missing weekly control window
- interpretation: reduced-form weekly relationship between supply reaching the public and long rates, with major balance-sheet/plumbing controls included

Inference uses HAC / Newey-West standard errors in every published pricing table.

## Robustness pack

Published robustness families in this round:

- `post_2009`
- `post_2014`
- `post_2020`
- `exclude_debt_limit`
- `flow_vs_stock_horse_race`
- `standardized_predictors`
- supporting `DGS30` outcome check

Artifacts:

- `pricing_spec_registry`
- `pricing_regression_summary`
- `pricing_subsample_grid`
- `pricing_regression_robustness`
- `pricing_scenario_translation`

## Figure pack

This round also publishes four SVG figures:

- `maturity_tilt_flow_vs_dgs10.svg`
- `excess_bills_stock_vs_threefytp10.svg`
- `pricing_headline_coefficients.svg`
- `pricing_scenario_translation.svg`

The two overlay charts are standardized time-series comparisons. They are meant to show timing and sign patterns, not literal level equivalence.

## Why the QRA lane stays supporting

The event-causal lane is intentionally kept as supporting/pilot because benchmark-ready clean financing rows are still narrow relative to full-sample claims.

Current boundary:

- the current-sample financing pilot exists and is audited
- causal review fields and blocker states are explicit and machine-readable
- the benchmark-search closure round is complete within the documented Treasury-hosted source universe
- but the event sample remains too thin to serve as the sole headline pricing coefficient source

So the project structure in this round is:

- headline center of gravity: official Maturity Tilt measurement + duration/plumbing + reduced-form pricing
- supporting validation: QRA event and causal-pilot surfaces, used for auditability and consistency checks

## Interpretation boundary

Pricing coefficients should be interpreted as reduced-form relationships in this round. They are useful for direction, scale checks, and scenario arithmetic, but they are not yet a settled structural causal estimate.

In practice:

- the monthly Maturity-Tilt Flow coefficient is the cleaner current headline quantity for quarter-level maturity composition
- the weekly Public Duration Supply coefficient is a useful duration-style pricing bridge, but it still needs careful sample scrutiny
- the Excess Bills Stock coefficient is published because it is substantively important, not because it is already the strongest signal
