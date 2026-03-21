# Research Design

## 1. Core empirical object

The note’s measurement object is the dollar value of **“missing coupons”**: the extra coupon issuance that would have occurred if Treasury had financed closer to a regular bill-share target.

Define for quarter `q`:

- `F_q` = total financing need / net new money raised
- `B_q` = net bill issuance
- `s_q = B_q / F_q` = net bill share
- `tau` = target bill share, baseline `0.18`

Then:

```text
MissingCoupons_q(tau) = (s_q - tau) * F_q
                      = B_q - tau * F_q
```

Two versions matter:

1. **Signed ATI**
   - can be negative if Treasury finances with fewer bills than target
2. **Positive-only missing coupons**
   - `max(MissingCoupons_q, 0)`
   - aligns most closely with the note’s easing narrative

Use both.

## 2. Baseline measurement choices

### Baseline

- target bill share = `18%`
- robustness = `15%`, `20%`
- bills = bills + cash management bills
- duration coupons = nominal notes + bonds + TIPS
- FRNs = separate bucket in the baseline, then sensitivity checks:
  - FRNs grouped with quasi-money
  - FRNs grouped with coupons

### Why not WAM first

WAM can be a useful robustness measure, but it is not the primary treatment variable here. The first-pass claim is about **dollar amounts of bills versus duration-bearing securities that the public must absorb**.

## 3. Main outcomes

### Pricing outcomes

Daily:

- 10-year term premium (`THREEFYTP10`)
- 10-year Treasury yield (`DGS10`)
- 2s10s slope (`DGS10 - DGS2`)
- 30s2s slope (`DGS30 - DGS2`)
- S&P 500 (`SP500`) as an optional risk-asset readout
- VIX (`VIXCLS`) as an optional stress / risk-premia readout

Weekly:

- reserves (`WRESBAL`)
- ON RRP (`RRPONTSYD`) and / or weekly reverse-repo liabilities (`WLRRAL`)
- Treasury General Account (`WDTGAL` or `WTREGEN`)
- Fed Treasury holdings (`TREAST`)

Quarterly / monthly:

- ATI / missing coupons
- bill share of financing
- buyback volumes
- stock bill share or marketable-debt composition
- average interest rates and debt structure for extensions

## 4. Identification strategy

### Design A. QRA announcement event study

The cleanest first design is a short-window event study around QRA statements.

Use two date definitions:

1. `official_release_date`
2. `market_pricing_marker_minus_1d`

The note visually emphasizes the day before some official release dates, so sensitivity to this date choice is part of the design.

#### Outcomes

For each event, compute:

- `d1_change`: `y[t] - y[t-1]`
- `d3_change`: `y[t+1] - y[t-1]`
- optional `d5_change`

#### Baseline event labels

- coupon-increase / term-out surprise
- coupon-restraint / bill-heavy surprise
- mixed / shorter-duration tilt

#### Strength

This limits the role of slow-moving deficit confounds.

#### Main threat

QRA dates can sit near other macro events, especially FOMC meetings and major data releases.

#### Fix

- annotate overlapping macro events
- run exclusion checks
- run both official date and T-1 marker
- inspect pre-trends within event windows

Implemented backend output:

- baseline event summary remains the stable v1 table
- robustness output is published separately with official-vs-T-1 splits and overlap-excluded sensitivity rows

### Design B. Weekly plumbing regressions

Weekly regressions test the mechanism that bills substitute more closely with reserves / RRP than coupons do.

Prototype specification:

```text
DeltaONRRP_t = alpha
             + beta1 * GrossBills_t
             + beta2 * GrossCoupons_t
             + beta3 * DeltaTGA_t
             + beta4 * QT_t
             + epsilon_t
```

```text
DeltaReserves_t = alpha
                + beta1 * GrossBills_t
                + beta2 * GrossCoupons_t
                + beta3 * DeltaTGA_t
                + beta4 * QT_t
                + epsilon_t
```

Where:

- `GrossBills_t` and `GrossCoupons_t` are first-round proxies from auction flows
- `QT_t = -Delta TREAST_t` so Fed runoff is positive when holdings decline

Expected signs in the note’s logic:

- more bills -> larger ON RRP drain than equivalent coupons
- more bills -> smaller reserve drain than equivalent coupons

These are not guaranteed, but they are the mechanism the note wants you to test.

### Design C. Quarterly / monthly pricing regressions

Once the ATI series is constructed:

```text
TermPremium_t = alpha + beta * ATI_q + gamma'X_t + epsilon_t
```

Controls can include:

- expected deficit / financing controls
- policy rate level
- inflation compensation or inflation controls
- macro surprise controls if available
- debt-limit episode dummies

Use this for medium-horizon interpretation, not as the only design.

### Design D. Flow-versus-stock horse race

Build:

- `FlowATI_t`
- `StockExcessBills_t`
- `PublicDurationSupply_t`

Then compare explanatory power:

```text
TermPremium_t = alpha + beta1 * FlowATI_t + beta2 * StockExcessBills_t + epsilon_t
```

The point is not to declare a winner too quickly, but to force the measurement debate into data.

## 5. Combined public duration supply

A strong extension of the note is to move from ATI alone to a broader object:

```text
PublicDurationSupply_t
  = NetCoupons_t
  + QT_t
  - Buybacks_t
```

First-round provisional version:

```text
ProvisionalPublicDurationSupply_t
  = GrossNominalCoupons_t
  + QT_t
  - Buybacks_t
```

because the full net quarter-level parser may arrive later than the first draft.

Implemented backend constructions now publish separate artifacts for:

- treasury-only nominal-plus-TIPS duration
- combined nominal-plus-TIPS plus QT minus buybacks
- combined nominal-plus-TIPS-plus-FRNs plus QT minus buybacks
- combined nominal-only plus QT minus buybacks

## 6. Data construction roadmap

### Stage 1

Use manual seed quarter inputs from the note to validate arithmetic.

### Stage 2

Download official QRA materials and FiscalData auction / MSPD / buyback sources.

### Stage 3

Replace manual quarter inputs with official captured values.

### Stage 4

Automate parsing only after the paper already has pricing and plumbing results.

## 7. Core robustness checks

- bill target at 15%, 18%, 20%
- positive-only vs signed ATI
- official event date vs T-1 event date
- including vs excluding overlapping macro-news dates
- bills only vs bills + FRNs as quasi-money
- nominal coupons only vs nominal + TIPS
- gross weekly auction proxy vs net quarterly financing measure
- flow-only vs stock-only vs combined models

## 8. What not to over-engineer first

Do not start with:

- structural term-structure models
- high-dimensional ML
- exhaustive NLP parsing of every Treasury release
- investor-allocation microdata before the main pricing facts exist

The first paper wins by being:

- transparent,
- measurement-driven,
- and institutionally grounded.
