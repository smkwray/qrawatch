# Regression Specs

## A. Quarter-level ATI construction

For quarter `q`:

```text
ATI_q(tau) = NetBills_q - tau * FinancingNeed_q
BillShare_q = NetBills_q / FinancingNeed_q
NetCouponsImplied_q = FinancingNeed_q - NetBills_q
```

Baseline:
- `tau = 0.18`

Robustness:
- `tau = 0.15`
- `tau = 0.20`

## B. Daily descriptive QRA event windows

For outcome `Y_t` and event day `t0`:

```text
d1 = Y_t0 - Y_t(-1)
d3 = Y_t(+1) - Y_t(-1)
```

Recommended outcomes:
- `THREEFYTP10`
- `DGS10`
- `slope_30y_2y`
- `slope_10y_2y`
- `SP500`
- `VIXCLS`

These windows are descriptive/supporting outputs in the current release. Rows without exact release timing remain non-causal, and T-1 comparisons are robustness checks rather than a substitute for exact-timestamp identification.

## C. Weekly plumbing regressions

```text
DeltaONRRP_t = a + b1 * Bills_t + b2 * Coupons_t + b3 * DeltaTGA_t + b4 * QT_t + e_t
DeltaReserves_t = a + b1 * Bills_t + b2 * Coupons_t + b3 * DeltaTGA_t + b4 * QT_t + e_t
```

Baseline implementation notes:
- `Bills_t` = exact official net bill issuance in the headline plumbing variant
- `Coupons_t` = exact official combined non-bill net issuance in the headline plumbing variant
- `QT_t = -Delta TREAST_t`
- labeled fallback variants may still use gross auction-flow constructions for comparison, continuity, or diagnostics

Expected sign pattern in the note's logic:
- `b1` more negative than `b2` in ON RRP equation
- `b1` less negative than `b2` in reserves equation

These regressions are mechanism tests, not proofs of causal identification on their own.

## D. Combined public-duration-supply measure

```text
PublicDurationSupply_t = Coupons_t + QT_t - Buybacks_t
```

Headline construction:
- `Coupons_t` = exact combined non-bill net issuance reaching the public
- `Buybacks_t` = accepted amount in buyback operations
- `QT_t` = Fed Treasury holdings decline, used as a proxy contribution in the current public release

Fallback comparison:
- provisional comparison variants still publish gross coupon-flow constructions with labeled fallback status

## E. Flow vs stock horse race

```text
TermPremium_t = a + b1 * FlowATI_t + b2 * StockExcessBills_t + e_t
```

Possible stock definitions:
- cumulative signed ATI
- stock bill share minus target bill share
- stock marketable bill share from MSPD

Use these specifications only after labeling the event layer correctly. The daily QRA event stack remains descriptive/supporting unless and until the causal ledger upgrades a row to causal-eligible.
