# Status Glossary

This file defines the readiness and source-quality labels used in publish artifacts and the site.

## Readiness tiers

- `headline_ready` — acceptable as a headline measurement surface for the current release.
- `supporting_ready` — coherent supporting artifact with its required source files and integrity checks satisfied, but not a headline claim.
- `supporting_provisional` — published for transparency or workflow support, but still missing causal-review maturity, full contracts, or both.
- `hybrid_ready` — usable output that still mixes official and fallback inputs.
- `fallback_only` — non-headline or fallback surface kept for comparison, continuity, or diagnostics.
- `summary_ready` — extension summary surface is built and publishable at summary depth.
- `inventory_ready` — extension inventory exists, but no publish-ready summary panel is complete yet.
- `raw_only` — raw artifacts exist without a finished processed summary layer.
- `not_started` — no usable artifact has been built yet.
- `missing` — expected artifact is absent.

## Source-quality labels

- `exact_official` — directly sourced from official Treasury materials without seed dependence.
- `exact_official_numeric` — exact-official source with the numeric fields needed for headline ATI arithmetic.
- `exact_official_net` — exact official net-flow construction used in the weekly plumbing headline.
- `official_hybrid` — official sourcing exists, but one or more required fields still depend on fallback or semi-automated capture.
- `manual_qra_shock_template_plus_event_panel` — event-layer output built from the reviewed/manual shock template joined to the market event panel.
- `derived_event_ledger`, `derived_shock_crosswalk`, `derived_qra_usability`, `derived_qra_robustness`, `derived_treatment_comparison` — publish-layer projections or summaries built from the underlying research tables.

## Review maturity

- `headline_ready` — mature enough for the repo’s current headline measurement claim.
- `supporting_ready` — internally coherent supporting surface.
- `provisional_supporting` — intentionally provisional; usable for transparency, diagnostics, or scaffolding but not a headline causal result.
- `not_started` — no reviewed surface exists yet.

## ATI terms

- `ATI_q(tau) = NetBills_q - tau * FinancingNeed_q`
- Baseline `tau` is `0.18`; robustness uses `0.15` and `0.20`.
- `bill_share = NetBills_q / FinancingNeed_q`
- `bill_share` is a signed financing-composition ratio, not a bounded literal share. It can exceed `1` or be negative when net bills exceed financing need or move opposite financing need.
- Positive ATI / missing-coupons values mean more bills and fewer coupons than the chosen target share.

## Headline flags

- `headline_ready = True` means the dataset is allowed to play a headline role in the current release.
- `fallback_only = True` means the dataset is published only as support, comparison, or diagnostic scaffolding.
- `public_role = headline` marks the intended headline series or table family.
- `public_role = supporting` marks non-headline but still published context.
