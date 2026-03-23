# Status Glossary

This file defines the readiness and source-quality labels used in publish artifacts and the site.
It also defines the event-quality vocabulary used to separate descriptive/supporting event surfaces from causal-eligible event surfaces.
The `qra_benchmark_evidence_registry` and `causal_claims_status` artifacts use these labels to keep benchmark provenance and claim scope explicit.

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
- `derived_event_ledger`, `derived_shock_crosswalk`, `derived_qra_usability`, `derived_qra_robustness`, `derived_treatment_comparison`, `derived_backfill_queue`, `derived_long_rate_translation` — publish-layer projections or summaries built from the underlying research tables.

## Review maturity

- `headline_ready` — mature enough for the repo’s current headline measurement claim.
- `supporting_ready` — internally coherent supporting surface.
- `provisional_supporting` — intentionally provisional; usable for transparency, diagnostics, or scaffolding but not a headline causal result.
- `not_started` — no reviewed surface exists yet.

Review maturity is not the same as causal eligibility. A dataset can be `supporting_ready` and still be descriptive/supporting only.

## Claim scope

`claim_scope` is the public claim boundary. It is orthogonal to readiness tiers and should be read first when deciding how a row may be cited.

- `descriptive_only` — descriptive/supporting context only; not a causal claim.
- `causal_pilot_only` — part of the causal pilot, but still narrower than the headline contract.
- `headline` — the only scope that should be read as the public headline claim.

If a row is not `claim_scope = headline`, it should not be described as the headline result even when it is reviewed or published.

## Event-quality tiers

The causal-credibility upgrade uses a hard `quality_tier` ladder for event-facing outputs:

- `Tier A` — causal-eligible component or event summary. Exact timing, component separability, benchmark coverage, and contamination checks are satisfied.
- `Tier B` — reviewed descriptive-only component or event summary. The release object is usable for descriptive monitoring, but at least one hard causal gate is still missing.
- `Tier C` — measurement-only official component or event summary. The official object exists, but reviewed causal gates are not yet complete.
- `Tier D` — provisional or scaffold component or event summary. Not ready for causal use and not fully reviewed.

Supporting status fields explain why a row is or is not causal-eligible:

- `eligibility_blockers` — pipe-delimited blocker reasons such as `missing_exact_timestamp`, `missing_expectation_benchmark`, `same_day_inseparable_bundle`, or `contamination_not_reviewed`
- `timestamp_precision` — whether the row carries `exact_time`, `date_only`, or `missing` timing
- `separability_status` — whether the release component is separable from same-day bundled Treasury communication
- `expectation_status` — whether a reviewed expectation benchmark exists
- `contamination_status` — whether overlap review is still pending or has been cleared
- `benchmark_search_disposition` — the current benchmark-hunt outcome for current-sample financing rows:
  - `upgraded_pre_release_external` means a credible Treasury-hosted pre-release public benchmark was verified
  - `blocked_source_family_exhausted` means the documented Treasury-hosted benchmark families were reviewed and no credible pre-release public benchmark was found
  - `blocked_open_candidate` means the row is still blocked, but a specific Treasury-hosted public source family remains to be adjudicated
- `macro_crosswalk_status` — whether broader same-day macro-overlap crosswalking is finished:
  - `reviewed_external_overlap` means a same-day external macro overlap is explicitly recorded
  - `reviewed_no_external_overlap` means the broader macro crosswalk was reviewed and found clean
  - `local_only_absent` means local Treasury-bundle review exists, but no separate external macro crosswalk artifact is attached
  - `pending_external_crosswalk` means the broader macro crosswalk has not been completed yet

These labels should be used for event-facing outputs, not for quarter-level Maturity Tilt or plumbing measurement surfaces.

`claim_scope` remains separate from `quality_tier`: a row can be reviewed, published, or causal-eligible and still stay out of the headline lane if its scope is `descriptive_only` or `causal_pilot_only`.

## Maturity Tilt Terms

- public label: `Maturity Tilt`; internal arithmetic identity: `ATI_q(tau) = NetBills_q - tau * FinancingNeed_q`
- public flow label: `Maturity-Tilt Flow`; internal field: `ati_baseline_bn`
- Baseline `tau` is `0.18`; robustness uses `0.15` and `0.20`.
- `bill_share = NetBills_q / FinancingNeed_q`
- `bill_share` is a signed financing-composition ratio, not a bounded literal share. It can exceed `1` or be negative when net bills exceed financing need or move opposite financing need.
- Positive Maturity-Tilt / missing-coupons values mean more bills and fewer coupons than the chosen target share.

## Headline flags

- `headline_ready = True` means the dataset is allowed to play a headline role in the current release.
- `fallback_only = True` means the dataset is published only as support, comparison, or diagnostic scaffolding.
- `public_role = headline` marks the intended headline series or table family.
- `public_role = supporting` marks non-headline but still published context.

For event-facing outputs, `public_role = supporting` should be read as descriptive/supporting unless the event-quality tier says otherwise.
