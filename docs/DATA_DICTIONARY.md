# Data Dictionary

## Manual seed files

### `data/manual/quarterly_refunding_seed.csv`

- `quarter` — quarter label for financing period
- `financing_need_bn` — total net financing need in billions
- `net_bills_bn` — net bill issuance in billions
- `seed_source` — note table or note section used
- `seed_quality` — estimate, adjusted estimate, or forecast
- `comments` — free text notes

### `data/manual/qra_event_seed.csv`

- `event_id` — stable event identifier
- `event_label` — human-readable label
- `official_release_date` — official QRA statement date used for descriptive event monitoring
- `market_pricing_marker_minus_1d` — alternative date to test in descriptive robustness checks
- `expected_direction` — qualitative event classification
- `notes` — summary interpretation
- `seed_source` — provenance

### `data/manual/official_quarterly_refunding_capture_template.csv`

- `quarter` — canonical quarter label such as `2024Q3`
- `qra_release_date` — official Treasury QRA release date
- `market_pricing_marker_minus_1d` — T-1 market marker used in robustness/event work
- `total_financing_need_bn` — total financing need in billions
- `net_bill_issuance_bn` — net bill issuance in billions
- `gross_coupon_schedule_bn` — gross coupon schedule if captured
- `net_coupon_issuance_bn` — net coupon issuance if captured
- `frn_issuance_bn` — FRN issuance if captured
- `guidance_nominal_coupons` — free-text coupon guidance notes
- `guidance_frns` — free-text FRN guidance notes
- `guidance_buybacks` — free-text buyback guidance notes
- `source_url` — official Treasury source URL
- `source_doc_local` — repo-relative internal provenance references used for validation and reproducibility; not exposed in public site artifacts
- `source_doc_type` — source family such as `seed_csv`, official statement, or attachment type
- `financing_source_*` — canonical provenance triplet for the Treasury borrowing-estimate / financing release
- `refunding_statement_source_*` — canonical provenance triplet for the official quarterly refunding statement
- `auction_reconstruction_source_*` — canonical provenance triplet for the FiscalData auction-reconstruction layer
- `qa_status` — `seed_only`, `manual_official_capture`, `semi_automated_capture`, or `parser_verified`
- `notes` — capture notes and caveats

### `data/manual/qra_release_component_registry.csv`

- `release_component_id` — canonical component identifier of the form `<event_id>__<component_type>`
- `event_id` — parent event identifier for grouping components back to a QRA episode
- `quarter` — canonical quarter label
- `component_type` — Treasury release component such as `financing_estimates` or `policy_statement`
- `release_timestamp_et` — reviewed Treasury release timestamp in Eastern Time
- `timestamp_precision` — `exact_time`, `date_only`, or `missing`
- `source_url` — official Treasury page for the specific release component
- `release_timestamp_source_method` — how the exact-time claim was evidenced
- `timestamp_evidence_url` / `timestamp_evidence_note` — supporting provenance for the exact-time claim
- `release_timezone_asserted` — timezone asserted for the release timestamp, currently Eastern Time for Treasury releases
- `bundle_decomposition_evidence` — short audit note explaining why the component is treated as separable within the broader QRA bundle
- `bundle_id` — grouping key for components belonging to the same broader QRA episode
- `release_sequence_label` — ordering label such as `financing_then_policy`
- `separable_component_flag` — whether the component is treated as separable from same-day bundled Treasury communication
- `review_status` / `review_notes` — manual review status and notes for the component split

### `data/manual/qra_component_expectation_template.csv`

- `release_component_id` — component key joined against `qra_release_component_registry.csv`
- `benchmark_timestamp_et` — timestamp for the benchmark expectation snapshot
- `benchmark_source` — provenance for the expectation benchmark
- `benchmark_document_url` / `benchmark_document_local` — canonical benchmark-document provenance
- `benchmark_release_timestamp_et` — observed publication timestamp or date for the external benchmark document
- `benchmark_release_timestamp_precision` — timing precision for that benchmark document, such as `date_only`
- `benchmark_timestamp_source_method` — how the benchmark timing was verified
- `benchmark_pre_release_verified_flag` — whether pre-release timing is actually evidenced rather than assumed
- `benchmark_observed_before_component_flag` — whether the benchmark was observed before the component release
- `expected_composition_bn` — expected maturity-composition path in billions
- `realized_composition_bn` — realized maturity-composition path in billions
- `composition_surprise_bn` — reviewed surprise measure used for causal treatment when present
- `surprise_construction_method` / `surprise_units` — explicit construction contract for the component-level surprise
- `benchmark_stale_flag` — whether the benchmark is considered stale
- `expectation_review_status` / `expectation_notes` — review status and notes for the expectation layer

### `data/manual/qra_event_contamination_reviews.csv`

- `release_component_id` — component key joined against `qra_release_component_registry.csv`
- `contamination_flag` — whether macro or policy contamination is present
- `contamination_status` — contamination adjudication state such as `pending_review` or `reviewed_clean`
- `contamination_review_status` — review status for the contamination decision
- `contamination_window_start_et` / `contamination_window_end_et` — reviewed event window used for the contamination call
- `confound_release_type` / `confound_release_timestamp_et` — structured provenance for the competing release when present
- `decision_rule` — short structured note describing the contamination rule applied
- `exclude_from_causal_pool` — whether contamination forces exclusion from the causal pool
- `decision_confidence` — manual confidence label for the contamination decision
- `contamination_label` / `contamination_notes` — short label and free-text notes for the overlap call

## Processed datasets

### `data/processed/ati_index_seed.csv`

- `bill_share` — net bill share of financing need
- `net_coupons_bn_implied` — financing need minus net bills
- `missing_coupons_15_bn` — ATI using 15% target
- `missing_coupons_18_bn` — ATI using 18% target
- `missing_coupons_20_bn` — ATI using 20% target
- `ati_baseline_bn` — alias for `missing_coupons_18_bn`

### `data/processed/qra_event_panel.csv`

- `event_date_requested` — requested event date from manual seed
- `event_date_aligned` — nearest available market-data date
- `event_date_type` — official vs T-1 marker
- `*_d1` — 1-day change used for descriptive event monitoring
- `*_d3` — 3-day change used for descriptive event monitoring

### `data/processed/plumbing_weekly_panel.csv`

- `bill_like` — gross bill flow proxy
- `coupon_like_total` — gross coupon flow proxy
- `delta_wdtgal` — weekly TGA change
- `qt_proxy` — negative change in Fed Treasury holdings

### `data/processed/public_duration_supply_provisional.csv`

- `buybacks_accepted` — weekly accepted buyback amount
- `provisional_public_duration_supply` — coupons + QT - buybacks
- `notes` — construction note

### `data/processed/official_quarterly_refunding_capture.csv`

- same column contract as `data/manual/official_quarterly_refunding_capture_template.csv`
- processed counterpart used for downstream official-quarter build steps
- `qa_status` semantics are enforced fail-fast by `scripts/13_build_official_qra_capture.py`

### `data/processed/qra_financing_release_map.csv`

- deterministic quarter-to-source map built from downloaded QRA borrowing-estimate releases
- includes `source_url`, internal provenance references, expected quarter period, and `announced_borrowing_bn`
- used to enrich the official-capture template with official borrowing-release provenance before fuller quarter capture is complete

### `data/processed/qra_refunding_statement_manifest.csv`

- deterministic quarter-to-statement link inventory built from the Treasury official-remarks archive page
- carries the quarter label, statement URL, source page, and archive provenance used for statement downloads

### `data/raw/qra/refunding_statement_downloads.csv`

- deterministic download log for quarter-matched official refunding statements
- mirrors the main QRA download metadata contract with local path, HTTP status, content type, and byte counts

### `data/processed/qra_refunding_statement_source_map.csv`

- quarter-to-statement guidance map built from downloaded official refunding statements
- includes merged statement provenance plus `guidance_nominal_coupons`, `guidance_frns`, `guidance_buybacks`, and statement-level bill-guidance notes

### `data/processed/ati_index_official_capture.csv`

- official-quarter ATI rebuild derived from `data/processed/official_quarterly_refunding_capture.csv`
- preserves official-capture provenance through `capture_quality` and `capture_source`

### `data/processed/qra_event_registry_v2.csv`

- `release_timestamp_et` — release timestamp in Eastern Time when available
- `release_bundle_type` — bundle classification for the event-level registry
- `timing_quality` — descriptive timing quality inherited from the calendar scaffold
- `financing_need_news_flag` / `composition_news_flag` / `forward_guidance_flag` — content-decomposition flags for the event-level registry
- `headline_eligibility_reason` — descriptive headline blocker label, not a causal-eligibility label
- `quality_tier` — event-level summary of the best available causal tier across its release components
- `eligibility_blockers` — event-level rollup of causal blockers inherited from the component registry
- `timestamp_precision` / `separability_status` / `expectation_status` / `contamination_status` — event-level summary governance fields for the causal path
- `release_component_count` / `causal_eligible_component_count` — counts used to distinguish descriptive coverage from true causal eligibility

### `data/processed/qra_release_component_registry.csv`

- component-level registry used by the causal-credibility upgrade
- carries release timestamps, component separability, expectation/surprise provenance, contamination review, `quality_tier`, `eligibility_blockers`, and `causal_eligible`

### `data/processed/qra_causal_qa_ledger.csv`

- event-level QA rollup derived from the component registry
- records the best available `quality_tier` for each event plus aggregated blocker fields and counts of causal-eligible components

### `data/processed/event_design_status.csv`

- compact status surface for the causal-design upgrade
- reports counts such as `tier_a_count`, `exact_time_component_count`, `reviewed_surprise_ready_count`, and `reviewed_clean_component_count`

### `data/processed/qra_event_elasticity.csv`

- `shock_bn` — reviewed/manual canonical shock used for supporting event-layer summaries
- `schedule_diff_10y_eq_bn` / `schedule_diff_dynamic_10y_eq_bn` / `schedule_diff_dv01_usd` — comparison treatments for audit and diagnostics
- `descriptive_headline_reason` / `usable_for_descriptive_headline` — preferred descriptive-usability aliases for one round of public compatibility
- `usable_for_headline` — compatibility field; descriptive usability flag, not a guarantee of causal eligibility

### `data/processed/qra_event_shock_summary.csv`

- dedicated event-level shock summary artifact used by the identification/publication layer
- mirrors the descriptive shock fields from the elasticity layer without treating elasticity output as the canonical upstream source

### `data/processed/qra_benchmark_blockers_by_event.csv`

- event-level blocker summary for the current-sample financing pilot
- reports counts such as `pre_release_external_count`, `external_timing_unverified_count`, `reviewed_surprise_ready_count`, and `tier_a_count`
- `benchmark_blockers` is the compact public blocker label rollup

### `data/processed/investor_allotments.csv`

- processed inventory of investor-allotments raw artifacts
- includes manifest/download provenance, internal local-path metadata, extension typing, and status fields for publish-layer reporting

### `data/processed/investor_allotments_panel.csv`

- normalized long panel of Treasury investor-allotment observations
- includes `auction_date`, `security_family`, `investor_class`, `measure`, `value`, units, and source provenance
- acts as the processed input for `output/publish/investor_allotments_summary.*`

### `data/processed/primary_dealer_inventory.csv`

- processed inventory of New York Fed primary-dealer statistics artifacts
- classifies dataset family, release scope, local file status, and observed artifact shape for CSV/JSON/XML downloads

### `data/processed/primary_dealer_panel.csv`

- normalized long panel for parseable New York Fed dealer-series and market-share sources
- standardizes date, series id, metric id, units, frequency, source dataset type, and provenance fields
- acts as the processed input for `output/publish/primary_dealer_summary.*`

### `data/processed/sec_nmfp_inventory.csv`

- processed inventory of SEC N-MFP dataset artifacts
- tracks dataset/version family, period coverage, documentation vs archive typing, and download provenance for version-aware extension reporting

### `data/processed/sec_nmfp_summary_panel.csv`

- summary-depth processed panel for SEC N-MFP archives
- includes version-aware coverage, archive/file availability, parse status, top-line counts, and field-availability flags for Treasury/repo-relevant exposure fields
- acts as the processed input for `output/publish/sec_nmfp_summary.*`

### `output/publish/dataset_status.csv`

- site-facing readiness/status index for core and extension datasets
- reports readiness tier, source quality, headline/fallback status, and regeneration freshness
- see `docs/STATUS_GLOSSARY.md` for label semantics used across publish artifacts and the site

### Event-quality terminology

The causal-credibility upgrade uses `quality_tier` values rather than prose labels:

- `Tier A` — causal-eligible
- `Tier B` — reviewed descriptive-only
- `Tier C` — measurement-only official component or event
- `Tier D` — provisional or scaffold

These are governance labels, not statistical results. Use `eligibility_blockers`, `timestamp_precision`, `separability_status`, `expectation_status`, and `contamination_status` to understand why a row remains non-causal.

### `output/publish/extension_status.csv`

- site-facing extension backend status table
- records whether each extension has raw artifacts, processed outputs, normalized panels, and published summaries

### `output/publish/series_metadata_catalog.csv`

- site-facing metadata catalog for headline and fallback series
- records frequency, units, sign conventions, source-quality labels, and series role

### `output/publish/pricing_spec_registry.csv`

- one row per locked pricing specification and headline outcome
- `pipeline_anchor_role` — backend workflow role such as `credibility_anchor`, `context`, or `supporting`
- `public_claim_role` — public-facing claim role such as `supporting_anchor`, `supporting_context`, or `supporting`
- `public_readiness` — public readiness tier for the pricing pack, currently `supporting_provisional`
- `window_definition` — fixed-horizon release window, monthly carry-forward window, or weekly duration window
- `sample_start` / `sample_end` — realized regression-sample bounds after required merges, missing-value filtering, and completed-month locking where applicable
- `predictor_set` / `control_set` — canonical pipe-delimited regressor lists used for the locked spec

### `output/publish/pricing_regression_summary.csv`

- headline summary pack for the locked pricing specifications
- includes one row per estimated term in each published baseline regression
- `pipeline_model_mode` — backend workflow bucket for the row, currently `baseline_summary`
- `pipeline_anchor_role` — backend workflow role inherited from the underlying spec
- `public_claim_role` — public-facing claim role for the underlying spec
- `public_readiness` — public readiness tier for the pricing family, currently `supporting_provisional`
- `effective_shock_count` — unique release markers or time rows used by the fitted regression after sample filtering
- `term_mode` — whether the row belongs to the baseline term pack, standardized-predictor pack, supporting outcome pack, or another robustness family

### `output/publish/pricing_regression_robustness.csv`

- primary-predictor rows for the published pricing robustness families
- `variant_id` / `variant_family` — named robustness design such as `post_2014`, `release_flow_placebo`, or `standardized_predictors`
- `pipeline_model_mode` — backend workflow bucket for the robustness row
- `public_claim_role` — public-facing claim role inherited from the underlying pricing family
- `public_readiness` — public readiness tier for the pricing family, currently `supporting_provisional`
- `sample_start` / `sample_end` — realized bounds for the fitted robustness regression

### `output/publish/pricing_subsample_grid.csv`

- primary-predictor rows for locked pricing specs under named sample restrictions
- `variant_id` / `variant_family` — named sample restriction such as `post_2014`, `post_2020`, or `exclude_debt_limit`
- `effective_shock_count` — unique rows or release markers retained after the named sample restriction

### `output/publish/pricing_release_flow_panel.csv`

- one row per unique market-pricing marker in the fixed-horizon release-flow design
- carries realized release dates, merged quarter labels, fixed-horizon end dates, cumulative rate changes, placebo-window deltas, and the release-level debt-limit control

### `output/publish/pricing_release_flow_leave_one_out.csv`

- leave-one-release-out diagnostic for the primary `+63bd` release-flow anchor
- one row per omitted release and headline outcome

### `output/publish/pricing_tau_sensitivity_grid.csv`

- stock-only pricing coefficients under alternate target bill-share anchors
- `tau` — target bill-share anchor used to reconstruct the Excess Bills Stock predictor

### `output/publish/pricing_scenario_translation.csv`

- scenario arithmetic derived from fitted pricing coefficients
- `scenario_role` — public interpretation boundary such as `supporting` or `illustrative_only`
- `coef_bp_per_100bn` — fitted coefficient used for the scenario translation
- `implied_bp_change` — scenario-implied basis-point change under the named shock size
