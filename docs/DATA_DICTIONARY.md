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
- `official_release_date` — official QRA statement date
- `market_pricing_marker_minus_1d` — alternative date to test
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
- `qa_status` — `seed_only`, `manual_official_capture`, `semi_automated_capture`, or `parser_verified`
- `notes` — capture notes and caveats

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
- `*_d1` — 1-day change
- `*_d3` — 3-day change

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

### `output/publish/extension_status.csv`

- site-facing extension backend status table
- records whether each extension has raw artifacts, processed outputs, normalized panels, and published summaries

### `output/publish/series_metadata_catalog.csv`

- site-facing metadata catalog for headline and fallback series
- records frequency, units, sign conventions, source-quality labels, and series role
