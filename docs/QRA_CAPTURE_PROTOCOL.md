# QRA Capture Protocol

Use this when upgrading from the seeded quarter panel to an official quarter panel.

## Goal

Fill `data/manual/official_quarterly_refunding_capture_template.csv` quarter by quarter using official Treasury source documents.

The QRA raw manifest now exposes document-selection fields intended to support this workflow:

- `doc_type`
- `quality_tier`
- `relevance_score`
- `quarter_relevant`

## Priority fields

1. `quarter`
2. `qra_release_date`
3. `market_pricing_marker_minus_1d`
4. `total_financing_need_bn`
5. `net_bill_issuance_bn`
6. `net_coupon_issuance_bn` or enough information to back it out
7. `guidance_nominal_coupons`
8. `guidance_frns`
9. `guidance_buybacks`
10. `source_url`
11. `source_doc_local` (internal provenance only; excluded from public publish/site outputs)
12. `qa_status`

## Recommended capture order

1. official quarterly refunding statement / press release
2. financing estimates page or related attachment
3. Office of Economic Policy statement to TBAC
4. TBAC charts if needed for supporting context

Manifest quality convention:

- `primary_document` and `official_release_page` rows should be preferred over `collection_page`
- `--limit` downloads are quality-ranked to favor quarter-relevant documents first

## QA labels

Use one of:

- `seed_only`
- `manual_official_capture`
- `semi_automated_capture`
- `parser_verified`

Operational contract:

- `manual_official_capture` is the current target for human-entered official rows.
- `parser_verified` is reserved for rows whose extraction is parser-produced and then checked.
- `seed_only` rows are allowed as placeholders and must be replaced by official rows over time.

## Build Path

Input template:

- `data/manual/official_quarterly_refunding_capture_template.csv`

Processed output:

- `data/processed/official_quarterly_refunding_capture.csv`

Build command:

```bash
source .env
"$HOME/venvs/qrawatch/bin/python" scripts/20_enrich_official_qra_capture.py
"$HOME/venvs/qrawatch/bin/python" scripts/13_build_official_qra_capture.py
```

The enrichment step builds:

- `data/processed/qra_financing_release_map.csv`
- `data/processed/qra_refunding_statement_manifest.csv`
- `data/raw/qra/refunding_statement_downloads.csv`
- `data/processed/qra_refunding_statement_source_map.csv`

It updates the capture template with both official borrowing-estimate provenance and official quarterly refunding statement guidance. Rows enriched this way remain `semi_automated_capture` until unresolved quarter fields, especially exact `net_bill_issuance_bn`, are fully sourced.

Optional deterministic seeding of missing quarters from local seeds:

```bash
source .env
"$HOME/venvs/qrawatch/bin/python" scripts/13_build_official_qra_capture.py --seed-missing-quarters
```

Validation is fail-fast and loud:

- exact capture-column contract is required (missing/extra columns raise)
- `qa_status` must be one of the allowed labels above
- base required fields are enforced for every row
- `manual_official_capture` and `parser_verified` rows require sourcing + key financing fields
- date consistency checks enforce `market_pricing_marker_minus_1d = qra_release_date - 1 day`

## Rule

Do not wait for perfect parser automation. A correctly sourced manual row is better than a brittle automated row with uncertain quarter mapping.
