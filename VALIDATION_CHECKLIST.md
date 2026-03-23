# Validation Checklist

Use this before treating any output as a real result.

## Measurement

- [ ] Are all bill-share calculations based on **net** financing need where intended?
- [ ] Are any fallback or provisional **gross auction** proxies labeled as such rather than mixed into headline series?
- [ ] Are the 15%, 18%, and 20% target calculations all present?
- [ ] Are bill shares expressed consistently in decimal form rather than percentage strings?
- [ ] Are seeded comparison rows clearly marked as seed inputs rather than promoted official values?
- [ ] Does the official capture path avoid seed contamination in promoted rows?

## Units

- [ ] Are FRED weekly reserves and TGA in millions, and did we convert when merging with billions-based series?
- [ ] Is ON RRP measured in the same units across daily and weekly versions?
- [ ] Are auction and buyback amounts consistently scaled?

## Dates

- [ ] Did we preserve both official QRA dates and T-1 markers?
- [ ] Are weekly series aligned to Wednesday consistently?
- [ ] Are quarterly labels based on a clearly documented convention?

## Causal eligibility

- [ ] Are event-facing tables explicitly labeled as `descriptive/supporting` unless a causal ledger promotes them to `causal-eligible`?
- [ ] Do any rows treated as causal-eligible carry exact timestamps rather than date-only proxies?
- [ ] Does every reviewed current-sample exact-time release component carry timestamp evidence, not just a hand-entered timestamp?
- [ ] Are bundled same-day Treasury releases marked as inseparable unless separate release components are documented?
- [ ] Is there a benchmark expectation / surprise source for any claimed causal treatment?
- [ ] Does any claimed `pre_release_external` benchmark status rest on verified pre-release evidence rather than an attached external-family label alone?
- [ ] Are overlap and contamination checks resolved before promotion to causal-eligible status?
- [ ] Do contamination calls distinguish `reviewed_clean`, `reviewed_contaminated_exclude`, and `reviewed_contaminated_context_only`?
- [ ] Do blocker reasons distinguish missing timestamps, missing benchmark, inseparable bundles, and unresolved overlap?

## Security classification

- [ ] Are bills and CMBs grouped correctly?
- [ ] Are FRNs handled explicitly rather than silently dropped?
- [ ] Are TIPS handled explicitly rather than silently dropped?
- [ ] Do classification functions fail loudly for unknown security types?
- [ ] Does `aggregate_auction_flows()` preserve the baseline column contract: `bill_like`, `frn`, `nominal_coupon`, `tips`, `coupon_like_total`, `coupon_plus_frn_total`, `gross_total`, `unknown`?

## Event studies

- [ ] Are outcome series differenced consistently?
- [ ] Is the 3-day window defined as `t+1 - t-1` in both code and output interpretation?
- [ ] Are overlapping macro-news dates annotated?
- [ ] Did we run both 1-day and 3-day windows?
- [ ] Did we compare results using official dates and T-1 markers?
- [ ] Are the daily event-study outputs described as monitoring / supporting evidence unless a causal tier is explicitly met?

## Plumbing regressions

- [ ] Are regressions run on changes where appropriate?
- [ ] Is the sign convention for QT explicit?
- [ ] Are Newey-West / HAC standard errors used for weekly time series?
- [ ] Did we compare bill and coupon coefficients directly?
- [ ] Does the headline plumbing path use labeled exact net inputs where available?

## Provenance

- [ ] Does every processed file identify its source files or source family?
- [ ] Are manual overrides or manual captures recorded in a field?
- [ ] Does the manual official-capture template carry explicit role-based provenance for financing, refunding statements, and auction reconstruction?
- [ ] Are output tables timestamped or versioned?
- [ ] Does `data/raw/qra/downloads.csv` record deterministic local filenames plus source URL, content type, HTTP status, and download timestamp?
- [ ] Does `output/publish/` contain the full site-facing artifact set with matching CSV/JSON/Markdown files?
- [ ] Do `dataset_status.csv` and `extension_status.csv` agree on extension readiness?

## Public Release Readiness

- [ ] Does `scripts/21_validate_backend.py` return `OK` after `make regenerate`?
- [ ] Are `investor_allotments`, `primary_dealer`, and `sec_nmfp` all `summary_ready` in `output/publish/dataset_status.csv`?
- [ ] Is `tic` the only extension still explicitly out of scope?
- [ ] Can the frontend render from `output/publish/` only, without reading `data/raw/` or `data/interim/`?

## Sanity checks against the seed note

- [ ] Does the baseline 18% ATI arithmetic reproduce the seeded quarter results?
- [ ] Does the QRA event table at least include the four key episodes from the note?
- [ ] Do provisional signs on event outcomes look directionally plausible?
