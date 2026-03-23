# GPT Pro Audit Brief

This brief is for an external audit of the current `qrawatch` state after the March 23, 2026 causal-governance and archive-extension pass.

## What changed materially

- Exact official quarter history now spans `2009Q1` through `2025Q4`.
- The public causal-boundary layer is now explicit:
  - `output/publish/qra_benchmark_evidence_registry.csv`
  - `output/publish/causal_claims_status.csv`
  - `claim_scope` propagated through supporting event/rates outputs
- Backend validation now enforces that supporting/provisional event datasets cannot present as headline causal outputs.
- The site now renders both `causal_claims_status` and the current-sample financing benchmark-evidence surface.

## Current factual state to audit

Inside the current-sample financing pilot (`2022Q3+` financing-estimates rows):

- `14` current-sample financing components
- `6` benchmark-ready rows
- `5` Tier A rows
- `1` reviewed context-only contamination row
- `8` post-release-invalid rows

The benchmark-ready / Tier A frontier remains narrow. The repo should still be read as a causal pilot, not a settled or full-sample rates-identification design.

## Files that should anchor the audit

Core publish/status surfaces:

- `output/publish/causal_claims_status.csv`
- `output/publish/qra_benchmark_evidence_registry.csv`
- `output/publish/qra_release_component_registry.csv`
- `output/publish/qra_benchmark_coverage.csv`
- `output/publish/qra_benchmark_blockers_by_event.csv`
- `output/publish/event_design_status.csv`
- `output/publish/qra_long_rate_translation_panel.csv`
- `output/publish/dataset_status.csv`

Manual source-of-truth review inputs:

- `data/manual/qra_component_expectation_template.csv`
- `data/manual/qra_event_contamination_reviews.csv`
- `data/manual/qra_event_overlap_annotations.csv`

Archive / official-history inputs:

- `data/manual/official_quarterly_refunding_capture_template.csv`
- `data/processed/official_capture_completion_status.csv`

Public messaging / contract docs:

- `README.md`
- `docs/STATUS_GLOSSARY.md`
- `scripts/21_validate_backend.py`

## Questions for the audit

Please focus on these questions:

1. Does the repo now draw the causal-claims boundary clearly enough that a careful reader cannot mistake descriptive/supporting outputs for headline causal estimates?
2. Are the current-sample financing benchmark and contamination contracts strong enough for an audited pilot, or are there still gaps in how terminal dispositions are recorded and enforced?
3. Is the current public benchmark-evidence surface the right one for auditing the path from manual review inputs to published causal claims?
4. Given the current `5` Tier A / `6` benchmark-ready / `8` post-release-invalid split, what is the most efficient next sequence of work if the goal is to make materially stronger causal claims about Treasury issuance on rates?
5. Should the next wave prioritize:
   - manual benchmark discovery/adjudication for blocked financing rows
   - contamination/separability review on borderline rows
   - stronger long-rate translation gating
   - further archive extension
   - something else

## Claims the repo still should not make

- It should not claim a settled or referee-grade causal estimate of Treasury issuance effects on long rates or term premia.
- It should not claim a full-sample or long-history causal QRA event design.
- It should not present `qra_event_elasticity` as a headline causal result.
- It should not present `qra_long_rate_translation_panel` as finished rates-translation evidence.
- It should not claim official-history completion before `2009Q1`.

## Desired audit outcome

The desired audit output is:

- a ranked list of remaining blockers to defensible causal claims
- a judgment on whether the new causal-governance layer is sufficient for a public pilot
- a concrete next-wave plan that separates software/plumbing work from manual evidence/adjudication work
