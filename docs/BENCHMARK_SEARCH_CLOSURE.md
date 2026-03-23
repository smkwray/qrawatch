# Benchmark Search Closure

## Purpose

This memo records the bounded closure round for the current-sample QRA financing causal pilot.

The objective of this round was not to open new source families. It was to confirm whether the currently documented Treasury-hosted benchmark families could expand the clean sample and to document terminal outcomes row by row.

## Scope and rule set

Scope for closure:

- current-sample financing components that remain blocked for causal surprise use
- Treasury-hosted benchmark families already documented in the repo workflow

Decision rules:

- `upgraded_pre_release_external` only when a benchmark is publicly available before the component release timestamp and is admissibly mappable to the component expectation field
- `blocked_source_family_exhausted` when documented source families were checked and no credible pre-release public benchmark passed both timing and mapping checks
- `blocked_open_candidate` reserved for named, unreviewed public candidates

## Closure result

Current-sample financing pilot status remains:

- `14` reviewed financing components
- `6` benchmark-ready
- `5` Tier A
- `1` benchmark-ready but context-only contaminated (`qra_2024_05__financing_estimates`)
- `8` blocked (`post_release_invalid`)
- blocked split: `8` source-family-exhausted, `0` open-candidate

## Row-specific note: `qra_2022_05__financing_estimates`

This row was explicitly re-checked during closure because an April 2022 Primary Dealer Auction Size Survey artifact appears on the Treasury archive page.

Final closure rationale for this row:

- the component release reference is `2022-05-02 15:00 ET`
- the benchmark entry that is admissibly recorded in the registry remains the Treasury-hosted TBAC financing table dated `2022-05-04`
- that benchmark is after the component release, so timing status remains `post_release_invalid`
- no reproducible pre-release benchmark entry from the reviewed Treasury-hosted survey family satisfied both verifiable public-before-release timing and admissible expectation mapping under the repo benchmark contract

Therefore `qra_2022_05__financing_estimates` remains `blocked_source_family_exhausted`.

## Implication for project strategy

The closure round supports a bounded interpretation:

- keep the QRA financing causal lane as a narrow audited pilot
- avoid open-ended benchmark hunting in this lane
- move headline effort to official Maturity Tilt measurement, duration-supply/plumbing mechanisms, and reduced-form pricing
