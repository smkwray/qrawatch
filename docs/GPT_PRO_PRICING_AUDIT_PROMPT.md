# GPT Pro Pricing Audit Prompt

Use this prompt **after the next push**. Replace `<COMMIT_SHA>` with the pushed commit hash before sending.

```text
Please do a deep strategic and empirical audit of this GitHub repo’s pricing layer and recommend the concrete next move.

Important constraints:
- You do NOT have local file access. Use only the GitHub URLs below.
- Analyze the repo as it exists on `main` at commit `<COMMIT_SHA>`.
- I want strategy tied to actual numerical results, not generic encouragement.
- The project is using neutral public language:
  - `Maturity Tilt`
  - `Maturity-Tilt Flow`
  - `Excess Bills Stock`
  - `Public Duration Supply`
- The project’s goal is a neutral maturity-to-rates estimate in the spirit of a Miran-style exercise, but without political framing.
- Be skeptical. If the current pricing design is still too weak to anchor the project, say so plainly.

Repo:
- Main repo: https://github.com/smkwray/qrawatch
- Exact commit: https://github.com/smkwray/qrawatch/commit/<COMMIT_SHA>

Core public docs:
- README: https://github.com/smkwray/qrawatch/blob/main/README.md
- Pricing methods: https://github.com/smkwray/qrawatch/blob/main/docs/PRICING_METHODS.md
- Pricing results memo: https://github.com/smkwray/qrawatch/blob/main/docs/PRICING_RESULTS_MEMO.md
- Benchmark closure memo: https://github.com/smkwray/qrawatch/blob/main/docs/BENCHMARK_SEARCH_CLOSURE.md
- Status glossary: https://github.com/smkwray/qrawatch/blob/main/docs/STATUS_GLOSSARY.md
- Project brief: https://github.com/smkwray/qrawatch/blob/main/PROJECT_BRIEF.md

Core pricing outputs:
- Spec registry: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/site/data/pricing_spec_registry.csv
- Pricing summary: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/site/data/pricing_regression_summary.csv
- Pricing subsample grid: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/site/data/pricing_subsample_grid.csv
- Pricing robustness: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/site/data/pricing_regression_robustness.csv
- Release flow panel: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/site/data/pricing_release_flow_panel.csv
- Release flow leave-one-out: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/site/data/pricing_release_flow_leave_one_out.csv
- Tau sensitivity grid: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/site/data/pricing_tau_sensitivity_grid.csv
- Pricing scenarios: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/site/data/pricing_scenario_translation.csv
- Dataset status: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/site/data/dataset_status.json
- Series metadata catalog: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/site/data/series_metadata_catalog.csv

Supporting measurement inputs:
- Official Maturity-Tilt panel: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/data/processed/official_ati_price_panel.csv
- Release-level flow panel: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/data/processed/pricing_release_flow_panel.csv
- Excess Bills Stock panel: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/data/processed/mspd_stock_excess_bills_panel.csv
- Weekly supply-price panel: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/data/processed/weekly_supply_price_panel.csv
- Public duration supply: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/data/processed/public_duration_supply.csv

Pricing figures:
- https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/output/figures/maturity_tilt_flow_vs_dgs10.svg
- https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/output/figures/excess_bills_stock_vs_threefytp10.svg
- https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/output/figures/pricing_headline_coefficients.svg
- https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/output/figures/pricing_scenario_translation.svg

Supporting event/casual-boundary surfaces:
- Causal claims status: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/site/data/causal_claims_status.json
- Event design status: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/site/data/event_design_status.json
- Release component registry: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/site/data/qra_release_component_registry.csv
- Benchmark evidence registry: https://raw.githubusercontent.com/smkwray/qrawatch/refs/heads/main/site/data/qra_benchmark_evidence_registry.csv

Current local interpretation to audit:
- The new release-level Maturity-Tilt Flow design is the conceptually cleaner pricing anchor.
- The current strongest numerical pricing signal is still the monthly Maturity-Tilt Flow coefficient, not the event lane.
- The standalone Excess Bills Stock coefficient is weak.
- The weekly Public Duration Supply coefficient is large in the full sample but unstable across subsamples.
- Stock scenario arithmetic is illustrative only.
- The supporting QRA event lane remains a narrow audited pilot and is not the headline coefficient source.

Current numerical results to audit:
- `release_flow_baseline_next_release`
  - `THREEFYTP10`: `-0.295` bp, `p = 0.630`, `58` effective shocks, sample `2010-02-03` to `2025-04-30`
  - `DGS10`: `-0.759` bp, `p = 0.511`, `58` effective shocks, sample `2010-02-03` to `2025-04-30`
- `release_flow_baseline_21bd`
  - `THREEFYTP10`: `0.168` bp, `p = 0.615`, `59` effective shocks, sample `2010-02-03` to `2025-07-30`
  - `DGS10`: `0.429` bp, `p = 0.488`, `59` effective shocks, sample `2010-02-03` to `2025-07-30`
- `monthly_flow_baseline`
  - `THREEFYTP10`: `-2.813` bp, `p = 0.023`
  - `DGS10`: `-5.215` bp, `p = 0.027`
- `monthly_stock_baseline`
  - `THREEFYTP10`: `0.835` bp, `p = 0.473`
  - `DGS10`: `1.495` bp, `p = 0.479`
- `weekly_duration_baseline`
  - `THREEFYTP10`: `-26.084` bp, `p = 3.33e-09`
  - `DGS10`: `-44.054` bp, `p = 4.92e-08`

Key release-level subsamples:
- `release_flow_baseline_next_release`
  - `post_2014`
    - `THREEFYTP10`: `-0.422` bp, `p = 0.492`
    - `DGS10`: `-0.998` bp, `p = 0.393`
  - `post_2020`
    - `THREEFYTP10`: `-0.832` bp, `p = 0.147`
    - `DGS10`: `-1.876` bp, `p = 0.097`
  - `exclude_debt_limit`
    - `THREEFYTP10`: `-0.758` bp, `p = 0.273`
    - `DGS10`: `-1.677` bp, `p = 0.248`

Leave-one-out range for `release_flow_baseline_next_release`:
- `THREEFYTP10`: coefficient range `-0.674` to `0.094`, p-value range `0.243` to `0.901`
- `DGS10`: coefficient range `-1.430` to `0.055`, p-value range `0.229` to `0.963`

Tau sensitivity for stock:
- `tau = 0.15`
  - `THREEFYTP10`: `0.407` bp, `p = 0.702`
  - `DGS10`: `0.695` bp, `p = 0.721`
- `tau = 0.20`
  - `THREEFYTP10`: `1.143` bp, `p = 0.331`
  - `DGS10`: `2.076` bp, `p = 0.329`

Please answer in this structure:

1. Bottom line
- One paragraph.
- Tell me whether the pricing layer is now strong enough to anchor the project.

2. Audit of the baseline pricing design
- Assess the locked specs themselves.
- Tell me what is well-designed and what is still fragile.
- Explicitly assess whether the release-level flow design is the right credibility pivot.

3. Audit of the current numerical results
- Evaluate the actual coefficients, signs, p-values, and subsample behavior.
- Tell me which result is most credible right now.
- Tell me which result should not be centered yet.

4. Highest-return next improvement
- Give me the single next empirical move that would most improve credibility.
- Be concrete.
- If the release-level flow result is still too weak, tell me whether the next move should be richer release-level controls, a different release window, or an event/QRA bridge rather than more monthly carry-forward work.

5. Ranked next steps for the next 2-4 weeks
- Separate:
  - empirical/model work
  - measurement/data work
  - writing/framing work
- Keep it operational.

6. Claim boundary
- State the strongest neutral maturity-to-rates claim the repo can honestly make now.
- State the claims it still should not make.

7. Decision
- End with a plain recommendation:
  - keep refining the release-level flow design,
  - pivot again,
  - or stop at a reduced-form measurement/pricing repo.
```
