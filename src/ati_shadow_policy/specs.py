from __future__ import annotations

from collections.abc import Iterable

import pandas as pd


SPEC_QRA_EVENT_V2 = "spec_qra_event_v2"
SPEC_DURATION_TREATMENT_V1 = "spec_duration_treatment_v1"
SPEC_AUCTION_ABSORPTION_V1 = "spec_auction_absorption_v1"
SPEC_QRA_COMPONENT_REGISTRY_V1 = "spec_qra_component_registry_v1"

TREATMENT_VARIANTS: tuple[dict[str, str], ...] = (
    {
        "treatment_variant": "canonical_shock_bn",
        "denominator_column": "shock_bn",
        "denominator_units": "USD billions",
        "elasticity_units": "basis points per $100bn descriptive announcement shock",
        "role": "headline",
        "notes": "Backward-compatible descriptive treatment from the reviewed shock template while the causal expectation/surprise layer is built separately.",
    },
    {
        "treatment_variant": "fixed_10y_eq_bn",
        "denominator_column": "schedule_diff_10y_eq_bn",
        "denominator_units": "USD billions 10y-equivalent",
        "elasticity_units": "basis points per $100bn fixed 10y-equivalent shock",
        "role": "comparison",
        "notes": "Fixed 10y-equivalent mapping from quarterly schedule differences.",
    },
    {
        "treatment_variant": "dynamic_10y_eq_bn",
        "denominator_column": "schedule_diff_dynamic_10y_eq_bn",
        "denominator_units": "USD billions dynamic 10y-equivalent",
        "elasticity_units": "basis points per $100bn dynamic 10y-equivalent shock",
        "role": "comparison",
        "notes": "Yield-curve-adjusted 10y-equivalent mapping from quarterly schedule differences.",
    },
    {
        "treatment_variant": "dv01_usd",
        "denominator_column": "schedule_diff_dv01_usd",
        "denominator_units": "USD DV01",
        "elasticity_units": "basis points per $1mm DV01-equivalent shock",
        "role": "comparison",
        "notes": "Dollar DV01 mapping from quarterly schedule differences.",
    },
)

SPEC_REGISTRY: tuple[dict[str, str], ...] = (
    {
        "spec_id": SPEC_QRA_COMPONENT_REGISTRY_V1,
        "version": "v1",
        "title": "QRA Release Component Registry",
        "scope": "component-level timing, separability, contamination, and causal-tier tracking",
        "status": "active",
        "notes": "One row per financing-estimates or policy-statement component, carrying the hard gates for the causal-eligible path.",
    },
    {
        "spec_id": SPEC_QRA_EVENT_V2,
        "version": "v2",
        "title": "QRA Event Registry",
        "scope": "event classification, timing, overlap, headline eligibility, and event-level causal-tier summary",
        "status": "active",
        "notes": "One row per QRA release event with timestamp lineage, bundle decomposition, overlap provenance, headline-eligibility checks, and summarized causal-tier status from the component registry.",
    },
    {
        "spec_id": SPEC_DURATION_TREATMENT_V1,
        "version": "v1",
        "title": "Duration Treatment Contract",
        "scope": "canonical shock plus fixed, dynamic, and DV01 treatment variants",
        "status": "active",
        "notes": "Treatments remain explicit and comparable without changing the headline estimand.",
    },
    {
        "spec_id": SPEC_AUCTION_ABSORPTION_V1,
        "version": "v1",
        "title": "Auction Absorption Bridge",
        "scope": "event-linked investor allotment and primary dealer absorption surfaces",
        "status": "active",
        "notes": "Narrow mechanism bridge built from currently summary-ready extension panels.",
    },
)


def spec_registry_frame(rows: Iterable[dict[str, str]] = SPEC_REGISTRY) -> pd.DataFrame:
    return pd.DataFrame(list(rows))
