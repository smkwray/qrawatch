from __future__ import annotations

import pandas as pd

from ati_shadow_policy.research.qra_component_seed import (
    seed_contamination_reviews,
    seed_expectation_template,
    seed_release_component_registry,
)


def test_seed_release_component_registry_preserves_existing_manual_rows() -> None:
    derived = pd.DataFrame(
        [
            {
                "release_component_id": "qra_2024_05__financing_estimates",
                "event_id": "qra_2024_05",
                "quarter": "2024Q3",
                "component_type": "financing_estimates",
                "release_timestamp_et": "2024-05-01T08:30:00-04:00",
                "timestamp_precision": "exact_time",
                "source_url": "https://example.com/financing",
                "bundle_id": "qra_2024_05",
                "release_sequence_label": "simultaneous_release",
                "separable_component_flag": False,
                "review_status": "reviewed",
                "review_notes": "derived row",
            },
            {
                "release_component_id": "qra_2024_05__policy_statement",
                "event_id": "qra_2024_05",
                "quarter": "2024Q3",
                "component_type": "policy_statement",
                "release_timestamp_et": "2024-05-01T09:00:00-04:00",
                "timestamp_precision": "exact_time",
                "source_url": "https://example.com/policy",
                "bundle_id": "qra_2024_05",
                "release_sequence_label": "simultaneous_release",
                "separable_component_flag": False,
                "review_status": "reviewed",
                "review_notes": "derived row",
            },
        ]
    )
    existing = pd.DataFrame(
        [
            {
                "release_component_id": "qra_2024_05__policy_statement",
                "event_id": "qra_2024_05",
                "quarter": "2024Q3",
                "component_type": "policy_statement",
                "release_timestamp_et": "2024-05-01T09:00:00-04:00",
                "timestamp_precision": "exact_time",
                "source_url": "https://example.com/policy",
                "bundle_id": "qra_2024_05",
                "release_sequence_label": "simultaneous_release",
                "separable_component_flag": False,
                "review_status": "reviewed",
                "review_notes": "existing manual note",
            }
        ]
    )

    seeded = seed_release_component_registry(derived, existing=existing)

    policy = seeded.loc[seeded["release_component_id"] == "qra_2024_05__policy_statement"].iloc[0]
    financing = seeded.loc[seeded["release_component_id"] == "qra_2024_05__financing_estimates"].iloc[0]

    assert list(seeded["release_component_id"]) == [
        "qra_2024_05__financing_estimates",
        "qra_2024_05__policy_statement",
    ]
    assert policy["review_notes"] == "existing manual note"
    assert financing["review_notes"] == "derived row"


def test_seed_expectation_and_contamination_templates_use_release_component_placeholders() -> None:
    derived = pd.DataFrame(
        [
            {
                "release_component_id": "qra_2024_05__financing_estimates",
                "event_id": "qra_2024_05",
                "component_type": "financing_estimates",
            },
            {
                "release_component_id": "qra_2024_05__policy_statement",
                "event_id": "qra_2024_05",
                "component_type": "policy_statement",
            },
        ]
    )
    shock_summary = pd.DataFrame(
        [
            {
                "event_id": "qra_2024_05",
                "event_date_type": "official_release_date",
                "shock_bn": 25.5,
                "shock_review_status": "reviewed",
            }
        ]
    )
    overlap_annotations = pd.DataFrame(
        [
            {
                "event_id": "qra_2024_05",
                "overlap_flag": False,
                "overlap_label": "",
                "overlap_note": "",
            }
        ]
    )
    expectation_existing = pd.DataFrame(
        [
            {
                "release_component_id": "qra_2024_05__policy_statement",
                "event_id": "qra_2024_05",
                "component_type": "policy_statement",
                "benchmark_source": "dealer_survey",
                "expectation_review_status": "reviewed",
                "expectation_notes": "pre-existing review note",
            }
        ]
    )
    contamination_existing = pd.DataFrame(
        [
            {
                "release_component_id": "qra_2024_05__policy_statement",
                "event_id": "qra_2024_05",
                "component_type": "policy_statement",
                "contamination_flag": False,
                "contamination_status": "reviewed_clean",
                "contamination_review_status": "reviewed",
                "contamination_label": "",
                "contamination_notes": "pre-existing contamination note",
            }
        ]
    )

    expectation_seeded = seed_expectation_template(
        derived,
        shock_summary=shock_summary,
        existing=expectation_existing,
    )
    contamination_seeded = seed_contamination_reviews(
        derived,
        overlap_annotations=overlap_annotations,
        existing=contamination_existing,
    )

    expectation_policy = expectation_seeded.loc[
        expectation_seeded["release_component_id"] == "qra_2024_05__policy_statement"
    ].iloc[0]
    expectation_financing = expectation_seeded.loc[
        expectation_seeded["release_component_id"] == "qra_2024_05__financing_estimates"
    ].iloc[0]
    contamination_policy = contamination_seeded.loc[
        contamination_seeded["release_component_id"] == "qra_2024_05__policy_statement"
    ].iloc[0]
    contamination_financing = contamination_seeded.loc[
        contamination_seeded["release_component_id"] == "qra_2024_05__financing_estimates"
    ].iloc[0]

    assert expectation_policy["expectation_review_status"] == "reviewed"
    assert expectation_policy["benchmark_source"] == "dealer_survey"
    assert expectation_financing["expectation_review_status"] == "pending"
    assert "shock_bn=25.5" in expectation_financing["expectation_notes"]
    assert contamination_policy["contamination_status"] == "reviewed_clean"
    assert contamination_policy["contamination_review_status"] == "reviewed"
    assert contamination_financing["contamination_status"] == "pending_review"
    assert "Confirm macro/policy contamination by hand." in contamination_financing["contamination_notes"]


def test_seed_expectation_refreshes_stale_default_notes_when_shock_context_is_available() -> None:
    derived = pd.DataFrame(
        [
            {
                "release_component_id": "qra_2023_05__policy_statement",
                "event_id": "qra_2023_05",
                "component_type": "policy_statement",
            }
        ]
    )
    shock_summary = pd.DataFrame(
        [
            {
                "event_id": "qra_2023_05",
                "event_date_type": "official_release_date",
                "shock_bn": 43.5,
                "shock_review_status": "reviewed",
            }
        ]
    )
    existing = pd.DataFrame(
        [
            {
                "release_component_id": "qra_2023_05__policy_statement",
                "event_id": "qra_2023_05",
                "component_type": "policy_statement",
                "expectation_review_status": "pending",
                "expectation_notes": (
                    "Seeded review row. Fill benchmark timestamp/source and component-level "
                    "expected/realized composition by hand. Event-level descriptive shock_bn=<NA>."
                ),
            }
        ]
    )

    seeded = seed_expectation_template(derived, shock_summary=shock_summary, existing=existing)
    row = seeded.iloc[0]

    assert "shock_bn=43.5" in row["expectation_notes"]
