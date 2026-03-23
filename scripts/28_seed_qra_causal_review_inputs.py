from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from ati_shadow_policy.io_utils import write_df
from ati_shadow_policy.paths import MANUAL_DIR, TABLES_DIR, ensure_project_dirs
from ati_shadow_policy.research.qra_component_seed import (
    seed_contamination_reviews,
    seed_expectation_template,
    seed_release_component_registry,
)

DEFAULT_COMPONENT_REGISTRY = TABLES_DIR / "qra_release_component_registry.csv"
DEFAULT_SHOCK_SUMMARY = TABLES_DIR / "qra_event_shock_summary.csv"
DEFAULT_PUBLISH_SHOCK_SUMMARY = ROOT / "output" / "publish" / "qra_event_shock_summary.csv"
DEFAULT_OVERLAP_ANNOTATIONS = MANUAL_DIR / "qra_event_overlap_annotations.csv"
DEFAULT_COMPONENT_OUTPUT = MANUAL_DIR / "qra_release_component_registry.csv"
DEFAULT_EXPECTATION_OUTPUT = MANUAL_DIR / "qra_component_expectation_template.csv"
DEFAULT_CONTAMINATION_OUTPUT = MANUAL_DIR / "qra_event_contamination_reviews.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Seed or reseed the manual QRA causal-review inputs from the derived component registry. "
            "By default, orphan rows are pruned to live release_component_id values."
        )
    )
    parser.add_argument("--component-registry", default=str(DEFAULT_COMPONENT_REGISTRY))
    parser.add_argument("--shock-summary", default=str(DEFAULT_SHOCK_SUMMARY))
    parser.add_argument("--overlap-annotations", default=str(DEFAULT_OVERLAP_ANNOTATIONS))
    parser.add_argument("--component-output", default=str(DEFAULT_COMPONENT_OUTPUT))
    parser.add_argument("--expectation-output", default=str(DEFAULT_EXPECTATION_OUTPUT))
    parser.add_argument("--contamination-output", default=str(DEFAULT_CONTAMINATION_OUTPUT))
    parser.add_argument(
        "--preserve-orphans",
        action="store_true",
        help="Keep orphan manual rows whose release_component_id is no longer in the live component registry.",
    )
    return parser.parse_args()


def _read_csv_if_exists(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    return pd.read_csv(path)


def _resolve_shock_summary_path(path: Path) -> Path:
    if path.exists():
        return path
    if path == DEFAULT_SHOCK_SUMMARY and DEFAULT_PUBLISH_SHOCK_SUMMARY.exists():
        return DEFAULT_PUBLISH_SHOCK_SUMMARY
    return path


def main() -> None:
    args = parse_args()
    ensure_project_dirs()

    component_registry_path = Path(args.component_registry)
    if not component_registry_path.exists():
        raise FileNotFoundError(f"Missing derived component registry: {component_registry_path}")

    component_registry = pd.read_csv(component_registry_path)
    shock_summary = _read_csv_if_exists(_resolve_shock_summary_path(Path(args.shock_summary)))
    overlap_annotations = _read_csv_if_exists(Path(args.overlap_annotations))

    component_output_path = Path(args.component_output)
    expectation_output_path = Path(args.expectation_output)
    contamination_output_path = Path(args.contamination_output)

    existing_component = _read_csv_if_exists(component_output_path)
    existing_expectation = _read_csv_if_exists(expectation_output_path)
    existing_contamination = _read_csv_if_exists(contamination_output_path)
    preserve_orphans = bool(args.preserve_orphans)

    seeded_component = seed_release_component_registry(
        component_registry,
        existing=existing_component,
        preserve_orphans=preserve_orphans,
    )
    seeded_expectation = seed_expectation_template(
        component_registry,
        shock_summary=shock_summary,
        existing=existing_expectation,
        preserve_orphans=preserve_orphans,
    )
    seeded_contamination = seed_contamination_reviews(
        component_registry,
        overlap_annotations=overlap_annotations,
        existing=existing_contamination,
        preserve_orphans=preserve_orphans,
    )

    write_df(seeded_component, component_output_path)
    write_df(seeded_expectation, expectation_output_path)
    write_df(seeded_contamination, contamination_output_path)
    print(
        "Seeded causal review inputs: "
        f"component_registry={len(seeded_component):,}, "
        f"expectation_template={len(seeded_expectation):,}, "
        f"contamination_reviews={len(seeded_contamination):,}, "
        f"preserve_orphans={preserve_orphans}"
    )


if __name__ == "__main__":
    main()
