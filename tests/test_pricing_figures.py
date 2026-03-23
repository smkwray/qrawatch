from __future__ import annotations

from pathlib import Path

import pandas as pd

from ati_shadow_policy.research.pricing_figures import build_horizontal_bar_svg, build_overlay_svg


def test_build_overlay_svg_writes_nonempty_svg(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-31", periods=5, freq="ME"),
            "ati_baseline_bn": [1, 2, 3, 4, 5],
            "DGS10": [100, 101, 103, 102, 104],
        }
    )
    out = tmp_path / "overlay.svg"
    build_overlay_svg(
        frame,
        date_col="date",
        left_col="ati_baseline_bn",
        right_col="DGS10",
        left_label="Maturity-Tilt Flow",
        right_label="10Y Yield",
        title="Overlay",
        subtitle="Test figure",
        output_path=out,
    )

    text = out.read_text(encoding="utf-8")
    assert "<svg" in text
    assert "polyline" in text


def test_build_horizontal_bar_svg_writes_nonempty_svg(tmp_path: Path) -> None:
    out = tmp_path / "bars.svg"
    build_horizontal_bar_svg(
        ["Flow baseline | 10Y Yield", "Stock baseline | 10Y Term Premium"],
        [1.2, -0.8],
        title="Bars",
        subtitle="Test figure",
        output_path=out,
    )

    text = out.read_text(encoding="utf-8")
    assert "<svg" in text
    assert "<rect" in text
