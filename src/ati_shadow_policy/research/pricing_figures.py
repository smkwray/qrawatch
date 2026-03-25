from __future__ import annotations

from pathlib import Path
from typing import Iterable
from xml.sax.saxutils import escape

import pandas as pd


SVG_BG = "#fbfaf5"
SVG_TEXT = "#1f2430"
SVG_GRID = "#d7d2c6"
SERIES_COLORS = ("#1d3557", "#c8553d", "#5b8e7d", "#8d6a9f")
BAR_POSITIVE = "#1d3557"
BAR_NEGATIVE = "#c8553d"


def _safe_float_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _standardize(series: pd.Series) -> pd.Series:
    numeric = _safe_float_series(series)
    std = float(numeric.std(ddof=0))
    if std <= 0 or pd.isna(std):
        return numeric * 0.0
    return (numeric - float(numeric.mean())) / std


def _svg_shell(width: int, height: int, body: str) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img">'
        f'<rect width="{width}" height="{height}" fill="{SVG_BG}"/>'
        f"{body}</svg>"
    )


def _date_ticks(dates: pd.Series, count: int = 5) -> list[pd.Timestamp]:
    clean = pd.to_datetime(dates, errors="coerce").dropna().sort_values()
    if clean.empty:
        return []
    if len(clean) <= count:
        return list(clean)
    step = max(len(clean) // (count - 1), 1)
    ticks = [clean.iloc[index] for index in range(0, len(clean), step)]
    if ticks[-1] != clean.iloc[-1]:
        ticks.append(clean.iloc[-1])
    return ticks[: count - 1] + [clean.iloc[-1]]


def build_overlay_svg(
    frame: pd.DataFrame,
    *,
    date_col: str,
    left_col: str,
    right_col: str,
    left_label: str,
    right_label: str,
    title: str,
    subtitle: str,
    output_path: Path,
) -> None:
    data = frame[[date_col, left_col, right_col]].copy()
    data[date_col] = pd.to_datetime(data[date_col], errors="coerce")
    data = data.dropna(subset=[date_col]).sort_values(date_col).reset_index(drop=True)
    data[left_col] = _standardize(data[left_col])
    data[right_col] = _standardize(data[right_col])
    data = data.dropna(subset=[left_col, right_col])
    if data.empty:
        output_path.write_text(_svg_shell(960, 420, ""), encoding="utf-8")
        return

    width, height = 960, 420
    margin_left, margin_right, margin_top, margin_bottom = 72, 28, 72, 52
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    y_min = float(min(data[left_col].min(), data[right_col].min()))
    y_max = float(max(data[left_col].max(), data[right_col].max()))
    if y_min == y_max:
        y_min -= 1.0
        y_max += 1.0

    def x_pos(index: int) -> float:
        if len(data) == 1:
            return margin_left + plot_w / 2
        return margin_left + (plot_w * index / (len(data) - 1))

    def y_pos(value: float) -> float:
        return margin_top + plot_h - ((value - y_min) / (y_max - y_min) * plot_h)

    series_paths = []
    for color, column in zip(SERIES_COLORS[:2], (left_col, right_col), strict=False):
        points = " ".join(f"{x_pos(i):.2f},{y_pos(float(value)):.2f}" for i, value in enumerate(data[column]))
        series_paths.append(f'<polyline fill="none" stroke="{color}" stroke-width="2.8" points="{points}"/>')

    ticks = _date_ticks(data[date_col])
    tick_elems = []
    for tick in ticks:
        nearest = int((data[date_col] - tick).abs().idxmin())
        xpos = x_pos(nearest)
        tick_elems.append(f'<line x1="{xpos:.2f}" y1="{margin_top}" x2="{xpos:.2f}" y2="{margin_top + plot_h}" stroke="{SVG_GRID}" stroke-width="1"/>')
        tick_elems.append(
            f'<text x="{xpos:.2f}" y="{height - 18}" text-anchor="middle" font-size="12" fill="{SVG_TEXT}">{tick.strftime("%Y-%m")}</text>'
        )

    y_ticks = []
    for raw in range(5):
        value = y_min + (y_max - y_min) * raw / 4
        ypos = y_pos(value)
        y_ticks.append(f'<line x1="{margin_left}" y1="{ypos:.2f}" x2="{width - margin_right}" y2="{ypos:.2f}" stroke="{SVG_GRID}" stroke-width="1"/>')
        y_ticks.append(
            f'<text x="{margin_left - 8}" y="{ypos + 4:.2f}" text-anchor="end" font-size="12" fill="{SVG_TEXT}">{value:.1f}z</text>'
        )

    legend = (
        f'<rect x="{margin_left}" y="18" width="14" height="14" fill="{SERIES_COLORS[0]}"/>'
        f'<text x="{margin_left + 22}" y="30" font-size="13" fill="{SVG_TEXT}">{escape(left_label)}</text>'
        f'<rect x="{margin_left + 280}" y="18" width="14" height="14" fill="{SERIES_COLORS[1]}"/>'
        f'<text x="{margin_left + 302}" y="30" font-size="13" fill="{SVG_TEXT}">{escape(right_label)}</text>'
    )
    body = (
        f'<text x="{margin_left}" y="48" font-size="24" font-weight="700" fill="{SVG_TEXT}">{escape(title)}</text>'
        f'<text x="{margin_left}" y="66" font-size="13" fill="{SVG_TEXT}">{escape(subtitle)}</text>'
        f"{legend}"
        f'<line x1="{margin_left}" y1="{margin_top + plot_h}" x2="{width - margin_right}" y2="{margin_top + plot_h}" stroke="{SVG_TEXT}" stroke-width="1.2"/>'
        f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_h}" stroke="{SVG_TEXT}" stroke-width="1.2"/>'
        + "".join(y_ticks)
        + "".join(tick_elems)
        + "".join(series_paths)
    )
    output_path.write_text(_svg_shell(width, height, body), encoding="utf-8")


def build_horizontal_bar_svg(
    labels: Iterable[str],
    values: Iterable[float],
    *,
    title: str,
    subtitle: str,
    output_path: Path,
) -> None:
    label_list = [str(label) for label in labels]
    value_list = [float(value) for value in values]
    if not label_list:
        output_path.write_text(_svg_shell(960, 320, ""), encoding="utf-8")
        return

    width = 960
    row_h = 24
    height = max(260, 120 + row_h * len(label_list))
    margin_left, margin_right, margin_top, margin_bottom = 280, 36, 72, 36
    plot_w = width - margin_left - margin_right

    min_value = min(0.0, min(value_list))
    max_value = max(0.0, max(value_list))
    if min_value == max_value:
        min_value -= 1.0
        max_value += 1.0
    span = max_value - min_value

    def x_pos(value: float) -> float:
        return margin_left + (value - min_value) / span * plot_w

    zero_x = x_pos(0.0)
    rows = []
    for idx, (label, value) in enumerate(zip(label_list, value_list, strict=False)):
        y = margin_top + idx * row_h
        bar_x = min(zero_x, x_pos(value))
        bar_w = abs(x_pos(value) - zero_x)
        color = BAR_POSITIVE if value >= 0 else BAR_NEGATIVE
        anchor = "start" if value >= 0 else "end"
        rows.append(f'<text x="{margin_left - 12}" y="{y + 15}" text-anchor="end" font-size="12" fill="{SVG_TEXT}">{escape(label)}</text>')
        rows.append(f'<rect x="{bar_x:.2f}" y="{y + 3}" width="{max(bar_w, 1):.2f}" height="16" fill="{color}" rx="2"/>')
        rows.append(f'<text x="{x_pos(value) + (8 if value >= 0 else -8):.2f}" y="{y + 15}" text-anchor="{anchor}" font-size="12" fill="{SVG_TEXT}">{value:.2f}</text>')

    ticks = []
    for raw in range(5):
        value = min_value + span * raw / 4
        xpos = x_pos(value)
        ticks.append(f'<line x1="{xpos:.2f}" y1="{margin_top - 6}" x2="{xpos:.2f}" y2="{height - margin_bottom}" stroke="{SVG_GRID}" stroke-width="1"/>')
        ticks.append(f'<text x="{xpos:.2f}" y="{height - 12}" text-anchor="middle" font-size="12" fill="{SVG_TEXT}">{value:.1f}</text>')

    body = (
        f'<text x="{margin_left}" y="44" font-size="24" font-weight="700" fill="{SVG_TEXT}">{escape(title)}</text>'
        f'<text x="{margin_left}" y="62" font-size="13" fill="{SVG_TEXT}">{escape(subtitle)}</text>'
        f'<line x1="{zero_x:.2f}" y1="{margin_top - 6}" x2="{zero_x:.2f}" y2="{height - margin_bottom}" stroke="{SVG_TEXT}" stroke-width="1.4"/>'
        + "".join(ticks)
        + "".join(rows)
    )
    output_path.write_text(_svg_shell(width, height, body), encoding="utf-8")


def build_horizon_profile_svg(
    frame: pd.DataFrame,
    *,
    horizon_col: str,
    value_col: str,
    series_col: str,
    title: str,
    subtitle: str,
    output_path: Path,
) -> None:
    data = frame[[horizon_col, value_col, series_col]].copy()
    data[horizon_col] = pd.to_numeric(data[horizon_col], errors="coerce")
    data[value_col] = _safe_float_series(data[value_col])
    data[series_col] = data[series_col].astype(str)
    data = data.dropna(subset=[horizon_col, value_col]).sort_values([series_col, horizon_col]).reset_index(drop=True)
    if data.empty:
        output_path.write_text(_svg_shell(960, 420, ""), encoding="utf-8")
        return

    width, height = 960, 420
    margin_left, margin_right, margin_top, margin_bottom = 72, 28, 72, 56
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    unique_horizons = sorted(data[horizon_col].dropna().unique().tolist())
    y_min = min(0.0, float(data[value_col].min()))
    y_max = max(0.0, float(data[value_col].max()))
    if y_min == y_max:
        y_min -= 1.0
        y_max += 1.0

    def x_pos(horizon: float) -> float:
        if len(unique_horizons) == 1:
            return margin_left + plot_w / 2
        return margin_left + (plot_w * unique_horizons.index(horizon) / (len(unique_horizons) - 1))

    def y_pos(value: float) -> float:
        return margin_top + plot_h - ((value - y_min) / (y_max - y_min) * plot_h)

    y_ticks = []
    for raw in range(5):
        value = y_min + (y_max - y_min) * raw / 4
        ypos = y_pos(value)
        y_ticks.append(f'<line x1="{margin_left}" y1="{ypos:.2f}" x2="{width - margin_right}" y2="{ypos:.2f}" stroke="{SVG_GRID}" stroke-width="1"/>')
        y_ticks.append(
            f'<text x="{margin_left - 8}" y="{ypos + 4:.2f}" text-anchor="end" font-size="12" fill="{SVG_TEXT}">{value:.1f}</text>'
        )

    x_ticks = []
    for horizon in unique_horizons:
        xpos = x_pos(horizon)
        x_ticks.append(f'<line x1="{xpos:.2f}" y1="{margin_top}" x2="{xpos:.2f}" y2="{margin_top + plot_h}" stroke="{SVG_GRID}" stroke-width="1"/>')
        x_ticks.append(
            f'<text x="{xpos:.2f}" y="{height - 18}" text-anchor="middle" font-size="12" fill="{SVG_TEXT}">{int(horizon)}</text>'
        )

    zero_y = y_pos(0.0)
    series_paths = []
    legend_parts = []
    for idx, series_name in enumerate(sorted(data[series_col].unique().tolist())):
        subset = data.loc[data[series_col] == series_name].sort_values(horizon_col)
        color = SERIES_COLORS[idx % len(SERIES_COLORS)]
        points = " ".join(
            f"{x_pos(float(horizon)):.2f},{y_pos(float(value)):.2f}"
            for horizon, value in zip(subset[horizon_col], subset[value_col], strict=False)
        )
        series_paths.append(f'<polyline fill="none" stroke="{color}" stroke-width="2.8" points="{points}"/>')
        for horizon, value in zip(subset[horizon_col], subset[value_col], strict=False):
            series_paths.append(
                f'<circle cx="{x_pos(float(horizon)):.2f}" cy="{y_pos(float(value)):.2f}" r="3.5" fill="{color}"/>'
            )
        legend_x = margin_left + idx * 240
        legend_parts.append(f'<rect x="{legend_x}" y="18" width="14" height="14" fill="{color}"/>')
        legend_parts.append(
            f'<text x="{legend_x + 22}" y="30" font-size="13" fill="{SVG_TEXT}">{escape(series_name)}</text>'
        )

    body = (
        f'<text x="{margin_left}" y="48" font-size="24" font-weight="700" fill="{SVG_TEXT}">{escape(title)}</text>'
        f'<text x="{margin_left}" y="66" font-size="13" fill="{SVG_TEXT}">{escape(subtitle)}</text>'
        + "".join(legend_parts)
        + f'<line x1="{margin_left}" y1="{margin_top + plot_h}" x2="{width - margin_right}" y2="{margin_top + plot_h}" stroke="{SVG_TEXT}" stroke-width="1.2"/>'
        + f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_h}" stroke="{SVG_TEXT}" stroke-width="1.2"/>'
        + f'<line x1="{margin_left}" y1="{zero_y:.2f}" x2="{width - margin_right}" y2="{zero_y:.2f}" stroke="{SVG_TEXT}" stroke-width="1.2" stroke-dasharray="4 4"/>'
        + "".join(y_ticks)
        + "".join(x_ticks)
        + "".join(series_paths)
        + f'<text x="{width / 2:.2f}" y="{height - 2}" text-anchor="middle" font-size="12" fill="{SVG_TEXT}">Business days after release</text>'
    )
    output_path.write_text(_svg_shell(width, height, body), encoding="utf-8")
