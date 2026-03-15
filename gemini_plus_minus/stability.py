"""
Estimate player stability over seasons using ARIMA on translated EPM series.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA


@dataclass
class Config:
    input_file: Path | None
    input_dir: Path
    output_file: Path
    min_minutes: int
    min_points: int
    order: Tuple[int, int, int]
    metric: str
    ewma_span: int


def season_key(val: object) -> int:
    s = str(val)
    if "_" in s:
        return int(s.split("_")[0])
    if s.isdigit() and len(s) == 4:
        return int("20" + s[:2])
    if s.isdigit():
        return int(s)
    return 0


def load_series(cfg: Config) -> Tuple[pd.DataFrame, str]:
    if cfg.input_file and cfg.input_file.exists():
        df = pd.read_csv(cfg.input_file)
        if cfg.metric != "auto":
            if cfg.metric not in df.columns:
                raise ValueError(f"Metric {cfg.metric} not found in {cfg.input_file}")
            value_col = cfg.metric
        else:
            value_col = "epm_translated" if "epm_translated" in df.columns else "epm"
        return df, value_col

    def load_glob(pattern: str, value_col: str) -> Tuple[pd.DataFrame, str] | None:
        files = sorted(cfg.input_dir.glob(pattern))
        if not files:
            return None
        frames = [pd.read_csv(f) for f in files]
        return pd.concat(frames, ignore_index=True), value_col

    if cfg.metric in ("gpm_translated", "auto"):
        gpm_translated = cfg.input_dir / "gpm_translated.csv"
        if gpm_translated.exists():
            return pd.read_csv(gpm_translated), "gpm_translated"

    if cfg.metric in ("gpm", "auto"):
        gpm_path = cfg.input_dir / "gpm.csv"
        if gpm_path.exists():
            return pd.read_csv(gpm_path), "gpm"
        loaded = load_glob("*_gpm.csv", "gpm")
        if loaded:
            return loaded

    if cfg.metric in ("epm_translated", "auto"):
        translated_path = cfg.input_dir / "epm_translated.csv"
        if translated_path.exists():
            df = pd.read_csv(translated_path)
            return df, "epm_translated"

    if cfg.metric in ("epm", "auto"):
        loaded = load_glob("*_epm.csv", "epm")
        if loaded:
            return loaded

    raise FileNotFoundError(f"No input data found for metric={cfg.metric} in {cfg.input_dir}")


def fit_player_series(series: np.ndarray, order: Tuple[int, int, int]) -> Tuple[float, float, float]:
    model = ARIMA(series, order=order, trend="c")
    res = model.fit()
    ar1 = float(res.params.get("ar.L1", np.nan))
    sigma2 = float(res.params.get("sigma2", np.nan))
    resid_std = float(np.sqrt(sigma2)) if not np.isnan(sigma2) else float(np.std(res.resid))
    return ar1, resid_std, float(res.aic)


def run(cfg: Config) -> None:
    df, value_col = load_series(cfg)
    required = {"player_id", "player", "season", "minutes", value_col}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns {missing} in input data")

    df = df[df["minutes"] >= cfg.min_minutes].copy()
    df = df.sort_values("season", key=lambda c: c.map(season_key))

    rows: List[dict] = []
    for pid, g in df.groupby("player_id"):
        g = g.sort_values("season", key=lambda c: c.map(season_key))
        values = g[value_col].astype(float).values
        if len(values) < cfg.min_points:
            continue
        try:
            ar1, resid_std, aic = fit_player_series(values, cfg.order)
        except Exception:
            ar1, resid_std, aic = np.nan, np.nan, np.nan

        ewma_resid_std = np.nan
        if len(values) >= 2:
            s = pd.Series(values)
            ewma = s.ewm(span=cfg.ewma_span, adjust=False).mean()
            ewma_resid_std = float((s - ewma).std(ddof=0))

        rows.append(
            {
                "player_id": pid,
                "player": g["player"].iloc[0],
                "seasons": len(values),
                "metric": value_col,
                "value_mean": float(np.mean(values)),
                "value_std": float(np.std(values, ddof=0)),
                "ar1": ar1,
                "resid_std": resid_std,
                "aic": aic,
                "ewma_resid_std": ewma_resid_std,
                "stability_score_arima": -resid_std if not np.isnan(resid_std) else np.nan,
                "stability_score_ewma": -ewma_resid_std if not np.isnan(ewma_resid_std) else np.nan,
            }
        )

    out = pd.DataFrame(rows)
    out.to_csv(cfg.output_file, index=False)
    print(f"Saved stability metrics to {cfg.output_file}")


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Estimate player stability with ARIMA on EPM series")
    parser.add_argument("--input-file", default=None)
    parser.add_argument("--input-dir", default="data/plus_minus")
    parser.add_argument("--output-file", default="data/plus_minus/player_stability.csv")
    parser.add_argument("--min-minutes", type=int, default=900)
    parser.add_argument("--min-points", type=int, default=4)
    parser.add_argument("--order", nargs=3, type=int, default=[1, 0, 0])
    parser.add_argument("--metric", default="auto", help="auto|epm|epm_translated|gpm|gpm_translated")
    parser.add_argument("--ewma-span", type=int, default=3)
    args = parser.parse_args()

    return Config(
        input_file=Path(args.input_file) if args.input_file else None,
        input_dir=Path(args.input_dir),
        output_file=Path(args.output_file),
        min_minutes=args.min_minutes,
        min_points=args.min_points,
        order=(args.order[0], args.order[1], args.order[2]),
        metric=args.metric,
        ewma_span=args.ewma_span,
    )


if __name__ == "__main__":
    cfg = parse_args()
    run(cfg)
