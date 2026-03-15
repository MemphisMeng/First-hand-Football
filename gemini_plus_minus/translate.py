"""
Compute league translation coefficients from per-season EPM outputs.

This uses players who move between leagues in consecutive seasons. For each
transfer, we model:
    epm_dest - epm_origin = coef_dest - coef_origin

Coefficients are re-centered to mean zero for identifiability.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.linear_model import Ridge


DEFAULT_ALPHA = 1.0
DEFAULT_GPM_QUANTILE = 0.95
DEFAULT_GPM_SHRINK_K = 1.0


@dataclass
class Config:
    input_dir: Path
    output_dir: Path
    min_minutes: int
    alpha: float
    write_translated: bool
    compute_gpm: bool
    gpm_quantile: float
    gpm_shrink_k: float
    gpm_scope: str


def season_key(val: object) -> int:
    s = str(val)
    if "_" in s:
        return int(s.split("_")[0])
    if s.isdigit() and len(s) == 4:
        return int("20" + s[:2])
    if s.isdigit():
        return int(s)
    return 0


def load_epm(input_dir: Path) -> pd.DataFrame:
    files = sorted(input_dir.glob("*_epm.csv"))
    if not files:
        raise FileNotFoundError(f"No *_epm.csv files found in {input_dir}")

    frames = []
    for f in files:
        df = pd.read_csv(f)
        required = {"player_id", "league", "season", "minutes", "epm"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns {missing} in {f}")
        frames.append(df[["player_id", "player", "position", "minutes", "league", "season", "epm"]])
    return pd.concat(frames, ignore_index=True)


def build_transfer_samples(epm_df: pd.DataFrame, min_minutes: int) -> Tuple[sparse.csr_matrix, np.ndarray, np.ndarray, List[str]]:
    leagues = sorted(epm_df["league"].unique().tolist())
    league_index = {l: i for i, l in enumerate(leagues)}

    X_rows: List[int] = []
    X_cols: List[int] = []
    X_data: List[float] = []
    y = []
    weights = []

    for pid, df in epm_df.groupby("player_id"):
        df_sorted = df.sort_values("season", key=lambda c: c.map(season_key)).reset_index(drop=True)
        for i in range(len(df_sorted) - 1):
            origin = df_sorted.loc[i]
            dest = df_sorted.loc[i + 1]
            if origin["league"] == dest["league"]:
                continue
            if origin["minutes"] < min_minutes or dest["minutes"] < min_minutes:
                continue

            row_idx = len(y)
            y.append(dest["epm"] - origin["epm"])
            weights.append(min(origin["minutes"], dest["minutes"]))

            X_rows.append(row_idx)
            X_cols.append(league_index[dest["league"]])
            X_data.append(1.0)
            X_rows.append(row_idx)
            X_cols.append(league_index[origin["league"]])
            X_data.append(-1.0)

    if not y:
        return sparse.csr_matrix((0, len(leagues))), np.array([]), np.array([]), leagues

    X = sparse.coo_matrix(
        (X_data, (X_rows, X_cols)),
        shape=(len(y), len(leagues)),
        dtype=float,
    ).tocsr()

    return X, np.array(y, dtype=float), np.array(weights, dtype=float), leagues


def fit_translation(X: sparse.csr_matrix, y: np.ndarray, weights: np.ndarray, leagues: List[str], alpha: float) -> Dict[str, float]:
    if X.shape[0] == 0:
        return {l: 0.0 for l in leagues}

    model = Ridge(alpha=alpha, fit_intercept=False)
    model.fit(X, y, sample_weight=weights)

    coef_vals = np.array(model.coef_, dtype=float)
    coef_vals = coef_vals - float(np.mean(coef_vals))
    return {l: float(c) for l, c in zip(leagues, coef_vals)}


def translate_epm(epm_df: pd.DataFrame, coefs: Dict[str, float]) -> pd.DataFrame:
    out = epm_df.copy()
    out["epm_translated"] = out.apply(lambda r: r["epm"] - coefs.get(r["league"], 0.0), axis=1)
    return out


def shrinkage_fn(x: np.ndarray, k: float) -> np.ndarray:
    return np.log1p(k * x) / k


def apply_quantile_scaling(values: np.ndarray, q: float, k: float) -> np.ndarray:
    if len(values) == 0:
        return values
    mu = float(np.mean(values))
    sigma = float(np.std(values, ddof=0))
    if sigma == 0:
        return values
    z = np.abs(values - mu) / sigma
    qv = float(np.quantile(z, q))
    z_prime = np.where(z > qv, qv + shrinkage_fn(z - qv, k), z)
    return mu + np.sign(values - mu) * z_prime * sigma


def run(cfg: Config) -> None:
    epm_df = load_epm(cfg.input_dir)
    X, y, weights, leagues = build_transfer_samples(epm_df, cfg.min_minutes)
    coefs = fit_translation(X, y, weights, leagues, cfg.alpha)

    out_coeff = pd.DataFrame({"league": list(coefs.keys()), "coef": list(coefs.values())})
    out_coeff.to_csv(cfg.output_dir / "league_translation.csv", index=False)

    translated = translate_epm(epm_df, coefs)
    if cfg.write_translated:
        translated.to_csv(cfg.output_dir / "epm_translated.csv", index=False)

    if cfg.compute_gpm:
        gpm_df = translated.copy()
        if cfg.gpm_scope == "league":
            gpm_df["gpm"] = np.nan
            for league, group in gpm_df.groupby("league"):
                idx = group.index
                gpm_df.loc[idx, "gpm"] = apply_quantile_scaling(
                    group["epm_translated"].values, cfg.gpm_quantile, cfg.gpm_shrink_k
                )
        else:
            gpm_df["gpm"] = apply_quantile_scaling(
                gpm_df["epm_translated"].values, cfg.gpm_quantile, cfg.gpm_shrink_k
            )
        gpm_df.to_csv(cfg.output_dir / "gpm.csv", index=False)

    print(f"Saved league_translation.csv to {cfg.output_dir}")


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Compute league translation coefficients from EPM outputs")
    parser.add_argument("--input-dir", default="data/plus_minus")
    parser.add_argument("--output-dir", default="data/plus_minus")
    parser.add_argument("--min-minutes", type=int, default=900)
    parser.add_argument("--alpha", type=float, default=DEFAULT_ALPHA)
    parser.add_argument("--write-translated", action="store_true")
    parser.add_argument("--no-gpm", action="store_true")
    parser.add_argument("--gpm-quantile", type=float, default=DEFAULT_GPM_QUANTILE)
    parser.add_argument("--gpm-shrink-k", type=float, default=DEFAULT_GPM_SHRINK_K)
    parser.add_argument("--gpm-scope", choices=["global", "league"], default="global")
    args = parser.parse_args()

    return Config(
        input_dir=Path(args.input_dir),
        output_dir=Path(args.output_dir),
        min_minutes=args.min_minutes,
        alpha=args.alpha,
        write_translated=args.write_translated,
        compute_gpm=not args.no_gpm,
        gpm_quantile=args.gpm_quantile,
        gpm_shrink_k=args.gpm_shrink_k,
        gpm_scope=args.gpm_scope,
    )


if __name__ == "__main__":
    cfg = parse_args()
    run(cfg)
