"""
Compute RAPM, SPM, BPM, and EPM from Understat CSVs.

This implementation mirrors the paper's pipeline as closely as possible with the
available Understat data. It reconstructs approximate stints using player minutes
and goal times. If you have exact substitution/red-card timings or possession data,
extend the loaders to use them for higher fidelity.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Dict, List, Tuple

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.linear_model import Ridge, RidgeCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


DEFAULT_ALPHA_GRID = np.logspace(-3, 3, 25)


@dataclass
class Config:
    data_root: Path
    output_dir: Path
    leagues: List[str]
    seasons: List[str] | None
    min_stint_minutes: float
    min_minutes_spm: int
    alpha_grid: np.ndarray
    output_metrics: List[str]


# --------------------------
# Utilities
# --------------------------

def list_seasons(league_dir: Path) -> List[str]:
    seasons = []
    for p in league_dir.glob("*_player_match_stats.csv"):
        season = p.name.replace("_player_match_stats.csv", "")
        seasons.append(season)
    return sorted(seasons)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def range_sum(prefix: np.ndarray, start: int, end: int) -> float:
    """Sum in [start, end) from prefix array built over integer minutes."""
    if end <= start:
        return 0.0
    end_idx = end - 1
    start_idx = start - 1
    total = prefix[end_idx] if end_idx >= 0 else 0.0
    before = prefix[start_idx] if start_idx >= 0 else 0.0
    return float(total - before)


# --------------------------
# Loading
# --------------------------

def load_league_season(data_root: Path, league: str, season: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    base = data_root / league
    schedule = pd.read_csv(base / f"{season}_schedule.csv")
    players = pd.read_csv(base / f"{season}_player_match_stats.csv")
    shots = pd.read_csv(base / f"{season}_shot_events.csv")
    return schedule, players, shots


# --------------------------
# Stints construction
# --------------------------

def build_match_end(players: pd.DataFrame, shots: pd.DataFrame) -> int:
    max_minutes = 90
    if not players.empty and "minutes" in players.columns:
        max_minutes = max(max_minutes, int(np.nanmax(players["minutes"].values)))
    if not shots.empty and "minute" in shots.columns:
        max_minutes = max(max_minutes, int(np.nanmax(shots["minute"].values)))
    return int(max_minutes)


def compute_intervals(match_players: pd.DataFrame, match_end: int) -> List[Dict]:
    intervals = []
    for row in match_players.itertuples(index=False):
        minutes = float(getattr(row, "minutes"))
        if minutes <= 0:
            continue
        position = str(getattr(row, "position"))
        if position.lower() == "sub":
            start = max(0.0, match_end - minutes)
            end = float(match_end)
        else:
            start = 0.0
            end = min(float(match_end), minutes)
        # Clamp to integer minute boundaries
        start_i = int(round(start))
        end_i = int(round(end))
        if end_i <= start_i:
            continue
        intervals.append(
            {
                "player_id": int(getattr(row, "player_id")),
                "player": getattr(row, "player"),
                "team_id": int(getattr(row, "team_id")),
                "start": start_i,
                "end": end_i,
                "red_cards": int(getattr(row, "red_cards")),
            }
        )
    return intervals


def build_prefix_arrays(shots: pd.DataFrame, max_minute: int, team_id: int) -> Tuple[np.ndarray, np.ndarray]:
    xg_by_min = np.zeros(max_minute + 1, dtype=float)
    goals_by_min = np.zeros(max_minute + 1, dtype=float)

    if shots.empty:
        return np.cumsum(xg_by_min), np.cumsum(goals_by_min)

    subset = shots[shots["team_id"] == team_id]
    if subset.empty:
        return np.cumsum(xg_by_min), np.cumsum(goals_by_min)

    for row in subset.itertuples(index=False):
        minute = int(getattr(row, "minute"))
        if minute < 0 or minute > max_minute:
            continue
        xg_by_min[minute] += float(getattr(row, "xg"))
        if str(getattr(row, "result")).lower() == "goal":
            goals_by_min[minute] += 1.0

    return np.cumsum(xg_by_min), np.cumsum(goals_by_min)


def build_red_card_prefix(intervals: List[Dict], max_minute: int, team_id: int) -> np.ndarray:
    red_by_min = np.zeros(max_minute + 1, dtype=float)
    for rec in intervals:
        if rec["team_id"] != team_id:
            continue
        if rec["red_cards"] <= 0:
            continue
        t = min(max_minute, max(0, rec["end"]))
        red_by_min[int(t)] += 1.0
    return np.cumsum(red_by_min)


def build_stints(schedule: pd.DataFrame, players: pd.DataFrame, shots: pd.DataFrame, min_minutes: float) -> List[Dict]:
    stints = []

    # Index for quick lookup
    players_by_game = {gid: df for gid, df in players.groupby("game_id")}
    shots_by_game = {gid: df for gid, df in shots.groupby("game_id")}

    for row in schedule.itertuples(index=False):
        game_id = int(getattr(row, "game_id"))
        home_team_id = int(getattr(row, "home_team_id"))
        away_team_id = int(getattr(row, "away_team_id"))

        match_players = players_by_game.get(game_id, pd.DataFrame())
        match_shots = shots_by_game.get(game_id, pd.DataFrame())

        match_end = build_match_end(match_players, match_shots)

        intervals = compute_intervals(match_players, match_end)
        if not intervals:
            continue

        # Boundaries include half time, starts/ends, and goals
        bounds = {0, 45, match_end}
        for rec in intervals:
            bounds.add(rec["start"])
            bounds.add(rec["end"])
        if not match_shots.empty:
            for m in match_shots["minute"].dropna().astype(int).tolist():
                bounds.add(int(m))

        bounds = sorted(b for b in bounds if 0 <= b <= match_end)
        if len(bounds) < 2:
            continue

        home_xg_prefix, home_goal_prefix = build_prefix_arrays(match_shots, match_end, home_team_id)
        away_xg_prefix, away_goal_prefix = build_prefix_arrays(match_shots, match_end, away_team_id)
        home_red_prefix = build_red_card_prefix(intervals, match_end, home_team_id)
        away_red_prefix = build_red_card_prefix(intervals, match_end, away_team_id)

        for i in range(len(bounds) - 1):
            start = bounds[i]
            end = bounds[i + 1]
            duration = end - start
            if duration < min_minutes:
                continue

            home_players = [r["player_id"] for r in intervals if r["team_id"] == home_team_id and r["start"] <= start and r["end"] >= end]
            away_players = [r["player_id"] for r in intervals if r["team_id"] == away_team_id and r["start"] <= start and r["end"] >= end]
            if not home_players or not away_players:
                continue

            xg_home = range_sum(home_xg_prefix, start, end)
            xg_away = range_sum(away_xg_prefix, start, end)
            xg_diff = xg_home - xg_away

            goals_home = range_sum(home_goal_prefix, 0, start)
            goals_away = range_sum(away_goal_prefix, 0, start)
            score_diff = goals_home - goals_away

            red_home = range_sum(home_red_prefix, 0, start)
            red_away = range_sum(away_red_prefix, 0, start)

            stints.append(
                {
                    "game_id": game_id,
                    "start": start,
                    "end": end,
                    "duration": duration,
                    "home_team_id": home_team_id,
                    "away_team_id": away_team_id,
                    "home_players": home_players,
                    "away_players": away_players,
                    "xg_diff": xg_diff,
                    "score_diff": score_diff,
                    "red_home": red_home,
                    "red_away": red_away,
                }
            )

    return stints


# --------------------------
# RAPM, SPM, BPM
# --------------------------

def build_design_matrix(stints: List[Dict], player_index: Dict[int, int]) -> Tuple[sparse.csr_matrix, np.ndarray, np.ndarray, np.ndarray]:
    rows: List[int] = []
    cols: List[int] = []
    data: List[float] = []

    y = np.zeros(len(stints), dtype=float)
    weights = np.zeros(len(stints), dtype=float)
    context = np.zeros((len(stints), 3), dtype=float)

    for i, stint in enumerate(stints):
        for pid in stint["home_players"]:
            if pid in player_index:
                rows.append(i)
                cols.append(player_index[pid])
                data.append(1.0)
        for pid in stint["away_players"]:
            if pid in player_index:
                rows.append(i)
                cols.append(player_index[pid])
                data.append(-1.0)

        duration = float(stint["duration"])
        xg_diff = float(stint["xg_diff"])
        y[i] = (xg_diff / duration) * 90.0
        weights[i] = duration

        context[i, 0] = float(stint["score_diff"])
        context[i, 1] = float(stint["red_home"])
        context[i, 2] = float(stint["red_away"])

    X_players = sparse.coo_matrix(
        (data, (rows, cols)),
        shape=(len(stints), len(player_index)),
        dtype=float,
    ).tocsr()

    X_context = sparse.csr_matrix(context)
    X = sparse.hstack([X_players, X_context], format="csr")
    return X, X_players, y, weights


def fit_rapm(X: sparse.csr_matrix, y: np.ndarray, weights: np.ndarray, alpha_grid: np.ndarray) -> np.ndarray:
    if len(alpha_grid) == 1:
        model = Ridge(alpha=float(alpha_grid[0]), fit_intercept=True)
    else:
        model = RidgeCV(alphas=alpha_grid, fit_intercept=True)
    model.fit(X, y, sample_weight=weights)
    return model.coef_, float(model.intercept_)


def build_player_season_stats(players: pd.DataFrame) -> pd.DataFrame:
    metrics = [
        "goals",
        "own_goals",
        "shots",
        "xg",
        "xg_chain",
        "xg_buildup",
        "assists",
        "xa",
        "key_passes",
        "yellow_cards",
        "red_cards",
    ]

    agg = {m: "sum" for m in metrics}
    agg.update({"minutes": "sum", "player": lambda x: x.value_counts().idxmax(), "position": lambda x: x.value_counts().idxmax()})

    grouped = players.groupby("player_id", as_index=False).agg(agg)

    # Per-90 rates
    minutes = grouped["minutes"].replace(0, np.nan)
    for m in metrics:
        grouped[f"{m}_p90"] = grouped[m] / (minutes / 90.0)
        grouped[f"{m}_p90"] = grouped[f"{m}_p90"].fillna(0.0)

    return grouped


def apply_padding_and_normalization(stats: pd.DataFrame, min_minutes: int) -> pd.DataFrame:
    metrics_p90 = [c for c in stats.columns if c.endswith("_p90")]

    stats = stats.copy()
    stats["position"] = stats["position"].fillna("UNK")

    # Lower bound scores by position
    lbs_by_pos = {}
    for pos, df in stats.groupby("position"):
        lbs = {}
        for m in metrics_p90:
            mean = df[m].mean()
            std = df[m].std(ddof=0) if df[m].std(ddof=0) > 0 else 0.0
            lbs[m] = mean - 1.5 * std
        lbs_by_pos[pos] = lbs

    padded = stats.copy()
    padding_minutes = np.maximum(0.0, min_minutes - padded["minutes"].values)

    for m in metrics_p90:
        lbs = padded["position"].map(lambda p: lbs_by_pos[p][m]).values
        padded[m] = (padded[m].values * padded["minutes"].values + lbs * padding_minutes) / (padded["minutes"].values + padding_minutes + 1e-9)

    return padded


def fit_spm(features: pd.DataFrame, rapm: pd.Series, minutes: pd.Series, alpha_grid: np.ndarray, min_minutes: int) -> np.ndarray:
    feature_cols = [c for c in features.columns if c.endswith("_p90")]
    X = features[feature_cols].values
    y = rapm.values
    mask = (minutes.values >= min_minutes) & (~np.isnan(y))

    model = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("ridge", RidgeCV(alphas=alpha_grid, fit_intercept=True)),
        ]
    )
    model.fit(X[mask], y[mask])
    return model.predict(X)


def fit_bpm(X_players: sparse.csr_matrix, y: np.ndarray, expected_y: np.ndarray, weights: np.ndarray, alpha_grid: np.ndarray) -> np.ndarray:
    target = y - expected_y
    if len(alpha_grid) == 1:
        model = Ridge(alpha=float(alpha_grid[0]), fit_intercept=True)
    else:
        model = RidgeCV(alphas=alpha_grid, fit_intercept=True)
    model.fit(X_players, target, sample_weight=weights)
    return model.coef_


# --------------------------
# No league translation / GPM in this version.


# --------------------------
# Main pipeline
# --------------------------

def process_league_season(cfg: Config, league: str, season: str) -> pd.DataFrame:
    schedule, players, shots = load_league_season(cfg.data_root, league, season)

    # Build stints
    stints = build_stints(schedule, players, shots, cfg.min_stint_minutes)
    if not stints:
        return pd.DataFrame()

    # Player index
    player_ids = sorted(players["player_id"].unique().astype(int).tolist())
    player_index = {pid: i for i, pid in enumerate(player_ids)}

    # RAPM
    X, X_players, y, weights = build_design_matrix(stints, player_index)
    rapm_coef, rapm_intercept = fit_rapm(X, y, weights, cfg.alpha_grid)

    rapm_player = rapm_coef[: len(player_ids)]
    rapm_context = rapm_coef[len(player_ids) :]

    rapm_df = pd.DataFrame({
        "player_id": player_ids,
        "rapm": rapm_player,
    })

    # Season stats & SPM
    stats = build_player_season_stats(players)
    stats = apply_padding_and_normalization(stats, cfg.min_minutes_spm)

    rapm_join = stats.merge(rapm_df, on="player_id", how="left")
    spm_values = fit_spm(rapm_join, rapm_join["rapm"], rapm_join["minutes"], cfg.alpha_grid, cfg.min_minutes_spm)
    rapm_join["spm"] = spm_values

    # BPM
    expected_y = X_players.dot(rapm_join.set_index("player_id").loc[player_ids]["spm"].values)
    expected_y = expected_y + (X[:, len(player_ids):].toarray().dot(rapm_context))
    bpm_coef = fit_bpm(X_players, y, expected_y, weights, cfg.alpha_grid)

    bpm_df = pd.DataFrame({"player_id": player_ids, "bpm": bpm_coef})
    rapm_join = rapm_join.merge(bpm_df, on="player_id", how="left")
    rapm_join["epm"] = rapm_join["spm"] + rapm_join["bpm"]

    rapm_join["league"] = league
    rapm_join["season"] = season
    return rapm_join


def write_metric_files(output_dir: Path, df: pd.DataFrame, league: str, season: str, metrics: Iterable[str]) -> None:
    base_cols = ["player_id", "player", "position", "minutes", "league", "season"]
    for metric in metrics:
        if metric not in df.columns:
            continue
        out = df[base_cols + [metric]].copy()
        out_path = output_dir / f"{league}_{season}_{metric}.csv"
        out.to_csv(out_path, index=False)


def run(cfg: Config) -> None:
    ensure_dir(cfg.output_dir)

    for league in cfg.leagues:
        league_dir = cfg.data_root / league
        seasons = cfg.seasons or list_seasons(league_dir)
        for season in seasons:
            print(f"Processing {league} {season}...")
            df = process_league_season(cfg, league, season)
            if df.empty:
                print(f"No data for {league} {season}")
                continue
            write_metric_files(cfg.output_dir, df, league, season, cfg.output_metrics)

    print(f"Saved outputs to {cfg.output_dir}")


# --------------------------
# CLI
# --------------------------

def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Compute RAPM/SPM/BPM/EPM from Understat data")
    parser.add_argument("--data-root", default="/Users/memphismeng/Google Drive/My Drive/Understat_Data")
    parser.add_argument("--output-dir", default="data/plus_minus")
    parser.add_argument("--leagues", nargs="*", default=["ENG", "ESP", "FRA", "GER", "ITA"])
    parser.add_argument("--seasons", nargs="*", default=None)
    parser.add_argument("--min-stint-minutes", type=float, default=1.0)
    parser.add_argument("--min-minutes-spm", type=int, default=1000)
    parser.add_argument("--metrics", nargs="*", default=["rapm", "spm", "bpm", "epm"])
    args = parser.parse_args()

    return Config(
        data_root=Path(args.data_root),
        output_dir=Path(args.output_dir),
        leagues=args.leagues,
        seasons=args.seasons,
        min_stint_minutes=args.min_stint_minutes,
        min_minutes_spm=args.min_minutes_spm,
        alpha_grid=DEFAULT_ALPHA_GRID,
        output_metrics=args.metrics,
    )


if __name__ == "__main__":
    cfg = parse_args()
    run(cfg)
