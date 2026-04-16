from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import requests

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - optional convenience dependency
    def load_dotenv(*_args: Any, **_kwargs: Any) -> bool:
        return False

from gemini_plus_minus.compute import (
    DEFAULT_ALPHA_GRID,
    apply_padding_and_normalization,
    build_design_matrix,
    fit_bpm,
    fit_rapm,
    fit_spm,
)


DEFAULT_BASE_URL = "https://v3.football.api-sports.io"
DEFAULT_LEAGUE_ID = 39
DEFAULT_SEASON = 2024
DEFAULT_FIXTURE_LIMIT = 12
FINISHED_STATUSES = {"FT", "AET", "PEN"}
RED_CARD_DETAILS = {"Red Card", "Yellow Red Card"}


@dataclass
class Config:
    api_key: str
    base_url: str
    league_id: int
    season: int
    fixture_limit: Optional[int]
    output_dir: Path
    min_stint_minutes: float
    min_minutes_spm: int
    alpha_grid: np.ndarray
    timeout: int
    cache_dir: Path


class ApiFootballClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: int = 30,
        cache_dir: Optional[Path] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.cache_dir = cache_dir
        self.session = requests.Session()
        self.session.headers.update({"x-apisports-key": api_key})

    def get(self, path: str, **params: Any) -> Dict[str, Any]:
        cache_path = self._cache_path(path, params)
        if cache_path is not None and cache_path.exists():
            return json.loads(cache_path.read_text(encoding="utf-8"))
        response = self.session.get(
            f"{self.base_url}/{path.lstrip('/')}",
            params={k: v for k, v in params.items() if v is not None},
            timeout=self.timeout,
        )
        if response.status_code == 429:
            raise RuntimeError(
                "API-Football rate limit reached (HTTP 429). "
                "Wait for the quota window to reset or rerun with cached fixtures."
            )
        response.raise_for_status()
        payload: Dict[str, Any] = response.json()
        errors = payload.get("errors")
        if isinstance(errors, dict) and errors:
            raise RuntimeError(f"API-Football error for {path}: {errors}")
        if cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(payload), encoding="utf-8")
        return payload

    def _cache_path(self, path: str, params: Dict[str, Any]) -> Optional[Path]:
        if self.cache_dir is None:
            return None
        normalized = "&".join(f"{key}={params[key]}" for key in sorted(params) if params[key] is not None)
        digest = hashlib.sha1(f"{path}?{normalized}".encode("utf-8")).hexdigest()[:16]
        stem = path.strip("/").replace("/", "_") or "root"
        return self.cache_dir / f"{stem}_{digest}.json"


def _load_env() -> None:
    repo_env = Path(__file__).resolve().parents[1] / ".env"
    loaded = load_dotenv()
    if repo_env.exists():
        loaded = load_dotenv(repo_env, override=False) or loaded
    if loaded or not repo_env.exists():
        return

    for line in repo_env.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _mode(values: Sequence[Any], default: Any = "UNK") -> Any:
    cleaned = [value for value in values if value not in (None, "", "null")]
    if not cleaned:
        return default
    counts = defaultdict(int)
    for value in cleaned:
        counts[value] += 1
    return max(counts, key=counts.get)


def _event_minute(event: Dict[str, Any]) -> int:
    time = event.get("time") or {}
    elapsed = _safe_int(time.get("elapsed")) or 0
    extra = _safe_int(time.get("extra")) or 0
    return elapsed + extra


def _chunked(values: Sequence[int], size: int) -> Iterable[List[int]]:
    for index in range(0, len(values), size):
        yield list(values[index:index + size])


def _write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _extract_expected_goals(statistics_response: List[Dict[str, Any]]) -> Dict[int, float]:
    result: Dict[int, float] = {}
    for team_stats in statistics_response:
        team_id = _safe_int(team_stats.get("team", {}).get("id"))
        if team_id is None:
            continue
        stats_map = {
            item.get("type"): item.get("value")
            for item in team_stats.get("statistics", [])
            if isinstance(item, dict)
        }
        xg = _safe_float(stats_map.get("expected_goals"))
        if xg is not None:
            result[team_id] = xg
    return result


def _extract_starting_players(lineups_response: List[Dict[str, Any]]) -> Dict[int, set[int]]:
    result: Dict[int, set[int]] = {}
    for lineup in lineups_response:
        team_id = _safe_int(lineup.get("team", {}).get("id"))
        if team_id is None:
            continue
        players = {
            player["player"]["id"]
            for player in lineup.get("startXI", [])
            if isinstance(player, dict) and _safe_int(player.get("player", {}).get("id")) is not None
        }
        result[team_id] = {int(pid) for pid in players}
    return result


def _extract_match_end(
    fixture: Dict[str, Any],
    events: List[Dict[str, Any]],
    player_blocks: List[Dict[str, Any]],
) -> int:
    max_minute = 90
    status_elapsed = _safe_int(fixture.get("fixture", {}).get("status", {}).get("elapsed"))
    if status_elapsed is not None:
        max_minute = max(max_minute, status_elapsed)
    for event in events:
        max_minute = max(max_minute, _event_minute(event))
    for block in player_blocks:
        for player in block.get("players", []):
            stats = (player.get("statistics") or [{}])[0]
            minutes = _safe_int((stats.get("games") or {}).get("minutes"))
            if minutes is not None:
                max_minute = max(max_minute, minutes)
    return max_minute


def _team_from_event(event: Dict[str, Any]) -> Optional[int]:
    return _safe_int((event.get("team") or {}).get("id"))


def _is_goal_event(event: Dict[str, Any]) -> bool:
    return (event.get("type") or "").lower() == "goal"


def _is_sub_event(event: Dict[str, Any]) -> bool:
    return (event.get("type") or "").lower() == "subst"


def _is_red_card_event(event: Dict[str, Any]) -> bool:
    return (event.get("type") or "").lower() == "card" and (event.get("detail") or "") in RED_CARD_DETAILS


def _goal_team(event: Dict[str, Any], home_team_id: int, away_team_id: int) -> Optional[int]:
    team_id = _team_from_event(event)
    if team_id is None:
        return None
    detail = event.get("detail") or ""
    if detail == "Own Goal":
        return away_team_id if team_id == home_team_id else home_team_id
    return team_id


def _apply_minute_events(
    minute_events: List[Dict[str, Any]],
    home_players: set[int],
    away_players: set[int],
    *,
    home_team_id: int,
    away_team_id: int,
    score_home: int,
    score_away: int,
    red_home: int,
    red_away: int,
) -> Tuple[int, int, int, int]:
    for event in minute_events:
        if _is_goal_event(event):
            team_id = _goal_team(event, home_team_id, away_team_id)
            if team_id == home_team_id:
                score_home += 1
            elif team_id == away_team_id:
                score_away += 1
        elif _is_red_card_event(event):
            team_id = _team_from_event(event)
            player_id = _safe_int((event.get("player") or {}).get("id"))
            if team_id == home_team_id:
                red_home += 1
                if player_id is not None:
                    home_players.discard(player_id)
            elif team_id == away_team_id:
                red_away += 1
                if player_id is not None:
                    away_players.discard(player_id)
        elif _is_sub_event(event):
            team_id = _team_from_event(event)
            player_out = _safe_int((event.get("player") or {}).get("id"))
            player_in = _safe_int((event.get("assist") or {}).get("id"))
            if team_id == home_team_id:
                if player_out is not None:
                    home_players.discard(player_out)
                if player_in is not None:
                    home_players.add(player_in)
            elif team_id == away_team_id:
                if player_out is not None:
                    away_players.discard(player_out)
                if player_in is not None:
                    away_players.add(player_in)
    return score_home, score_away, red_home, red_away


def _build_stints_for_fixture(
    fixture: Dict[str, Any],
    statistics: List[Dict[str, Any]],
    events: List[Dict[str, Any]],
    lineups: List[Dict[str, Any]],
    player_blocks: List[Dict[str, Any]],
    min_stint_minutes: float,
) -> List[Dict[str, Any]]:
    fixture_info = fixture.get("fixture", {})
    teams = fixture.get("teams", {})
    home_team_id = _safe_int((teams.get("home") or {}).get("id"))
    away_team_id = _safe_int((teams.get("away") or {}).get("id"))
    fixture_id = _safe_int(fixture_info.get("id"))
    if fixture_id is None or home_team_id is None or away_team_id is None:
        return []

    starters = _extract_starting_players(lineups)
    home_players = set(starters.get(home_team_id, set()))
    away_players = set(starters.get(away_team_id, set()))
    if not home_players or not away_players:
        return []

    match_end = _extract_match_end(fixture, events, player_blocks)
    expected_goals = _extract_expected_goals(statistics)
    home_metric = expected_goals.get(home_team_id)
    away_metric = expected_goals.get(away_team_id)
    if home_metric is None:
        home_metric = float((fixture.get("goals") or {}).get("home") or 0.0)
    if away_metric is None:
        away_metric = float((fixture.get("goals") or {}).get("away") or 0.0)

    rate_home = float(home_metric) / max(match_end, 1)
    rate_away = float(away_metric) / max(match_end, 1)

    boundary_events = [event for event in events if _is_goal_event(event) or _is_sub_event(event) or _is_red_card_event(event)]
    grouped: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for event in boundary_events:
        grouped[_event_minute(event)].append(event)

    boundary_minutes = {45, match_end}
    boundary_minutes.update(grouped.keys())
    boundary_minutes = {minute for minute in boundary_minutes if 0 <= minute <= match_end}
    sorted_minutes = sorted(boundary_minutes)

    stints: List[Dict[str, Any]] = []
    start = 0
    score_home = 0
    score_away = 0
    red_home = 0
    red_away = 0

    for minute in sorted_minutes:
        duration = minute - start
        if duration >= min_stint_minutes and home_players and away_players:
            stints.append(
                {
                    "game_id": fixture_id,
                    "start": start,
                    "end": minute,
                    "duration": duration,
                    "home_team_id": home_team_id,
                    "away_team_id": away_team_id,
                    "home_players": sorted(home_players),
                    "away_players": sorted(away_players),
                    "xg_diff": (rate_home - rate_away) * duration,
                    "score_diff": score_home - score_away,
                    "red_home": red_home,
                    "red_away": red_away,
                }
            )

        minute_events = grouped.get(minute, [])
        score_home, score_away, red_home, red_away = _apply_minute_events(
            minute_events,
            home_players,
            away_players,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            score_home=score_home,
            score_away=score_away,
            red_home=red_home,
            red_away=red_away,
        )
        start = minute

    return stints


def _player_match_rows(fixture: Dict[str, Any], player_blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    fixture_id = _safe_int((fixture.get("fixture") or {}).get("id"))
    if fixture_id is None:
        return []

    rows: List[Dict[str, Any]] = []
    for block in player_blocks:
        team_id = _safe_int((block.get("team") or {}).get("id"))
        if team_id is None:
            continue
        for player in block.get("players", []):
            player_id = _safe_int((player.get("player") or {}).get("id"))
            if player_id is None:
                continue
            stats = (player.get("statistics") or [{}])[0]
            games = stats.get("games") or {}
            shots = stats.get("shots") or {}
            goals = stats.get("goals") or {}
            passes = stats.get("passes") or {}
            tackles = stats.get("tackles") or {}
            duels = stats.get("duels") or {}
            dribbles = stats.get("dribbles") or {}
            fouls = stats.get("fouls") or {}
            cards = stats.get("cards") or {}
            penalty = stats.get("penalty") or {}

            rows.append(
                {
                    "game_id": fixture_id,
                    "team_id": team_id,
                    "player_id": player_id,
                    "player": (player.get("player") or {}).get("name"),
                    "minutes": _safe_float(games.get("minutes")) or 0.0,
                    "position": games.get("position") or "UNK",
                    "goals": _safe_float(goals.get("total")) or 0.0,
                    "assists": _safe_float(goals.get("assists")) or 0.0,
                    "shots": _safe_float(shots.get("total")) or 0.0,
                    "shots_on": _safe_float(shots.get("on")) or 0.0,
                    "key_passes": _safe_float(passes.get("key")) or 0.0,
                    "passes_total": _safe_float(passes.get("total")) or 0.0,
                    "tackles_total": _safe_float(tackles.get("total")) or 0.0,
                    "interceptions": _safe_float(tackles.get("interceptions")) or 0.0,
                    "blocks": _safe_float(tackles.get("blocks")) or 0.0,
                    "duels_won": _safe_float(duels.get("won")) or 0.0,
                    "dribbles_success": _safe_float(dribbles.get("success")) or 0.0,
                    "fouls_drawn": _safe_float(fouls.get("drawn")) or 0.0,
                    "fouls_committed": _safe_float(fouls.get("committed")) or 0.0,
                    "yellow_cards": _safe_float(cards.get("yellow")) or 0.0,
                    "red_cards": _safe_float(cards.get("red")) or 0.0,
                    "penalties_scored": _safe_float(penalty.get("scored")) or 0.0,
                    "penalties_missed": _safe_float(penalty.get("missed")) or 0.0,
                    "penalties_won": _safe_float(penalty.get("won")) or 0.0,
                    "penalties_committed": _safe_float(penalty.get("commited")) or 0.0,
                }
            )
    return rows


def fetch_fixtures(client: ApiFootballClient, league_id: int, season: int) -> List[Dict[str, Any]]:
    payload = client.get("fixtures", league=league_id, season=season)
    fixtures: List[Dict[str, Any]] = payload.get("response", [])
    fixtures = [
        fixture for fixture in fixtures
        if (fixture.get("fixture", {}).get("status", {}) or {}).get("short") in FINISHED_STATUSES
    ]
    fixtures.sort(key=lambda fixture: fixture.get("fixture", {}).get("date") or "")
    return fixtures


def fetch_fixture_bundle(client: ApiFootballClient, fixture_id: int) -> Dict[str, Any]:
    return {
        "events": client.get("fixtures/events", fixture=fixture_id).get("response", []),
        "lineups": client.get("fixtures/lineups", fixture=fixture_id).get("response", []),
        "players": client.get("fixtures/players", fixture=fixture_id).get("response", []),
        "statistics": client.get("fixtures/statistics", fixture=fixture_id).get("response", []),
    }


def build_player_season_stats_api(player_rows: pd.DataFrame) -> pd.DataFrame:
    metric_cols = [
        "goals",
        "assists",
        "shots",
        "shots_on",
        "key_passes",
        "passes_total",
        "tackles_total",
        "interceptions",
        "blocks",
        "duels_won",
        "dribbles_success",
        "fouls_drawn",
        "fouls_committed",
        "yellow_cards",
        "red_cards",
        "penalties_scored",
        "penalties_missed",
        "penalties_won",
        "penalties_committed",
    ]

    agg = {column: "sum" for column in metric_cols}
    agg.update(
        {
            "minutes": "sum",
            "player": lambda values: _mode(list(values), default="Unknown"),
            "position": lambda values: _mode(list(values), default="UNK"),
        }
    )
    grouped = player_rows.groupby("player_id", as_index=False).agg(agg)
    minutes = grouped["minutes"].replace(0, np.nan)
    for column in metric_cols:
        grouped[f"{column}_p90"] = (grouped[column] / (minutes / 90.0)).fillna(0.0)
    return grouped


def fit_spm_safe(
    features: pd.DataFrame,
    rapm: pd.Series,
    minutes: pd.Series,
    alpha_grid: np.ndarray,
    min_minutes: int,
) -> np.ndarray:
    feature_cols = [column for column in features.columns if column.endswith("_p90")]
    if not feature_cols:
        return rapm.fillna(0.0).values
    mask = (minutes.values >= min_minutes) & (~np.isnan(rapm.values))
    if int(mask.sum()) < 2:
        return rapm.fillna(0.0).values
    return fit_spm(features, rapm, minutes, alpha_grid, min_minutes)


def compute_metrics(
    stints: List[Dict[str, Any]],
    player_rows: pd.DataFrame,
    alpha_grid: np.ndarray,
    min_minutes_spm: int,
) -> pd.DataFrame:
    player_ids = sorted({
        int(pid)
        for stint in stints
        for pid in (stint.get("home_players") or []) + (stint.get("away_players") or [])
    })
    if not player_ids:
        return pd.DataFrame()

    player_index = {pid: index for index, pid in enumerate(player_ids)}
    X, X_players, y, weights = build_design_matrix(stints, player_index)
    rapm_coef, rapm_intercept = fit_rapm(X, y, weights, alpha_grid)
    rapm_context = rapm_coef[len(player_ids):]

    rapm_df = pd.DataFrame({"player_id": player_ids, "rapm": rapm_coef[:len(player_ids)]})
    stats = build_player_season_stats_api(player_rows)
    merged = stats.merge(rapm_df, on="player_id", how="outer")
    numeric_cols = merged.select_dtypes(include=[np.number]).columns.tolist()
    for column in numeric_cols:
        if column != "rapm":
            merged[column] = merged[column].fillna(0.0)
    merged["minutes"] = merged["minutes"].fillna(0.0)
    merged["player"] = merged["player"].fillna("Unknown")
    merged["position"] = merged["position"].fillna("UNK")
    merged = apply_padding_and_normalization(merged, min_minutes_spm)

    merged["spm"] = fit_spm_safe(merged, merged["rapm"], merged["minutes"], alpha_grid, min_minutes_spm)

    player_spm = merged.set_index("player_id")["spm"].reindex(player_ids).fillna(0.0).values
    expected_y = X_players.dot(player_spm) + X[:, len(player_ids):].toarray().dot(rapm_context)
    bpm_coef = fit_bpm(X_players, y, expected_y, weights, alpha_grid)

    bpm_df = pd.DataFrame({"player_id": player_ids, "bpm": bpm_coef})
    merged = merged.merge(bpm_df, on="player_id", how="left")
    merged["bpm"] = merged["bpm"].fillna(0.0)
    merged["epm"] = merged["spm"].fillna(0.0) + merged["bpm"]
    merged["rapm_intercept"] = float(rapm_intercept)
    return merged.sort_values(["epm", "rapm"], ascending=False).reset_index(drop=True)


def run(cfg: Config) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    client = ApiFootballClient(
        api_key=cfg.api_key,
        base_url=cfg.base_url,
        timeout=cfg.timeout,
        cache_dir=cfg.cache_dir,
    )
    fixtures = fetch_fixtures(client, cfg.league_id, cfg.season)
    if cfg.fixture_limit is not None:
        fixtures = fixtures[:cfg.fixture_limit]
    if not fixtures:
        raise RuntimeError("No finished fixtures were found for the requested league and season.")

    all_stints: List[Dict[str, Any]] = []
    all_player_rows: List[Dict[str, Any]] = []
    fixture_rows: List[Dict[str, Any]] = []

    for fixture in fixtures:
        fixture_id = _safe_int((fixture.get("fixture") or {}).get("id"))
        if fixture_id is None:
            continue
        bundle = fetch_fixture_bundle(client, fixture_id)
        stints = _build_stints_for_fixture(
            fixture,
            statistics=bundle["statistics"],
            events=bundle["events"],
            lineups=bundle["lineups"],
            player_blocks=bundle["players"],
            min_stint_minutes=cfg.min_stint_minutes,
        )
        all_stints.extend(stints)
        all_player_rows.extend(_player_match_rows(fixture, bundle["players"]))

        fixture_rows.append(
            {
                "fixture_id": fixture_id,
                "date": (fixture.get("fixture") or {}).get("date"),
                "home": (fixture.get("teams", {}).get("home") or {}).get("name"),
                "away": (fixture.get("teams", {}).get("away") or {}).get("name"),
                "status": (fixture.get("fixture", {}).get("status") or {}).get("short"),
                "stints": len(stints),
                "events": len(bundle["events"]),
                "lineups": len(bundle["lineups"]),
                "player_rows": len(_player_match_rows(fixture, bundle["players"])),
            }
        )

    if not all_stints:
        raise RuntimeError("No stints could be reconstructed from the fetched fixtures.")
    if not all_player_rows:
        raise RuntimeError("No player match statistics were returned for the fetched fixtures.")

    player_df = pd.DataFrame(all_player_rows)
    metrics_df = compute_metrics(all_stints, player_df, cfg.alpha_grid, cfg.min_minutes_spm)
    stints_df = pd.DataFrame(all_stints)
    fixtures_df = pd.DataFrame(fixture_rows)

    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    metrics_df.to_csv(cfg.output_dir / "metrics.csv", index=False)
    stints_df.to_csv(cfg.output_dir / "stints.csv", index=False)
    player_df.to_csv(cfg.output_dir / "player_match_stats.csv", index=False)
    fixtures_df.to_csv(cfg.output_dir / "fixtures_used.csv", index=False)

    metadata = {
        "league_id": cfg.league_id,
        "season": cfg.season,
        "fixture_limit": cfg.fixture_limit,
        "fixtures_used": len(fixtures_df),
        "stints_built": len(stints_df),
        "players_scored": int(metrics_df["player_id"].nunique()) if not metrics_df.empty else 0,
        "xg_model_note": "Team-level match expected_goals is distributed uniformly across each match when constructing stint targets.",
    }
    (cfg.output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return metrics_df, stints_df, fixtures_df


def parse_args() -> Config:
    _load_env()
    parser = argparse.ArgumentParser(description="Compute RAPM/SPM/BPM/EPM from API-Football fixture data")
    parser.add_argument("--league-id", type=int, default=int(os.getenv("API_FOOTBALL_LEAGUE_ID", str(DEFAULT_LEAGUE_ID))))
    parser.add_argument("--season", type=int, default=DEFAULT_SEASON)
    parser.add_argument("--fixture-limit", type=int, default=DEFAULT_FIXTURE_LIMIT)
    parser.add_argument("--output-dir", default="data/api_football_plus_minus")
    parser.add_argument("--cache-dir", default="data/api_football_plus_minus_cache")
    parser.add_argument("--min-stint-minutes", type=float, default=1.0)
    parser.add_argument("--min-minutes-spm", type=int, default=300)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()

    api_key = os.getenv("API_FOOTBALL_KEY")
    if not api_key:
        raise ValueError("Missing API_FOOTBALL_KEY in the environment.")

    fixture_limit = args.fixture_limit
    if fixture_limit is not None and fixture_limit <= 0:
        fixture_limit = None

    return Config(
        api_key=api_key,
        base_url=os.getenv("API_FOOTBALL_BASE_URL", DEFAULT_BASE_URL),
        league_id=args.league_id,
        season=args.season,
        fixture_limit=fixture_limit,
        output_dir=Path(args.output_dir),
        min_stint_minutes=args.min_stint_minutes,
        min_minutes_spm=args.min_minutes_spm,
        alpha_grid=DEFAULT_ALPHA_GRID,
        timeout=args.timeout,
        cache_dir=Path(args.cache_dir),
    )


if __name__ == "__main__":
    config = parse_args()
    metrics_df, stints_df, fixtures_df = run(config)
    print(
        json.dumps(
            {
                "output_dir": str(config.output_dir),
                "fixtures_used": len(fixtures_df),
                "stints_built": len(stints_df),
                "players_scored": int(metrics_df["player_id"].nunique()) if not metrics_df.empty else 0,
                "top_players": metrics_df[["player_id", "player", "rapm", "spm", "bpm", "epm"]].head(10).to_dict("records"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
