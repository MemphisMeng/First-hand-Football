from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from gemini_plus_minus.api_football_compute import (
    ApiFootballClient,
    DEFAULT_BASE_URL,
    DEFAULT_LEAGUE_ID,
    DEFAULT_SEASON,
    DEFAULT_ALPHA_GRID,
    FINISHED_STATUSES,
    _build_stints_for_fixture,
    _extract_expected_goals,
    _load_env,
    _player_match_rows,
    _safe_float,
    _safe_int,
    compute_metrics,
    fetch_fixture_bundle,
    fetch_fixtures,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class RealtimeConfig:
    api_key: str
    base_url: str
    league_id: int
    season: int
    db_path: Path
    cache_dir: Path
    output_dir: Path
    timeout: int
    min_stint_minutes: float
    min_minutes_spm: int
    fixture_limit: Optional[int]
    poll_interval: int


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS fixtures (
            fixture_id INTEGER PRIMARY KEY,
            league_id INTEGER,
            season INTEGER,
            date TEXT,
            status TEXT,
            elapsed INTEGER,
            home_team_id INTEGER,
            away_team_id INTEGER,
            home_name TEXT,
            away_name TEXT,
            goals_home REAL,
            goals_away REAL,
            xg_home REAL,
            xg_away REAL,
            is_live INTEGER NOT NULL DEFAULT 0,
            is_finished INTEGER NOT NULL DEFAULT 0,
            last_synced_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS stints (
            fixture_id INTEGER NOT NULL,
            stint_index INTEGER NOT NULL,
            start_minute INTEGER NOT NULL,
            end_minute INTEGER NOT NULL,
            duration REAL NOT NULL,
            home_team_id INTEGER NOT NULL,
            away_team_id INTEGER NOT NULL,
            home_players_json TEXT NOT NULL,
            away_players_json TEXT NOT NULL,
            xg_diff REAL NOT NULL,
            score_diff REAL NOT NULL,
            red_home REAL NOT NULL,
            red_away REAL NOT NULL,
            is_live INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (fixture_id, stint_index)
        );

        CREATE TABLE IF NOT EXISTS player_match_stats (
            fixture_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            player_id INTEGER NOT NULL,
            player TEXT,
            minutes REAL,
            position TEXT,
            goals REAL,
            assists REAL,
            shots REAL,
            shots_on REAL,
            key_passes REAL,
            passes_total REAL,
            tackles_total REAL,
            interceptions REAL,
            blocks REAL,
            duels_won REAL,
            dribbles_success REAL,
            fouls_drawn REAL,
            fouls_committed REAL,
            yellow_cards REAL,
            red_cards REAL,
            penalties_scored REAL,
            penalties_missed REAL,
            penalties_won REAL,
            penalties_committed REAL,
            PRIMARY KEY (fixture_id, player_id)
        );
        """
    )
    conn.commit()


def _fixture_record(fixture: Dict[str, Any], statistics: List[Dict[str, Any]], *, is_live: bool) -> Dict[str, Any]:
    fixture_info = fixture.get("fixture") or {}
    teams = fixture.get("teams") or {}
    goals = fixture.get("goals") or {}
    xg = _extract_expected_goals(statistics)
    home_team_id = _safe_int((teams.get("home") or {}).get("id"))
    away_team_id = _safe_int((teams.get("away") or {}).get("id"))

    return {
        "fixture_id": _safe_int(fixture_info.get("id")),
        "league_id": _safe_int((fixture.get("league") or {}).get("id")),
        "season": _safe_int((fixture.get("league") or {}).get("season")),
        "date": fixture_info.get("date"),
        "status": (fixture_info.get("status") or {}).get("short"),
        "elapsed": _safe_int((fixture_info.get("status") or {}).get("elapsed")),
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "home_name": (teams.get("home") or {}).get("name"),
        "away_name": (teams.get("away") or {}).get("name"),
        "goals_home": _safe_float(goals.get("home")),
        "goals_away": _safe_float(goals.get("away")),
        "xg_home": xg.get(home_team_id) if home_team_id is not None else None,
        "xg_away": xg.get(away_team_id) if away_team_id is not None else None,
        "is_live": int(is_live),
        "is_finished": int(((fixture_info.get("status") or {}).get("short") in FINISHED_STATUSES)),
        "last_synced_at": _utc_now(),
    }


def upsert_fixture(conn: sqlite3.Connection, record: Dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO fixtures (
            fixture_id, league_id, season, date, status, elapsed,
            home_team_id, away_team_id, home_name, away_name,
            goals_home, goals_away, xg_home, xg_away,
            is_live, is_finished, last_synced_at
        ) VALUES (
            :fixture_id, :league_id, :season, :date, :status, :elapsed,
            :home_team_id, :away_team_id, :home_name, :away_name,
            :goals_home, :goals_away, :xg_home, :xg_away,
            :is_live, :is_finished, :last_synced_at
        )
        ON CONFLICT(fixture_id) DO UPDATE SET
            league_id=excluded.league_id,
            season=excluded.season,
            date=excluded.date,
            status=excluded.status,
            elapsed=excluded.elapsed,
            home_team_id=excluded.home_team_id,
            away_team_id=excluded.away_team_id,
            home_name=excluded.home_name,
            away_name=excluded.away_name,
            goals_home=excluded.goals_home,
            goals_away=excluded.goals_away,
            xg_home=excluded.xg_home,
            xg_away=excluded.xg_away,
            is_live=excluded.is_live,
            is_finished=excluded.is_finished,
            last_synced_at=excluded.last_synced_at
        """,
        record,
    )


def replace_stints(conn: sqlite3.Connection, fixture_id: int, stints: List[Dict[str, Any]], *, is_live: bool) -> None:
    conn.execute("DELETE FROM stints WHERE fixture_id = ?", (fixture_id,))
    rows = []
    for index, stint in enumerate(stints):
        rows.append(
            {
                "fixture_id": fixture_id,
                "stint_index": index,
                "start_minute": stint["start"],
                "end_minute": stint["end"],
                "duration": stint["duration"],
                "home_team_id": stint["home_team_id"],
                "away_team_id": stint["away_team_id"],
                "home_players_json": json.dumps(stint["home_players"]),
                "away_players_json": json.dumps(stint["away_players"]),
                "xg_diff": stint["xg_diff"],
                "score_diff": stint["score_diff"],
                "red_home": stint["red_home"],
                "red_away": stint["red_away"],
                "is_live": int(is_live),
            }
        )
    conn.executemany(
        """
        INSERT INTO stints (
            fixture_id, stint_index, start_minute, end_minute, duration,
            home_team_id, away_team_id, home_players_json, away_players_json,
            xg_diff, score_diff, red_home, red_away, is_live
        ) VALUES (
            :fixture_id, :stint_index, :start_minute, :end_minute, :duration,
            :home_team_id, :away_team_id, :home_players_json, :away_players_json,
            :xg_diff, :score_diff, :red_home, :red_away, :is_live
        )
        """,
        rows,
    )


PLAYER_STAT_COLUMNS = [
    "fixture_id",
    "team_id",
    "player_id",
    "player",
    "minutes",
    "position",
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


def replace_player_rows(conn: sqlite3.Connection, fixture_id: int, rows: List[Dict[str, Any]]) -> None:
    conn.execute("DELETE FROM player_match_stats WHERE fixture_id = ?", (fixture_id,))
    if not rows:
        return
    normalized_rows: List[Dict[str, Any]] = []
    for row in rows:
        normalized = dict(row)
        normalized["fixture_id"] = normalized.pop("fixture_id", normalized.pop("game_id", fixture_id))
        normalized_rows.append(normalized)
    conn.executemany(
        f"""
        INSERT INTO player_match_stats ({", ".join(PLAYER_STAT_COLUMNS)})
        VALUES ({", ".join(f":{column}" for column in PLAYER_STAT_COLUMNS)})
        """,
        normalized_rows,
    )


def fetch_live_fixtures(client: ApiFootballClient, league_id: int) -> List[Dict[str, Any]]:
    payload = client.get("fixtures", live="all")
    fixtures = payload.get("response", [])
    return [fixture for fixture in fixtures if _safe_int((fixture.get("league") or {}).get("id")) == league_id]


def fetch_live_bundle(client: ApiFootballClient, fixture_id: int) -> Dict[str, Any]:
    return {
        "events": client.get("fixtures/events", fixture=fixture_id).get("response", []),
        "lineups": client.get("fixtures/lineups", fixture=fixture_id).get("response", []),
        "statistics": client.get("fixtures/statistics", fixture=fixture_id).get("response", []),
    }


def sync_finished(cfg: RealtimeConfig) -> Dict[str, int]:
    client = ApiFootballClient(
        api_key=cfg.api_key,
        base_url=cfg.base_url,
        timeout=cfg.timeout,
        cache_dir=cfg.cache_dir,
    )
    fixtures = fetch_fixtures(client, cfg.league_id, cfg.season)
    if cfg.fixture_limit is not None:
        fixtures = fixtures[:cfg.fixture_limit]

    with _connect(cfg.db_path) as conn:
        ensure_schema(conn)
        loaded = 0
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
            player_rows = _player_match_rows(fixture, bundle["players"])
            upsert_fixture(conn, _fixture_record(fixture, bundle["statistics"], is_live=False))
            replace_stints(conn, fixture_id, stints, is_live=False)
            replace_player_rows(conn, fixture_id, player_rows)
            loaded += 1
        conn.commit()
    return {"fixtures": loaded}


def sync_live(cfg: RealtimeConfig) -> Dict[str, int]:
    client = ApiFootballClient(
        api_key=cfg.api_key,
        base_url=cfg.base_url,
        timeout=cfg.timeout,
        cache_dir=cfg.cache_dir,
    )
    fixtures = fetch_live_fixtures(client, cfg.league_id)
    with _connect(cfg.db_path) as conn:
        ensure_schema(conn)
        live_ids = set()
        synced = 0
        for fixture in fixtures:
            fixture_id = _safe_int((fixture.get("fixture") or {}).get("id"))
            if fixture_id is None:
                continue
            live_ids.add(fixture_id)
            bundle = fetch_live_bundle(client, fixture_id)
            stints = _build_stints_for_fixture(
                fixture,
                statistics=bundle["statistics"],
                events=bundle["events"],
                lineups=bundle["lineups"],
                player_blocks=[],
                min_stint_minutes=cfg.min_stint_minutes,
            )
            upsert_fixture(conn, _fixture_record(fixture, bundle["statistics"], is_live=True))
            if stints:
                replace_stints(conn, fixture_id, stints, is_live=True)
            synced += 1

        if live_ids:
            placeholders = ",".join("?" for _ in live_ids)
            conn.execute(
                f"UPDATE fixtures SET is_live = 0 WHERE league_id = ? AND fixture_id NOT IN ({placeholders})",
                (cfg.league_id, *sorted(live_ids)),
            )
        else:
            conn.execute("UPDATE fixtures SET is_live = 0 WHERE league_id = ?", (cfg.league_id,))
        conn.commit()
    return {"live_fixtures": synced}


def _load_stints(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT fixture_id, start_minute, end_minute, duration, home_team_id, away_team_id,
               home_players_json, away_players_json, xg_diff, score_diff, red_home, red_away
        FROM stints
        ORDER BY fixture_id, stint_index
        """
    ).fetchall()
    stints: List[Dict[str, Any]] = []
    for row in rows:
        stints.append(
            {
                "game_id": row["fixture_id"],
                "start": row["start_minute"],
                "end": row["end_minute"],
                "duration": row["duration"],
                "home_team_id": row["home_team_id"],
                "away_team_id": row["away_team_id"],
                "home_players": json.loads(row["home_players_json"]),
                "away_players": json.loads(row["away_players_json"]),
                "xg_diff": row["xg_diff"],
                "score_diff": row["score_diff"],
                "red_home": row["red_home"],
                "red_away": row["red_away"],
            }
        )
    return stints


def _load_player_rows(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"SELECT {', '.join(PLAYER_STAT_COLUMNS)} FROM player_match_stats"
    return pd.read_sql_query(query, conn)


def recompute_metrics(cfg: RealtimeConfig) -> Dict[str, Any]:
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    with _connect(cfg.db_path) as conn:
        ensure_schema(conn)
        stints = _load_stints(conn)
        if not stints:
            raise RuntimeError("No stints are stored yet. Run sync-finished or sync-live first.")
        player_rows = _load_player_rows(conn)
        if player_rows.empty:
            raise RuntimeError("No player match stats are stored yet. Run sync-finished first.")
        metrics_df = compute_metrics(stints, player_rows, DEFAULT_ALPHA_GRID, cfg.min_minutes_spm)
        fixtures_df = pd.read_sql_query("SELECT * FROM fixtures ORDER BY date", conn)

    metrics_path = cfg.output_dir / "metrics_current.csv"
    fixtures_path = cfg.output_dir / "fixtures_current.csv"
    metrics_df.to_csv(metrics_path, index=False)
    fixtures_df.to_csv(fixtures_path, index=False)

    summary = {
        "metrics_path": str(metrics_path),
        "fixtures_path": str(fixtures_path),
        "players_scored": int(metrics_df["player_id"].nunique()) if not metrics_df.empty else 0,
        "live_fixtures": int(fixtures_df["is_live"].sum()) if not fixtures_df.empty else 0,
        "top_players": metrics_df[["player_id", "player", "rapm", "spm", "bpm", "epm"]].head(10).to_dict("records"),
        "updated_at": _utc_now(),
    }
    (cfg.output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def loop_live(cfg: RealtimeConfig, *, backfill_first: bool) -> None:
    if backfill_first:
        backfill = sync_finished(cfg)
        print(json.dumps({"action": "sync-finished", **backfill}, ensure_ascii=False))
    while True:
        live = sync_live(cfg)
        summary = recompute_metrics(cfg)
        print(json.dumps({"action": "live-cycle", **live, **summary}, ensure_ascii=False))
        time.sleep(cfg.poll_interval)


def parse_args() -> Tuple[str, RealtimeConfig, argparse.Namespace]:
    _load_env()
    parser = argparse.ArgumentParser(description="Real-time API-Football plus-minus pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command in ("sync-finished", "sync-live", "recompute", "loop-live"):
        sub = subparsers.add_parser(command)
        sub.add_argument("--league-id", type=int, default=int(os.getenv("API_FOOTBALL_LEAGUE_ID", str(DEFAULT_LEAGUE_ID))))
        sub.add_argument("--season", type=int, default=DEFAULT_SEASON)
        sub.add_argument("--db-path", default="data/realtime_plus_minus/plusminus.sqlite")
        sub.add_argument("--cache-dir", default="data/realtime_plus_minus/cache")
        sub.add_argument("--output-dir", default="data/realtime_plus_minus/out")
        sub.add_argument("--timeout", type=int, default=30)
        sub.add_argument("--min-stint-minutes", type=float, default=1.0)
        sub.add_argument("--min-minutes-spm", type=int, default=300)
        sub.add_argument("--fixture-limit", type=int, default=12)
        sub.add_argument("--poll-interval", type=int, default=600)

    loop_parser = subparsers.choices["loop-live"]
    loop_parser.add_argument("--backfill-first", action="store_true")

    args = parser.parse_args()
    api_key = os.getenv("API_FOOTBALL_KEY")
    if not api_key:
        raise ValueError("Missing API_FOOTBALL_KEY in the environment.")

    fixture_limit = args.fixture_limit
    if fixture_limit is not None and fixture_limit <= 0:
        fixture_limit = None

    cfg = RealtimeConfig(
        api_key=api_key,
        base_url=os.getenv("API_FOOTBALL_BASE_URL", DEFAULT_BASE_URL),
        league_id=args.league_id,
        season=args.season,
        db_path=Path(args.db_path),
        cache_dir=Path(args.cache_dir),
        output_dir=Path(args.output_dir),
        timeout=args.timeout,
        min_stint_minutes=args.min_stint_minutes,
        min_minutes_spm=args.min_minutes_spm,
        fixture_limit=fixture_limit,
        poll_interval=args.poll_interval,
    )
    return args.command, cfg, args


def main() -> None:
    command, cfg, args = parse_args()
    if command == "sync-finished":
        print(json.dumps(sync_finished(cfg), ensure_ascii=False, indent=2))
    elif command == "sync-live":
        print(json.dumps(sync_live(cfg), ensure_ascii=False, indent=2))
    elif command == "recompute":
        print(json.dumps(recompute_metrics(cfg), ensure_ascii=False, indent=2))
    elif command == "loop-live":
        loop_live(cfg, backfill_first=args.backfill_first)


if __name__ == "__main__":
    main()
