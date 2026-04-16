from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - optional convenience dependency
    def load_dotenv(*_args: Any, **_kwargs: Any) -> bool:
        return False


DEFAULT_BASE_URL = "https://v3.football.api-sports.io"
DEFAULT_LEAGUE_ID = 39
DEFAULT_COUNTRY = "England"
DEFAULT_LEAGUE_NAME = "Premier League"


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


class ApiFootballClient:
    def __init__(self, api_key: str, base_url: str = DEFAULT_BASE_URL, timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "x-apisports-key": api_key,
            }
        )

    def get(self, path: str, **params: Any) -> Dict[str, Any]:
        response = self.session.get(
            f"{self.base_url}/{path.lstrip('/')}",
            params={k: v for k, v in params.items() if v is not None},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload: Dict[str, Any] = response.json()
        return payload


def _extract_league_record(payload: Dict[str, Any], league_id: int) -> Dict[str, Any]:
    for item in payload.get("response", []):
        league = item.get("league", {})
        if league.get("id") == league_id:
            return item
    raise RuntimeError(f"League id {league_id} was not found in the API response.")


def _current_or_latest_season(league_record: Dict[str, Any]) -> int:
    seasons = league_record.get("seasons", [])
    if not seasons:
        raise RuntimeError("No seasons were returned for the requested league.")
    for season in seasons:
        if season.get("current"):
            return int(season["year"])
    return int(seasons[-1]["year"])


def _season_record(league_record: Dict[str, Any], season_year: int) -> Dict[str, Any]:
    for season in league_record.get("seasons", []):
        if int(season.get("year")) == season_year:
            return season
    raise RuntimeError(f"Season {season_year} was not found for the requested league.")


def _iter_candidate_seasons(current_season: int) -> Iterable[int]:
    yield current_season
    for offset in range(1, 4):
        yield current_season - offset


def _pick_fixture(client: ApiFootballClient, league_id: int, current_season: int) -> Dict[str, Any]:
    for season in _iter_candidate_seasons(current_season):
        payload = client.get("fixtures", league=league_id, season=season, round="Regular Season - 1")
        if payload.get("response"):
            fixture = payload["response"][0]
            fixture["season_used"] = season
            return fixture
    raise RuntimeError("No fixture was found for the requested league in the checked seasons.")


def _statistics_to_map(statistics_response: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    mapped: Dict[str, Dict[str, Any]] = {}
    for entry in statistics_response:
        team_name = entry.get("team", {}).get("name", "Unknown")
        mapped[team_name] = {stat["type"]: stat["value"] for stat in entry.get("statistics", [])}
    return mapped


def _event_has_player_context(event: Dict[str, Any]) -> bool:
    player = event.get("player") or {}
    return bool(player.get("id"))


def build_probe_summary(client: ApiFootballClient, league_id: int) -> Dict[str, Any]:
    leagues_payload = client.get("leagues", id=league_id)
    league_record = _extract_league_record(leagues_payload, league_id)
    season_year = _current_or_latest_season(league_record)
    fixture = _pick_fixture(client, league_id, season_year)
    coverage = _season_record(league_record, fixture["season_used"]).get("coverage", {})

    fixture_id = fixture["fixture"]["id"]
    events_payload = client.get("fixtures/events", fixture=fixture_id)
    lineups_payload = client.get("fixtures/lineups", fixture=fixture_id)
    players_payload = client.get("fixtures/players", fixture=fixture_id)
    statistics_payload = client.get("fixtures/statistics", fixture=fixture_id)

    events = events_payload.get("response", [])
    lineups = lineups_payload.get("response", [])
    players = players_payload.get("response", [])
    statistics = statistics_payload.get("response", [])
    statistics_map = _statistics_to_map(statistics)

    team_xg_available = any("expected_goals" in stats for stats in statistics_map.values())
    event_level_xg_available = any("xg" in event or "expected_goals" in event for event in events)

    player_stat_fields: List[str] = []
    if players and players[0].get("players"):
        sample_player = players[0]["players"][0]
        first_stats = (sample_player.get("statistics") or [{}])[0]
        player_stat_fields = sorted(first_stats.keys())

    sample_events = [
        {
            "minute": event.get("time", {}).get("elapsed"),
            "team": event.get("team", {}).get("name"),
            "player_id": event.get("player", {}).get("id"),
            "type": event.get("type"),
            "detail": event.get("detail"),
        }
        for event in events[:5]
    ]

    lineups_ready = all(item.get("startXI") for item in lineups) if lineups else False

    support = {
        "live_fixtures_endpoint": True,
        "events_with_player_ids": bool(events) and all(_event_has_player_context(event) for event in events),
        "lineups_available": lineups_ready,
        "player_match_stats_available": bool(players),
        "team_expected_goals_available": team_xg_available,
        "event_level_xg_available": event_level_xg_available,
    }

    verdict = {
        "supports_live_stint_reconstruction": support["events_with_player_ids"] and support["lineups_available"],
        "supports_batch_player_evaluation": support["player_match_stats_available"],
        "supports_paper_style_xg_rapm_exactly": support["event_level_xg_available"],
        "supports_team_xg_proxy_if_polled_live": support["team_expected_goals_available"],
    }

    return {
        "league": {
            "id": league_record["league"]["id"],
            "name": league_record["league"]["name"],
            "country": league_record["country"]["name"],
            "season_selected": fixture["season_used"],
            "coverage": coverage,
        },
        "fixture": {
            "id": fixture_id,
            "date": fixture["fixture"]["date"],
            "status": fixture["fixture"]["status"]["short"],
            "home": fixture["teams"]["home"]["name"],
            "away": fixture["teams"]["away"]["name"],
        },
        "sample_events": sample_events,
        "player_stat_fields": player_stat_fields,
        "team_statistics": statistics_map,
        "expected_goals_by_team": {
            team_name: stats.get("expected_goals")
            for team_name, stats in statistics_map.items()
            if "expected_goals" in stats
        },
        "support": support,
        "verdict": verdict,
    }


def build_live_probe(client: ApiFootballClient) -> Optional[Dict[str, Any]]:
    live_payload = client.get("fixtures", live="all")
    live_fixtures = live_payload.get("response", [])
    if not live_fixtures:
        return None

    fixture = live_fixtures[0]
    fixture_id = fixture["fixture"]["id"]
    events = client.get("fixtures/events", fixture=fixture_id).get("response", [])
    statistics = client.get("fixtures/statistics", fixture=fixture_id).get("response", [])
    lineups = client.get("fixtures/lineups", fixture=fixture_id).get("response", [])

    statistics_map = _statistics_to_map(statistics)
    expected_goals_present = any("expected_goals" in stats for stats in statistics_map.values())

    return {
        "fixture": {
            "id": fixture_id,
            "league": fixture["league"]["name"],
            "status": fixture["fixture"]["status"],
            "home": fixture["teams"]["home"]["name"],
            "away": fixture["teams"]["away"]["name"],
        },
        "counts": {
            "events": len(events),
            "statistics": len(statistics),
            "lineups": len(lineups),
        },
        "expected_goals_present": expected_goals_present,
    }


def main() -> None:
    _load_env()

    parser = argparse.ArgumentParser(description="Probe API-Football for RAPM-related coverage.")
    parser.add_argument(
        "--league-id",
        type=int,
        default=int(os.getenv("API_FOOTBALL_LEAGUE_ID", str(DEFAULT_LEAGUE_ID))),
        help="League id to inspect. Premier League is 39.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print full JSON instead of the compact summary.",
    )
    parser.add_argument(
        "--check-live",
        action="store_true",
        help="Also inspect one currently live fixture to see what the API is returning in real time.",
    )
    args = parser.parse_args()

    api_key = os.getenv("API_FOOTBALL_KEY")
    if not api_key:
        raise ValueError("Missing API_FOOTBALL_KEY in the environment.")

    base_url = os.getenv("API_FOOTBALL_BASE_URL", DEFAULT_BASE_URL)
    client = ApiFootballClient(api_key=api_key, base_url=base_url)
    summary = build_probe_summary(client, args.league_id)
    if args.check_live:
        summary["live_probe"] = build_live_probe(client)

    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return

    compact = {
        "league": summary["league"],
        "fixture": summary["fixture"],
        "expected_goals_by_team": summary["expected_goals_by_team"],
        "player_stat_fields": summary["player_stat_fields"],
        "support": summary["support"],
        "verdict": summary["verdict"],
        "sample_events": summary["sample_events"][:3],
    }
    if args.check_live:
        compact["live_probe"] = summary.get("live_probe")
    print(json.dumps(compact, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
