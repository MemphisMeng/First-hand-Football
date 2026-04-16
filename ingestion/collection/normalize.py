from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Iterable, List, Optional, Tuple


MATCH_ID_KEYS = ("match_id", "matchId", "matchID", "game_id", "gameId", "fixture_id", "fixtureId", "id")
TEAM_ID_KEYS = ("team_id", "teamId", "team", "teamID", "club_id", "clubId")
PLAYER_ID_KEYS = ("player_id", "playerId", "player", "playerID")
EVENT_TYPE_KEYS = ("event_type", "eventType", "type", "event", "action")
MINUTE_KEYS = ("minute", "time", "timer", "elapsed", "clock")
XG_KEYS = ("xg", "expected_goals", "expectedGoals", "xG")


def _first_key(d: Dict[str, Any], keys: Iterable[str]) -> Optional[Any]:
    for key in keys:
        if key in d and d[key] is not None:
            return d[key]
    return None


def _parse_minute(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    # Handle formats like "45+2"
    if "+" in text:
        base, extra = text.split("+", 1)
        if base.isdigit() and extra.isdigit():
            return int(base) + int(extra)
    digits = "".join(ch for ch in text if ch.isdigit())
    return int(digits) if digits else None


def _parse_score(value: Any) -> Tuple[Optional[int], Optional[int]]:
    if value is None:
        return None, None
    if isinstance(value, dict):
        home = value.get("home") or value.get("home_score") or value.get("homeScore")
        away = value.get("away") or value.get("away_score") or value.get("awayScore")
        return _safe_int(home), _safe_int(away)
    text = str(value)
    if "-" in text:
        left, right = text.split("-", 1)
        return _safe_int(left), _safe_int(right)
    return None, None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except (ValueError, TypeError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (ValueError, TypeError):
        return None


def _find_candidate_lists(payload: Any) -> List[List[Dict[str, Any]]]:
    candidates: List[List[Dict[str, Any]]] = []
    if isinstance(payload, list) and payload and all(isinstance(x, dict) for x in payload):
        candidates.append(payload)
        return candidates
    if isinstance(payload, dict):
        for key in ("matches", "fixtures", "events", "live", "result", "data", "response"):
            value = payload.get(key)
            if isinstance(value, list) and value and all(isinstance(x, dict) for x in value):
                candidates.append(value)
        # Fallback: scan one level deep
        for value in payload.values():
            if isinstance(value, list) and value and all(isinstance(x, dict) for x in value):
                candidates.append(value)
    return candidates


def normalize_payload(payload: Any, source: str = "rapidapi/free-api-live-football-data") -> List[Dict[str, Any]]:
    if payload is None:
        return []

    base = payload.get("result") if isinstance(payload, dict) and "result" in payload else payload
    lists = _find_candidate_lists(base)
    if not lists:
        return []

    ingested_at = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    events: List[Dict[str, Any]] = []

    for items in lists:
        for item in items:
            if not isinstance(item, dict):
                continue

            match_id = _first_key(item, MATCH_ID_KEYS)
            home_team_id = item.get("home_team_id") or item.get("homeTeamId") or item.get("home_id")
            away_team_id = item.get("away_team_id") or item.get("awayTeamId") or item.get("away_id")

            embedded_events = item.get("events")
            if isinstance(embedded_events, list) and embedded_events:
                for ev in embedded_events:
                    if not isinstance(ev, dict):
                        continue
                    events.append(
                        _normalize_event(
                            ev,
                            ingested_at=ingested_at,
                            source=source,
                            match_id=match_id,
                            home_team_id=home_team_id,
                            away_team_id=away_team_id,
                            match_context=item,
                        )
                    )
            else:
                events.append(
                    _normalize_event(
                        item,
                        ingested_at=ingested_at,
                        source=source,
                        match_id=match_id,
                        home_team_id=home_team_id,
                        away_team_id=away_team_id,
                        match_context=None,
                    )
                )

    return events


def _normalize_event(
    event: Dict[str, Any],
    *,
    ingested_at: str,
    source: str,
    match_id: Any,
    home_team_id: Any,
    away_team_id: Any,
    match_context: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    event_type = _first_key(event, EVENT_TYPE_KEYS) or "match_snapshot"
    minute = _parse_minute(_first_key(event, MINUTE_KEYS))

    team_id = _first_key(event, TEAM_ID_KEYS)
    player_id = _first_key(event, PLAYER_ID_KEYS)

    xg = _safe_float(_first_key(event, XG_KEYS))

    score_home = None
    score_away = None
    if "score" in event:
        score_home, score_away = _parse_score(event.get("score"))
    if score_home is None and match_context:
        score_home, score_away = _parse_score(
            match_context.get("score")
            or match_context.get("score_full")
            or match_context.get("full_score")
        )

    normalized: Dict[str, Any] = {
        "ingested_at": ingested_at,
        "source": source,
        "match_id": match_id,
        "minute": minute,
        "event_type": str(event_type).lower() if event_type else "match_snapshot",
        "team_id": team_id,
        "player_id": player_id,
        "xg": xg,
        "score_home": score_home,
        "score_away": score_away,
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "player_in_id": event.get("player_in_id") or event.get("playerInId"),
        "player_out_id": event.get("player_out_id") or event.get("playerOutId"),
        "lineup_home": event.get("lineup_home") or event.get("home_lineup"),
        "lineup_away": event.get("lineup_away") or event.get("away_lineup"),
        "raw": event,
    }
    return normalized
