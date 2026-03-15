import argparse
import csv
import datetime as _dt
import json
import os
import random
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Comment


BASE_URL = "https://fbref.com"
MATCH_ID_RE = re.compile(r"/matches/([A-Za-z0-9]{6,})")
PLAYER_ID_RE = re.compile(r"/players/([A-Za-z0-9]{6,})")


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _slugify(value: str) -> str:
    value = _clean_text(value)
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", value)
    value = value.strip("_")
    return value or "table"


def _extract_match_id(url: str) -> Optional[str]:
    if not url:
        return None
    match = MATCH_ID_RE.search(url)
    if match:
        return match.group(1)
    return None


def _extract_player_id_from_link(tag) -> Optional[str]:
    if tag is None:
        return None
    data_id = tag.get("data-append-csv")
    if data_id:
        return data_id
    href = tag.get("href", "")
    match = PLAYER_ID_RE.search(href)
    if match:
        return match.group(1)
    return None


def _parse_minute(raw_text: str) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    text = _clean_text(raw_text)
    if not text:
        return None, None, None
    match = re.search(r"(\d+)(?:\+(\d+))?", text)
    if not match:
        return None, None, text
    base = int(match.group(1))
    extra = int(match.group(2)) if match.group(2) else 0
    return base, extra, match.group(0)


def _make_soup(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")


def _looks_like_cloudflare(html: Optional[str]) -> bool:
    if not html:
        return False
    markers = [
        "Just a moment...",
        "cf-browser-verification",
        "/cdn-cgi/",
        "Attention Required! | Cloudflare",
    ]
    return any(marker in html for marker in markers)


class FetchError(RuntimeError):
    def __init__(
        self,
        message: str,
        url: Optional[str] = None,
        status: Optional[int] = None,
        body: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.url = url
        self.status = status
        self.body = body


class FBrefScraper:
    def __init__(
        self,
        base_url: str = BASE_URL,
        out_root: str = "fbref_data",
        sleep: float = 2.5,
        jitter: float = 0.75,
        timeout: float = 30.0,
        max_retries: int = 3,
        force: bool = False,
        verbose: bool = False,
        fetch_mode: str = "auto",
        playwright_headful: bool = False,
        playwright_wait: float = 3.0,
        playwright_storage_state: Optional[str] = None,
        playwright_save_state: Optional[str] = None,
        playwright_pause: bool = False,
        user_agent: Optional[str] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.out_root = Path(out_root)
        self.sleep = sleep
        self.jitter = jitter
        self.timeout = timeout
        self.max_retries = max_retries
        self.force = force
        self.verbose = verbose
        self.fetch_mode = fetch_mode.lower()
        self.playwright_headful = playwright_headful
        self.playwright_wait = playwright_wait
        self.playwright_storage_state = playwright_storage_state
        self.playwright_save_state = playwright_save_state
        self.playwright_pause = playwright_pause
        self.session = requests.Session()
        self.user_agents = [
            user_agent
            or "Mozilla/5.0 (compatible; FBrefScraper/1.0; +https://fbref.com)",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
            "(KHTML, like Gecko) Version/17.6 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        ]
        self._ua_index = 0
        self.session.headers.update(
            {
                "User-Agent": self.user_agents[self._ua_index],
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": f"{self.base_url}/",
                "Connection": "keep-alive",
            }
        )

    def _log(self, message: str) -> None:
        if self.verbose:
            print(f"[FBrefScraper] {message}")

    def _sleep(self) -> None:
        if self.sleep <= 0:
            return
        time.sleep(self.sleep + random.random() * self.jitter)

    def _sleep_for(self, seconds: float) -> None:
        if seconds <= 0:
            return
        time.sleep(seconds)

    def _backoff_seconds(self, attempt: int, retry_after: Optional[str] = None) -> float:
        if retry_after:
            try:
                return float(retry_after) + random.random() * self.jitter
            except ValueError:
                pass
        base = self.sleep if self.sleep > 0 else 1.0
        return base * (1.6 ** (attempt - 1)) + random.random() * self.jitter

    def _rotate_user_agent(self) -> None:
        if not self.user_agents:
            return
        self._ua_index = (self._ua_index + 1) % len(self.user_agents)
        self.session.headers["User-Agent"] = self.user_agents[self._ua_index]

    def _playwright_available(self) -> bool:
        try:
            import playwright.sync_api  # noqa: F401
        except Exception:
            return False
        return True

    def _get_playwright(self, url: str) -> str:
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:
            raise RuntimeError(
                "Playwright is not installed. Run: pip install playwright && playwright install"
            ) from exc

        self._log("Using Playwright for fetch")
        timeout_ms = int(self.timeout * 1000)
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=not self.playwright_headful)
            context_kwargs: Dict[str, Any] = {
                "user_agent": self.session.headers.get("User-Agent")
            }
            if self.playwright_storage_state:
                storage_path = Path(self.playwright_storage_state)
                if storage_path.exists():
                    context_kwargs["storage_state"] = str(storage_path)
                else:
                    self._log(f"Playwright storage state not found: {storage_path}")

            context = browser.new_context(**context_kwargs)
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            if self.playwright_wait and self.playwright_wait > 0:
                page.wait_for_timeout(int(self.playwright_wait * 1000))
            if self.playwright_pause:
                self._log("Playwright pause enabled. Solve any challenge in the browser.")
                input("Press Enter after the page loads and any challenge is solved...")
            html = page.content()
            if self.playwright_save_state:
                context.storage_state(path=self.playwright_save_state)
            context.close()
            browser.close()

        if _looks_like_cloudflare(html):
            raise RuntimeError(
                "Playwright returned a Cloudflare challenge page. Try --playwright-headful "
                "with a longer --playwright-wait, then save cookies with --playwright-save-state."
            )
        return html

    def _get_requests(self, url: str) -> str:
        last_exc = None
        last_body = None
        last_status = None
        for attempt in range(1, self.max_retries + 1):
            try:
                self._log(f"GET {url} (attempt {attempt}/{self.max_retries})")
                response = self.session.get(url, timeout=self.timeout)
                status = response.status_code
                body = response.text
                last_body = body
                last_status = status
                self._log(f"HTTP {status} for {url} (bytes={len(body)})")

                if status in {429, 500, 502, 503, 504}:
                    retry_after = response.headers.get("Retry-After")
                    last_exc = FetchError(f"HTTP {status} for {url}", url=url, status=status, body=body)
                    if attempt < self.max_retries:
                        if retry_after:
                            self._log(f"Retry-After header: {retry_after}")
                        self._sleep_for(self._backoff_seconds(attempt, retry_after))
                        continue

                if status == 403 and attempt < self.max_retries:
                    self._rotate_user_agent()
                    self._log("HTTP 403 encountered, rotating User-Agent and retrying")
                    self._sleep_for(self._backoff_seconds(attempt))
                    continue

                if status >= 400:
                    snippet = _clean_text(body[:200])
                    raise FetchError(
                        f"HTTP {status} for {url} ({snippet})", url=url, status=status, body=body
                    )

                if _looks_like_cloudflare(body):
                    raise FetchError(
                        "Cloudflare challenge page returned",
                        url=url,
                        status=status,
                        body=body,
                    )

                return body
            except requests.RequestException as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    self._rotate_user_agent()
                    self._log(f"Request error: {exc}. Rotating User-Agent and retrying.")
                    self._sleep_for(self._backoff_seconds(attempt))
            except Exception as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    self._log(f"Error: {exc}. Retrying.")
                    self._sleep()
        raise FetchError(
            f"Failed to fetch {url}", url=url, status=last_status, body=last_body
        ) from last_exc

    def _get(self, url: str) -> str:
        mode = self.fetch_mode
        if mode not in {"auto", "requests", "playwright"}:
            mode = "auto"

        if mode == "requests":
            return self._get_requests(url)
        if mode == "playwright":
            return self._get_playwright(url)

        try:
            return self._get_requests(url)
        except FetchError as exc:
            if (
                exc.status in {403, 429, 503}
                or _looks_like_cloudflare(exc.body)
                or "Cloudflare" in str(exc)
            ):
                if self._playwright_available():
                    self._log("Cloudflare detected, falling back to Playwright.")
                    return self._get_playwright(url)
                raise RuntimeError(
                    "Cloudflare challenge detected. Install Playwright and retry with "
                    "--fetch-mode playwright or --fetch-mode auto."
                ) from exc
            raise

    def _soups_with_comments(self, soup: BeautifulSoup) -> List[BeautifulSoup]:
        soups = [soup]
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            if "<table" in comment or "data-stat" in comment:
                soups.append(_make_soup(comment))
        return soups

    def _absolute_url(self, href: str) -> str:
        if not href:
            return ""
        if href.startswith("http://") or href.startswith("https://"):
            return href
        return urljoin(self.base_url, href)

    def iter_schedule_matches(
        self, schedule_url: str, only_reports: bool = True
    ) -> List[Dict[str, Any]]:
        html = self._get(schedule_url)
        soup = _make_soup(html)
        soups = self._soups_with_comments(soup)
        matches: List[Dict[str, Any]] = []
        seen: set = set()

        def cell_text(row, data_stat: str) -> Optional[str]:
            cell = row.find(["th", "td"], {"data-stat": data_stat})
            if not cell:
                return None
            return _clean_text(cell.get_text(" ", strip=True))

        for soup_part in soups:
            for row in soup_part.select("table tbody tr"):
                row_classes = row.get("class", [])
                if "spacer" in row_classes:
                    continue
                report_url = None
                report_cell = row.find("td", {"data-stat": "match_report"})
                if report_cell:
                    report_link = report_cell.find("a")
                    if report_link and report_link.get("href"):
                        report_url = self._absolute_url(report_link["href"])
                    elif _clean_text(report_cell.get_text()) == "Match Report":
                        report_url = None

                if not report_url:
                    for link in row.select("a[href*='/matches/']"):
                        link_text = _clean_text(link.get_text()).lower()
                        if only_reports and link_text != "match report":
                            continue
                        report_url = self._absolute_url(link.get("href", ""))
                        break

                if only_reports and not report_url:
                    continue
                if report_url in seen:
                    continue
                seen.add(report_url)

                match_id = _extract_match_id(report_url)
                matches.append(
                    {
                        "match_id": match_id,
                        "match_url": report_url,
                        "date": cell_text(row, "date"),
                        "home_team": cell_text(row, "home_team"),
                        "away_team": cell_text(row, "away_team"),
                        "score": cell_text(row, "score"),
                        "venue": cell_text(row, "venue"),
                    }
                )

        return matches

    def _parse_scorebox_meta(self, soup: BeautifulSoup) -> Dict[str, Any]:
        meta: Dict[str, Any] = {}
        scorebox = soup.select_one("div.scorebox")
        if not scorebox:
            return meta

        team_links = scorebox.select("a[href*='/squads/']")
        teams = []
        for link in team_links:
            name = _clean_text(link.get_text(" ", strip=True))
            if name and name not in teams:
                teams.append(name)
        if teams:
            meta["teams"] = teams

        scores = [
            _clean_text(node.get_text(" ", strip=True))
            for node in scorebox.select(".score")
            if _clean_text(node.get_text(" ", strip=True))
        ]
        if scores:
            meta["scores"] = scores

        time_node = scorebox.find("time")
        if time_node and time_node.get("datetime"):
            meta["datetime"] = time_node["datetime"]

        meta_box = scorebox.select_one(".scorebox_meta")
        if meta_box:
            meta_text = _clean_text(meta_box.get_text(" ", strip=True))
            if meta_text:
                meta["summary"] = meta_text

        return meta

    def _parse_lineups(self, soups: Iterable[BeautifulSoup]) -> List[Dict[str, Any]]:
        lineups: List[Dict[str, Any]] = []
        for soup in soups:
            for table in soup.find_all("table"):
                table_id = table.get("id", "")
                caption = table.find("caption")
                caption_text = _clean_text(caption.get_text(" ", strip=True)) if caption else ""
                if "lineup" not in table_id.lower() and "lineup" not in caption_text.lower():
                    continue

                team_from_caption = None
                if caption_text:
                    team_from_caption = re.sub(r"\blineups?\b", "", caption_text, flags=re.I)
                    team_from_caption = _clean_text(team_from_caption)

                in_bench_section = False
                for row in table.select("tbody tr"):
                    if "class" in row.attrs and "thead" in row.get("class", []):
                        header_text = _clean_text(row.get_text(" ", strip=True)).lower()
                        if "bench" in header_text or "sub" in header_text:
                            in_bench_section = True
                        elif "starter" in header_text or "lineup" in header_text:
                            in_bench_section = False
                        continue
                    if in_bench_section:
                        continue

                    player_cell = row.find(["th", "td"], {"data-stat": "player"})
                    if not player_cell:
                        continue
                    player_link = player_cell.find("a", href=re.compile(r"/players/"))
                    player_name = _clean_text(player_cell.get_text(" ", strip=True))
                    player_id = _extract_player_id_from_link(player_link) if player_link else None

                    pos_cell = row.find(["th", "td"], {"data-stat": "pos"})
                    position = _clean_text(pos_cell.get_text(" ", strip=True)) if pos_cell else None

                    team = table.get("data-team") or team_from_caption

                    lineups.append(
                        {
                            "team": team,
                            "player_name": player_name,
                            "player_id": player_id,
                            "position": position,
                            "is_starter": True,
                            "source": table_id or caption_text or "lineup_table",
                        }
                    )
        return lineups

    def _parse_events(self, soups: Iterable[BeautifulSoup]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        seen = set()

        def record_event(payload: Dict[str, Any]) -> None:
            signature = (
                payload.get("minute_raw"),
                payload.get("event_type"),
                payload.get("player_id"),
                payload.get("assist_id"),
                payload.get("player_in_id"),
                payload.get("player_out_id"),
                payload.get("team"),
                payload.get("raw_text"),
            )
            if signature in seen:
                return
            seen.add(signature)
            events.append(payload)

        for soup in soups:
            containers = soup.select("#events, #match_summary, .match-summary, .event")
            if not containers:
                containers = soup.find_all("div", class_=re.compile(r"event|summary", re.I))

            for container in containers:
                event_nodes = container.select(".event, tr, li, div")
                if container in event_nodes:
                    event_nodes.remove(container)
                if not event_nodes:
                    event_nodes = [container]

                for node in event_nodes:
                    text = _clean_text(node.get_text(" ", strip=True))
                    if not text:
                        continue

                    minute_text = ""
                    minute_node = node.find(
                        lambda tag: tag.name in ("span", "div", "td")
                        and tag.get("class")
                        and any("min" in cls for cls in tag.get("class"))
                    )
                    if minute_node:
                        minute_text = minute_node.get_text(" ", strip=True)
                    else:
                        minute_match = re.match(r"^\d+\+?\d*", text)
                        if minute_match:
                            minute_text = minute_match.group(0)

                    minute, stoppage, minute_raw = _parse_minute(minute_text)

                    event_type = None
                    icon = node.find("img")
                    if icon:
                        icon_text = _clean_text(icon.get("alt") or icon.get("title") or "")
                        icon_text = icon_text.lower()
                        if "goal" in icon_text:
                            event_type = "goal"
                        elif "yellow" in icon_text:
                            event_type = "yellow_card"
                        elif "red" in icon_text:
                            event_type = "red_card"
                        elif "sub" in icon_text:
                            event_type = "substitution"

                    if not event_type:
                        lower_text = text.lower()
                        if "goal" in lower_text:
                            event_type = "goal"
                        elif "yellow" in lower_text:
                            event_type = "yellow_card"
                        elif "red" in lower_text:
                            event_type = "red_card"
                        elif "substitution" in lower_text or "subbed" in lower_text:
                            event_type = "substitution"

                    player_links = node.find_all("a", href=re.compile(r"/players/"))
                    players = []
                    for link in player_links:
                        name = _clean_text(link.get_text(" ", strip=True))
                        player_id = _extract_player_id_from_link(link)
                        if name or player_id:
                            players.append({"player_name": name, "player_id": player_id})

                    assist_name = None
                    assist_id = None
                    player_name = None
                    player_id = None
                    player_in_name = None
                    player_in_id = None
                    player_out_name = None
                    player_out_id = None

                    if players:
                        player_name = players[0].get("player_name")
                        player_id = players[0].get("player_id")

                    if event_type == "goal" and len(players) > 1:
                        assist_name = players[1].get("player_name")
                        assist_id = players[1].get("player_id")

                    if event_type == "substitution" and len(players) >= 2:
                        player_in_name = players[0].get("player_name")
                        player_in_id = players[0].get("player_id")
                        player_out_name = players[1].get("player_name")
                        player_out_id = players[1].get("player_id")

                    for in_node in node.select(".player_in a, .event-player-in a"):
                        player_in_name = _clean_text(in_node.get_text(" ", strip=True))
                        player_in_id = _extract_player_id_from_link(in_node)
                    for out_node in node.select(".player_out a, .event-player-out a"):
                        player_out_name = _clean_text(out_node.get_text(" ", strip=True))
                        player_out_id = _extract_player_id_from_link(out_node)

                    team = None
                    team_node = node.find(
                        lambda tag: tag.name in ("div", "span", "td")
                        and tag.get("class")
                        and any(cls in ("team", "teamname") for cls in tag.get("class"))
                    )
                    if team_node:
                        team = _clean_text(team_node.get_text(" ", strip=True))

                    record_event(
                        {
                            "minute": minute,
                            "stoppage_minute": stoppage,
                            "minute_raw": minute_raw,
                            "event_type": event_type,
                            "team": team,
                            "player_name": player_name,
                            "player_id": player_id,
                            "assist_name": assist_name,
                            "assist_id": assist_id,
                            "player_in_name": player_in_name,
                            "player_in_id": player_in_id,
                            "player_out_name": player_out_name,
                            "player_out_id": player_out_id,
                            "raw_text": text,
                        }
                    )

        return events

    def _parse_table(self, table) -> Dict[str, Any]:
        table_id = table.get("id") or ""
        caption = table.find("caption")
        caption_text = _clean_text(caption.get_text(" ", strip=True)) if caption else ""
        team = table.get("data-team")

        rows: List[Dict[str, Any]] = []
        for row in table.select("tbody tr"):
            if "class" in row.attrs and "thead" in row.get("class", []):
                continue
            row_data: Dict[str, Any] = {}
            for cell in row.find_all(["th", "td"]):
                stat = cell.get("data-stat")
                if not stat:
                    stat = _slugify(_clean_text(cell.get("aria-label") or "")) or "col"
                text = _clean_text(cell.get_text(" ", strip=True))
                row_data[stat] = text
                if stat == "player":
                    player_link = cell.find("a", href=re.compile(r"/players/"))
                    if player_link:
                        row_data["player_id"] = _extract_player_id_from_link(player_link)
                        row_data["player_name"] = _clean_text(
                            player_link.get_text(" ", strip=True)
                        )
            if row_data:
                rows.append(row_data)

        return {
            "table_id": table_id or None,
            "caption": caption_text or None,
            "team": team or None,
            "rows": rows,
        }

    def _collect_tables(self, soups: Iterable[BeautifulSoup]) -> List[Dict[str, Any]]:
        tables: List[Dict[str, Any]] = []
        seen: set = set()

        for soup in soups:
            for table in soup.find_all("table"):
                table_id = table.get("id") or ""
                caption = table.find("caption")
                caption_text = _clean_text(caption.get_text(" ", strip=True)) if caption else ""
                key = table_id or caption_text
                if not key:
                    key = f"table_{len(tables) + 1}"
                if key in seen:
                    continue
                seen.add(key)
                parsed = self._parse_table(table)
                if parsed["rows"]:
                    tables.append(parsed)
        return tables

    def _derive_players(
        self, tables: List[Dict[str, Any]], starters: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        players: Dict[Tuple[str, str], Dict[str, Any]] = {}
        starter_ids = {p.get("player_id") for p in starters if p.get("player_id")}

        for table in tables:
            team = table.get("team")
            for row in table.get("rows", []):
                player_name = row.get("player_name") or row.get("player")
                player_id = row.get("player_id")
                if not player_name:
                    continue
                minutes = row.get("min") or row.get("minutes")
                if minutes is not None:
                    try:
                        minutes_val = int(minutes)
                    except ValueError:
                        minutes_val = None
                    if minutes_val == 0:
                        continue

                key = (player_id or player_name, team or "")
                if key not in players:
                    players[key] = {
                        "player_name": player_name,
                        "player_id": player_id,
                        "team": team,
                        "is_starter": player_id in starter_ids if player_id else False,
                    }

        for starter in starters:
            key = (starter.get("player_id") or starter.get("player_name"), starter.get("team") or "")
            if key not in players:
                players[key] = {
                    "player_name": starter.get("player_name"),
                    "player_id": starter.get("player_id"),
                    "team": starter.get("team"),
                    "is_starter": True,
                }
            else:
                players[key]["is_starter"] = True

        return list(players.values())

    def _write_csv(
        self, path: Path, rows: List[Dict[str, Any]], field_order: Optional[List[str]] = None
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not rows:
            if field_order:
                with path.open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=field_order)
                    writer.writeheader()
            return
        fieldnames = field_order or []
        for row in rows:
            for key in row.keys():
                if key not in fieldnames:
                    fieldnames.append(key)
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def scrape_match(
        self, match_url: str, schedule_meta: Optional[Dict[str, Any]] = None
    ) -> Optional[Path]:
        match_id = _extract_match_id(match_url)
        if not match_id:
            parsed = urlparse(match_url)
            match_id = _slugify(parsed.path)
        match_dir = self.out_root / match_id
        if match_dir.exists() and not self.force:
            return match_dir

        html = self._get(match_url)
        soup = _make_soup(html)
        soups = self._soups_with_comments(soup)

        meta = {
            "match_id": match_id,
            "match_url": match_url,
            "fetched_at": _dt.datetime.utcnow().isoformat() + "Z",
        }
        if schedule_meta:
            meta.update({k: v for k, v in schedule_meta.items() if v is not None})
        meta.update(self._parse_scorebox_meta(soup))

        starters = self._parse_lineups(soups)
        events = self._parse_events(soups)
        tables = self._collect_tables(soups)
        players = self._derive_players(tables, starters)

        match_dir.mkdir(parents=True, exist_ok=True)
        with (match_dir / "meta.json").open("w", encoding="utf-8") as handle:
            json.dump(meta, handle, ensure_ascii=False, indent=2)

        self._write_csv(
            match_dir / "players.csv",
            players,
            field_order=["player_name", "player_id", "team", "is_starter"],
        )
        self._write_csv(
            match_dir / "starters.csv",
            starters,
            field_order=["player_name", "player_id", "team", "position", "is_starter", "source"],
        )
        self._write_csv(
            match_dir / "events.csv",
            events,
            field_order=[
                "minute",
                "stoppage_minute",
                "minute_raw",
                "event_type",
                "team",
                "player_name",
                "player_id",
                "assist_name",
                "assist_id",
                "player_in_name",
                "player_in_id",
                "player_out_name",
                "player_out_id",
                "raw_text",
            ],
        )

        tables_dir = match_dir / "tables"
        tables_dir.mkdir(parents=True, exist_ok=True)
        index_entries = []
        for idx, table in enumerate(tables, start=1):
            table_id = table.get("table_id") or f"table_{idx}"
            file_name = _slugify(table_id or f"table_{idx}") + ".csv"
            file_path = tables_dir / file_name
            self._write_csv(file_path, table.get("rows", []))
            index_entries.append(
                {
                    "table_id": table.get("table_id"),
                    "caption": table.get("caption"),
                    "team": table.get("team"),
                    "csv_file": str(file_path.relative_to(match_dir)),
                    "row_count": len(table.get("rows", [])),
                }
            )
        with (tables_dir / "index.json").open("w", encoding="utf-8") as handle:
            json.dump(index_entries, handle, ensure_ascii=False, indent=2)

        return match_dir

    def scrape_season(
        self,
        schedule_url: str,
        limit: Optional[int] = None,
        only_reports: bool = True,
    ) -> List[Path]:
        matches = self.iter_schedule_matches(schedule_url, only_reports=only_reports)
        if limit:
            matches = matches[:limit]
        results: List[Path] = []
        for match in matches:
            match_url = match.get("match_url")
            if not match_url:
                continue
            match_dir = self.scrape_match(match_url, schedule_meta=match)
            if match_dir:
                results.append(match_dir)
            self._sleep()
        return results


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="FBref match report scraper")
    parser.add_argument(
        "--schedule-url",
        default="https://fbref.com/en/comps/9/2024-2025/schedule",
        help="Schedule page URL",
    )
    parser.add_argument(
        "--out",
        default="fbref_data/2024-2025_epl",
        help="Output directory",
    )
    parser.add_argument("--limit", type=int, default=None, help="Limit number of matches")
    parser.add_argument("--sleep", type=float, default=2.5, help="Sleep between requests")
    parser.add_argument("--timeout", type=float, default=30.0, help="Request timeout")
    parser.add_argument("--force", action="store_true", help="Overwrite existing match dirs")
    parser.add_argument("--include-previews", action="store_true", help="Include preview links")
    parser.add_argument("--verbose", action="store_true", help="Verbose HTTP logging")
    parser.add_argument(
        "--fetch-mode",
        choices=["auto", "requests", "playwright"],
        default="auto",
        help="Fetch mode: requests, playwright, or auto fallback",
    )
    parser.add_argument(
        "--playwright-headful",
        action="store_true",
        help="Run Playwright in headed mode (useful for manual challenges)",
    )
    parser.add_argument(
        "--playwright-wait",
        type=float,
        default=3.0,
        help="Seconds to wait after page load when using Playwright",
    )
    parser.add_argument(
        "--playwright-storage-state",
        default=None,
        help="Path to a Playwright storage state JSON file to load",
    )
    parser.add_argument(
        "--playwright-save-state",
        default=None,
        help="Path to save Playwright storage state JSON",
    )
    parser.add_argument(
        "--playwright-pause",
        action="store_true",
        help="Pause after page load to allow manual challenge solving",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    scraper = FBrefScraper(
        out_root=args.out,
        sleep=args.sleep,
        timeout=args.timeout,
        force=args.force,
        verbose=args.verbose,
        fetch_mode=args.fetch_mode,
        playwright_headful=args.playwright_headful,
        playwright_wait=args.playwright_wait,
        playwright_storage_state=args.playwright_storage_state,
        playwright_save_state=args.playwright_save_state,
        playwright_pause=args.playwright_pause,
    )
    scraper.scrape_season(
        schedule_url=args.schedule_url,
        limit=args.limit,
        only_reports=not args.include_previews,
    )


if __name__ == "__main__":
    main()
