# Gemini Plus-Minus

Compute RAPM, SPM, BPM, and EPM from Understat CSV exports.

## Usage

```bash
python gemini_plus_minus/compute.py \
  --data-root "/Users/memphismeng/Google Drive/My Drive/Understat_Data" \
  --leagues ENG ESP FRA GER ITA \
  --seasons 2023_2024 \
  --output-dir data/plus_minus
```

Outputs one file per metric, per league-season:

```
data/plus_minus/ENG_2023_2024_rapm.csv
data/plus_minus/ENG_2023_2024_spm.csv
data/plus_minus/ENG_2023_2024_bpm.csv
data/plus_minus/ENG_2023_2024_epm.csv
```

## API-Football Prototype

This prototype computes approximate RAPM, SPM, BPM, and EPM from API-Football fixture data.
It reconstructs stints from lineups, substitutions, goals, and red cards, then uses team-level
match `expected_goals` as the target signal. Because API-Football does not expose event-level xG,
the script distributes each team's match xG uniformly across the match when building stints.

```bash
.venv/bin/python -m gemini_plus_minus.api_football_compute \
  --league-id 39 \
  --season 2024 \
  --fixture-limit 12 \
  --output-dir data/api_football_plus_minus
```

Outputs:

```
data/api_football_plus_minus/metrics.csv
data/api_football_plus_minus/stints.csv
data/api_football_plus_minus/player_match_stats.csv
data/api_football_plus_minus/fixtures_used.csv
data/api_football_plus_minus/metadata.json
```

The script caches raw endpoint responses under `data/api_football_plus_minus_cache` by default to
reduce API usage on the free tier.

## API-Football Real-Time

This is the simplified real-time path for the project. It keeps a local SQLite store of:

- finished fixtures and player match stats for the season baseline
- live fixture snapshots and reconstructed stints during matches
- current RAPM, SPM, BPM, and EPM outputs written from the local store

The live path is built around one file:

```bash
.venv/bin/python -m gemini_plus_minus.api_football_realtime
```

Seed the store with finished fixtures:

```bash
.venv/bin/python -m gemini_plus_minus.api_football_realtime sync-finished \
  --league-id 39 \
  --season 2024 \
  --fixture-limit 12 \
  --db-path data/realtime_plus_minus/plusminus.sqlite \
  --output-dir data/realtime_plus_minus/out
```

Pull current live fixtures for the league and refresh live stints:

```bash
.venv/bin/python -m gemini_plus_minus.api_football_realtime sync-live \
  --league-id 39 \
  --season 2024 \
  --db-path data/realtime_plus_minus/plusminus.sqlite \
  --output-dir data/realtime_plus_minus/out
```

Recompute the current metrics snapshot from the local store:

```bash
.venv/bin/python -m gemini_plus_minus.api_football_realtime recompute \
  --league-id 39 \
  --season 2024 \
  --db-path data/realtime_plus_minus/plusminus.sqlite \
  --output-dir data/realtime_plus_minus/out
```

Run a simple live loop during match windows:

```bash
.venv/bin/python -m gemini_plus_minus.api_football_realtime loop-live \
  --league-id 39 \
  --season 2024 \
  --db-path data/realtime_plus_minus/plusminus.sqlite \
  --output-dir data/realtime_plus_minus/out \
  --poll-interval 600 \
  --backfill-first
```

Outputs:

```
data/realtime_plus_minus/plusminus.sqlite
data/realtime_plus_minus/out/metrics_current.csv
data/realtime_plus_minus/out/fixtures_current.csv
data/realtime_plus_minus/out/summary.json
```

Notes:

- `sync-finished` is what gives SPM and EPM enough player-stat history to mean anything.
- `sync-live` does not fetch live player-match stats, which keeps API usage lower on the free tier.
- `recompute` combines finished-match baseline data with any currently live stints already stored.
- The xG target is still an approximation because API-Football exposes team-level match `expected_goals`, not event-level xG.

## League Translation Coefficients

Compute league translation coefficients from the EPM outputs:

```bash
python gemini_plus_minus/translate.py \
  --input-dir data/plus_minus \
  --output-dir data/plus_minus \
  --min-minutes 900
```

This writes:

```
data/plus_minus/league_translation.csv
data/plus_minus/gpm.csv
```

If you also want a translated EPM table:

```bash
python gemini_plus_minus/translate.py --write-translated
```

This writes:

```
data/plus_minus/epm_translated.csv
```

You can disable GPM or change scaling:

```bash
python gemini_plus_minus/translate.py --no-gpm
python gemini_plus_minus/translate.py --gpm-quantile 0.90 --gpm-shrink-k 0.5 --gpm-scope league
```

## Player Stability (ARIMA)

Estimate player stability across seasons using ARIMA on translated EPM series:

```bash
python gemini_plus_minus/stability.py \
  --input-dir data/plus_minus \
  --output-file data/plus_minus/player_stability.csv \
  --min-minutes 900 \
  --min-points 4
```

This writes:

```
data/plus_minus/player_stability.csv
```

You can switch the metric and add EWMA-based stability:

```bash
python gemini_plus_minus/stability.py \
  --metric gpm \
  --ewma-span 3
```

## Notes
- Stints are approximated from minutes played and goal timestamps. Understat CSVs do not include exact substitution or red-card timing.
- SPM features use available Understat per-90 stats. Possession-normalization is not applied because possession data is not present.
