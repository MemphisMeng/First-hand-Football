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
