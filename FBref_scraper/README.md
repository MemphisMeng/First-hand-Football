To execute the scraper, run
```
source .venv/bin/activate
python -m FBref_scraper.FBrefScraper --schedule-url https://fbref.com/en/comps/9/2024-2025/schedule --out fbref_data/2024-2025_epl --limit 1
```

If FBref returns a Cloudflare "Just a moment..." page or HTTP 403, enable verbose logging and use the
Playwright fallback:
```
python -m FBref_scraper.FBrefScraper --schedule-url https://fbref.com/en/comps/9/2024-2025/schedule --out fbref_data/2024-2025_epl --limit 1 --verbose --fetch-mode playwright
```

Playwright is optional but may be required in environments where requests are blocked:
```
pip install playwright
playwright install
```

If Playwright still hits a challenge page, run it headful once, solve the challenge, and save cookies:
```
python -m FBref_scraper.FBrefScraper --schedule-url https://fbref.com/en/comps/9/2024-2025/schedule --out fbref_data/2024-2025_epl --limit 1 --verbose --fetch-mode playwright --playwright-headful --playwright-wait 20 --playwright-save-state fbref_state.json
```

If the challenge requires interaction, add `--playwright-pause` to wait for you to solve it before scraping:
```
python -m FBref_scraper.FBrefScraper --schedule-url https://fbref.com/en/comps/9/2024-2025/schedule --out fbref_data/2024-2025_epl --limit 1 --verbose --fetch-mode playwright --playwright-headful --playwright-wait 20 --playwright-pause --playwright-save-state fbref_state.json
```

Then reuse the saved state on future runs:
```
python -m FBref_scraper.FBrefScraper --schedule-url https://fbref.com/en/comps/9/2024-2025/schedule --out fbref_data/2024-2025_epl --limit 1 --verbose --fetch-mode playwright --playwright-storage-state fbref_state.json
```
