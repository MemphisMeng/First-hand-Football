This is the module that handles the data source(s). It hits the API every 10 seconds when it is on.

## Run
```bash
python ingestion/publication/publisher.py
```

## Environment
```env
API_KEY=...
API_HOST=free-api-live-football-data.p.rapidapi.com
API_BASE_URL=https://free-api-live-football-data.p.rapidapi.com
API_ENDPOINT=/football-current-live
API_QUERYSTRING={}

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_RAW_TOPIC=live-raw
KAFKA_EVENT_TOPIC=live-events
API_POLL_INTERVAL=10
RAW_OUT_DIR=data/live/raw
EVENT_OUT_DIR=data/live/normalized
```
