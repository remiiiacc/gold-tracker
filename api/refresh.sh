#!/bin/bash
# Gold Tracker — data refresh script
# Called by cron for FRED (daily) and CFTC (weekly)

export FRED_API_KEY="2b85c4ec0784560d342a8159fa277e7b"
LOG="/opt/gold-tracker/api/refresh.log"
API="http://127.0.0.1:5000/api"

echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Refresh started: $1" >> "$LOG"

case "$1" in
  fred)
    curl -sf "${API}/fred?refresh=1" -o /dev/null && echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] FRED refreshed OK" >> "$LOG" || echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] FRED refresh FAILED" >> "$LOG"
    curl -sf "${API}/yfinance?refresh=1" -o /dev/null && echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] yfinance refreshed OK" >> "$LOG" || echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] yfinance refresh FAILED" >> "$LOG"
    ;;
  cot)
    curl -sf "${API}/cot?refresh=1" -o /dev/null && echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] COT refreshed OK" >> "$LOG" || echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] COT refresh FAILED" >> "$LOG"
    ;;
  all)
    curl -sf "${API}/fred?refresh=1" -o /dev/null
    curl -sf "${API}/cot?refresh=1" -o /dev/null
    curl -sf "${API}/yfinance?refresh=1" -o /dev/null
    echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] All sources refreshed" >> "$LOG"
    ;;
  *)
    echo "Usage: $0 {fred|cot|all}" >&2
    exit 1
    ;;
esac
