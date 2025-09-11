import json
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)

def _to_naive_utc(ts: str) -> str:
    """Convert ISO8601 string (with Z or offset) to naive UTC 'YYYY-MM-DDTHH:MM:SS'."""
    # Normalize Z to +00:00 so fromisoformat can parse it
    if ts.endswith('Z'):
        ts = ts.replace('Z', '+00:00')
        
    dt = datetime.fromisoformat(ts)          # aware dt
    dt_utc = dt.astimezone(timezone.utc)     # convert to UTC
    dt_utc_naive = dt_utc.replace(tzinfo=None)
    return dt_utc_naive.strftime('%Y-%m-%dT%H:%M:%S')

def parse_consumption_results(consumption_json):
    logging.info("Parsing consumption results...")
    # If it’s a dict, try to pull the list out of common keys
    if isinstance(consumption_json, dict):
        items = (consumption_json.get("results")
                 or consumption_json.get("data")
                 or [])
    elif isinstance(consumption_json, list):
        items = consumption_json
    else:
        raise TypeError(f"Unexpected JSON type: {type(consumption_json)}")

    out = []
    for row in items:
        # Defensive checks in case a row is malformed
        if not isinstance(row, dict):
            logging.debug("Skipping non-dict row: %r", row)
            continue
        try:
            consumption = float(row["consumption"])
            ts = row["interval_end"]
        except KeyError as e:
            logging.debug("Skipping row missing key %s: %r", e, row)
            continue

        ts_norm = _to_naive_utc(ts)
        out.append([ts_norm, consumption])

    return out

# Example usage with your file:
with open('response.json') as f:
    d = json.load(f)

rows = parse_consumption_results(d)
print(rows[:3])  # peek at first few
