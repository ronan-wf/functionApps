import requests, logging, pg8000
from datetime import datetime, timezone, time
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


#Get aggrdata via API request with the JWT token
def get_aggrdata(jwt_token: str, headers, url: str, max_retries: int = 3, backoff_secs: int = 5):
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            logging.info("✅ Data fetched successfully.")
            return resp.json()
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "n/a"
            body   = e.response.text[:500] if e.response is not None else ""
            logging.error("HTTPError on GET %s (status %s): %s", url, status, body)
        except requests.RequestException as e:
            logging.error("RequestException on GET %s: %s", url, e)
        if attempt < max_retries:
            wait = backoff_secs * (2 ** (attempt - 1))
            logging.info("Retrying in %s seconds (attempt %d/%d)...", wait, attempt + 1, max_retries)
            time.sleep(wait)
    logging.error("Exhausted retries for %s", url)
    return None

# Generates tsdb insert values based on data received from Fronius API
# Returns the records to be inserted
def generate_tsdb_inserts(data, *, client_id, location_id, gateway: str, metric: str, metrics_map: dict):
    rows = []
    if not data:
        return rows
    try:
        entry = data["data"][0]
        ts = datetime.now(timezone.utc)  
        channels = entry.get("channels", []) or []
        for ch in channels:
            metric_name = ch.get("channelName")
            if not metric_name:
                continue
            sensor_id = metrics_map.get(metric_name)
            if sensor_id is None:
                continue
            
            value = ch.get("value", 0) or 0
            note = metric_name
            rows.append((
                ts,                         # time (datetime with tz)
                client_id,                  # client_id
                location_id,                # location_id
                metric,                     # metric 
                float(value),               # value
                str(gateway),               # gateway
                int(sensor_id),             # sensor 
                note                        # note
            ))
        logging.info("Prepared %d rows for insert", len(rows))
    except Exception as e:
        logging.exception("[generate_tsdb_inserts] Failed to build rows: %s", e)
    return rows

def _query_db(db_conf: dict, query, params=None, fetch=True, many=False):
    try:
        with pg8000.connect(
            host=db_conf["host"],
            database=db_conf["name"],
            user=db_conf["user"],
            password=db_conf["password"],
            port=int(db_conf["port"]),
        ) as connection:
            with connection.cursor() as cursor:
                if many:
                    cursor.executemany(query, params)
                else:
                    cursor.execute(query, params or [])
                if fetch:
                    return cursor.fetchall()
            connection.commit()
    except Exception as e:
        logging.exception("Database operation failed: %s", e)
        return [] if fetch else False
    
# Writes to tsdb using SQL insert format and records from generate_tsdb_inserts
def write_to_timescale(db_conf, rows):
    if not rows:
        logging.info("No rows to insert.")
        return
    insert_sql = """
        INSERT INTO test_table_main
          (time, client_id, location_id, metric, value, gateway, sensor, note)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (time, client_id, location_id, metric, gateway, sensor)
        DO NOTHING;
    """
    # IMPORTANT: pass rows as params and set many=True
    ok = _query_db(db_conf, insert_sql, params=rows, fetch=False, many=True)
    if ok is False:
        logging.error("Insert failed; see previous exception.")
    else:
        logging.info("Inserted %d rows", len(rows))