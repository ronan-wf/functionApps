import json, logging, os, requests, time, pytz, pg8000
from typing import Tuple, Optional
from datetime import datetime as dt, timedelta

def _load_token_from_tmp(token_path, token_info) -> None:
    if not token_path.exists():
        return
    try:
        data = json.loads(token_path.read_text())
        for k in ("access_token", "refresh_token", "access_expires", "refresh_expires"):
            if k in data:
                token_info[k] = data[k]
        logging.info("Loaded tokens from %s", token_path)
    except Exception as e:
        logging.warning("Could not read token store %s: %s", token_path, e)

def _write_token_to_tmp(token_path, token_info) -> None:
    tmp_path = token_path.with_suffix(".tmp")
    payload = {
        "access_token": token_info["access_token"],
        "refresh_token": token_info["refresh_token"],
        "access_expires": int(token_info["access_expires"]),
        "refresh_expires": int(token_info["refresh_expires"]),
    }
    try:
        tmp_path.write_text(json.dumps(payload))
        os.replace(tmp_path, token_path) 
        logging.info("Updated token store at %s", token_path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass

def fetch_token(grant_type: str, payload: dict, TOKEN_URL, REFRESH_URL, token_info) -> dict:
    url = TOKEN_URL if grant_type == "password" else REFRESH_URL + token_info["refresh_token"]
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(url, data=payload, headers=headers, timeout=10)
    try:
        resp.raise_for_status()
    except requests.HTTPError:
        logging.error("Token request failed [%s]: %s", resp.status_code, resp.text)
        raise
    return resp.json()

def get_token(token_path, token_info, TOKEN_URL, REFRESH_URL, sf_user: dict, use_refresh: bool = False) -> None:
    if use_refresh:
        logging.info("Refreshing access token…")
        payload = {"token": token_info["refresh_token"]}
        grant = "refresh"
    else:
        logging.info("Acquiring new access token (password grant)…")
        payload = {
            "grant_type": "password",
            "username": sf_user["sf_username"],
            "password": sf_user["password"],
        }
        grant = "password"

    data = fetch_token(grant, payload, TOKEN_URL, REFRESH_URL, token_info)

    token_info["access_token"] = data["access_token"]
    token_info["refresh_token"] = data["refresh_token"]

    now = int(time.time())
    # 1 hour
    token_info["access_expires"] = now + 3600
    # 1 day
    token_info["refresh_expires"] = now + 3600 * 24   

    _write_token_to_tmp(token_path, token_info)
    logging.info("✅ Tokens updated.")

def get_active_token(token_path, token_info, TOKEN_URL, REFRESH_URL, sf_user: Optional[dict] = None) -> Tuple[str, str]:
    _load_token_from_tmp(token_path, token_info)
    now = int(time.time())

    if not token_info["access_token"]:
        if sf_user is None:
            raise RuntimeError("sf_user required for initial token acquisition.")
        get_token(token_path, token_info, TOKEN_URL, REFRESH_URL, sf_user, use_refresh=False)
    elif now >= token_info["refresh_expires"]:
        if sf_user is None:
            raise RuntimeError("sf_user required to re-acquire expired refresh token.")
        get_token(token_path, token_info, TOKEN_URL, REFRESH_URL, sf_user, use_refresh=False)
    elif now >= token_info["access_expires"]:
        get_token(token_path, token_info, TOKEN_URL, REFRESH_URL, sf_user, use_refresh=True)
    else:
        logging.info("✅ Access token still valid.")

    return token_info["access_token"], token_info["refresh_token"]

def clear_token_store(token_path) -> None:
    try:
        if token_path.exists():
            token_path.unlink()
            logging.info("Cleared token store %s", token_path)
    except Exception as e:
        logging.warning("Could not clear token store: %s", e)

# ---------------------------
# DB helper
# ---------------------------
def _query_db(db_conf: dict, query, params=None, fetch=True, many=False):
    logging.info("Running db query fetch=%s many=%s", fetch, many)
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

# ---------------------------
# SmartFlow helpers
# ---------------------------
def _get_devices(device_url: str, headers: dict) -> dict:
    resp = requests.get(url=device_url, headers=headers, timeout=15)
    logging.info("Devices response: %s", resp.status_code)
    resp.raise_for_status()
    payload = resp.json()

    if isinstance(payload, list):
        devices = payload
    elif isinstance(payload, dict) and "devices" in payload:
        devices = payload["devices"]
    elif isinstance(payload, dict) and "data" in payload:
        devices = payload["data"]
    else:
        logging.error("Unexpected response format. No devices found %s", payload)
        return {}

    did_name = {}
    for device in devices:
        device_id = device.get("device_id")
        settings = (device.get("device_settings") or {})
        device_name = settings.get("device_name", "")
        if device_id is not None and device_name and "Tullamore Court" in device_name:
            did_name[device_id] = device_name
        else:
            logging.debug("Skipping device %s (%s)", device_id, device_name)

    logging.info("Found %d Tullamore Court devices", len(did_name))
    return did_name

def _get_data_smartflow(devices: dict, headers: dict, usage_url: str) -> dict:
    now = dt.now(pytz.utc).replace(minute=0, second=0, microsecond=0)
    time_to = now.strftime("%Y-%m-%dT%H:%M:%S")
    time_from = (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")

    params = {
        "agg_type": "Hour",
        "start_date": time_from,
        "end_date": time_to,
        "id_type": "device_id",
        "uom": "Liters",
    }

    results = {
        "time": [], "gateway": [], "value": [],
        "note": [], "client_id": [], "location_id": [],
        "metric": [], "sensor": []
    }

    for device_id, device_name in devices.items():
        url = f"{usage_url}/{device_id}/aggregated"
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=20)
            logging.info("Usage %s -> %s", url, resp.status_code)
            if resp.status_code != 200:
                logging.warning("Skipping %s: HTTP %s", device_id, resp.status_code)
                continue

            payload = resp.json()
            _process_data(payload, device_id, device_name, results, time_to)
        except Exception as e:
            logging.exception("Usage fetch failed for %s: %s", device_id, e)

    return results

def _process_data(payload, device_id, device_name, results, time_to):
    usage = payload.get("usage_data", []) or []
    value = float((usage[0].get("Usage") if usage else 0) or 0)

    results["time"].append(time_to)
    results["gateway"].append(device_id)
    results["value"].append(value)
    results["note"].append(device_name)
    results["client_id"].append("CTvx846sF9Y")
    results["location_id"].append("Lpj1hzfJimw")
    results["metric"].append("water")
    results["sensor"].append("1")

    logging.info("Device %s (%s): %s litres", device_id, device_name, value)

def _create_rows(results) -> list:
    rows = list(zip(
        results["time"],
        results["client_id"],
        results["location_id"],
        results["metric"],
        results["value"],
        results["gateway"],
        results["sensor"],
        results["note"],
    ))
    logging.info("Prepared %d rows", len(rows))
    return rows

def _write_to_tsdb(rows: list, db_conf: dict):
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
    ok = _query_db(db_conf, insert_sql, rows, fetch=False, many=True)
    if ok is False:
        logging.error("Insert failed.")
    else:
        logging.info("Inserted %d rows", len(rows))