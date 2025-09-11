# 22-08-2025 09:30.
import logging, requests, pg8000
from datetime import datetime as dt, timedelta, timezone

import azure.functions as func

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential

app = func.FunctionApp()

# Helpers > no network at import time. Avoids indexing issues

# Initialise Key Vault Client
def _get_kv_client():
    logging.info("Initialising Key Vault Client with ManagedIdentityCredential")
    kv_uri = "https://wffunctionappsvault.vault.azure.net/"
    credential = ManagedIdentityCredential()
    return SecretClient(vault_url=kv_uri, credential=credential)

# Gets secrets using KV Client
def _get_secret(client: SecretClient, name: str) -> str:
    return client.get_secret(name).value

# Query database
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
        logging.exception(f"Database operation failed: {e}")
        return [] if fetch else False

# Get consumption data from Octopus Energy
def _get_consumption_data(api_key: str, url: str):
    logging.info("Getting consumption data...")
    all_results, next_url = [], url
    while next_url:
        resp = requests.get(next_url, auth=(api_key, ""), timeout=10)
        logging.info("Request URL: %s, status: %s", next_url, resp.status_code)
        resp.raise_for_status()
        data = resp.json()
        all_results.extend(data.get("results", []))
        next_url = data.get("next")
    return all_results

# Parse consumption results
def _parse_consumption_results(consumption_json):
    logging.info("Parsing consumption results...")
    out = []
    for r in consumption_json:
        # Scaling up for metric visibility on web app. Potential for change
        consumption = r["consumption"] * 1000
        ts = r["interval_end"]
        if ts.endswith("Z"):
            ts = ts.replace("Z", "+00:00")
        dts_utc = dt.fromisoformat(ts).astimezone(timezone.utc)
        out.append([dts_utc.strftime("%Y-%m-%dT%H:%M:%S"), consumption])
    return out

# Create rows for writing to database
def _create_rows(parsed, client_id, location_id, mpan):
    logging.info("Creating rows....")
    gateway = "A"
    sensor_id = "1"
    metric = "electricity"
    rows = [
        [ts, client_id, location_id, metric, val, gateway, sensor_id, mpan]
        for ts, val in parsed
    ]
    return rows

#  Timer entrypoint, safe to index after this point

@app.timer_trigger(
    schedule="0 0 * * * *",
    arg_name="octoTimer",
    run_on_startup=False,
    use_monitor=False,
)
def octopusEnergy(octoTimer: func.TimerRequest) -> None:
    logging.info("Timer fired.")
    if octoTimer.past_due:
        logging.info("The timer is past due!")

    try:
        # All external setup happens here
        kv = _get_kv_client()

        db_conf = {
            "password": _get_secret(kv, "tsdbPassword"),
            "port": _get_secret(kv, "tsdbPort"),
            "host": _get_secret(kv, "tsdbHost"),
            "name": _get_secret(kv, "tsdbName"),
            "user": _get_secret(kv, "tsdbUser"),
        }

        # Get active Octopus Energy clients
        clients = _query_db(
            db_conf,
            """
            SELECT client_id, mpan, serial, api_key_secret_name
            FROM octopus_energy_config
            WHERE is_active = true;
            """,
        )
        if not clients:
            logging.warning("No active clients found.")
            return

        # Timeframe calculated at runtime
        period_from = (dt.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%dT%H:%MZ")
        period_to = dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")

        #Create consumption URL for each active client
        for client_id, mpan, serial, secret_name in clients:
            logging.info(f"Processing client {client_id}")
            try:
                api_key = _get_secret(kv, secret_name)
                url = (
                    f"https://api.octopus.energy/v1/electricity-meter-points/"
                    f"{mpan}/meters/{serial}/consumption?"
                    f"page_size=100&period_from={period_from}&period_to={period_to}&order_by=period"
                )
                data = _get_consumption_data(api_key, url)
                if not data:
                    logging.warning("No data for client %s", client_id)
                    continue
                parsed = _parse_consumption_results(data)

                # Fetch location for client
                loc_rows = _query_db(
                    db_conf, "SELECT location_id FROM locations WHERE client_id = %s;", [client_id]
                )
                if not loc_rows:
                    logging.warning("No location for client %s", client_id)
                    continue
                location_id = loc_rows[0][0]

                # Write created rows to tsdb
                rows = _create_rows(parsed, client_id, location_id, mpan)
                if rows:
                    logging.info("Writing rows to database....")
                    _query_db(
                        db_conf,
                        """
                        INSERT INTO main
                          (time, client_id, location_id, metric, value, gateway, sensor, note)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (time, client_id, location_id, metric, gateway, sensor) DO NOTHING;
                        """,
                        rows,
                        fetch=False,
                        many=True,
                    )
                    logging.info("Inserted %d rows for client %s", len(rows), client_id)
            except Exception as e:
                logging.exception("Client %s failed: %s", client_id, e)

    except Exception as e:
        logging.exception("Startup failure in timer handler: %s", e)

    logging.info("Timer run complete.")
