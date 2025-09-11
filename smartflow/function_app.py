# 11/09/25 11:25 Writing to main
import logging
import tempfile
from pathlib import Path

import azure.functions as func
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

from helpers.helpers import _create_rows, _get_data_smartflow, _get_devices, _write_to_tsdb, clear_token_store, get_active_token

app = func.FunctionApp()

@app.timer_trigger(
    schedule="0 0/30 * * * *", 
    arg_name="sfTimer",
    run_on_startup=False,
    use_monitor=False
)
def smartflowIngest(sfTimer: func.TimerRequest) -> None:
    logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.info("SmartFlow timer fired.")
    try:
        KV_URI = "https://wffunctionappsvault.vault.azure.net/"
        credential = DefaultAzureCredential()
        kv = SecretClient(vault_url=KV_URI, credential=credential)

        # DB config
        db_conf = {
            "password": kv.get_secret("tsdbPassword").value,
            "port":     kv.get_secret("tsdbPort").value,
            "host":     kv.get_secret("tsdbHost").value,
            "name":     kv.get_secret("tsdbName").value,
            "user":     kv.get_secret("tsdbUser").value,
        }

        # SmartFlow creds
        sf_user = {
            "sf_username": kv.get_secret("smartflowUsername").value,
            "password":    kv.get_secret("smartflowPassword").value,
        }

        # Endpoints
        sf_urls = {
            "usage":   "https://api.smartflowmonitoring.com/v3.1/water_usage",
            "devices": "https://api.smartflowmonitoring.com/v3.1/devices",
        }
        # SmartFlow endpoints
        TOKEN_URL = "https://api.smartflowmonitoring.com/v3.1/users/login/token/"
        REFRESH_URL = "https://api.smartflowmonitoring.com/v3.1/users/refresh/?token="

        # Store in /tmp (Linux) or sandbox temp (Windows)
        TOKEN_STORE_PATH = Path(tempfile.gettempdir()) / "smartflow_tokens.json"

        # In-memory copy
        token_info = {
            "access_token": "",
            "refresh_token": "",
            "access_expires": 0,  
            "refresh_expires": 0, 
        }

        # Token
        access_token, _ = get_active_token(TOKEN_STORE_PATH, token_info, TOKEN_URL, REFRESH_URL, sf_user )
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }

        # Pull > transform > load
        devices = _get_devices(sf_urls["devices"], headers)
        results = _get_data_smartflow(devices, headers, sf_urls["usage"])
        rows = _create_rows(results)
        _write_to_tsdb(rows, db_conf)
        clear_token_store(TOKEN_STORE_PATH)

    except Exception as e:
        logging.exception("Startup failure in SmartFlow timer handler: %s", e)


    logging.info("SmartFlow timer run complete.")