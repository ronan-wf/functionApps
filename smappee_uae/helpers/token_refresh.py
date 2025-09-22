import json
import logging
import os
import time
import tempfile
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Writable in Azure Functions (Linux). Cleared when the instance recycles.
TOKEN_STORE_PATH = Path(tempfile.gettempdir()) / "uae_smappee_token.json"

# In-memory cache
token_info = {
    "access_token": None,
    "expires_at": 0.0,  # epoch seconds
}

def clear_token_store(token_path) -> None:
    logging.info("Clearing tokens")
    try:
        if token_path.exists():
            token_path.unlink()
            logging.info("Cleared token store %s", token_path)
    except Exception as e:
        logging.warning("Could not clear token store: %s", e)

def _load_token_from_tmp() -> None:
    if not TOKEN_STORE_PATH.exists():
        return
    try:
        data = json.loads(TOKEN_STORE_PATH.read_text())
        token_info["access_token"] = data.get("access_token")
        token_info["expires_at"] = float(data.get("expires_at") or 0)
        logging.info("Loaded Smappee token from %s", TOKEN_STORE_PATH)
        logging.info(token_info)
    except Exception as e:
        logging.warning("Could not read token store %s: %s", TOKEN_STORE_PATH, e)

def _write_token_to_tmp() -> None:
    tmp = TOKEN_STORE_PATH.with_suffix(".tmp")
    payload = {
        "access_token": token_info["access_token"],
        "expires_at": float(token_info["expires_at"] or 0),
    }
    try:
        tmp.write_text(json.dumps(payload))
        os.replace(tmp, TOKEN_STORE_PATH)  # atomic on same filesystem
        logging.info("Updated token store at %s", TOKEN_STORE_PATH)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass

def _get_token(sm_conf: dict) -> None:
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(
        "https://app1pub.smappee.net/dev/v1/oauth2/token",
        data=sm_conf,
        headers=headers,
        timeout=15,
    )
    try:
        resp.raise_for_status()
    except requests.HTTPError:
        logging.error("Token request failed [%s]: %s", resp.status_code, resp.text)
        raise

    data = resp.json()
    access_token = data["access_token"]

    expires_in = int(data.get("expires_in", 1800))
    now = time.time()

    token_info["access_token"] = access_token
    token_info["expires_at"] = now + max(expires_in - 60, 0)
    _write_token_to_tmp()

    logging.info(
        "Smappee token refreshed; expires at %s",
        time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(token_info["expires_at"])),
    )

def _is_token_valid() -> bool:
    try:
        return bool(token_info["access_token"]) and time.time() < float(token_info["expires_at"] or 0)
    except Exception:
        return False

def _get_active_token(sm_conf: dict) -> str:
    """Per-instance, ephemeral cache in /tmp."""
    _load_token_from_tmp()
    if not _is_token_valid():
        _get_token(sm_conf)
    return token_info["access_token"]
