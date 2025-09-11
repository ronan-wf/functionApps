import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
import requests
import time
from typing import Tuple
from datetime import datetime

TOKEN_PY_PATH = "/tmp/fronius_token_info.py"


def write_token_to_file(token_info) -> None:
    content = f"""# Auto-generated Fronius token info
                jwt_token = "{token_info['jwt_token']}"
                refresh_token = "{token_info['refresh_token']}"
                jwt_expires = {token_info['jwt_expires']}
                """
    with open(TOKEN_PY_PATH, "w") as f:
        f.write(content)
    logging.info(f"✅ Token info written to {TOKEN_PY_PATH}")

def fetch_token(token_info, fronius_urls, client_conf, grant_type: str, payload: dict) -> dict:
    JWT_URL        = fronius_urls["BASE_URL"] + fronius_urls["JWT_ENDPOINT"]
    REFRESH_URL    = JWT_URL + "/" + token_info["refresh_token"]
    headers = {
    "accept": "application/json",
    "AccessKeyId": client_conf["access_key_id"],
    "AccessKeyValue": client_conf["access_key_value"]
    }
    
    url = JWT_URL if grant_type == "password" else REFRESH_URL
    method = requests.post if grant_type == "password" else requests.patch

    response = method(url, json=payload if grant_type == "password" else None, headers=headers, timeout=10)

    try:
        response.raise_for_status()
        logging.info(f"✅ Token request successful: {response.status_code}")
    except requests.HTTPError as e:
        logging.error(f"Token request failed [{response.status_code}]: {response.text}")
        raise
    return response.json()

def get_token(token_info, fronius_urls, client_conf, use_refresh: bool = False) -> None:
    if use_refresh:
        logging.info("🔄 Refreshing jwt token…")
        payload = {}
        grant = "refresh"
    else:
        logging.info("🆕 Acquiring new jwt token…")
        payload = {"userId": client_conf["user_id"], "password": client_conf["user_password"]}
        grant = "password"

    data = fetch_token(token_info, fronius_urls, client_conf, grant, payload)

    token_info["jwt_token"] = data["jwtToken"]
    token_info["refresh_token"] = data["refreshToken"]
    token_info["jwt_expires"] = timestamp_to_epoch(data["jwtTokenExpiration"])

    write_token_to_file(token_info)
    logging.info("✅ Tokens and expiries updated.")

def timestamp_to_epoch(timestamp: str) -> int:
    base, frac = timestamp[:-1].split('.')
    frac = (frac + "000000")[:6]
    iso = f"{base}.{frac}+00:00"

    dt = datetime.fromisoformat(iso)
    epoch = int(dt.timestamp())

    return epoch

def get_active_token(token_info, fronius_urls, client_conf) -> Tuple[str, str]:
    now = int(time.time())

    if not token_info["jwt_token"]:
        get_token(token_info, fronius_urls, client_conf, use_refresh=False)
    elif now >= token_info["jwt_expires"]:
        get_token(token_info, fronius_urls, client_conf, use_refresh=True)
    else:
        logging.info("✅ Access token still valid.")

    return token_info["jwt_token"], token_info["refresh_token"]

if __name__ == "__main__":
    jwt, refresh = get_active_token()
