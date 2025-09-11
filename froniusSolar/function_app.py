# 11/09/2025 10:00
import logging

import azure.functions as func
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

from helpers.helpers import generate_tsdb_inserts, get_aggrdata, write_to_timescale # type: ignore
from helpers.token_refresh import get_active_token  # type: ignore

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0/30 * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def froniusIngest(myTimer: func.TimerRequest) -> None:
    try:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        logging.info("Fronius Ingest started")
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

        #TODO Add function to get client and location ID from TSDB    
        #_get_client_pv_details()
        client_id = "CTa9Xz7FbL2"
        location_id = "Lpx4WvbHkq"
        gateway = "A"
        metric = "solar"

        client_conf = {
            "pv_id": kv.get_secret(f"{client_id}-pvId").value,
            "access_key_id": kv.get_secret(f"{client_id}-accessKeyId").value,
            "access_key_value": kv.get_secret(f"{client_id}-accessKeyValue").value,
            "user_id": kv.get_secret(f"{client_id}-userId").value,
            "user_password": kv.get_secret(f"{client_id}-userPassword").value,
        }

        PV_ID = '5e008b2b-d907-4a39-8cf7-3e7d949c9e3e'

        fronius_urls = {
                # Define the API endpoint and JWT token endpoint
                "BASE_URL": 'https://api.solarweb.com/swqapi/',
                "AGGRDATA_URL": "pvsystems/" + PV_ID + "/aggrdata?period=total",
                "JWT_ENDPOINT": 'iam/jwt'
            }
        
        agg_url = fronius_urls["BASE_URL"] + fronius_urls["AGGRDATA_URL"]

        # Hardcoded channel values requested by client.
        # Manually assigned sensor IDs for tsdb format
        METRICS_MAP = {"EnergyProductionTotal" :'8',
                   "EnergySelfConsumption":'2',
                   "EnergyPurchased":'5',
                   "EnergyFeedIn":'6',
                   "EnergyBattCharge":'7',
                   "EnergyBattDischarge":'4',
                   "EnergyConsumptionTotal":'3',
                   "EnergyBattChargeGrid":'1'}
        
        # In-memory cache
        token_info = {
            "jwt_token": "",
            "refresh_token": "",
            "jwt_expires": 0,}
        
        headers = {
            "accept": "application/json",
            "AccessKeyId": client_conf["access_key_id"],
            "AccessKeyValue": client_conf["access_key_value"]
            }

        token = get_active_token(token_info, fronius_urls, client_conf)
        data  = get_aggrdata(token, headers, agg_url)
        print(data)

        rows = generate_tsdb_inserts(
            data,
            client_id=client_id,
            location_id=location_id,
            gateway=gateway,
            metric=metric,
            metrics_map=METRICS_MAP,
        )
        print(rows)

        write_to_timescale(db_conf, rows)

    except Exception as e:
        logging.exception("Startup failure in SmartFlow timer handler: %s", e)

    logging.info('Fronius Ingest timer trigger function executed.')

def test():
    try:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        logging.info("Fronius Ingest started")
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

        #TODO Add function to get client and location ID from TSDB    
        #_get_client_pv_details()
        client_id = "CTa9Xz7FbL2"
        location_id = "Lpx4WvbHkq"
        gateway = "A"
        metric = "solar"

        client_conf = {
            "pv_id": kv.get_secret(f"{client_id}-pvId").value,
            "access_key_id": kv.get_secret(f"{client_id}-accessKeyId").value,
            "access_key_value": kv.get_secret(f"{client_id}-accessKeyValue").value,
            "user_id": kv.get_secret(f"{client_id}-userId").value,
            "user_password": kv.get_secret(f"{client_id}-userPassword").value,
        }

        PV_ID = '5e008b2b-d907-4a39-8cf7-3e7d949c9e3e'

        fronius_urls = {
                # Define the API endpoint and JWT token endpoint
                "BASE_URL": 'https://api.solarweb.com/swqapi/',
                "AGGRDATA_URL": "pvsystems/" + PV_ID + "/aggrdata?period=total",
                "JWT_ENDPOINT": 'iam/jwt'
            }
        
        agg_url = fronius_urls["BASE_URL"] + fronius_urls["AGGRDATA_URL"]

        # Hardcoded channel values requested by client.
        # Manually assigned sensor IDs for tsdb format
        METRICS_MAP = {"EnergyProductionTotal" :'8',
                   "EnergySelfConsumption":'2',
                   "EnergyPurchased":'5',
                   "EnergyFeedIn":'6',
                   "EnergyBattCharge":'7',
                   "EnergyBattDischarge":'4',
                   "EnergyConsumptionTotal":'3',
                   "EnergyBattChargeGrid":'1'}
        
        # In-memory cache
        token_info = {
            "jwt_token": "",
            "refresh_token": "",
            "jwt_expires": 0,}
        
        headers = {
            "accept": "application/json",
            "AccessKeyId": client_conf["access_key_id"],
            "AccessKeyValue": client_conf["access_key_value"]
            }

        token = get_active_token(token_info, fronius_urls, client_conf)
        data  = get_aggrdata(token, headers, agg_url)
        print(data)

        rows = generate_tsdb_inserts(
            data,
            client_id=client_id,
            location_id=location_id,
            gateway=gateway,
            metric=metric,
            metrics_map=METRICS_MAP,
        )
        print(rows)

        write_to_timescale(db_conf, rows)

    except Exception as e:
        logging.exception("Startup failure in SmartFlow timer handler: %s", e)

if __name__ == "__main__":
    test()