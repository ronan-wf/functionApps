# 11/09/25 12:10 Writing to main

import logging
from helpers.helpers import _get_service_locations,_get_index_for_sensors,_get_unique_sensor_names,_get_consumption_data,_get_gateway_sensor_info,_generate_insert,_write_to_tsdb, log_timing
from helpers.token_refresh import _get_active_token

import azure.functions as func
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
@log_timing()
def smappeeIngest(myTimer: func.TimerRequest) -> None:
    logging.info("Starting Smappee ingest")
    try:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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

        sm_conf = {
            "grant_type": "password",
            "client_id": kv.get_secret("smappeeClientID").value,
            "client_secret": kv.get_secret("smappeeClientSecret").value,
            "username": kv.get_secret("smappeeUsername").value,
            "password": kv.get_secret("smappeePassword").value,
        }
        

        SM_TOKEN = _get_active_token(sm_conf)

        HEADERS = {
        "Authorization": f"Bearer {SM_TOKEN}",
        "Accept": "application/json"
        }

        #Get service locations, client ID and location ID from tsdb 
        service_locations = _get_service_locations(db_conf) 

        #Get index for sensors per sensor location
        sensor_index = _get_index_for_sensors(service_locations, HEADERS)

        #Get unique sensor names from the index
        sensor_set = _get_unique_sensor_names(sensor_index)
        
        #Get consumption data for and maps to each service location
        consumption_data_map = _get_consumption_data(service_locations, HEADERS)

        ## Get gateway and sensor information for each service location from tsdb
        gateway_sensor_info = _get_gateway_sensor_info(db_conf, service_locations, sensor_index, sensor_set)

        # Assemble CSV to create insert statements for tsdb
        #_generate_insert(consumption_data_map, sensor_index, service_locations, gateway_sensor_info)

        # Write to tsdb test_main table
        #_write_to_tsdb(db_conf, sensor_index, service_locations, gateway_sensor_info, consumption_data_map)

    except Exception as e:
        logging.exception("Startup failure in SmartFlow timer handler: %s", e)

    logging.info('Completed Smappee ingest')

@log_timing()
def test():
    try:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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

        sm_conf = {
            "grant_type": "password",
            "client_id": kv.get_secret("smappeeClientID").value,
            "client_secret": kv.get_secret("smappeeClientSecret").value,
            "username": kv.get_secret("smappeeUsername").value,
            "password": kv.get_secret("smappeePassword").value,
        }
        

        SM_TOKEN = _get_active_token(sm_conf)

        HEADERS = {
        "Authorization": f"Bearer {SM_TOKEN}",
        "Accept": "application/json"
        }

        #Get service locations, client ID and location ID from tsdb 
        service_locations = _get_service_locations(db_conf) 

        #Get index for sensors per sensor location
        sensor_index = _get_index_for_sensors(service_locations, HEADERS)

        #Get unique sensor names from the index
        sensor_set = _get_unique_sensor_names(sensor_index)
        
        #Get consumption data for and maps to each service location
        consumption_data_map = _get_consumption_data(service_locations, HEADERS)

        ## Get gateway and sensor information for each service location from tsdb
        gateway_sensor_info = _get_gateway_sensor_info(db_conf, service_locations, sensor_index, sensor_set)

        # Assemble CSV to create insert statements for tsdb
        _generate_insert(consumption_data_map, sensor_index, service_locations, gateway_sensor_info)

        # Write to tsdb test_main table
        #_write_to_tsdb(db_conf, sensor_index, service_locations, gateway_sensor_info, consumption_data_map)

    except Exception as e:
        logging.exception("Startup failure in SmartFlow timer handler: %s", e)

if __name__ == "__main__":
    test()