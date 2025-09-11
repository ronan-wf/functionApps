import requests, logging, pg8000
from datetime import datetime as dt, timedelta, timezone
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',  
    datefmt='%Y-%m-%d %H:%M:%S'
)

#Key Vault Details
keyVaultName = "wfFunctionAppsVault"
KVUri = f"https://wfFunctionAppsVault.vault.azure.net"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=KVUri, credential=credential)

#Get secret funtion simplified for use
def get_secret(name: str) -> str:
    try:
        return client.get_secret(name).value
    except Exception:
        raise ValueError(f"Secret '{name}' not found in Key Vault. Check secret name.")

#Static Values
gateway = "A"
sensor_id = "1"
metric = 'electricity'

#Database details stored in Key Vault
DB_PASSWORD= get_secret("tsdbPassword")
DB_PORT= get_secret("tsdbPort")
DB_HOST= get_secret("tsdbHost")
DB_NAME= get_secret("tsdbName")
DB_USER= get_secret("tsdbUser")

#Time, get last 3 days of data
last_three_days = dt.now() - timedelta(days=3)
period_from = '2025-08-01T09:00Z' #last_three_days.strftime("%Y-%m-%dT%H:%MZ")
period_to = '2025-08-18T09:00Z'#dt.now().strftime("%Y-%m-%dT%H:%MZ")

#Get consumption data from Octopus Energy
def get_consumption_data(API_KEY, url):
    logging.info("Getting consumption data...")
    try:
        all_results = []
        next_url = url

        #Handles multiple pages if present
        while next_url:
            response = requests.get(
                url=next_url,
                auth=(API_KEY, ''),
                timeout=10
            )
            logging.info(f"Request URL: {next_url}, status: {response.status_code}")
            response.raise_for_status()
            data = response.json()
            results = data.get("results",[])
            #Add results from 1st and additional pages
            all_results.extend(results)

            logging.info(f"Retrieved {len(results)}, total results: {len(all_results)}")
            #Get next page of results url until 'None'
            next_url = data.get("next")
        return all_results
    except Exception as e:
        logging.exception(f"Exception: {e}")

#Parses consumptions data into time and kWh values
def parse_consumption_results(consumption_json):
    logging.info(f"Parsing consumption results...")
    consumption_output = []

    for result in consumption_json:
        consumption = result["consumption"] * 1000
        ts = result["interval_end"]

        #Normalise to time with timezone
        #Handle DST and return UTC timestamps
        if ts.endswith('Z'):
            ts = ts.replace('Z', '+00:00')
        dts = dt.fromisoformat(ts)
        dts_utc = dts.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
        consumption_output.append([dts_utc, consumption])
    return consumption_output

#Queries database with statement
def query_db(query, params=None, fetch=True, many=False):
    try:
        with pg8000.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        ) as connection:
            with connection.cursor() as cursor:
                if many:
                    logging.debug(f"Executing SQL (many): {query.strip()} | Rows: {len(params)}")
                    cursor.executemany(query, params)
                else:
                    logging.debug(f"Executing SQL: {query.strip()} | Params: {params}")
                    cursor.execute(query, params or [])
                if fetch:
                    return cursor.fetchall()
            connection.commit()
    except Exception as e:
        logging.exception(f"Database operation failed: {e}")
        return [] if fetch else False

#Get active Octopus Client info from tsdb
def get_client_config():
    logging.info("Getting client information from tsdb")
    query = """
    SELECT client_id, mpan, serial, api_key_secret_name
    FROM octopus_energy_config
    WHERE is_active = true;
    """
    result = query_db(query)
    logging.info(result)
    if result:
        return result
    else:
        raise ValueError(f"No active clients found.")
    
def get_client_location(client_id):
    logging.info(f"Getting location ID for {client_id}...")
    query = """
    SELECT location_id
    FROM locations
    WHERE client_id = %s;
    """
    result = query_db(query, [client_id])
    location_id = result[0][0]
    logging.info(location_id)
    if location_id:
        return location_id
    else:
        raise ValueError(f"No location for {client_id} found.")
    
#Writes row data to timescale
def write_to_tsdb(rows):
    logging.info(f"Writing to database....")
    query = """
    INSERT INTO main (time, client_id, location_id, metric, value, gateway, sensor, note)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (time, client_id, location_id, metric, gateway, sensor) DO NOTHING;
        """
    
    inserts = query_db(query, rows, fetch=False, many=True)
    if inserts is not False:
        logging.info(f"Processed {len(rows)} rows.")

#Creates rows from client & consumption information
def create_rows(parsed_consumption, client_id,  location_id, MPAN):
    logging.info("Creating rows...")
    rows = []
    for timestamp, value in parsed_consumption:

        rows.append([
            timestamp, 
            client_id, 
            location_id,
            metric,
            value, 
            gateway,
            sensor_id,
            MPAN
        ])

    logging.info(f"Created rows: {rows}")
    if not rows:
        logging.warning("No rows to write to database, exiting...")
    return rows

#Main method with functions
def main():
    logging.info("Starting...")
    #Get all clients stored in DB that are active
    clients = get_client_config()

    for client in clients:
        try:
            #Extract client details from response
            client_id, MPAN, SERIAL, secret_name = client
            location_id = get_client_location(client_id)
            #Get API_KEY based on name stored in tsdb
            #Actual API_KEY secret stored in Key Vault
            API_KEY = get_secret(secret_name)

            logging.info(f"Processing client {client_id}")

            #Generate URL based on client info from db
            url = (
                f"https://api.octopus.energy/v1/electricity-meter-points/"
                f"{MPAN}/meters/{SERIAL}/consumption?"
                f"page_size=100&period_from={period_from}&period_to={period_to}&order_by=period")
            
            #Get consumption DATA for MPRN/Serial combination
            consumption_json = get_consumption_data(API_KEY, url)
            if not consumption_json:
                logging.warning(f"No data in response from API for {client_id}")
                continue
            
            #Parse the consumption response
            parsed_consumption = parse_consumption_results(consumption_json)

            #Create rows from parsed consumption and client info
            rows = create_rows(parsed_consumption, client_id, location_id, MPAN)
            #If there are rows write to tsdb
            if rows:
                #logging.info("Testing writing")
                write_to_tsdb(rows)

        except Exception as e:
            logging.exception(f"Exception: {e}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(f"Unhandled exception: {e}")
    