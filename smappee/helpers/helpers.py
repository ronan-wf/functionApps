import logging, pg8000, requests, pytz
from datetime import timedelta, datetime as dt

session = requests.Session()
session.mount("https://", requests.adapters.HTTPAdapter(max_retries=3))

# Rolling 30 minute window from current time
now = dt.now(pytz.utc)
time_to   = int(now.timestamp())
time_from = int((now - timedelta(minutes=30)).timestamp())
METRIC = 'electricity'

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
    
# Get service_location, client, location info
def _get_service_locations(db_conf):
    logging.info("Getting service locations")
    query = """
        select sl.service_loc_id, sl.client_id, l.location_id
        from service_locations sl
        left join locations l on l.location_id = sl.location_id
    """
    records = _query_db(db_conf, query, params=None, fetch=True, many=False)
    logging.info(f"Fetched {len(records)} service locations from the database")
    if not records:
        logging.warning("No service locations found in the database")
        return {}
    return {
            rec[0]: {"client_id": rec[1], "location_id": rec[2]}
            for rec in records if rec
    }

# Queries metering configuration for each service location to get the sensor index. This is used to match the sensor name to the consumption values
# Gets the index and the sensor name for each service location

def _get_index_for_sensors(service_locations, HEADERS):
    logging.info("Getting index for sensors")
    sensor_index = {}
    for slid in service_locations:
        url = f"https://app1pub.smappee.net/dev/v3/servicelocation/{slid}/meteringconfiguration"
        resp = session.get(url, headers=HEADERS, timeout=10)

        if resp.status_code != 200:
            logging.error(f"Failed to fetch data for {slid}: {resp.status_code}")
            continue

        data = resp.json()
        measurements = data.get("measurements", [])

        # Checks if serviceLocationId is in response for measurements section. If yes, this is a parent location and should be skipped.
        if measurements and "serviceLocationId" in measurements[0]:
            logging.error(f"Parent location, serviceLocationId in measurements for {slid}")
            continue    

        index_map = {
            ch["consumptionIndex"]: ch["name"] 
            for m in measurements
            for ch in m.get("channels", []) 
            if "consumptionIndex" in ch and "name" in ch
            }
        
        sensor_index[slid] = index_map
    logging.info(f"Processed {len(sensor_index)} service locations for sensor index")
    return sensor_index

# Gets unique sensor names from the sensor index mapping. Possible to have single or multi-phase sensors in the index
# Sensor names from Index mappings contains 1 - 3 duplicates, one for each phase
# Returns unique sensor names for all active service locations
def _get_unique_sensor_names(sensor_index):
    logging.info("Getting unique sensor names")
    sensor_names = set()

    for sensor_list in sensor_index.values():
        sensor_names.update(sensor_list.values())
    logging.info(f"Processed {len(sensor_names)} unique sensor names.")
    return sorted(sensor_names)

# Gets consumption data per service location id
# Rolling 30 minute window from current time
def _get_consumption_data(service_locations, HEADERS):
    logging.info("Getting consumption data")
    consumption_data_map = {}
    for slid in service_locations:
        url = f"https://app1pub.smappee.net/dev/v3/servicelocation/{slid}/consumption?aggregation=1&from={time_from}&to={time_to}"
        logging.info(f"Fetching data from {url}")
        response = session.get(url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            logging.warning(f"⚠️ Failed to fetch data for {slid}: {response.status_code}")
            continue
        # Gets consumption data for the service location slid
        data = response.json().get("consumptions", [])
        # Get only entries with active power values
        filtered = [entry for entry in data if any(p is not None for p in entry.get("active", []))]
        if filtered:
            consumption_data_map[slid] = filtered
    logging.info(f"✅ Processed {len(consumption_data_map)} service locations with consumption data")
    return consumption_data_map 

# Gets gateway and sensor information for each service location. Uses the unique sensor names from sensor set with the sensor index.
def _get_gateway_sensor_info(db_conf, service_locations, sensor_index, sensor_set):
        logging.info("Fetching gateway and sensor information for service locations...")
        sensor_gateway_map = {}

        if isinstance(service_locations, dict):
            for slid, info in service_locations.items():
                if slid not in sensor_index:
                    logging.warning(f"⚠️ Service location {slid} not in sensor index")
                    continue
                
                # Checks if the service location id is in the service_location_info dictionary
                # If not, it means the service location is not in tsdb. eg Parent loc, inactive, etc.
                client_id, location_id = info["client_id"], info["location_id"]
                if not client_id or not location_id:
                    logging.warning(f"⚠️ Service location {slid} has no client or location id in database.")
                    continue
                # for each sensor name with client/location id combination
                placeholders = ', '.join(['%s'] * len(sensor_set))

                query = f'''
                    SELECT DISTINCT gateway, sensor, note
                    FROM main
                    WHERE client_id = %s
                      AND location_id = %s
                      AND note IN ({placeholders})
                      AND time >= current_timestamp - INTERVAL '1 day';
                '''

                parameters = [client_id, location_id] + list(sensor_set)
                records = _query_db(db_conf, query, parameters)

                sensor_gateway_map[slid] = [
                     {"sensor": rec[1], "gateway": rec[0], "sensor_name": rec[2]}
                     for rec in records if rec
                ]
        else: 
            logging.error("⚠️ Service locations should be a dictionary")
            return {}
        logging.info(f"Processed {len(sensor_gateway_map)} service locations for gateway and sensor information")
        return sensor_gateway_map

# Sum the active power values for each sensor name
def sum_active_power_per_sensor(active_power, index_mapping):
    if not active_power:
        return {}
    summed_active_power = {}
    # For each index and sensor name in the index mapping, sum the active power values
    for consumption_index, sensor_name in index_mapping.items():

        P_values = [entry[consumption_index] if consumption_index < len(entry) and entry[consumption_index] is not None else 0 for entry in active_power]
        summed_value = round(sum(P_values), 4)

        # Sums values of active power for each sensor name
        if sensor_name in summed_active_power:
            summed_active_power[sensor_name] += summed_value
        else:
            summed_active_power[sensor_name] = summed_value

    return summed_active_power

# Assemble rows for CSV, modify to write to tsdb
def _generate_insert(consumption_data_map, sensor_index, service_locations, gateway_sensor_info):
        logging.info("Generating insert statment")
        rows = []
        for slid, entries in consumption_data_map.items():
            if slid not in sensor_index:
                continue
            for entry in entries:
                timestamp = dt.fromtimestamp(entry["timestamp"] / 1000, pytz.UTC)
                summed = sum_active_power_per_sensor([entry["active"]], sensor_index[slid])
                for sensor_name, power_value in summed.items():
                    client_id = service_locations.get(slid, {}).get("client_id", "")
                    location_id = service_locations.get(slid, {}).get("location_id", "")
                    gateway = sensor_id = ""
                    for s in gateway_sensor_info.get(slid, []):
                        if s["sensor_name"] == sensor_name:
                            gateway, sensor_id = s["gateway"], s["sensor"]
                            break
                    sensor_note = f"{sensor_name}"
                    if all([client_id, location_id, gateway, sensor_id]):
                        rows.append([timestamp, client_id, location_id, METRIC, round(power_value,4), gateway, sensor_id, sensor_note])
                    else:
                        logging.warning(f"⚠️ Missing data for {slid}: {client_id}, {location_id}, {power_value}, {gateway}, {sensor_id}")

        sql_query = (
            "INSERT INTO main (time, sensor, value, gateway, client_id, location_id, note, metric) "
            "VALUES "
        )

        value_lines = []
        for row in rows:
            formatted_time = row[0].strftime("%Y-%m-%d %H:%M:%S")
            values = f"('{formatted_time}', '{row[6]}', '{row[4]}', '{row[5]}', '{row[1]}', '{row[2]}', '{row[7]}', '{row[3]}')"
            value_lines.append(values)

        values_sql = ",\n".join(value_lines)

        final_sql = (
            sql_query + values_sql + "\n"
            "ON CONFLICT (time, client_id, location_id, metric, gateway, sensor) DO NOTHING;"
        )
        logging.info("Generated insert statement")
        # Write to file for local testing and verification
        #file = f"tsdb_insert{dt.now().strftime('%Y-%m-%d')}.txt"
        #with open(file, "w") as f:
        #    f.write(final_sql)
        #logging.info(f"SQL insert statement written to {file}")

def _write_to_tsdb(db_conf, sensor_index, service_locations, gateway_sensor_info, consumption_data_map):
    logging.info("Writing to database")
    rows = []

    for slid, entries in consumption_data_map.items():
        if slid not in sensor_index: 
            logging.warning(f"⚠️ Sensor index missing for {slid}, skipping...")
            continue

        for entry in entries:
            timestamp = dt.fromtimestamp(entry["timestamp"] / 1000, pytz.UTC)
            summed = sum_active_power_per_sensor([entry["active"]], sensor_index[slid])

            for sensor_name, power_value in summed.items():
                client_id = service_locations.get(slid, {}).get("client_id", "")
                location_id = service_locations.get(slid, {}).get("location_id", "")
                gateway, sensor_id = "", ""

                for s in gateway_sensor_info.get(slid, []):
                    if s["sensor_name"] == sensor_name:
                        gateway, sensor_id = s["gateway"], s["sensor"]
                        break

                if all ([client_id, location_id, gateway, sensor_id]):
                    rows.append([
                        timestamp, 
                        sensor_id,
                        round(power_value, 4), 
                        gateway,
                        client_id, 
                        location_id,
                        sensor_name, 
                        METRIC,
                    ]) 
                else:
                    logging.warning(f"⚠️ Missing data for {slid}: {client_id}, {location_id}, {power_value}, {gateway}, {sensor_id}")
    if not rows:
        logging.warning("⚠️ No rows to write to database, exiting...")
        return
    
    query = """
        INSERT INTO main (time, sensor, value, gateway, client_id, location_id, note, metric)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (time, client_id, location_id, metric, gateway, sensor) DO NOTHING;
            """
    
    inserts = _query_db(db_conf, query, rows, fetch=False, many=True)
    if inserts is not False:
        logging.info(f"✅ Successfully inserted new rows into the database.")