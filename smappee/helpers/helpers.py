import logging, pg8000, requests, pytz, time, string
from datetime import timedelta, datetime as dt
from io import StringIO
from functools import wraps
from contextlib import contextmanager
from collections import OrderedDict

session = requests.Session()
session.mount("https://", requests.adapters.HTTPAdapter(max_retries=3))

METRIC = 'electricity'

#Decoration for timing functions
def log_timing(name=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            label = name or func.__name__
            start = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                elapsed = (time.perf_counter() - start) * 1000
                logging.info(f"⏱ {label} took {elapsed:.2f} ms")
        return wrapper
    return decorator

#Function for timing code blocks
@contextmanager
def timing_block(label: str):
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = (time.perf_counter() - start) * 1000
        logging.info(f"   ↳ {label} took {elapsed:.2f} ms")

@log_timing()
def _query_db(db_conf: dict, query, params=None, fetch=True, many=False):
    try:
        with pg8000.connect(
            host=db_conf["host"],
            database=db_conf["name"],
            user=db_conf["user"],
            password=db_conf["password"],
            port=int(db_conf["port"]),
            application_name="smappee_ingest"
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
@log_timing()
def _get_service_locations(db_conf):
    logging.info("1. Getting service locations")
    query = """
        select sl.service_loc_id, sl.client_id, l.location_id
        from service_locations sl
        left join locations l on l.location_id = sl.location_id
    """
    records = _query_db(db_conf, query, params=None, fetch=True, many=False)
    logging.info(records)
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
@log_timing()
def _get_index_for_sensors(service_locations, HEADERS):
    logging.info("2. Getting index for sensors")
    sensor_index = {}

    for slid in service_locations:
        url = f"https://app1pub.smappee.net/dev/v3/servicelocation/{slid}/meteringconfiguration"
        resp = session.get(url, headers=HEADERS, timeout=10)

        if resp.status_code != 200:
            logging.error(f"Failed to fetch data for {slid}: {resp.status_code}")
            continue

        data = resp.json()
        measurements = data.get("measurements", [])

        # Skip parent locations
        if measurements and "serviceLocationId" in measurements[0]:
            logging.error(f"Parent location, serviceLocationId in measurements for {slid}")
            continue

        # Collect (consumptionIndex, name) pairs
        pairs = [
            (ch["consumptionIndex"], ch["name"])
            for m in measurements
            for ch in m.get("channels", [])
            if "consumptionIndex" in ch and "name" in ch
        ]

        # Sort by index to lock the order 0,1,2,... regardless of API list order
        pairs.sort(key=lambda x: x[0])
        index_map = OrderedDict(pairs)

        sensor_index[slid] = index_map

    logging.info(sensor_index)  # this will now show OrderedDicts in index order
    logging.info(f"Processed {len(sensor_index)} service locations for sensor index")
    return sensor_index

# Gets unique sensor names from the sensor index mapping. Possible to have single or multi-phase sensors in the index
# Sensor names from Index mappings contains 1 - 3 duplicates, one for each phase
# Returns unique sensor names for all active service locations
@log_timing()
def _get_unique_sensor_names(sensor_index):
    logging.info("3. Getting unique sensor names (order-preserving)")
    ordered_unique = OrderedDict()

    # deterministic traversal across locations (adjust if you prefer insertion order)
    for slid in sorted(sensor_index.keys()):
        idx_map = sensor_index[slid]  # this is an OrderedDict sorted by consumptionIndex
        for name in idx_map.values():
            if name not in ordered_unique:
                ordered_unique[name] = None

    names = list(ordered_unique.keys())
    logging.info(f"Processed {len(names)} unique sensor names.")
    logging.info(names)  # keep as list; avoid sets/sorted() which wreck order
    return names

# Gets consumption data per service location id
# Rolling 30 minute window from current time
@log_timing()
def _get_consumption_data(service_locations, HEADERS):
    logging.info("4. Getting consumption data")

    # Rolling 30 minute window from current time
    now = dt.now(pytz.utc)
    time_to   = int(now.timestamp())
    time_from = int((now - timedelta(hours=24)).timestamp())
    consumption_data_map = {}
    
    with timing_block("Get consumption data"):
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
@log_timing()
def _get_gateway_sensor_info(db_conf, service_locations, sensor_index, sensor_set):
    logging.info("5. Fetching gateway and sensor information for service locations...")
    sensor_gateway_map = {}

    # Tracks next created gateway index per service locations client_id/location_id
    new_gateway_counter = {}

    def new_gateway_id(n: int) -> str:
        n = int(n)
        letters = []
        while True:
            n, rem = divmod(n, 26)
            letters.append(string.ascii_uppercase[rem])
            if n == 0:
                break
            n -= 1
        return ''.join(reversed(letters))

    if not isinstance(service_locations, dict):
        logging.error("⚠️ Service locations should be a dictionary")
        return {}

    if not sensor_set:
        logging.warning("⚠️ sensor_set is empty; nothing to map")
        return {}

    #TODO: Check if sensor_index or sensor_set should be used, add slid to sensor_set
    for slid, info in service_locations.items():
        if slid not in sensor_index:
            logging.warning(f"⚠️ Service location {slid} not in sensor index")
            continue

        client_id = info.get("client_id")
        location_id = info.get("location_id")
        if not client_id or not location_id:
            logging.warning(f"⚠️ Service location {slid} has no client or location id in database.")
            continue

        # Query existing gateway/sensor rows for these notes in the last day
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

        if records:
            # Use the data from tsdb
            sensor_gateway_map[slid] = [
                {"sensor": rec[1], "gateway": rec[0], "sensor_name": rec[2]}
                for rec in records if rec
            ]
        else:
            # No records found: create a new gateway for this client/location and map all sensors
            key = (client_id, location_id)
            next_idx = new_gateway_counter.get(key, 0)
            gateway_label = new_gateway_id(next_idx)
            new_gateway_counter[key] = next_idx + 1

            # Adds sensor_id, gateway, sensor name to sensor_gateway_map for each sensor in sensor_set
            sensor_gateway_map[slid] = [
                {"sensor": i + 1, "gateway": gateway_label, "sensor_name": sname}
                for i, sname in enumerate(sensor_set)
            ]
            logging.info(
                f"Created gateway '{gateway_label}' for client_id {client_id} & location_id {location_id} "
                f"for service_location_id {slid} with {len(sensor_set)} sensors (IDs 1..{len(sensor_set)})."
            )

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
@log_timing()
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
            "INSERT INTO test_table_main (time, sensor, value, gateway, client_id, location_id, note, metric) "
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
        file = f"tsdb_insert{dt.now().strftime('%Y-%m-%d')}.txt"
        with open(file, "w") as f:
            f.write(final_sql)
        logging.info(f"SQL insert statement written to {file}")

@log_timing()
def _write_to_tsdb(db_conf, sensor_index, service_locations, gateway_sensor_info, consumption_data_map):
    logging.info("Writing to database via COPY -> INSERT ON CONFLICT")
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
                gateway = sensor_id = ""

                for s in gateway_sensor_info.get(slid, []):
                    if s["sensor_name"] == sensor_name:
                        gateway, sensor_id = s["gateway"], s["sensor"]
                        break

                if all([client_id, location_id, gateway, sensor_id]):
                    rows.append([
                        timestamp,          # time (timestamptz)
                        sensor_id,          # sensor (text)
                        round(power_value, 4), # value (numeric/float)
                        gateway,            # gateway (text)
                        client_id,          # client_id (text/int)
                        location_id,        # location_id (text/int)
                        sensor_name,        # note (text)
                        METRIC,             # metric (text)
                    ])
                else:
                    logging.warning(f"Missing data for {slid}: {client_id}, {location_id}, {power_value}, {gateway}, {sensor_id}")

    if not rows:
        logging.warning("No rows to write to database, exiting...")
        return

    logging.info(f"Prepared {len(rows)} rows")

    # Build a CSV/TSV stream for COPY, Using TEXT tab-delimited
    with timing_block("Streaming"):
        buf = StringIO()
        for r in rows:
            ts = r[0].isoformat()  #timestamp to iso
            # Use \N for NULLs if needed, should never be NULLs
            line = f"{ts}\t{r[1]}\t{r[2]}\t{r[3]}\t{r[4]}\t{r[5]}\t{r[6]}\t{r[7]}\n"
            buf.write(line)
        buf.seek(0)

    try:
        with timing_block("Copying to temp"):
            with pg8000.connect(
                host=db_conf["host"],
                database=db_conf["name"],
                user=db_conf["user"],
                password=db_conf["password"],
                port=int(db_conf["port"]),
                application_name="smappee_ingest"
            ) as conn:
                cur = conn.cursor()
                # Faster commits for ingest; acceptable tiny durability risk.
                cur.execute("SET LOCAL synchronous_commit = off;")

                # Make a temp table that matches target table
                cur.execute("""
                    CREATE TEMP TABLE _ingest_main
                    (LIKE test_table_main INCLUDING DEFAULTS INCLUDING CONSTRAINTS)
                    ON COMMIT DROP;
                """)

                # COPY from in-memory stream into the temp table.
                # Using TEXT format with tabs; tell Postgres we're sending from stdin
                cur.execute(
                    "COPY _ingest_main (time, sensor, value, gateway, client_id, location_id, note, metric) "
                    "FROM stdin WITH (FORMAT text)",
                    stream=buf
                )  # pg8000 streams the data to COPY. :contentReference[oaicite:1]{index=1}

                # Dedup into main table with ON CONFLICT DO NOTHING.
                # Reduces time to insert data to table
                with timing_block("Execute insert from temp to main"):
                    cur.execute("""
                        INSERT INTO test_table_main (time, sensor, value, gateway, client_id, location_id, note, metric)
                        SELECT time, sensor, value, gateway, client_id, location_id, note, metric
                        FROM _ingest_main
                        ON CONFLICT (time, client_id, location_id, metric, gateway, sensor) DO NOTHING;
                    """)

                    conn.commit()
                    logging.info("COPY to temp > INSERT to main completed")
    except Exception as e:
        logging.exception("Database COPY/INSERT failed: %s", e)
