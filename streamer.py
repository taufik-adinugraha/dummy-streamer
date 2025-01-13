#!/usr/bin/env python3

import time
import random
from datetime import datetime, timedelta

# Depending on your environment, install these if needed:
#   pip install mysql-connector-python
#   pip install psycopg2

import sqlite3
try:
    import mysql.connector
except ImportError:
    mysql = None
try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None

# --------------------------------------------------------------------------------
# Constants / sample data for dummy metadata
# --------------------------------------------------------------------------------

BUILDING_TYPES = ["Rumah", "Apartemen", "Ruko", "Kantor", "Sekolah", "Rumah Sakit", "Pabrik"]

LOCATION_DATA = [
    # (MinLat, MaxLat, MinLon, MaxLon, 'Region')
    (-6.40, -6.15, 106.60, 106.90, "Jakarta"),
    (-6.50, -6.35, 106.65, 106.90, "Tangerang"),
    (-6.45, -6.20, 106.90, 107.00, "Bekasi"),
    (-6.60, -6.30, 106.70, 106.85, "Depok"),
    (-6.65, -6.45, 106.70, 106.90, "Bogor"),
]

# --------------------------------------------------------------------------------
# Functions to simulate usage and location
# --------------------------------------------------------------------------------

def weekday_usage_factor(dt: datetime, building_type: str) -> float:
    """
    Return a multiplier for weekday usage based on building_type and time of day.
    
    - Kantor, Sekolah, Pabrik: most usage between 07:00 - 17:00, lower at night
    - Rumah, Apartemen, Ruko, Rumah Sakit: more uniform over 24h, but peaks morning/evening
    """
    hour = dt.hour

    # Kantor, Sekolah, Pabrik => strong usage 07-17, lower otherwise
    if building_type in ["Kantor", "Sekolah", "Pabrik"]:
        if 7 <= hour < 17:
            # Peak
            return random.uniform(2.0, 3.0)
        else:
            # Off-hours
            return random.uniform(0.5, 1.2)

    # Rumah, Apartemen, Ruko, Rumah Sakit => more uniform, but slightly higher morning/evening
    else:
        if 5 <= hour < 9:
            # Morning bump
            return random.uniform(1.5, 2.5)
        elif 18 <= hour < 22:
            # Evening bump
            return random.uniform(1.5, 2.5)
        else:
            # Normal usage
            return random.uniform(1.0, 2.0)

def weekend_usage_factor(dt: datetime, building_type: str) -> float:
    """
    Return a multiplier for weekend usage based on building_type and time of day.
    
    - Kantor, Sekolah, Pabrik: generally lower usage on weekends, but still some use mid-day
    - Rumah, Apartemen, Ruko, Rumah Sakit: tends to remain somewhat similar to weekdays,
      but might be slightly different distribution.
    """
    hour = dt.hour

    if building_type in ["Kantor", "Sekolah", "Pabrik"]:
        # Lower usage on weekends
        if 8 <= hour < 16:
            return random.uniform(1.0, 2.0)
        else:
            return random.uniform(0.3, 1.0)
    else:
        # For Rumah, Apartemen, Ruko, Rumah Sakit, weekends are somewhat similar,
        # but let's put a small shift to reflect people possibly being home
        if 6 <= hour < 10 or 18 <= hour < 22:
            return random.uniform(1.5, 2.5)
        else:
            return random.uniform(1.0, 2.0)

def generate_power_usage_at(device_id: int,
                            dt: datetime,
                            building_type: str,
                            base_usage=1.0) -> dict:
    """
    Generate a random power usage record for a specific device_id, datetime 'dt',
    and building type. Includes day-of-week logic, time-of-day logic, etc.
    """
    day_of_week = dt.weekday()  # Monday=0, Sunday=6
    if day_of_week < 5:  # Mon-Fri
        factor = weekday_usage_factor(dt, building_type)
    else:  # Sat-Sun
        factor = weekend_usage_factor(dt, building_type)

    # Combine factor with base usage and some random noise
    usage = base_usage * factor * random.uniform(0.8, 1.2)

    # Simulate occasional spikes (1% chance)
    if random.random() < 0.01:
        usage *= random.uniform(1.5, 2.5)

    usage = round(usage, 2)
    return {
        "timestamp": dt.isoformat(),
        "device_id": device_id,
        "power_usage_kWh": usage
    }

def generate_power_usage_now(device_id: int, building_type: str) -> dict:
    """Shortcut to generate usage at the current moment with a building type."""
    return generate_power_usage_at(device_id, datetime.now(), building_type)

def random_location() -> tuple:
    """
    Pick a random bounding box from LOCATION_DATA, then generate lat/lon within it.
    Return (latitude, longitude, region).
    """
    (min_lat, max_lat, min_lon, max_lon, region) = random.choice(LOCATION_DATA)
    lat = round(random.uniform(min_lat, max_lat), 5)
    lon = round(random.uniform(min_lon, max_lon), 5)
    return (lat, lon, region)

def random_building_type() -> str:
    """Return one of our building types."""
    return random.choice(BUILDING_TYPES)

# --------------------------------------------------------------------------------
# DB Connection / Setup
# --------------------------------------------------------------------------------

def create_connection(db_type='sqlite', config=None):
    """Create a database connection based on db_type."""
    if db_type == 'sqlite':
        db_path = config.get("database", "demo.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        return conn, cursor
    elif db_type == 'mysql':
        if not mysql:
            raise ImportError("mysql-connector-python not installed.")
        conn = mysql.connector.connect(
            host=config.get("host", "localhost"),
            user=config.get("user", "root"),
            password=config.get("password", ""),
            database=config.get("database", "test"),
        )
        cursor = conn.cursor()
        return conn, cursor
    elif db_type == 'postgres':
        if not psycopg2:
            raise ImportError("psycopg2 not installed.")
        conn = psycopg2.connect(
            host=config.get("host", "localhost"),
            user=config.get("user", "postgres"),
            password=config.get("password", ""),
            dbname=config.get("database", "test"),
            port=config.get("port", 5432)
        )
        cursor = conn.cursor()
        return conn, cursor
    else:
        raise ValueError(f"Unsupported db_type: {db_type}")

def setup_power_usage_table(db_type, conn, cursor):
    """Create/replace the power_usage table."""
    if db_type == 'sqlite':
        cursor.execute('DROP TABLE IF EXISTS power_usage;')
        cursor.execute('''
            CREATE TABLE power_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                device_id INTEGER NOT NULL,
                power_usage_kWh REAL NOT NULL
            )
        ''')
        conn.commit()
    else:
        statements = [
            "DROP TABLE IF EXISTS power_usage",
            """
            CREATE TABLE power_usage (
                id SERIAL PRIMARY KEY,
                timestamp VARCHAR(50) NOT NULL,
                device_id INT NOT NULL,
                power_usage_kWh FLOAT NOT NULL
            )
            """
        ]
        for stmt in statements:
            cursor.execute(stmt)
        conn.commit()

def setup_device_metadata_table(db_type, conn, cursor):
    """
    Create/replace device_metadata table:
      device_id, customer_id, latitude, longitude, building_type, region
    """
    if db_type == 'sqlite':
        cursor.execute('DROP TABLE IF EXISTS device_metadata;')
        cursor.execute('''
            CREATE TABLE device_metadata (
                device_id INTEGER NOT NULL,
                customer_id INTEGER NOT NULL,
                latitude REAL,
                longitude REAL,
                building_type TEXT,
                region TEXT
            )
        ''')
        conn.commit()
    else:
        statements = [
            "DROP TABLE IF EXISTS device_metadata",
            """
            CREATE TABLE device_metadata (
                device_id INT NOT NULL,
                customer_id INT NOT NULL,
                latitude FLOAT,
                longitude FLOAT,
                building_type VARCHAR(50),
                region VARCHAR(50)
            )
            """
        ]
        for stmt in statements:
            cursor.execute(stmt)
        conn.commit()

def insert_device_metadata(db_type, conn, cursor,
                           device_id, customer_id,
                           lat, lon, btype, region):
    """Insert a single row into device_metadata."""
    if db_type == 'sqlite':
        sql = '''
            INSERT INTO device_metadata 
            (device_id, customer_id, latitude, longitude, building_type, region)
            VALUES (?, ?, ?, ?, ?, ?)
        '''
        params = (device_id, customer_id, lat, lon, btype, region)
    else:
        sql = '''
            INSERT INTO device_metadata 
            (device_id, customer_id, latitude, longitude, building_type, region)
            VALUES (%s, %s, %s, %s, %s, %s)
        '''
        params = (device_id, customer_id, lat, lon, btype, region)

    cursor.execute(sql, params)
    conn.commit()

def populate_device_metadata(db_type, conn, cursor, num_devices=5):
    """
    Insert dummy metadata for 'num_devices' devices with random location in Greater Jakarta.
    We also ensure that one customer_id can have many device_id by creating a smaller pool
    of customer_ids and reusing them.
    """
    # Let's define ~25% as many customer_ids as devices, so each customer can have multiple devices
    num_customers = max(1, num_devices // 4)
    customer_ids = random.sample(range(1000, 9999), num_customers)

    for device_id in range(1, num_devices + 1):
        cust_id = random.choice(customer_ids)  # re-use from a smaller set
        lat, lon, region = random_location()
        btype = random_building_type()
        insert_device_metadata(db_type, conn, cursor, device_id, cust_id, lat, lon, btype, region)

def get_registered_devices(db_type, conn, cursor):
    """
    Fetch (device_id, building_type) from device_metadata
    so we can use the building type in usage generation.
    """
    if db_type == 'sqlite':
        sql = 'SELECT device_id, building_type FROM device_metadata;'
    else:
        sql = 'SELECT device_id, building_type FROM device_metadata;'

    cursor.execute(sql)
    rows = cursor.fetchall()
    # Each row is (device_id, building_type)
    return [(row[0], row[1]) for row in rows]

# --------------------------------------------------------------------------------
# Insert / Cleanup helpers
# --------------------------------------------------------------------------------

def insert_power_usage(db_type, conn, cursor, record):
    """Insert one power usage record."""
    if db_type == 'sqlite':
        sql = '''
            INSERT INTO power_usage (timestamp, device_id, power_usage_kWh)
            VALUES (?, ?, ?)
        '''
        params = (record['timestamp'], record['device_id'], record['power_usage_kWh'])
    else:
        sql = '''
            INSERT INTO power_usage (timestamp, device_id, power_usage_kWh)
            VALUES (%s, %s, %s)
        '''
        params = (record['timestamp'], record['device_id'], record['power_usage_kWh'])

    cursor.execute(sql, params)
    conn.commit()

def cleanup_old_data(db_type, conn, cursor, retention_days=100):
    """
    Remove data older than 'retention_days' from power_usage.
    """
    cutoff = datetime.now() - timedelta(days=retention_days)
    cutoff_iso = cutoff.isoformat()

    if db_type == 'sqlite':
        sql = 'DELETE FROM power_usage WHERE timestamp < ?'
        cursor.execute(sql, (cutoff_iso,))
    else:
        sql = 'DELETE FROM power_usage WHERE timestamp < %s'
        cursor.execute(sql, (cutoff_iso,))

    conn.commit()

# --------------------------------------------------------------------------------
# Historical data insertion
# --------------------------------------------------------------------------------

def insert_historical_data(db_type, conn, cursor, devices, days=100):
    """
    Insert 'days' of historical data for each device.
    e.g., 1 reading per day for each device, over 'days' days in the past.
    'devices' is a list of (device_id, building_type).
    """
    now = datetime.now()
    start_date = now - timedelta(days=days)

    print(f"Inserting {days} days of historical data for {len(devices)} devices...")

    for day_offset in range(days):
        current_day = start_date + timedelta(days=day_offset)
        for (dev_id, btype) in devices:
            # random hour/minute for that day
            rand_hour = random.randint(0, 23)
            rand_min = random.randint(0, 59)
            dt = current_day.replace(hour=rand_hour, minute=rand_min, second=0, microsecond=0)

            record = generate_power_usage_at(dev_id, dt, btype)
            insert_power_usage(db_type, conn, cursor, record)

    print("Historical data insertion complete.\n")

# --------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------

def main(db_type='sqlite',
         db_config=None,
         num_devices=10,
         interval_seconds=3600,
         preload_days=100,
         retention_days=100):
    """
    1) Create DB connection
    2) Setup both tables
    3) Populate device_metadata (device_id from 1..num_devices)
    4) Fetch (device_id, building_type) so that usage can reference building_type
    5) Insert 'preload_days' of historical data
    6) Begin streaming new data at 'interval_seconds' intervals
    7) Cleanup old data older than 'retention_days' (every 100 cycles)
    """
    if db_config is None:
        db_config = {}

    conn, cursor = create_connection(db_type, db_config)

    # Recreate power_usage table
    setup_power_usage_table(db_type, conn, cursor)
    # Recreate device_metadata table
    setup_device_metadata_table(db_type, conn, cursor)

    # Insert device metadata for the requested number of devices
    populate_device_metadata(db_type, conn, cursor, num_devices=num_devices)

    # Now fetch the actual (device_id, building_type) from device_metadata
    devices = get_registered_devices(db_type, conn, cursor)
    device_ids = [d[0] for d in devices]
    print(f"Registered devices = {devices}")

    # Preload some historical data
    if preload_days > 0:
        insert_historical_data(db_type, conn, cursor, devices, days=preload_days)

    cleanup_counter = 0

    # Start the real-time streaming loop
    print(f"Starting power usage stream for {len(devices)} devices using {db_type} DB.")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            # Periodic cleanup
            cleanup_counter += 1
            if cleanup_counter >= 100:
                cleanup_old_data(db_type, conn, cursor, retention_days=retention_days)
                cleanup_counter = 0

            # Insert random usage for a subset of the known device_ids
            num_reports = random.randint(int(len(device_ids) * 0.5), len(device_ids))
            sampled_devices = random.sample(devices, k=num_reports)

            for (dev_id, btype) in sampled_devices:
                record = generate_power_usage_now(dev_id, btype)
                insert_power_usage(db_type, conn, cursor, record)
                print(record)

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\nData stream stopped by user.")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    """
    Example usage:

    1) SQLite (default):
       python streamer.py --num_devices=100 --interval_seconds=3600 --preload_days=100 --retention_days=100

    2) MySQL:
       python streamer.py --db_type=mysql \
         --db_config='{"host":"localhost","user":"root","password":"secret","database":"test"}' \
         --num_devices=100 --interval_seconds=3600 --preload_days=100 --retention_days=100

    3) Postgres:
       python streamer.py --db_type=postgres \
         --db_config='{"host":"localhost","user":"postgres","password":"secret","database":"test"}' \
         --num_devices=100 --interval_seconds=3600 --preload_days=100 --retention_days=100
    """

    import argparse, json

    parser = argparse.ArgumentParser()
    parser.add_argument("--db_type", type=str, default="sqlite",
                        help="Database type: sqlite, mysql, postgres")
    parser.add_argument("--db_config", type=str, default="{}",
                        help="JSON string with db config, e.g. '{\"database\":\"demo.db\"}'")
    parser.add_argument("--num_devices", type=int, default=100,
                        help="Number of devices (metadata rows)")
    parser.add_argument("--interval_seconds", type=int, default=3600,
                        help="Seconds between streaming inserts")
    parser.add_argument("--preload_days", type=int, default=100,
                        help="Days of historical data to insert first")
    parser.add_argument("--retention_days", type=int, default=100,
                        help="How many days of data to keep before deletion")

    args = parser.parse_args()

    db_type = args.db_type
    db_config = json.loads(args.db_config)
    num_devices = args.num_devices
    interval_seconds = args.interval_seconds
    preload_days = args.preload_days
    retention_days = args.retention_days

    main(db_type=db_type,
         db_config=db_config,
         num_devices=num_devices,
         interval_seconds=interval_seconds,
         preload_days=preload_days,
         retention_days=retention_days)