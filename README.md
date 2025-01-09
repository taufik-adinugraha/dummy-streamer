# Streamer

This script is used to simulate a real-time data stream into an OLTP database. It inserts historical data first, then streams new records in near-real time.
Tables created:
- **power_usage**: Fields: timestamp, device_id, power_usage_kWh
- **device_metadata**: Fields: device_id, customer_id, latitude, longitude, building_type

## How to run

SQLite:
```bash
python3 streamer.py --db_type=sqlite --num_devices=100 --interval_seconds=3600 --preload_days=100 --retention_days=100
```

MySQL:
```bash
python3 streamer.py --db_type=mysql --db_config='{"host":"localhost","user":"root","password":"secret","database":"test"}' --num_devices=100 --interval_seconds=3600 --preload_days=100 --retention_days=100
```

Postgres:
```bash
python3 streamer.py --db_type=postgres --db_config='{"host":"localhost","user":"postgres","password":"secret","database":"test"}' --num_devices=100 --interval_seconds=3600 --preload_days=100 --retention_days=100
```

## How It Works

1. **Recreate Tables**
   - `setup_database(db_type, conn, cursor)` re-creates `power_usage`
   - `setup_device_metadata_table(db_type, conn, cursor)` re-creates `device_metadata`
   - **Note**: if the tables already exist, they are dropped and recreated

2. **Metadata**
   - `populate_device_metadata(..., num_devices)` inserts random location, building_type, and a random customer_id for each device from 1 to num_devices into `device_metadata`

3. **Preload Data**
   - `insert_historical_data(...)` loops over the last preload_days (default 100) and inserts one reading per day per device into `power_usage`
   - Each day, a random hour/minute is chosen, we call `generate_power_usage_at(dev_id, that_datetime)` to generate usage consistent with that day/time

4. **Start Streaming**
   - After the historical data is in place, the script begins real-time inserts (one batch every interval seconds)
   - Data is inserted into `power_usage` in near-real time
   - **Note**: not every device generates a reading on each interval; it’s a random subset. At least 50% of devices generate a reading on each interval.

5. **Cleanup**
   - Every 100 cycles, we call `cleanup_old_data(...)` to remove data older than retention_days in `power_usage` table
   - You can adjust this to keep more or less historical data

## Result

- On script launch:
  - X days of "backfilled" data is inserted specified by preload_days
  - Then the script streams new records, simulating real-time device updates
  - We now have an OLTP DB with both historical and live IoT data