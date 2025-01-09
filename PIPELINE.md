Below is a demo scenario that integrates IoT data with metadata using a ClickHouse pipeline (L1→L4), then surfaces insights in Looker. We'll walk through how to join streaming IoT data (like power usage) with a metadata table (customer/device info), create materialized views for fast queries, and finally configure Looker to access ClickHouse.

## 1. Overall Demo Flow
1. Data Stream (IoT): A script continuously sends minimal device usage data (e.g., device_id, timestamp, power_usage_kWh) to ClickHouse.
2. Metadata Table: A static or slowly changing table (e.g., device_metadata) holds additional info (e.g., coordinates, customer_id, building type).
3. Pipeline Layers:
   - L1 (Raw): Store IoT usage data as-is.
   - L2 (Cleaned): Filter out invalid usage values.
   - L3 (Joined): Join with device_metadata to enrich each record.
   - L4 (Aggregated): Materialized views for high-performance queries and dashboards.
4. Looker Dashboard: Connect Looker to ClickHouse, define a model (dimensions, measures), and create dynamic insights.

## 2. Data Model: L1→L4 with Metadata

### 2.1 L1 (Raw Data)

Table: l1_power_raw
```sql
CREATE TABLE l1_power_raw
(
    timestamp        DateTime,
    device_id        UInt32,
    power_usage_kWh  Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, device_id);
```

- Ingestion: Your streaming script inserts records (e.g., from Kafka or direct Python inserts).
- No transformations yet.

### 2.2 L2 (Cleaned Data)

Table: l2_power_cleaned
```sql
CREATE TABLE l2_power_cleaned
(
    timestamp        DateTime,
    device_id        UInt32,
    power_usage_kWh  Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, device_id);
```

Materialized View to filter invalid data:
```sql
CREATE MATERIALIZED VIEW mv_l1_to_l2
TO l2_power_cleaned
AS
SELECT
    timestamp,
    device_id,
    power_usage_kWh
FROM l1_power_raw
WHERE power_usage_kWh >= 0
  AND power_usage_kWh < 100;  -- Arbitrary upper threshold
```

- Result: l2_power_cleaned auto-updates, containing only "good" usage values.

### 2.3 Metadata Table (Static or Slowly-Changing)

Table: device_metadata
```sql
CREATE TABLE device_metadata
(
    device_id       UInt32,
    customer_id     String,
    building_type   String,
    latitude        Float64,
    longitude       Float64,
)
ENGINE = MergeTree()
ORDER BY (device_id);
```

- You can manually or periodically update this table (e.g., CSV import, ETL from a CRM system).

### 2.4 L3 (Joined/Conformed)

Table: l3_power_joined
```sql
CREATE TABLE l3_power_joined
(
    timestamp        DateTime,
    device_id        UInt32,
    customer_id      String,
    building_type    String,
    latitude         Float64,
    longitude        Float64,
    power_usage_kWh  Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, device_id);
```

Materialized View to join L2 with device_metadata:
```sql
CREATE MATERIALIZED VIEW mv_l2_to_l3
TO l3_power_joined
AS
SELECT
    l2.timestamp,
    l2.device_id,
    m.customer_id,
    m.building_type,
    m.latitude,
    m.longitude,
    l2.power_usage_kWh
FROM l2_power_cleaned AS l2
LEFT JOIN device_metadata AS m ON l2.device_id = m.device_id;
```

- Result: Each usage record is enriched with customer_id/building_type/location info.
- Real-time or near real-time: as soon as data hits L2, it's joined with metadata in L3.

### 2.5 L4 (Aggregated / Data Marts)

Table: l4_power_agg_hourly
```sql
CREATE TABLE l4_power_agg_hourly
(
    ts_hour          DateTime,      -- truncated to hour
    device_id        UInt32,
    customer_id      String,
    building_type    String,
    latitude         Float64,
    longitude        Float64,
    total_kWh        Float64,
    avg_kWh          Float64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(ts_hour)
ORDER BY (ts_hour, device_id);
```

Materialized View to aggregate L3 data by hour:
```sql
CREATE MATERIALIZED VIEW mv_l3_to_l4_hourly
TO l4_power_agg_hourly
AS
SELECT
    toStartOfHour(timestamp) AS ts_hour,
    device_id,
    customer_id,
    building_type,
    latitude,
    longitude,
    sum(power_usage_kWh) AS total_kWh,
    avg(power_usage_kWh) AS avg_kWh
FROM l3_power_joined
GROUP BY
    ts_hour,
    device_id,
    customer_id,
    building_type,
    latitude,
    longitude;
```

- Queries on l4_power_agg_hourly can quickly show usage by hour, customer_id, or building type.

## 3. Demo Dashboard Ideas
- Hourly Usage Over Time: A line or bar chart from l4_power_agg_hourly showing total usage across all devices.
- Usage pattern by hour of day (0-23)
- Usage pattern by day of week (0-6)
- Building Type Comparison: Compare average usage for "Apartments" vs. "Houses" vs. "Offices," etc.
- Top N Devices: A table listing which devices (households) used the most electricity in the last 24 hours.
- A map showing usage by location / coordinates, the size of the circle representing the amount of electricity used, the color representing the building type.
