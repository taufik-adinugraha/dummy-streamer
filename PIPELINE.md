Below is a demo scenario that integrates IoT data with metadata using a ClickHouse pipeline (L1→L4). We'll walk through how to join streaming IoT data (like power usage) with a metadata table (customer/device info), and create materialized views for fast queries.

## 1. Overall Demo Flow
1. Data Stream (IoT): streamer.py continuously generates device usage data (e.g., device_id, timestamp, power_usage_kWh) to OLTP Database.
2. Metadata Table: A static (or slowly changing) table holds additional info (e.g., customer_id, building type, latitude, longitude).
3. OLTP Database: A database (could be MySQL or PostgreSQL) that holds the data from the data stream and metadata table.
4. OLAP Database: A database (ClickHouse) that holds the data that should be streamed from OLTP Database using Debezium and Kafka.
5. OLAP Pipeline Layers:
   - L1 (Raw): Store IoT usage data as-is.
   - L2 (Cleaned): Filter out invalid usage values.
   - L3 (Joined): Join with device_metadata to enrich each record.
   - L4 (Aggregated): Materialized views for high-performance queries and dashboards.

So the sequence is:
1. streamer.py → OLTP Database (inserts)
2. Debezium → reads DB's binlog/WAL → Kafka
3. Kafka → ClickHouse (via Kafka Engine or a consumer job)
4. ClickHouse populates L1 (raw table)

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

- Ingestion: The streaming script inserts records (from Kafka).
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
WHERE power_usage_kWh >= 0;  -- Negative values are not allowed
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
    longitude       Float64
)
ENGINE = MergeTree()
ORDER BY (device_id);
```

- We can manually or periodically update this table (e.g., CSV import, ETL from a CRM system).

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

Table: l4_power_agg_stats
```sql
CREATE TABLE l4_power_agg_stats
(
    ts_hour         DateTime,
    device_id       UInt32,
    customer_id     String,
    building_type   String,
    latitude        Float64,
    longitude       Float64,

    -- These fields store partial aggregation states for different metrics:
    usage_count     AggregateFunction(count, Float64),
    usage_sum       AggregateFunction(sum, Float64),
    usage_min       AggregateFunction(min, Float64),
    usage_max       AggregateFunction(max, Float64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(ts_hour)
ORDER BY (ts_hour, device_id);
```
Explanation:
- AggregateFunction(count, Float64): Will hold a partial state of how many records are counted.
- AggregateFunction(sum, Float64): Will hold a running sum of power usage.
- AggregateFunction(min, Float64) & AggregateFunction(max, Float64): Will hold running min/max usage values.

Materialized View to aggregate L3 data by grouping records from l3_power_joined:
```sql
CREATE MATERIALIZED VIEW mv_l3_to_l4_stats
TO l4_power_agg_stats
AS
SELECT
    toStartOfHour(timestamp)   AS ts_hour,
    device_id,
    customer_id,
    building_type,
    latitude,
    longitude,

    countState(power_usage_kWh) AS usage_count,
    sumState(power_usage_kWh)   AS usage_sum,
    minState(power_usage_kWh)   AS usage_min,
    maxState(power_usage_kWh)   AS usage_max
FROM l3_power_joined
GROUP BY
    ts_hour,
    device_id,
    customer_id,
    building_type,
    latitude,
    longitude;
```
How It Works:
- AggregatingMergeTree allows us to store partial aggregate states (count, sum, min, max, avg, etc.) instead of simple sums.
- Every time new rows arrive in l3_power_joined, ClickHouse updates l4_power_agg_stats by accumulating partial states (countState, sumState, etc.).
- The actual numeric values (e.g., total count or total sum) are not stored as simple numbers; they're stored as partial aggregates that need a final merge step at query time.
- Average can be calculated as sum / count.

## 3. Querying the Data
When we want to retrieve the final aggregated metrics (e.g., count, sum, min, max), we use countMerge(), sumMerge(), minMerge(), maxMerge() in our SELECT query. For example:
```sql
SELECT
    ts_hour,
    device_id,
    customer_id,
    building_type,
    latitude,
    longitude,

    countMerge(usage_count) AS final_count,
    sumMerge(usage_sum)     AS final_sum,
    minMerge(usage_min)     AS final_min,
    maxMerge(usage_max)     AS final_max
FROM l4_power_agg_stats
WHERE ts_hour >= today() - 7  -- example filter
GROUP BY
    ts_hour,
    device_id,
    customer_id,
    building_type,
    latitude,
    longitude
ORDER BY ts_hour, device_id
```
Explanation
- The query on l4_power_agg_stats is typically executed by the BI tool or dashboard engine.
- This provides near-real-time aggregated metrics (count, sum, min, max, and any other aggregation states we stored).
- countMerge(usage_count) merges all the partial states of countState.
- sumMerge(usage_sum) merges partial sum states into one final sum.
- minMerge(usage_min), maxMerge(usage_max) similarly produce the final min/max.
- Average can be calculated as sum / count.

## 4. Demo Dashboard Ideas

### 1. Hourly Usage Over Time (All Devices)
**Goal**: Line/bar chart showing total usage by hour.

```sql
SELECT 
    ts_hour,
    sumMerge(usage_sum) AS total_usage
FROM l4_power_agg_stats
WHERE 1 = 1
    -- AND ts_hour >= '{start_date}'
    -- AND ts_hour < '{end_date}'
    -- AND device_id IN (...)
GROUP BY ts_hour
ORDER BY ts_hour;
```

### 2. Usage Pattern by Hour of Day (0–23)
**Goal**: Analyze usage across the 24-hour cycle.

```sql
SELECT
    toHour(ts_hour) AS hour_of_day,
    sumMerge(usage_sum) AS total_usage,
    sumMerge(usage_sum) / countMerge(usage_count) AS avg_usage
FROM l4_power_agg_stats
WHERE 1 = 1
GROUP BY hour_of_day
ORDER BY hour_of_day;
```

### 3. Usage Pattern by Day of Week (0–6)
**Goal**: Analyze usage across Monday–Sunday.

```sql
SELECT
    toDayOfWeek(ts_hour) AS day_of_week,
    sumMerge(usage_sum)  AS total_usage
FROM l4_power_agg_stats
WHERE 1 = 1
GROUP BY day_of_week
ORDER BY day_of_week;
```

### 4. Building Type Comparison
**Goal**: Compare average usage by building type.

```sql
SELECT
    building_type,
    countMerge(usage_count) AS total_records,
    sumMerge(usage_sum)     AS total_usage,
    sumMerge(usage_sum) / countMerge(usage_count) AS avg_usage
FROM l4_power_agg_stats
WHERE 1 = 1
GROUP BY building_type
ORDER BY avg_usage DESC;
```

### 5. Top N Devices in the Last 24 Hours
**Goal**: Rank devices by total usage.

```sql
SELECT
    device_id,
    sumMerge(usage_sum) AS total_usage
FROM l4_power_agg_stats
WHERE ts_hour >= now() - INTERVAL 1 DAY
GROUP BY device_id
ORDER BY total_usage DESC
LIMIT 10;
```

### 6. Map of Usage by Location
**Goal**: Plot usage by lat/lon on a map.

```sql
SELECT
    latitude,
    longitude,
    sumMerge(usage_sum) AS total_usage
FROM l4_power_agg_stats
WHERE 1 = 1
GROUP BY latitude, longitude
ORDER BY total_usage DESC;
```

### Notes
- All queries support additional filters (date range, building type, customer ID, etc.)
- Use `xxxMerge()` functions to finalize partial states from AggregatingMergeTree
- Consider partitioning and sorting keys for query optimization
- BI tools can parameterize these queries for interactive filtering

This set of queries covers hourly trends, usage patterns, building type comparisons, top devices, and location-based usage—fulfilling your typical IoT power usage analytics requirements.