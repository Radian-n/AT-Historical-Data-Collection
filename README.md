# AT Historical Data Collection

Collects real-time GTFS data from the Auckland Transport API and stores it
in Delta Lake tables for historical analysis.

## Features

- Collects vehicle positions, trip updates, ferry positions, and service
  alerts from AT's GTFS-Realtime API
- Writes to Delta Lake with date/hour/route partitioning
- Uses APScheduler for job scheduling
- MD5-based feed deduplication (skips unchanged responses)

## Getting Started

### Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- Auckland Transport API key ([register here](https://dev-portal.at.govt.nz/))

### Installation

```bash
uv sync
```

Or with pip:

```bash
pip install -e .
```

### Configuration

Create a `.env` file in the project root:

```env
AT_API_KEY=your_api_key_here
```

Optional settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `POLL_INTERVAL_SECONDS` | `30` | Seconds between API requests |
| `DATA_PATH` | `data` | Directory for Delta Lake tables |

### Usage

```bash
uv run python main.py
```

The application runs continuously, polling the API on a schedule and writing
to Delta Lake tables. Every hour, a clean up task will de-duplicate and compact
the Delta Lake tables for final storage.

## Data Storage

### Output Structure

Data is stored as Delta Lake tables with Hive-style partitioning. The
storage location is configured via the `DATA_PATH` environment variable
(defaults to `data/`).

```
$DATA_PATH/
├── vehicle_positions/
│   ├── _delta_log/
│   └── feed_date=2025-01-15/
│       └── feed_hour=14/
│           └── route_id=123/
│               └── *.parquet
└── trip_updates/
    ├── _delta_log/
    └── ...
```

### Why Delta Lake?

Delta Lake is a storage layer on top of Parquet that adds:

- **ACID transactions** - atomic writes, no partial failures or corruption
- **Immediate durability** - data is safe on disk within seconds of ingestion
- **Built-in compaction** - consolidate small files via `OPTIMIZE`
- **Schema enforcement** - prevents accidental schema drift
- **Time travel** - query historical versions if needed

### Data Cleanup

An hourly cleanup job (not yet implemented) will:

1. Deduplicate rows (the AT API returns ~50% stale data each poll)
2. Compact small files into larger ones
3. Optionally sync to S3 for durable archive

Until cleanup runs, expect temporary duplicates and many small files.

## Data Dictionary

### vehicle_positions

One row per vehicle position update.

| Column | Type | Description |
|--------|------|-------------|
| `poll_time` | timestamp(s, UTC) | When the API was polled |
| `feed_timestamp` | timestamp(s, UTC) | Timestamp from the vehicle position |
| `vehicle_id` | string | Unique vehicle identifier |
| `label` | string | Vehicle label (e.g., fleet number) |
| `license_plate` | string | Vehicle license plate |
| `trip_id` | string | GTFS trip identifier |
| `route_id` | string | GTFS route identifier |
| `direction_id` | int32 | Direction of travel (0 or 1) |
| `schedule_relationship` | int32 | Trip schedule status |
| `start_date` | string | Trip start date (YYYYMMDD) |
| `start_time` | string | Trip start time (HH:MM:SS) |
| `latitude` | float64 | Vehicle latitude |
| `longitude` | float64 | Vehicle longitude |
| `bearing` | float32 | Vehicle heading (degrees) |
| `speed` | float32 | Vehicle speed (m/s) |
| `odometer` | float64 | Odometer reading (metres) |
| `occupancy_status` | int32 | Vehicle occupancy level |
| `entity_is_deleted` | bool | Whether entity was marked deleted |
| `feed_date` | string | Derived: date for partitioning |
| `feed_hour` | int32 | Derived: hour for partitioning |

**Dedupe key:** `(vehicle_id, feed_timestamp)`

**Partitioned by:** `(feed_date, feed_hour, route_id)`

### trip_updates

One row per stop time update. Denormalized with trip and vehicle info.

| Column | Type | Description |
|--------|------|-------------|
| `poll_time` | timestamp(s, UTC) | When the API was polled |
| `feed_timestamp` | timestamp(s, UTC) | Timestamp from the trip update |
| `vehicle_id` | string | Vehicle identifier |
| `label` | string | Vehicle label |
| `license_plate` | string | Vehicle license plate |
| `trip_id` | string | GTFS trip identifier |
| `route_id` | string | GTFS route identifier |
| `direction_id` | int32 | Direction of travel (0 or 1) |
| `schedule_relationship` | int32 | Trip schedule status |
| `start_date` | string | Trip start date (YYYYMMDD) |
| `start_time` | string | Trip start time (HH:MM:SS) |
| `delay` | int32 | Trip-level delay in seconds |
| `stop_sequence` | int32 | Order of stop in trip |
| `stop_id` | string | GTFS stop identifier |
| `stop_schedule_relationship` | int32 | Stop status (SCHEDULED, SKIPPED, etc.) |
| `arrival_delay` | int32 | Arrival delay in seconds (negative = early) |
| `arrival_time` | int64 | Predicted/actual arrival time (Unix timestamp) |
| `arrival_uncertainty` | int32 | Arrival prediction uncertainty |
| `departure_delay` | int32 | Departure delay in seconds |
| `departure_time` | int64 | Predicted/actual departure time (Unix timestamp) |
| `departure_uncertainty` | int32 | Departure prediction uncertainty |
| `entity_is_deleted` | bool | Whether entity was marked deleted |
| `feed_date` | string | Derived: date for partitioning |
| `feed_hour` | int32 | Derived: hour for partitioning |

**Dedupe key:** `(trip_id, start_date, stop_sequence, feed_timestamp)`

**Partitioned by:** `(feed_date, feed_hour, route_id)`

## Reading the Data

Delta Lake stores data as standard Parquet files, so you can read them
directly with any Parquet-compatible tool.

### DuckDB

```python
import duckdb

# Use glob patterns to target specific partitions, reducing I/O.
# Select only the columns you need for faster reads.
duckdb.sql("""
    SELECT vehicle_id, latitude, longitude
    FROM read_parquet('data/vehicle_positions/feed_date=2025-01-15/**/*.parquet')
""")
```

### Polars

```python
import polars as pl

# scan_parquet is lazy - filters and column selection are pushed down,
# so only matching partitions and requested columns are read from disk.
df = (
    pl.scan_parquet("data/vehicle_positions/**/route_id=101/*.parquet")
    .select(["vehicle_id", "latitude", "longitude", "feed_timestamp"])
    .filter(pl.col("feed_hour") == 14)
    .collect()
)
```

### pandas

```python
import pandas as pd

# Read a specific partition with selected columns.
df = pd.read_parquet(
    "data/vehicle_positions/feed_date=2025-01-15/feed_hour=14",
    columns=["vehicle_id", "latitude", "longitude"],
)
```

## Technical Reference

### API Rate Limits

Auckland Transport's API has a rate limit of approximately 3.4 requests per
minute. To stay within this limit while collecting multiple entity types
(vehicle positions, trip updates, ferry positions, service alerts) every 30s,
we use the **combined feed endpoint** (`/realtime/legacy/`) which returns all
entity types in a single response.

### Project Structure

```
app/
├── config.py           # Configuration and environment variables
├── logging_config.py   # Logging setup
├── columns.py          # Column definitions and schema builder
├── ingest.py           # Feed fetcher, base class, entity classes
└── utils.py            # Utility functions
main.py                 # Entry point
```

### Architecture

**CombinedFeedFetcher** handles HTTP requests to the combined feed endpoint,
MD5-based deduplication, and protobuf decoding.

**Ingest** is the base class for entity processors. Each subclass defines:
- PyArrow schema
- Partition columns
- `normalise()` method to parse protobuf entities into rows

**Entity classes** (e.g., `VehiclePositions`, `TripUpdates`) implement
entity-specific parsing logic and write to separate Delta Lake tables.

## License

MIT
