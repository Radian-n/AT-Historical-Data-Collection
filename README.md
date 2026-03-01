# AT Historical Data Collection

Collects real-time and static GTFS data from the Auckland Transport API and
stores it in Delta Lake tables for historical analysis.

## Features

- Collects vehicle positions, trip updates, and stop time updates from AT's
  GTFS-Realtime API
- Collects versioned GTFS static data (routes, stops, trips, schedules, etc.)
- Two-layer storage architecture:
  - **Raw layer**: Captures all data with minimal filtering
  - **Processed layer**: Deduplicated and transformed outputs
- Hourly compaction reduces file count and enforces retention
- Daily processing transforms raw data into clean, deduplicated outputs
- Uses APScheduler for job scheduling
- MD5-based feed deduplication (skips unchanged responses)
- Efficient static data change detection via If-Modified-Since/If-None-Match
  headers

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
| `POLL_INTERVAL_SECONDS` | `30` | Seconds between realtime API requests |
| `RAW_RETENTION_DAYS` | `7` | Days to retain raw data before deletion |
| `PROCESSING_HOUR_NZT` | `4` | Hour of day (NZ time) to run daily processing |
| `STATIC_INGEST_HOUR_NZT` | `3` | Hour of day (NZ time) to check for static updates |
| `GTFS_STATIC_URL` | `https://gtfs.at.govt.nz/gtfs.zip` | GTFS static data endpoint |
| `DATA_PATH` | `data` | Directory for Delta Lake tables |

### Usage

The application runs continuously with three scheduled tasks:
- **Realtime ingest** (every 30s): Polls API and writes to raw layer
- **Compaction** (hourly): Consolidates files and enforces retention
- **Processing** (daily): Transforms raw data into deduplicated outputs

The app can be run locally as is, or via docker. Docker is reccomended, 
as the ingestion app is deployed via docker container.

#### Local

```bash
uv run python main.py
```

#### Docker

```bash
docker compose build
```

```bash
docker compose up
```

## Data Storage

### Output Structure

Data is stored as Delta Lake tables with Hive-style partitioning. The
storage location is configured via the `DATA_PATH` environment variable
(defaults to `data/`).

Realtime data uses a two-layer architecture:
- **Raw layer** (`data/raw/`): All captured data, retained for 7 days
- **Processed layer** (`data/processed/`): Deduplicated and transformed outputs

```
$DATA_PATH/
├── raw/                          # Raw realtime data (7-day retention)
│   ├── vehicle_positions/
│   │   ├── _delta_log/
│   │   └── start_date=20250115/
│   │       └── route_id=123/
│   │           └── *.parquet
│   ├── trip_updates/
│   │   └── ...
│   └── stop_time_updates/        # Includes predictions
│       └── ...
├── processed/                    # Deduplicated outputs
│   ├── vehicle_positions/        # Dedupe by (vehicle_id, feed_timestamp)
│   │   └── ...
│   ├── trip_updates/             # Latest state per (trip_id, start_date)
│   │   └── ...
│   └── stop_time_events/         # Merged arrival/departure, predictions filtered
│       └── ...
└── static/
    ├── static_metadata.json
    ├── agency/
    │   ├── _delta_log/
    │   └── valid_from=20250115/
    │       └── *.parquet
    ├── stops/
    ├── routes/
    ├── trips/
    ├── stop_times/
    ├── calendar/
    ├── calendar_dates/
    ├── shapes/
    └── ...
```

### Why Delta Lake?

Delta Lake is a storage layer on top of Parquet that adds:

- **ACID transactions** - atomic writes, no partial failures or corruption
- **Immediate durability** - data is safe on disk within seconds of ingestion
- **Built-in compaction** - consolidate small files via `OPTIMIZE`
- **Schema enforcement** - prevents accidental schema drift
- **Time travel** - query historical versions if needed

### Compaction (Hourly)

Compaction runs hourly (at minute 20) to maintain the raw layer:

1. **Consolidates files** - Runs Delta OPTIMIZE to merge small files into
   larger ones for better query performance.
2. **Enforces retention** - Deletes partitions older than `RAW_RETENTION_DAYS`
   (default 7 days) to bound storage growth.
3. **Vacuums** - Physically removes unreferenced files from disk.

### Processing (Daily)

Processing runs daily at `PROCESSING_HOUR_NZT` (default 4:00 AM NZ time) to
transform the previous day's raw data into deduplicated outputs:

1. **vehicle_positions** - Deduplicated by (vehicle_id, feed_timestamp).
   One row per unique vehicle observation.
2. **trip_updates** - Keeps latest observation per (trip_id, start_date).
   Preserves final trip state (important for cancellations).
3. **stop_time_events** - Filters predictions (uncertainty != 0) and merges
   separate arrival/departure rows into single events per stop.

The 4am NZ time default provides a buffer after the end of the operational
day to ensure all trips have completed before processing begins. The
scheduler handles daylight saving (NZST/NZDT) transitions automatically.

## Data Dictionary

### vehicle_positions

One row per vehicle position update.

| Column | Type | Description |
|--------|------|-------------|
| `poll_time` | timestamp(s, UTC) | When the API was polled |
| `feed_timestamp` | timestamp(s, UTC) | Timestamp from the vehicle |
| `vehicle_id` | string | Unique vehicle identifier |
| `label` | string | Vehicle label (e.g., fleet number) |
| `license_plate` | string | Vehicle license plate |
| `trip_id` | string | GTFS trip identifier |
| `route_id` | string | GTFS route identifier |
| `direction_id` | int32 | Direction of travel (0 or 1) |
| `schedule_relationship` | int32 | Trip schedule status |
| `start_date` | string | Trip start date (YYYYMMDD, "UNKNOWN" if no trip) |
| `start_time` | string | Trip start time (HH:MM:SS) |
| `latitude` | float64 | Vehicle latitude |
| `longitude` | float64 | Vehicle longitude |
| `bearing` | float32 | Vehicle heading (degrees) |
| `speed` | float32 | Vehicle speed (m/s) |
| `odometer` | float64 | Odometer reading (metres) |
| `occupancy_status` | int32 | Vehicle occupancy level |
| `entity_is_deleted` | bool | Whether entity was marked deleted |

**Dedupe key:** `(vehicle_id, feed_timestamp)`

**Partitioned by:** `(start_date, route_id)`

### trip_updates

One row per trip. Captures trip-level status (especially cancellations).

| Column | Type | Description |
|--------|------|-------------|
| `poll_time` | timestamp(s, UTC) | When the API was polled |
| `feed_timestamp` | timestamp(s, UTC) | Timestamp from the trip update |
| `trip_id` | string | GTFS trip identifier |
| `route_id` | string | GTFS route identifier |
| `direction_id` | int32 | Direction of travel (0 or 1) |
| `schedule_relationship` | int32 | Trip schedule status (CANCELED, etc.) |
| `start_date` | string | Trip start date (YYYYMMDD) |
| `start_time` | string | Trip start time (HH:MM:SS) |
| `vehicle_id` | string | Assigned vehicle identifier |
| `label` | string | Vehicle label |
| `license_plate` | string | Vehicle license plate |
| `entity_is_deleted` | bool | Whether entity was marked deleted |

**Dedupe key:** `(trip_id, start_date)` - keeps latest feed_timestamp

**Partitioned by:** `(start_date, route_id)`

### stop_time_updates (raw layer)

One row per stop time update. Captures all updates including predictions.

| Column | Type | Description |
|--------|------|-------------|
| `poll_time` | timestamp(s, UTC) | When the API was polled |
| `feed_timestamp` | timestamp(s, UTC) | Timestamp from the trip update |
| `trip_id` | string | GTFS trip identifier |
| `start_date` | string | Trip start date (YYYYMMDD) |
| `route_id` | string | GTFS route identifier |
| `stop_sequence` | int32 | Order of stop in trip |
| `stop_id` | string | GTFS stop identifier |
| `stop_schedule_relationship` | int32 | Stop status (SCHEDULED, SKIPPED) |
| `arrival_delay` | int32 | Arrival delay in seconds (negative = early) |
| `arrival_time` | int64 | Actual arrival time (Unix timestamp) |
| `arrival_uncertainty` | int32 | Arrival uncertainty (0 = confirmed) |
| `departure_delay` | int32 | Departure delay in seconds |
| `departure_time` | int64 | Actual departure time (Unix timestamp) |
| `departure_uncertainty` | int32 | Departure uncertainty (0 = confirmed) |

**Partitioned by:** `(start_date, route_id)`

### stop_time_events (processed layer)

One row per stop per trip. Predictions filtered, arrival/departure merged.

Same schema as stop_time_updates, but:
- Only confirmed times (uncertainty = 0) are included
- Arrival and departure rows merged into single events

**Dedupe key:** `(trip_id, start_date, stop_sequence, stop_id)`

**Partitioned by:** `(start_date, route_id)`

## Reading the Data

Delta Lake stores data as standard Parquet files, so you can read them
directly with any Parquet-compatible tool.

**Which layer to query:**
- **Processed layer** (`data/processed/`): Use for analysis. Deduplicated,
  clean data with one row per logical event.
- **Raw layer** (`data/raw/`): Use for debugging or when you need prediction
  data. Contains duplicates and all captured updates.

### DuckDB

```python
import duckdb

# Query processed data (deduplicated)
duckdb.sql("""
    SELECT vehicle_id, latitude, longitude
    FROM read_parquet('data/processed/vehicle_positions/start_date=20250115/**/*.parquet')
""")

# Query raw data (includes duplicates)
duckdb.sql("""
    SELECT vehicle_id, latitude, longitude, poll_time
    FROM read_parquet('data/raw/vehicle_positions/start_date=20250115/**/*.parquet')
""")
```

### Polars

```python
import polars as pl

# scan_parquet is lazy - filters and column selection are pushed down.
df = (
    pl.scan_parquet("data/processed/vehicle_positions/**/route_id=101/*.parquet")
    .select(["vehicle_id", "latitude", "longitude", "feed_timestamp"])
    .collect()
)
```

### pandas

```python
import pandas as pd

# Read a specific partition with selected columns.
df = pd.read_parquet(
    "data/processed/vehicle_positions/start_date=20250115/route_id=101",
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
├── realtime_ingest.py  # Realtime feed fetcher, base class, entity classes
├── compaction.py       # Hourly Delta OPTIMIZE and retention cleanup
├── processing.py       # Daily raw -> processed transformations
├── static_ingest.py    # Static data fetcher, base class, entity classes
├── columns.py          # Column definitions, schema builder, dedupe keys
├── config.py           # Configuration and environment variables
├── logging_config.py   # Logging setup
└── utils.py            # Utility functions
tests/
├── conftest.py         # Pytest fixtures and test utilities
├── test_compaction.py  # Compaction module tests
└── test_processing.py  # Processing module tests
main.py                 # Entry point
```

### Architecture

#### Data Flow

```
API (30s) -> raw ingest (stale filter) -> data/raw/
Hourly    -> compaction (OPTIMIZE + vacuum) + retention cleanup
Daily     -> processing (dedupe + transform) -> data/processed/
```

#### Realtime Data (GTFS-Realtime)

**CombinedFeedFetcher** handles HTTP requests to the combined feed endpoint,
MD5-based deduplication, and protobuf decoding.

**Ingest** is the base class for entity processors. Each subclass defines:
- PyArrow schema
- Partition columns
- `normalise()` method to parse protobuf entities into rows

**Entity classes** (`VehiclePositions`, `TripUpdates`, `StopTimeUpdates`)
implement entity-specific parsing logic and write to the raw layer.

**Compaction** (`app/compaction.py`) consolidates files hourly and enforces
retention by deleting old partitions.

**Processing** (`app/processing.py`) runs daily to transform raw data into
deduplicated outputs in the processed layer.

#### Static Data (GTFS Static)

**GTFSStaticFetcher** handles HTTP requests to the GTFS static endpoint using
conditional requests (If-Modified-Since, If-None-Match headers) to avoid
downloading unchanged files.

**StaticDataIngest** is the base class for GTFS static CSV file processors.
Each subclass defines:
- Entity name (derives CSV filename and write path)
- PyArrow schema
- Partition columns (valid_from)
- Optional `transform()` method for custom data transformations

**Static entity classes** (AgencyData, StopsData, RoutesData, TripsData,
StopTimesData, CalendarData, CalendarDatesData, ShapesData, etc.) implement
file-specific schemas and write versioned data to separate Delta Lake tables
with `valid_from`, `valid_to`, `feed_version`, and `downloaded_at` columns
for temporal queries.

## Development

```bash
# Install pre-commit hooks (runs ruff on commit)
uv run pre-commit install
```

## Testing

```bash
# Install test dependencies
uv sync --extra test

# Run all tests
uv run pytest

# Run only unit tests (fast, no external dependencies)
uv run pytest -m unit

# Run with coverage
uv run pytest --cov=app
```

## License

MIT
