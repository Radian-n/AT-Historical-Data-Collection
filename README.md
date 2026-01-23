# AT Historical Data Collection

Collects real-time GTFS data from the Auckland Transport API and stores it
in Delta Lake tables for historical analysis.

## Features

- Collects vehicle positions, trip updates, and stop time updates from AT's
  GTFS-Realtime API
- Writes to Delta Lake with (start_date, route_id) partitioning
- Daily cleanup job deduplicates and compacts data
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
to Delta Lake tables. A daily cleanup task deduplicates and compacts each
day's data.

## Data Storage

### Output Structure

Data is stored as Delta Lake tables with Hive-style partitioning. The
storage location is configured via the `DATA_PATH` environment variable
(defaults to `data/`).

```
$DATA_PATH/
├── vehicle_positions/
│   ├── _delta_log/
│   └── start_date=20250115/
│       └── route_id=123/
│           └── *.parquet
├── trip_updates/
│   ├── _delta_log/
│   └── start_date=20250115/
│       └── ...
└── stop_time_updates/
    ├── _delta_log/
    └── start_date=20250115/
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

A daily cleanup job runs hourly (at minute 20) targeting the previous day's
data. Running hourly ensures cleanup happens soon after midnight while
remaining idempotent.

The cleanup job:

1. **Deduplicates rows** - Rows with identical dedupe keys are consolidated.
   For vehicle_positions: one row per (vehicle_id, feed_timestamp).
   For trip_updates: one row per (trip_id, start_date), keeping the latest.
   For stop_time_updates: merges separate arrival/departure observations.
2. **Compacts files** - The deduplicated data is written as a single parquet
   file per partition, replacing the many small files created during ingestion.
3. **Vacuums** - Physically deletes unreferenced files from disk.

Until cleanup runs for a given day, expect temporary duplicates and many
small files.

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

### stop_time_updates

One row per stop per trip. Arrival and departure data merged during cleanup.

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

**Dedupe key:** `(trip_id, start_date, stop_sequence)` - merges arrival/departure

**Partitioned by:** `(start_date, route_id)`

## Reading the Data

Delta Lake stores data as standard Parquet files, so you can read them
directly with any Parquet-compatible tool.

### DuckDB

```python
import duckdb

# Use glob patterns to target specific partitions, reducing I/O.
duckdb.sql("""
    SELECT vehicle_id, latitude, longitude
    FROM read_parquet('data/vehicle_positions/start_date=20250115/**/*.parquet')
""")
```

### Polars

```python
import polars as pl

# scan_parquet is lazy - filters and column selection are pushed down.
df = (
    pl.scan_parquet("data/vehicle_positions/**/route_id=101/*.parquet")
    .select(["vehicle_id", "latitude", "longitude", "feed_timestamp"])
    .collect()
)
```

### pandas

```python
import pandas as pd

# Read a specific partition with selected columns.
df = pd.read_parquet(
    "data/vehicle_positions/start_date=20250115/route_id=101",
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
├── cleanup.py          # Daily deduplication and compaction
├── columns.py          # Column definitions, schema builder, dedupe keys
├── config.py           # Configuration and environment variables
├── ingest.py           # Feed fetcher, base class, entity classes
├── logging_config.py   # Logging setup
└── utils.py            # Utility functions
tests/
├── conftest.py         # Pytest fixtures and test utilities
└── test_cleanup.py     # Cleanup module tests
main.py                 # Entry point
```

### Architecture

**CombinedFeedFetcher** handles HTTP requests to the combined feed endpoint,
MD5-based deduplication, and protobuf decoding.

**Ingest** is the base class for entity processors. Each subclass defines:
- PyArrow schema
- Partition columns
- `normalise()` method to parse protobuf entities into rows

**Entity classes** (`VehiclePositions`, `TripUpdates`, `StopTimeUpdates`)
implement entity-specific parsing logic and write to separate Delta Lake
tables.

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
