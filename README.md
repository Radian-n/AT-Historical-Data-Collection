# AT Historical Data Collection

Collects real-time GTFS data from the Auckland Transport API and stores it as partitioned Parquet files for historical analysis.

## Features

- Polls AT's GTFS-Realtime API for vehicle positions
- Deduplicates records within each collection window
- Writes hourly partitioned Parquet files
- Checkpoints buffer state for crash recovery
- Uses APScheduler for job scheduling

## Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- Auckland Transport API key ([register here](https://dev-portal.at.govt.nz/))

## Installation

```bash
uv sync
```

Or with pip:

```bash
pip install -e .
```

## Configuration

Create a `.env` file in the project root:

```env
AT_API_KEY=your_api_key_here
```

Optional settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `POLL_INTERVAL_SECONDS` | `30.0` | Seconds between API requests |
| `SAFE_DELAY_MINS` | `16.0` | Minutes to wait after hour ends before writing |

## Usage

```bash
uv run python main.py
```

The application runs continuously, polling the API on a schedule and writing hourly Parquet files to `data/`.

## Output Structure

```
data/
└── vehicle_positions/
    └── feed_date=2025-01-15/
        └── feed_hour=14/
            └── route_id=123/
                └── *.parquet
```

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
| `entity_id` | string | Entity ID from the feed |
| `entity_is_deleted` | bool | Whether entity was marked deleted |
| `trip_id` | string | GTFS trip identifier |
| `route_id` | string | GTFS route identifier |
| `direction_id` | int32 | Direction of travel (0 or 1) |
| `start_date` | string | Trip start date (YYYYMMDD) |
| `start_time` | string | Trip start time (HH:MM:SS) |
| `trip_schedule_relationship` | int32 | Trip schedule status |
| `vehicle_id` | string | Vehicle identifier |
| `vehicle_label` | string | Vehicle label |
| `license_plate` | string | Vehicle license plate |
| `stop_sequence` | int32 | Order of stop in trip |
| `stop_id` | string | GTFS stop identifier |
| `stop_schedule_relationship` | int32 | Stop status (SCHEDULED, SKIPPED, NO_DATA) |
| `arrival_delay` | int32 | Arrival delay in seconds (negative = early) |
| `arrival_time` | timestamp(s, UTC) | Predicted/actual arrival time |
| `departure_delay` | int32 | Departure delay in seconds |
| `departure_time` | timestamp(s, UTC) | Predicted/actual departure time |
| `feed_date` | string | Derived: date for partitioning |
| `feed_hour` | int32 | Derived: hour for partitioning |

**Dedupe key:** `(trip_id, start_date, stop_sequence, feed_timestamp)`

**Partitioned by:** `(feed_date, feed_hour, route_id)`

**Filtering actual vs predicted:** Compare `arrival_time`/`departure_time` with
`feed_timestamp`. If arrival/departure time <= feed_timestamp, the event has
occurred (actual). If > feed_timestamp, it's a prediction.

## Project Structure

```
app/
├── config.py           # Configuration and environment variables
├── logging_config.py   # Logging setup
├── pipeline.py         # RealtimePipeline class
└── entities/           # Entity definitions
    ├── base.py         # BaseEntity abstract class
    └── vehicle_positions.py  # VehiclePositionEntity
main.py                 # Entry point
```

### Architecture

**Entities** define the complete specification for a GTFS data type:
- Feed URL and table name
- Column definitions
- Protobuf parsing logic (`normalise`)
- Derived columns (`add_derived_columns`)
- Partitioning and deduplication keys
- PyArrow schema

**RealtimePipeline** is a generic, reusable pipeline that:
- Fetches data from the entity's URL
- Parses using the entity's `normalise` method
- Buffers and deduplicates data
- Writes partitioned Parquet files

To add a new entity type (e.g., trip updates), create a new entity class and add it to the scheduler in `main.py`.

## License

MIT
