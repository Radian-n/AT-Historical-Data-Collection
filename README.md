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
