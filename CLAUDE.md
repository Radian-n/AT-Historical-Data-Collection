# Project Context

When working with this codebase, prioritize readability over cleverness. Ask
clarifying questions before making architectural changes.

## About This Project

Collects real-time and static GTFS data from the Auckland Transport API and
stores it in Delta Lake tables for historical analysis.

Features:
- Collects vehicle positions, trip updates, and stop time updates from AT's
  GTFS-Realtime API
- Collects versioned GTFS static data (routes, stops, trips, schedules, etc.)
- Two-layer storage: raw data capture + processed/deduplicated outputs
- Hourly compaction reduces file count and enforces retention
- Daily processing transforms raw data into deduplicated outputs
- Daily GTFS static data check using HTTP conditional requests
- Uses APScheduler for job scheduling
- MD5-based feed deduplication (skips unchanged responses)
- Efficient static data change detection via If-Modified-Since/If-None-Match
  headers

## Key Files

- `main.py` - entry point, scheduler setup
- `app/realtime_ingest.py` - CombinedFeedFetcher, Ingest base class, entity
  classes (VehiclePositions, TripUpdates, StopTimeUpdates)
- `app/compaction.py` - hourly Delta OPTIMIZE and retention cleanup
- `app/processing.py` - daily raw -> processed transformations
- `app/static_ingest.py` - GTFSStaticFetcher, StaticDataIngest base class,
  static data entity classes (routes, stops, trips, etc.)
- `app/config.py` - configuration and environment variables
- `app/columns.py` - column definitions, schema builder, and dedupe keys
- `app/cleanup.py` - legacy cleanup module (deprecated, kept for reference)
- `app/utils.py` - utility functions
- `tests/conftest.py` - pytest fixtures and test utilities

## Architecture

### Data Flow

```
API (30s) -> raw ingest (stale filter) -> data/raw/
Hourly    -> compaction (OPTIMIZE + vacuum) + retention cleanup
Daily     -> processing (dedupe + transform) -> data/processed/
```

### Real-time Data (GTFS-Realtime)

**CombinedFeedFetcher** handles HTTP requests to the combined feed endpoint,
MD5-based deduplication, and protobuf decoding.

**Ingest** is the base class for entity processors. Each subclass defines:
- PyArrow schema
- Partition columns
- `normalise()` method to parse protobuf entities into rows

**Entity classes** (`VehiclePositions`, `TripUpdates`, `StopTimeUpdates`)
implement entity-specific parsing logic and write to `data/raw/`.

**Raw layer** captures all data including predictions (uncertainty filter
removed from ingest). Only stale data (>15 min old) is filtered at ingest.

**Compaction** (`app/compaction.py`) runs hourly to:
- Run Delta OPTIMIZE to consolidate small files
- Delete partitions older than retention period (default 7 days)
- Vacuum unreferenced files

**Processing** (`app/processing.py`) runs daily to transform raw -> processed:
- `vehicle_positions`: dedupe by (vehicle_id, feed_timestamp)
- `trip_updates`: keep latest per (trip_id, start_date)
- `stop_time_events`: filter predictions + merge arrival/departure rows

### Static Data (GTFS Static)

**GTFSStaticFetcher** handles HTTP requests to the GTFS static endpoint using
conditional requests (If-Modified-Since, If-None-Match headers) to avoid
downloading unchanged files. Stores metadata about the last successful fetch
to enable efficient change detection.

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

## Standards

- Type hints required on all functions
- Type hints required on all variables
- PEP 8 with 79 character lines for code, 72 characters for comments/docstrings
- Format code with `uv run ruff format <file>`

## Common Commands

- `uv run python main.py` - run the application
- `uv sync --extra test` - install test dependencies
- `uv run pytest` - run all tests
- `uv run pytest -m unit` - run unit tests only
- `uv run ruff format .` - format all files
- `uv run ruff format <file>` - format a specific file

## Configuration

Create a `.env` file in the project root:

```env
AT_API_KEY=your_api_key_here
```

Optional settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `POLL_INTERVAL_SECONDS` | `30` | Seconds between realtime API requests |
| `RAW_RETENTION_DAYS` | `7` | Days to retain raw data before deletion |
| `PROCESSING_DELAY_HOURS` | `12` | Hour (UTC) to run daily processing |
| `STATIC_INGEST_HOUR` | `15` | Hour (UTC) to check for GTFS static updates |
| `GTFS_STATIC_URL` | `https://gtfs.at.govt.nz/gtfs.zip` | GTFS static data endpoint URL |
| `DATA_PATH` | `data` | Directory for Delta Lake tables |

## Testing

- Framework: pytest (config in `pyproject.toml`)
- Test dependencies: `uv sync --extra test`
- Tests are in `tests/` directory
- Use `@pytest.mark.unit` for fast tests with no external dependencies
- Use `tmp_path` fixture for file I/O tests (creates isolated directories)
- Time-dependent functions accept a `now` parameter for testability
- Path-dependent functions accept a `data_path` parameter for testability

## Notes
None
