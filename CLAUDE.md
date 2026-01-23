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
- Writes to Delta Lake with (start_date, route_id) partitioning (realtime) and
  version-based partitioning (static)
- Daily cleanup job deduplicates and compacts realtime data
- Daily GTFS static data check using HTTP conditional requests
- Uses APScheduler for job scheduling
- MD5-based feed deduplication (skips unchanged responses)
- Efficient static data change detection via If-Modified-Since/If-None-Match
  headers

## Key Files

- `main.py` - entry point, scheduler setup
- `app/ingest.py` - CombinedFeedFetcher, Ingest base class, entity classes
  (VehiclePositions, TripUpdates, StopTimeUpdates)
- `app/static_ingest.py` - GTFSStaticFetcher, StaticDataIngest base class,
  static data entity classes (routes, stops, trips, etc.)
- `app/config.py` - configuration and environment variables
- `app/columns.py` - column definitions, schema builder, and dedupe keys
- `app/cleanup.py` - daily deduplication and compaction (uses DuckDB)
- `app/utils.py` - utility functions
- `tests/conftest.py` - pytest fixtures and test utilities

## Architecture

### Real-time Data (GTFS-Realtime)

**CombinedFeedFetcher** handles HTTP requests to the combined feed endpoint,
MD5-based deduplication, and protobuf decoding.

**Ingest** is the base class for entity processors. Each subclass defines:
- PyArrow schema
- Partition columns
- `normalise()` method to parse protobuf entities into rows

**Entity classes** (`VehiclePositions`, `TripUpdates`, `StopTimeUpdates`)
implement entity-specific parsing logic and write to separate Delta Lake
tables.

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
| `STATIC_INGEST_HOUR` | `15` | Hour of day (UTC, 0-23) to check for GTFS static updates |
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
