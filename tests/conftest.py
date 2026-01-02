"""Pytest configuration and fixtures for testing."""

import os
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from app.columns import Columns, make_schema


def pytest_configure(config: pytest.Config) -> None:
    """Set required environment variables for tests."""
    os.environ.setdefault("AT_API_KEY", "test-api-key")


# Minimal schema for vehicle positions tests
VEHICLE_POSITIONS_TEST_SCHEMA = make_schema(
    [
        Columns.POLL_TIME,
        Columns.FEED_TIMESTAMP,
        Columns.VEHICLE_ID,
        Columns.ROUTE_ID,
        Columns.LATITUDE,
        Columns.LONGITUDE,
        Columns.FEED_DATE,
        Columns.FEED_HOUR,
    ]
)

# Minimal schema for trip updates tests
TRIP_UPDATES_TEST_SCHEMA = make_schema(
    [
        Columns.POLL_TIME,
        Columns.FEED_TIMESTAMP,
        Columns.TRIP_ID,
        Columns.START_DATE,
        Columns.STOP_SEQUENCE,
        Columns.ROUTE_ID,
        Columns.FEED_DATE,
        Columns.FEED_HOUR,
    ]
)


@pytest.fixture
def sample_vehicle_positions_data() -> list[dict]:
    """Sample vehicle positions data with duplicates.

    Contains 4 rows with 2 unique (vehicle_id, feed_timestamp) combinations.
    After deduplication, should result in 2 rows.
    """
    base_ts = datetime(2026, 1, 2, 10, 30, 0, tzinfo=timezone.utc)
    feed_ts = datetime(2026, 1, 2, 10, 25, 0, tzinfo=timezone.utc)

    return [
        # First observation of vehicle A
        {
            Columns.POLL_TIME: base_ts,
            Columns.FEED_TIMESTAMP: feed_ts,
            Columns.VEHICLE_ID: "vehicle_A",
            Columns.ROUTE_ID: "route_1",
            Columns.LATITUDE: -36.8485,
            Columns.LONGITUDE: 174.7633,
            Columns.FEED_DATE: "2026-01-02",
            Columns.FEED_HOUR: 10,
        },
        # Duplicate of vehicle A (same vehicle_id + feed_timestamp)
        {
            Columns.POLL_TIME: datetime(
                2026, 1, 2, 10, 30, 30, tzinfo=timezone.utc
            ),
            Columns.FEED_TIMESTAMP: feed_ts,
            Columns.VEHICLE_ID: "vehicle_A",
            Columns.ROUTE_ID: "route_1",
            Columns.LATITUDE: -36.8485,
            Columns.LONGITUDE: 174.7633,
            Columns.FEED_DATE: "2026-01-02",
            Columns.FEED_HOUR: 10,
        },
        # First observation of vehicle B
        {
            Columns.POLL_TIME: base_ts,
            Columns.FEED_TIMESTAMP: feed_ts,
            Columns.VEHICLE_ID: "vehicle_B",
            Columns.ROUTE_ID: "route_2",
            Columns.LATITUDE: -36.8500,
            Columns.LONGITUDE: 174.7650,
            Columns.FEED_DATE: "2026-01-02",
            Columns.FEED_HOUR: 10,
        },
        # Duplicate of vehicle B
        {
            Columns.POLL_TIME: datetime(
                2026, 1, 2, 10, 30, 30, tzinfo=timezone.utc
            ),
            Columns.FEED_TIMESTAMP: feed_ts,
            Columns.VEHICLE_ID: "vehicle_B",
            Columns.ROUTE_ID: "route_2",
            Columns.LATITUDE: -36.8500,
            Columns.LONGITUDE: 174.7650,
            Columns.FEED_DATE: "2026-01-02",
            Columns.FEED_HOUR: 10,
        },
    ]


@pytest.fixture
def sample_trip_updates_data() -> list[dict]:
    """Sample trip updates data with duplicates.

    Contains 4 rows with 2 unique key combinations.
    After deduplication, should result in 2 rows.
    """
    base_ts = datetime(2026, 1, 2, 10, 30, 0, tzinfo=timezone.utc)
    feed_ts = datetime(2026, 1, 2, 10, 25, 0, tzinfo=timezone.utc)

    return [
        # First observation of trip A, stop 1
        {
            Columns.POLL_TIME: base_ts,
            Columns.FEED_TIMESTAMP: feed_ts,
            Columns.TRIP_ID: "trip_A",
            Columns.START_DATE: "20260102",
            Columns.STOP_SEQUENCE: 1,
            Columns.ROUTE_ID: "route_1",
            Columns.FEED_DATE: "2026-01-02",
            Columns.FEED_HOUR: 10,
        },
        # Duplicate of trip A, stop 1
        {
            Columns.POLL_TIME: datetime(
                2026, 1, 2, 10, 30, 30, tzinfo=timezone.utc
            ),
            Columns.FEED_TIMESTAMP: feed_ts,
            Columns.TRIP_ID: "trip_A",
            Columns.START_DATE: "20260102",
            Columns.STOP_SEQUENCE: 1,
            Columns.ROUTE_ID: "route_1",
            Columns.FEED_DATE: "2026-01-02",
            Columns.FEED_HOUR: 10,
        },
        # First observation of trip A, stop 2
        {
            Columns.POLL_TIME: base_ts,
            Columns.FEED_TIMESTAMP: feed_ts,
            Columns.TRIP_ID: "trip_A",
            Columns.START_DATE: "20260102",
            Columns.STOP_SEQUENCE: 2,
            Columns.ROUTE_ID: "route_1",
            Columns.FEED_DATE: "2026-01-02",
            Columns.FEED_HOUR: 10,
        },
        # Duplicate of trip A, stop 2
        {
            Columns.POLL_TIME: datetime(
                2026, 1, 2, 10, 30, 30, tzinfo=timezone.utc
            ),
            Columns.FEED_TIMESTAMP: feed_ts,
            Columns.TRIP_ID: "trip_A",
            Columns.START_DATE: "20260102",
            Columns.STOP_SEQUENCE: 2,
            Columns.ROUTE_ID: "route_1",
            Columns.FEED_DATE: "2026-01-02",
            Columns.FEED_HOUR: 10,
        },
    ]


def create_test_delta_table(
    path: str,
    data: list[dict],
    schema: pa.Schema,
    partition_cols: list[str],
) -> None:
    """Create a Delta Lake table with test data.

    Writes data in two batches to simulate multiple small files
    that would be created during normal ingestion.
    """
    # Split data into two batches to create multiple files
    mid: int = len(data) // 2
    batches: list[list[dict[Any, Any]]] = [data[:mid], data[mid:]]

    for batch in batches:
        table = pa.Table.from_pylist(batch, schema=schema)
        write_deltalake(
            path,
            table,
            partition_by=partition_cols,
            mode="append",
        )
