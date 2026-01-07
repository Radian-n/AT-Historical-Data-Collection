"""Pytest configuration and fixtures for testing."""

import os
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from app.columns import Columns, REALTIME_FIELD_TYPES, make_schema


def pytest_configure(config: pytest.Config) -> None:
    """Set required environment variables for tests."""
    os.environ.setdefault("AT_API_KEY", "test-api-key")


# Minimal schema for vehicle positions tests
VEHICLE_POSITIONS_TEST_SCHEMA = make_schema(
    columns=[
        Columns.POLL_TIME,
        Columns.FEED_TIMESTAMP,
        Columns.VEHICLE_ID,
        Columns.ROUTE_ID,
        Columns.LATITUDE,
        Columns.LONGITUDE,
        Columns.FEED_DATE,
        Columns.FEED_HOUR,
    ],
    field_types=REALTIME_FIELD_TYPES,
)

# Schema for trip updates tests (includes arrival/departure columns for merge)
TRIP_UPDATES_TEST_SCHEMA = make_schema(
    columns=[
        Columns.POLL_TIME,
        Columns.FEED_TIMESTAMP,
        Columns.VEHICLE_ID,
        Columns.LABEL,
        Columns.LICENSE_PLATE,
        Columns.TRIP_ID,
        Columns.ROUTE_ID,
        Columns.DIRECTION_ID,
        Columns.SCHEDULE_RELATIONSHIP,
        Columns.START_DATE,
        Columns.START_TIME,
        Columns.DELAY,
        Columns.STOP_SEQUENCE,
        Columns.STOP_ID,
        Columns.STOP_SCHEDULE_RELATIONSHIP,
        Columns.ARRIVAL_DELAY,
        Columns.ARRIVAL_TIME,
        Columns.ARRIVAL_UNCERTAINTY,
        Columns.DEPARTURE_DELAY,
        Columns.DEPARTURE_TIME,
        Columns.DEPARTURE_UNCERTAINTY,
        Columns.ENTITY_IS_DELETED,
        Columns.FEED_DATE,
        Columns.FEED_HOUR,
    ],
    field_types=REALTIME_FIELD_TYPES,
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
    """Sample trip updates data with separate arrival/departure rows.

    Contains 4 rows representing 2 stops, each with separate arrival and
    departure observations. After merge, should result in 2 rows with both
    arrival and departure data combined.
    """
    arrival_ts = datetime(2026, 1, 2, 10, 25, 0, tzinfo=timezone.utc)
    departure_ts = datetime(2026, 1, 2, 10, 26, 0, tzinfo=timezone.utc)

    # Base row data shared by all rows
    base: dict[str, Any] = {
        Columns.VEHICLE_ID: "vehicle_1",
        Columns.LABEL: "BUS001",
        Columns.LICENSE_PLATE: "ABC123",
        Columns.ROUTE_ID: "route_1",
        Columns.DIRECTION_ID: 0,
        Columns.SCHEDULE_RELATIONSHIP: 0,
        Columns.START_TIME: "10:00:00",
        Columns.DELAY: 60,
        Columns.STOP_SCHEDULE_RELATIONSHIP: 0,
        Columns.ENTITY_IS_DELETED: False,
        Columns.FEED_DATE: "2026-01-02",
        Columns.FEED_HOUR: 10,
    }

    return [
        # Stop 1: arrival row
        {
            **base,
            Columns.POLL_TIME: arrival_ts,
            Columns.FEED_TIMESTAMP: arrival_ts,
            Columns.TRIP_ID: "trip_A",
            Columns.START_DATE: "20260102",
            Columns.STOP_SEQUENCE: 1,
            Columns.STOP_ID: "stop_100",
            Columns.ARRIVAL_DELAY: 30,
            Columns.ARRIVAL_TIME: arrival_ts,
            Columns.ARRIVAL_UNCERTAINTY: 0,
            Columns.DEPARTURE_DELAY: None,
            Columns.DEPARTURE_TIME: None,
            Columns.DEPARTURE_UNCERTAINTY: None,
        },
        # Stop 1: departure row (different feed_timestamp)
        {
            **base,
            Columns.POLL_TIME: departure_ts,
            Columns.FEED_TIMESTAMP: departure_ts,
            Columns.TRIP_ID: "trip_A",
            Columns.START_DATE: "20260102",
            Columns.STOP_SEQUENCE: 1,
            Columns.STOP_ID: "stop_100",
            Columns.ARRIVAL_DELAY: None,
            Columns.ARRIVAL_TIME: None,
            Columns.ARRIVAL_UNCERTAINTY: None,
            Columns.DEPARTURE_DELAY: 45,
            Columns.DEPARTURE_TIME: departure_ts,
            Columns.DEPARTURE_UNCERTAINTY: 0,
        },
        # Stop 2: arrival row
        {
            **base,
            Columns.POLL_TIME: arrival_ts,
            Columns.FEED_TIMESTAMP: arrival_ts,
            Columns.TRIP_ID: "trip_A",
            Columns.START_DATE: "20260102",
            Columns.STOP_SEQUENCE: 2,
            Columns.STOP_ID: "stop_200",
            Columns.ARRIVAL_DELAY: 60,
            Columns.ARRIVAL_TIME: arrival_ts,
            Columns.ARRIVAL_UNCERTAINTY: 0,
            Columns.DEPARTURE_DELAY: None,
            Columns.DEPARTURE_TIME: None,
            Columns.DEPARTURE_UNCERTAINTY: None,
        },
        # Stop 2: departure row
        {
            **base,
            Columns.POLL_TIME: departure_ts,
            Columns.FEED_TIMESTAMP: departure_ts,
            Columns.TRIP_ID: "trip_A",
            Columns.START_DATE: "20260102",
            Columns.STOP_SEQUENCE: 2,
            Columns.STOP_ID: "stop_200",
            Columns.ARRIVAL_DELAY: None,
            Columns.ARRIVAL_TIME: None,
            Columns.ARRIVAL_UNCERTAINTY: None,
            Columns.DEPARTURE_DELAY: 75,
            Columns.DEPARTURE_TIME: departure_ts,
            Columns.DEPARTURE_UNCERTAINTY: 0,
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
