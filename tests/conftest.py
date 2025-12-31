"""Shared fixtures for tests."""

import os
from datetime import datetime, timezone
from typing import Any

import pytest
from google.transit import gtfs_realtime_pb2


def pytest_configure(config: pytest.Config) -> None:
    """Set up test environment before collection.

    This runs before any tests are collected, ensuring environment
    variables are set before app.config is imported.
    """
    os.environ.setdefault("AT_API_KEY", "test_api_key")


# Import after pytest_configure sets environment
from app.const import Columns  # noqa: E402
from app.entities.vehicle_positions import VehiclePositionEntity  # noqa: E402


@pytest.fixture
def vehicle_entity() -> type[VehiclePositionEntity]:
    """Return the VehiclePositionEntity class."""
    return VehiclePositionEntity


@pytest.fixture
def sample_poll_time() -> datetime:
    """Return a sample poll time for testing."""
    return datetime(2024, 12, 15, 10, 30, 45, tzinfo=timezone.utc)


@pytest.fixture
def sample_feed_timestamp() -> int:
    """Return a sample feed timestamp (unix seconds) for testing."""
    # 2024-12-15 10:30:00 UTC
    return 1734258600


@pytest.fixture
def sample_feed_message(sample_feed_timestamp: int) -> gtfs_realtime_pb2.FeedMessage:
    """Create a FeedMessage with sample vehicle position entities."""
    feed = gtfs_realtime_pb2.FeedMessage()

    # Set header
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = sample_feed_timestamp

    # Add first vehicle entity
    entity1 = feed.entity.add()
    entity1.id = "entity_1"
    entity1.is_deleted = False
    entity1.vehicle.vehicle.id = "vehicle_001"
    entity1.vehicle.vehicle.label = "Bus 101"
    entity1.vehicle.vehicle.license_plate = "ABC123"
    entity1.vehicle.trip.trip_id = "trip_100"
    entity1.vehicle.trip.route_id = "route_50"
    entity1.vehicle.trip.direction_id = 0
    entity1.vehicle.trip.schedule_relationship = 0
    entity1.vehicle.trip.start_date = "20241215"
    entity1.vehicle.trip.start_time = "10:00:00"
    entity1.vehicle.position.latitude = -36.8485
    entity1.vehicle.position.longitude = 174.7633
    entity1.vehicle.position.bearing = 90.0
    entity1.vehicle.position.speed = 12.5
    entity1.vehicle.position.odometer = 50000.0
    entity1.vehicle.timestamp = sample_feed_timestamp
    entity1.vehicle.occupancy_status = 1

    # Add second vehicle entity
    entity2 = feed.entity.add()
    entity2.id = "entity_2"
    entity2.is_deleted = False
    entity2.vehicle.vehicle.id = "vehicle_002"
    entity2.vehicle.vehicle.label = "Bus 202"
    entity2.vehicle.vehicle.license_plate = "XYZ789"
    entity2.vehicle.trip.trip_id = "trip_200"
    entity2.vehicle.trip.route_id = "route_75"
    entity2.vehicle.trip.direction_id = 1
    entity2.vehicle.trip.schedule_relationship = 0
    entity2.vehicle.trip.start_date = "20241215"
    entity2.vehicle.trip.start_time = "10:15:00"
    entity2.vehicle.position.latitude = -36.8600
    entity2.vehicle.position.longitude = 174.7700
    entity2.vehicle.position.bearing = 180.0
    entity2.vehicle.position.speed = 0.0  # Stopped
    entity2.vehicle.position.odometer = 75000.0
    entity2.vehicle.timestamp = sample_feed_timestamp + 30
    entity2.vehicle.occupancy_status = 2

    return feed


@pytest.fixture
def sample_row(sample_poll_time: datetime) -> dict[str, Any]:
    """Return a single normalized row dict."""
    return {
        Columns.POLL_TIME: sample_poll_time,
        Columns.FEED_TIMESTAMP: datetime(
            2024, 12, 15, 10, 30, 0, tzinfo=timezone.utc
        ),
        Columns.VEHICLE_ID: "vehicle_001",
        Columns.LABEL: "Bus 101",
        Columns.LICENSE_PLATE: "ABC123",
        Columns.TRIP_ID: "trip_100",
        Columns.ROUTE_ID: "route_50",
        Columns.DIRECTION_ID: 0,
        Columns.SCHEDULE_RELATIONSHIP: 0,
        Columns.START_DATE: "20241215",
        Columns.START_TIME: "10:00:00",
        Columns.LATITUDE: -36.8485,
        Columns.LONGITUDE: 174.7633,
        Columns.BEARING: 90.0,
        Columns.SPEED: 12.5,
        Columns.ODOMETER: 50000.0,
        Columns.OCCUPANCY_STATUS: 1,
        Columns.ENTITY_IS_DELETED: False,
    }


@pytest.fixture
def sample_rows(sample_row: dict[str, Any]) -> list[dict[str, Any]]:
    """Return a list of normalized row dicts."""
    row2 = sample_row.copy()
    row2[Columns.VEHICLE_ID] = "vehicle_002"
    row2[Columns.FEED_TIMESTAMP] = datetime(
        2024, 12, 15, 10, 30, 30, tzinfo=timezone.utc
    )
    row2[Columns.LABEL] = "Bus 202"
    row2[Columns.ROUTE_ID] = "route_75"

    row3 = sample_row.copy()
    row3[Columns.VEHICLE_ID] = "vehicle_003"
    row3[Columns.FEED_TIMESTAMP] = datetime(
        2024, 12, 15, 10, 31, 0, tzinfo=timezone.utc
    )
    row3[Columns.LABEL] = "Bus 303"
    row3[Columns.ROUTE_ID] = "route_100"

    return [sample_row, row2, row3]
