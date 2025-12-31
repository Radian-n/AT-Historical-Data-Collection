"""Tests for entity normalisation logic."""

from datetime import datetime, timezone

import pytest
from google.transit import gtfs_realtime_pb2

from app.const import Columns
from app.entities.vehicle_positions import VehiclePositionEntity

pytestmark = pytest.mark.unit


class TestVehiclePositionNormalise:
    """Tests for VehiclePositionEntity.normalise()."""

    def test_valid_vehicle_entity(
        self,
        sample_feed_message: gtfs_realtime_pb2.FeedMessage,
        sample_poll_time: datetime,
    ) -> None:
        """Valid vehicle entities are parsed into row dicts."""
        rows = VehiclePositionEntity.normalise(
            sample_feed_message, sample_poll_time
        )

        assert len(rows) == 2
        assert rows[0][Columns.VEHICLE_ID] == "vehicle_001"
        assert rows[1][Columns.VEHICLE_ID] == "vehicle_002"

    def test_poll_time_preserved(
        self,
        sample_feed_message: gtfs_realtime_pb2.FeedMessage,
        sample_poll_time: datetime,
    ) -> None:
        """Poll time is included in each row."""
        rows = VehiclePositionEntity.normalise(
            sample_feed_message, sample_poll_time
        )

        for row in rows:
            assert row[Columns.POLL_TIME] == sample_poll_time

    def test_feed_timestamp_converted(
        self,
        sample_feed_message: gtfs_realtime_pb2.FeedMessage,
        sample_poll_time: datetime,
        sample_feed_timestamp: int,
    ) -> None:
        """Feed timestamp is converted from unix to datetime."""
        rows = VehiclePositionEntity.normalise(
            sample_feed_message, sample_poll_time
        )

        expected_dt = datetime.fromtimestamp(
            sample_feed_timestamp, tz=timezone.utc
        )
        assert rows[0][Columns.FEED_TIMESTAMP] == expected_dt

    def test_empty_feed(self, sample_poll_time: datetime) -> None:
        """Empty feed returns empty list."""
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"

        rows = VehiclePositionEntity.normalise(feed, sample_poll_time)

        assert rows == []

    def test_non_vehicle_entity_skipped(
        self, sample_poll_time: datetime
    ) -> None:
        """Entities without vehicle field are skipped."""
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"

        # Add a trip_update entity (not a vehicle)
        entity = feed.entity.add()
        entity.id = "trip_entity"
        entity.trip_update.trip.trip_id = "trip_123"

        rows = VehiclePositionEntity.normalise(feed, sample_poll_time)

        assert rows == []

    def test_empty_string_fields_become_none(
        self, sample_poll_time: datetime
    ) -> None:
        """Empty string fields are converted to None."""
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"

        entity = feed.entity.add()
        entity.id = "entity_1"
        entity.vehicle.vehicle.id = "v1"
        entity.vehicle.vehicle.label = ""  # Empty string
        entity.vehicle.vehicle.license_plate = ""  # Empty string
        entity.vehicle.trip.trip_id = ""
        entity.vehicle.trip.route_id = ""
        entity.vehicle.position.latitude = -36.85
        entity.vehicle.position.longitude = 174.76
        entity.vehicle.timestamp = 1734258600

        rows = VehiclePositionEntity.normalise(feed, sample_poll_time)

        assert len(rows) == 1
        assert rows[0][Columns.LABEL] is None
        assert rows[0][Columns.LICENSE_PLATE] is None
        assert rows[0][Columns.TRIP_ID] is None
        assert rows[0][Columns.ROUTE_ID] is None

    def test_zero_numeric_fields_preserved(
        self, sample_poll_time: datetime
    ) -> None:
        """Zero values for numeric fields are preserved (not None)."""
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"

        entity = feed.entity.add()
        entity.id = "entity_1"
        entity.vehicle.vehicle.id = "v1"
        entity.vehicle.position.latitude = 0.0
        entity.vehicle.position.longitude = 0.0
        entity.vehicle.position.bearing = 0.0  # North
        entity.vehicle.position.speed = 0.0  # Stopped
        entity.vehicle.position.odometer = 0.0
        entity.vehicle.trip.direction_id = 0
        entity.vehicle.trip.schedule_relationship = 0
        entity.vehicle.occupancy_status = 0
        entity.vehicle.timestamp = 1734258600

        rows = VehiclePositionEntity.normalise(feed, sample_poll_time)

        assert len(rows) == 1
        assert rows[0][Columns.LATITUDE] == 0.0
        assert rows[0][Columns.LONGITUDE] == 0.0
        assert rows[0][Columns.BEARING] == 0.0
        assert rows[0][Columns.SPEED] == 0.0
        assert rows[0][Columns.ODOMETER] == 0.0
        assert rows[0][Columns.DIRECTION_ID] == 0
        assert rows[0][Columns.SCHEDULE_RELATIONSHIP] == 0
        assert rows[0][Columns.OCCUPANCY_STATUS] == 0

    def test_is_deleted_flag(self, sample_poll_time: datetime) -> None:
        """Entity is_deleted flag is captured."""
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"

        entity = feed.entity.add()
        entity.id = "entity_1"
        entity.is_deleted = True
        entity.vehicle.vehicle.id = "v1"
        entity.vehicle.position.latitude = -36.85
        entity.vehicle.position.longitude = 174.76
        entity.vehicle.timestamp = 1734258600

        rows = VehiclePositionEntity.normalise(feed, sample_poll_time)

        assert len(rows) == 1
        assert rows[0][Columns.ENTITY_IS_DELETED] is True

    def test_all_fields_extracted(
        self,
        sample_feed_message: gtfs_realtime_pb2.FeedMessage,
        sample_poll_time: datetime,
    ) -> None:
        """All expected fields are present in output."""
        rows = VehiclePositionEntity.normalise(
            sample_feed_message, sample_poll_time
        )

        expected_keys = {
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
            Columns.LATITUDE,
            Columns.LONGITUDE,
            Columns.BEARING,
            Columns.SPEED,
            Columns.ODOMETER,
            Columns.OCCUPANCY_STATUS,
            Columns.ENTITY_IS_DELETED,
        }

        assert set(rows[0].keys()) == expected_keys
