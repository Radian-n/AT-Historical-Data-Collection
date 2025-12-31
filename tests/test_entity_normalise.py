"""Tests for entity normalisation logic."""

from datetime import datetime, timezone

import pytest
from google.transit import gtfs_realtime_pb2

from app.entities.vehicle_positions import VehiclePositionEntity


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
        assert rows[0][VehiclePositionEntity.VEHICLE_ID] == "vehicle_001"
        assert rows[1][VehiclePositionEntity.VEHICLE_ID] == "vehicle_002"

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
            assert row[VehiclePositionEntity.POLL_TIME] == sample_poll_time

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
        assert rows[0][VehiclePositionEntity.FEED_TIMESTAMP] == expected_dt

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
        assert rows[0][VehiclePositionEntity.LABEL] is None
        assert rows[0][VehiclePositionEntity.LICENSE_PLATE] is None
        assert rows[0][VehiclePositionEntity.TRIP_ID] is None
        assert rows[0][VehiclePositionEntity.ROUTE_ID] is None

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
        assert rows[0][VehiclePositionEntity.LATITUDE] == 0.0
        assert rows[0][VehiclePositionEntity.LONGITUDE] == 0.0
        assert rows[0][VehiclePositionEntity.BEARING] == 0.0
        assert rows[0][VehiclePositionEntity.SPEED] == 0.0
        assert rows[0][VehiclePositionEntity.ODOMETER] == 0.0
        assert rows[0][VehiclePositionEntity.DIRECTION_ID] == 0
        assert rows[0][VehiclePositionEntity.SCHEDULE_RELATIONSHIP] == 0
        assert rows[0][VehiclePositionEntity.OCCUPANCY_STATUS] == 0

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
        assert rows[0][VehiclePositionEntity.ENTITY_IS_DELETED] is True

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
            VehiclePositionEntity.POLL_TIME,
            VehiclePositionEntity.FEED_TIMESTAMP,
            VehiclePositionEntity.VEHICLE_ID,
            VehiclePositionEntity.LABEL,
            VehiclePositionEntity.LICENSE_PLATE,
            VehiclePositionEntity.TRIP_ID,
            VehiclePositionEntity.ROUTE_ID,
            VehiclePositionEntity.DIRECTION_ID,
            VehiclePositionEntity.SCHEDULE_RELATIONSHIP,
            VehiclePositionEntity.START_DATE,
            VehiclePositionEntity.START_TIME,
            VehiclePositionEntity.LATITUDE,
            VehiclePositionEntity.LONGITUDE,
            VehiclePositionEntity.BEARING,
            VehiclePositionEntity.SPEED,
            VehiclePositionEntity.ODOMETER,
            VehiclePositionEntity.OCCUPANCY_STATUS,
            VehiclePositionEntity.ENTITY_IS_DELETED,
        }

        assert set(rows[0].keys()) == expected_keys
