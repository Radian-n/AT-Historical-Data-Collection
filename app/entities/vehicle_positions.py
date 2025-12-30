"""Entity definition for vehicle positions data."""

import json
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from google.transit import gtfs_realtime_pb2

from app.entities.base import BaseEntity


class VehiclePositionEntity(BaseEntity):
    """Complete entity definition for vehicle positions."""

    URL = "https://api.at.govt.nz/realtime/legacy/vehiclelocations"
    TABLE_NAME = "vehicle_positions"

    # Timestamps
    POLL_TIME = "poll_time"
    FEED_TIMESTAMP = "feed_timestamp"

    # Vehicle details
    VEHICLE_ID = "vehicle_id"
    LABEL = "label"
    LICENSE_PLATE = "license_plate"

    # Trip/route info
    TRIP_ID = "trip_id"
    ROUTE_ID = "route_id"
    DIRECTION_ID = "direction_id"
    SCHEDULE_RELATIONSHIP = "schedule_relationship"
    START_DATE = "start_date"
    START_TIME = "start_time"

    # Position data
    LATITUDE = "latitude"
    LONGITUDE = "longitude"
    BEARING = "bearing"
    SPEED = "speed"
    ODOMETER = "odometer"
    OCCUPANCY_STATUS = "occupancy_status"
    ENTITY_IS_DELETED = "entity_is_deleted"

    # Derived (computed in add_derived_columns)
    FEED_DATE = "feed_date"
    FEED_HOUR = "feed_hour"

    @classmethod
    def normalise(
        cls,
        feed: gtfs_realtime_pb2.FeedMessage,
        poll_time: datetime,
    ) -> list[dict[str, Any]]:
        """Parse vehicle position entities from protobuf feed.

        String fields use `or None` to convert empty strings to None
        (protobuf returns "" for unset strings). Numeric fields do not
        use this pattern as 0 is a valid value (e.g., bearing=0 means
        north).
        """
        rows: list[dict[str, Any]] = []

        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue

            v = entity.vehicle
            row = {
                cls.POLL_TIME: poll_time,
                cls.FEED_TIMESTAMP: datetime.fromtimestamp(
                    v.timestamp, tz=timezone.utc
                ),
                cls.VEHICLE_ID: v.vehicle.id or None,
                cls.LABEL: v.vehicle.label or None,
                cls.LICENSE_PLATE: v.vehicle.license_plate or None,
                cls.TRIP_ID: v.trip.trip_id or None,
                cls.ROUTE_ID: v.trip.route_id or None,
                cls.DIRECTION_ID: v.trip.direction_id,
                cls.SCHEDULE_RELATIONSHIP: v.trip.schedule_relationship,
                cls.START_DATE: v.trip.start_date or None,
                cls.START_TIME: v.trip.start_time or None,
                cls.LATITUDE: v.position.latitude,
                cls.LONGITUDE: v.position.longitude,
                cls.BEARING: v.position.bearing,
                cls.SPEED: v.position.speed,
                cls.ODOMETER: v.position.odometer,
                cls.OCCUPANCY_STATUS: v.occupancy_status,
                cls.ENTITY_IS_DELETED: entity.is_deleted,
            }
            rows.append(row)

        return rows

    @classmethod
    def dedupe_keys(cls) -> list[str]:
        """Return columns used to deduplicate vehicle position records."""
        return [cls.VEHICLE_ID, cls.FEED_TIMESTAMP]

    @classmethod
    def partition_cols(cls) -> list[str]:
        """Return columns used to partition parquet files."""
        return [cls.FEED_DATE, cls.FEED_HOUR, cls.ROUTE_ID]

    @classmethod
    def add_derived_columns(cls, table: pa.Table) -> pa.Table:
        """Derive date and hour columns from feed timestamp."""
        ts = table[cls.FEED_TIMESTAMP]
        return (
            table
            .append_column(cls.FEED_DATE, pc.strftime(ts, format="%Y-%m-%d"))
            .append_column(cls.FEED_HOUR, pc.hour(ts))
        )

    @classmethod
    def pa_schema(cls) -> pa.Schema:
        """Return the PyArrow schema for vehicle positions."""
        partition_cols_json = json.dumps(cls.partition_cols()).encode("utf-8")
        return pa.schema(
            [
                pa.field(cls.POLL_TIME, pa.timestamp("s", tz="+00:00")),
                pa.field(cls.FEED_TIMESTAMP, pa.timestamp("s", tz="+00:00")),
                pa.field(cls.VEHICLE_ID, pa.string()),
                pa.field(cls.LABEL, pa.string()),
                pa.field(cls.LICENSE_PLATE, pa.string()),
                pa.field(cls.TRIP_ID, pa.string()),
                pa.field(cls.ROUTE_ID, pa.string()),
                pa.field(cls.DIRECTION_ID, pa.int32()),
                pa.field(cls.SCHEDULE_RELATIONSHIP, pa.int32()),
                pa.field(cls.START_DATE, pa.string()),
                pa.field(cls.START_TIME, pa.string()),
                pa.field(cls.LATITUDE, pa.float64()),
                pa.field(cls.LONGITUDE, pa.float64()),
                pa.field(cls.BEARING, pa.float32()),
                pa.field(cls.SPEED, pa.float32()),
                pa.field(cls.ODOMETER, pa.float64()),
                pa.field(cls.OCCUPANCY_STATUS, pa.int32()),
                pa.field(cls.ENTITY_IS_DELETED, pa.bool_()),
            ],
            metadata={
                b"entity": b"vehicle_positions",
                b"version": b"1",
                b"partition_columns": partition_cols_json,
            },
        )
