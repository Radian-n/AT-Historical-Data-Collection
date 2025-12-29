"""Schema definition for vehicle positions data."""

import json

import pyarrow as pa

from app.schemas.base import BaseColumns


class VehiclePositionColumns(BaseColumns):
    """Column definitions and schema for vehicle positions table."""

    POLL_TIME = "poll_time"
    FEED_TIMESTAMP = "feed_timestamp"
    VEHICLE_ID = "vehicle_id"
    LABEL = "label"
    LICENSE_PLATE = "license_plate"
    TRIP_ID = "trip_id"
    ROUTE_ID = "route_id"
    DIRECTION_ID = "direction_id"
    SCHEDULE_RELATIONSHIP = "schedule_relationship"
    START_DATE = "start_date"
    START_TIME = "start_time"
    LATITUDE = "latitude"
    LONGITUDE = "longitude"
    BEARING = "bearing"
    SPEED = "speed"
    ODOMETER = "odometer"
    OCCUPANCY_STATUS = "occupancy_status"
    ENTITY_IS_DELETED = "entity_is_deleted"
    # Derived
    FEED_DATE = "feed_date"
    FEED_HOUR = "feed_hour"

    @classmethod
    def dedupe_keys(cls) -> list[str]:
        """Return columns used to deduplicate vehicle position records."""
        return [cls.VEHICLE_ID, cls.FEED_TIMESTAMP]

    @classmethod
    def partition_cols(cls) -> list[str]:
        """Return columns used to partition parquet files."""
        return [cls.FEED_DATE, cls.FEED_HOUR, cls.ROUTE_ID]

    @classmethod
    def schema(cls) -> pa.Schema:
        """Return the PyArrow schema for vehicle positions."""
        partition_cols_json = json.dumps(cls.partition_cols()).encode("utf-8")
        return pa.schema(
            [
                # Timestamps
                pa.field(cls.POLL_TIME, pa.timestamp("s", tz="+00:00")),
                pa.field(cls.FEED_TIMESTAMP, pa.timestamp("s", tz="+00:00")),
                # Vehicle details
                pa.field(cls.VEHICLE_ID, pa.string()),
                pa.field(cls.LABEL, pa.string()),
                pa.field(cls.LICENSE_PLATE, pa.string()),
                # Foreign keys and supplementary info
                pa.field(cls.TRIP_ID, pa.string()),
                pa.field(cls.ROUTE_ID, pa.string()),
                pa.field(cls.DIRECTION_ID, pa.int32()),
                pa.field(cls.SCHEDULE_RELATIONSHIP, pa.int32()),
                pa.field(cls.START_DATE, pa.string()),
                pa.field(cls.START_TIME, pa.string()),
                # Data
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
