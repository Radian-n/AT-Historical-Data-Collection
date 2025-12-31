"""Entity definition for vehicle positions data."""

import json
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from google.transit import gtfs_realtime_pb2

from app.const import Columns
from app.entities.base import BaseEntity


class VehiclePositionEntity(BaseEntity):
    """Complete entity definition for vehicle positions."""

    URL = "https://api.at.govt.nz/realtime/legacy/vehiclelocations"
    TABLE_NAME = "vehicle_positions"

    _schema: dict[Columns, pa.DataType] = {
        Columns.POLL_TIME: pa.timestamp("s", tz="+00:00"),
        Columns.FEED_TIMESTAMP: pa.timestamp("s", tz="+00:00"),
        Columns.VEHICLE_ID: pa.string(),
        Columns.LABEL: pa.string(),
        Columns.LICENSE_PLATE: pa.string(),
        Columns.TRIP_ID: pa.string(),
        Columns.ROUTE_ID: pa.string(),
        Columns.DIRECTION_ID: pa.int32(),
        Columns.SCHEDULE_RELATIONSHIP: pa.int32(),
        Columns.START_DATE: pa.string(),
        Columns.START_TIME: pa.string(),
        Columns.LATITUDE: pa.float64(),
        Columns.LONGITUDE: pa.float64(),
        Columns.BEARING: pa.float32(),
        Columns.SPEED: pa.float32(),
        Columns.ODOMETER: pa.float64(),
        Columns.OCCUPANCY_STATUS: pa.int32(),
        Columns.ENTITY_IS_DELETED: pa.bool_(),
    }

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
                Columns.POLL_TIME: poll_time,
                Columns.FEED_TIMESTAMP: datetime.fromtimestamp(
                    v.timestamp, tz=timezone.utc
                ),
                Columns.VEHICLE_ID: v.vehicle.id or None,
                Columns.LABEL: v.vehicle.label or None,
                Columns.LICENSE_PLATE: v.vehicle.license_plate or None,
                Columns.TRIP_ID: v.trip.trip_id or None,
                Columns.ROUTE_ID: v.trip.route_id or None,
                Columns.DIRECTION_ID: v.trip.direction_id,
                Columns.SCHEDULE_RELATIONSHIP: v.trip.schedule_relationship,
                Columns.START_DATE: v.trip.start_date or None,
                Columns.START_TIME: v.trip.start_time or None,
                Columns.LATITUDE: v.position.latitude,
                Columns.LONGITUDE: v.position.longitude,
                Columns.BEARING: v.position.bearing,
                Columns.SPEED: v.position.speed,
                Columns.ODOMETER: v.position.odometer,
                Columns.OCCUPANCY_STATUS: v.occupancy_status,
                Columns.ENTITY_IS_DELETED: entity.is_deleted,
            }
            rows.append(row)

        return rows

    @classmethod
    def dedupe_keys(cls) -> list[str]:
        """Return columns used to deduplicate vehicle position records."""
        return [Columns.VEHICLE_ID, Columns.FEED_TIMESTAMP]

    @classmethod
    def partition_cols(cls) -> list[str]:
        """Return columns used to partition parquet files."""
        return [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID]

    @classmethod
    def add_derived_columns(cls, table: pa.Table) -> pa.Table:
        """Derive date and hour columns from feed timestamp."""
        ts = table[Columns.FEED_TIMESTAMP]
        return (
            table
            .append_column(Columns.FEED_DATE, pc.strftime(ts, format="%Y-%m-%d"))
            .append_column(Columns.FEED_HOUR, pc.hour(ts))
        )

    @classmethod
    def pa_schema(cls) -> pa.Schema:
        """Return the PyArrow schema for vehicle positions."""
        partition_cols_json = json.dumps(cls.partition_cols()).encode("utf-8")
        fields = [pa.field(col, dtype) for col, dtype in cls._schema.items()]
        return pa.schema(
            fields,
            metadata={
                b"entity": b"vehicle_positions",
                b"version": b"1",
                b"partition_columns": partition_cols_json,
            },
        )
