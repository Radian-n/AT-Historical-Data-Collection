"""Entity definition for trip updates data."""

import json
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from google.transit import gtfs_realtime_pb2

from app.const import Columns
from app.entities.base import BaseEntity


class TripUpdateEntity(BaseEntity):
    """Complete entity definition for vehicle positions."""

    URL = "https://api.at.govt.nz/realtime/legacy/tripupdates"
    TABLE_NAME = "trip_updates"

    _schema: dict[Columns, pa.DataType] = {
        # Timestamps
        Columns.POLL_TIME: pa.timestamp("s", tz="+00:00"),
        Columns.FEED_TIMESTAMP: pa.timestamp("s", tz="+00:00"),
        # Vehicle details
        Columns.VEHICLE_ID: pa.string(),
        Columns.LABEL: pa.string(),
        Columns.LICENSE_PLATE: pa.string(),
        # Trip/route info
        Columns.TRIP_ID: pa.string(),
        Columns.ROUTE_ID: pa.string(),
        Columns.DIRECTION_ID: pa.int32(),
        Columns.SCHEDULE_RELATIONSHIP: pa.int32(),
        Columns.START_DATE: pa.string(),
        Columns.START_TIME: pa.string(),
        # Stop update data
        Columns.STOP_SEQUENCE: pa.int32(),
        Columns.STOP_ID: pa.string(),
        Columns.STOP_SCHEDULE_RELATIONSHIP: pa.int32(),
        Columns.ARRIVAL_DELAY: pa.int32(),
        Columns.ARRIVAL_TIME: pa.timestamp("s", tz="+00:00"),
        Columns.ARRIVAL_UNCERTAINTY: pa.int32(),
        Columns.DEPARTURE_DELAY: pa.int32(),
        Columns.DEPARTURE_TIME: pa.timestamp("s", tz="+00:00"),
        Columns.DEPARTURE_UNCERTAINTY: pa.int32(),
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
            if not entity.HasField("trip_update"):
                continue

            e = entity.trip_update
            row = {
                # Timestamps
                Columns.POLL_TIME: poll_time,
                Columns.FEED_TIMESTAMP: datetime.fromtimestamp(
                    e.timestamp, tz=timezone.utc
                ),
                # Vehicle details
                Columns.VEHICLE_ID: e.vehicle.id or None,
                Columns.LABEL: e.vehicle.label or None,
                Columns.LICENSE_PLATE: e.vehicle.license_plate or None,
                # Trip/route info
                Columns.TRIP_ID: e.trip.trip_id or None,
                Columns.ROUTE_ID: e.trip.route_id or None,
                Columns.DIRECTION_ID: e.trip.direction_id,
                Columns.SCHEDULE_RELATIONSHIP: e.trip.schedule_relationship,
                Columns.START_DATE: e.trip.start_date or None,
                Columns.START_TIME: e.trip.start_time or None,
                # Stop update data
                Columns.STOP_SEQUENCE: e.stop_time_update.stop_sequence or None,
                Columns.STOP_ID: e.stop_time_update.stop_id,
                Columns.STOP_SCHEDULE_RELATIONSHIP:  e.stop_time_update.schedule_relationship,
                Columns.ARRIVAL_DELAY: e.stop_time_update.arrival.delay,
                Columns.ARRIVAL_TIME:  e.stop_time_update.arrival.time,
                Columns.ARRIVAL_UNCERTAINTY:  e.stop_time_update.arrival.uncertainty,
                Columns.DEPARTURE_DELAY: e.stop_time_update.departure.delay,
                Columns.DEPARTURE_TIME: e.stop_time_update.departure.time,
                Columns.DEPARTURE_UNCERTAINTY: e.stop_time_update.departure.uncertainty,
                Columns.ENTITY_IS_DELETED: entity.is_deleted,
            }

            # TODO: Handle case where trip has no 'stop_time_update attribute.

            rows.append(row)

        return rows

    @classmethod
    def dedupe_keys(cls) -> list[str]:
        """Return columns used to deduplicate vehicle position records."""
        return [Columns.TRIP_ID, Columns.START_DATE, Columns.STOP_SEQUENCE, Columns.FEED_TIMESTAMP]

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
