"""Column definitions and PyArrow schema construction for GTFS entities.

Provides a single source of truth for column names and their PyArrow types,
ensuring consistency across all ingester schemas.
"""

from enum import StrEnum

import pyarrow as pa


class Columns(StrEnum):
    """Column names used across GTFS entity schemas."""

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
    DELAY = "delay"

    # Position data (vehicle_positions only)
    LATITUDE = "latitude"
    LONGITUDE = "longitude"
    BEARING = "bearing"
    SPEED = "speed"
    ODOMETER = "odometer"
    OCCUPANCY_STATUS = "occupancy_status"
    ENTITY_IS_DELETED = "entity_is_deleted"

    # Stop update data
    STOP_SEQUENCE = "stop_sequence"
    STOP_ID = "stop_id"
    STOP_SCHEDULE_RELATIONSHIP = "stop_schedule_relationship"
    DEPARTURE_DELAY = "departure_delay"
    DEPARTURE_TIME = "departure_time"
    DEPARTURE_UNCERTAINTY = "departure_uncertainty"
    ARRIVAL_DELAY = "arrival_delay"
    ARRIVAL_TIME = "arrival_time"
    ARRIVAL_UNCERTAINTY = "arrival_uncertainty"

    # Derived columns (computed in add_derived_columns)
    FEED_DATE = "feed_date"
    FEED_HOUR = "feed_hour"


# Dedupe keys for each entity type.
# Used during hourly cleanup to identify duplicate records.
# For each unique combination of these keys, we keep only the row with the
# latest poll_time (most recent observation).
VEHICLE_POSITIONS_DEDUPE_KEYS: tuple[Columns, ...] = (
    Columns.VEHICLE_ID,
    Columns.FEED_TIMESTAMP,
)

TRIP_UPDATES_DEDUPE_KEYS: tuple[Columns, ...] = (
    Columns.TRIP_ID,
    Columns.START_DATE,
    Columns.STOP_SEQUENCE,
    Columns.FEED_TIMESTAMP,
)


# PyArrow type for each column.
FIELD_TYPES: dict[Columns, pa.DataType] = {
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
    Columns.DELAY: pa.int32(),
    # Position data
    Columns.LATITUDE: pa.float64(),
    Columns.LONGITUDE: pa.float64(),
    Columns.BEARING: pa.float32(),
    Columns.SPEED: pa.float32(),
    Columns.ODOMETER: pa.float64(),
    Columns.OCCUPANCY_STATUS: pa.int32(),
    Columns.ENTITY_IS_DELETED: pa.bool_(),
    # Stop update data
    Columns.STOP_SEQUENCE: pa.int32(),
    Columns.STOP_ID: pa.string(),
    Columns.STOP_SCHEDULE_RELATIONSHIP: pa.int32(),
    Columns.DEPARTURE_DELAY: pa.int32(),
    Columns.DEPARTURE_TIME: pa.timestamp("s", tz="+00:00"),
    Columns.DEPARTURE_UNCERTAINTY: pa.int32(),
    Columns.ARRIVAL_DELAY: pa.int32(),
    Columns.ARRIVAL_TIME: pa.timestamp("s", tz="+00:00"),
    Columns.ARRIVAL_UNCERTAINTY: pa.int32(),
    # Derived columns
    Columns.FEED_DATE: pa.string(),
    Columns.FEED_HOUR: pa.int32(),
}


def make_schema(
    columns: list[Columns],
    metadata: dict[bytes, bytes] | None = None,
) -> pa.Schema:
    """Build a PyArrow schema from a list of column names.

    Args:
        columns: List of Columns enum values defining schema fields.
        metadata: Optional schema-level metadata.

    Returns:
        PyArrow schema with fields in the specified order.

    Raises:
        KeyError: If a column is not defined in FIELD_TYPES.
    """
    fields = [pa.field(col, FIELD_TYPES[col]) for col in columns]
    return pa.schema(fields, metadata=metadata)
