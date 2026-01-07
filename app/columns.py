"""Column definitions and PyArrow schema construction for GTFS entities.

Provides a single source of truth for column names and their PyArrow types

ensuring consistency across all ingester schemas.
"""

from app.utils import encode_metadata_dict
from enum import StrEnum

import pyarrow as pa


class Columns(StrEnum):
    """Column names used across GTFS entity schemas."""

    # Realtime Columns ----
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

    # Static Columns ----
    # Agency
    AGENCY_ID = "agency_id"
    AGENCY_NAME = "agency_name"
    AGENCY_URL = "agency_url"
    AGENCY_TIMEZONE = "agency_timezone"
    AGENCY_LANG = "agency_lang"
    AGENCY_PHONE = "agency_phone"
    AGENCY_FARE_URL = "agency_fare_url"
    AGENCY_EMAIL = "agency_email"

    # Stop
    STOP_CODE = "stop_code"
    STOP_NAME = "stop_name"
    STOP_DESC = "stop_desc"
    STOP_LAT = "stop_lat"
    STOP_LON = "stop_lon"
    ZONE_ID = "zone_id"
    STOP_URL = "stop_url"
    LOCATION_TYPE = "location_type"
    PARENT_STATION = "parent_station"
    STOP_TIMEZONE = "stop_timezone"
    PLATFORM_CODE = "platform_code"
    WHEELCHAIR_BOARDING = "wheelchair_boarding"
    END_DATE = "end_date"

    # Routes
    ROUTE_SHORT_NAME = "route_short_name"
    ROUTE_LONG_NAME = "route_long_name"
    ROUTE_DESC = "route_desc"
    ROUTE_TYPE = "route_type"
    ROUTE_URL = "route_url"
    ROUTE_COLOR = "route_color"
    ROUTE_TEXT_COLOR = "route_text_color"
    ROUTE_SORT_ORDER = "route_sort_order"
    CONTRACT_ID = "contract_id"

    # Trips
    SERVICE_ID = "service_id"
    TRIP_HEADSIGN = "trip_headsign"
    TRIP_SHORT_NAME = "trip_short_name"
    BLOCK_ID = "block_id"
    SHAPE_ID = "shape_id"
    WHEELCHAIR_ACCESSIBLE = "wheelchair_accessible"
    BIKES_ALLOWED = "bikes_allowed"

    # Stop Times
    STOP_HEADSIGN = "stop_headsign"
    PICKUP_TYPE = "pickup_type"
    DROP_OFF_TYPE = "drop_off_type"
    SHAPE_DIST_TRAVELED = "shape_dist_traveled"
    TIMEPOINT = "timepoint"
    # Static stop times (HH:MM:SS strings, can exceed 24:00:00)
    ARRIVAL_TIME_STR = "arrival_time"
    DEPARTURE_TIME_STR = "departure_time"

    # Calendar
    MONDAY = "monday"
    TUESDAY = "tuesday"
    WEDNESDAY = "wednesday"
    THURSDAY = "thursday"
    FRIDAY = "friday"
    SATURDAY = "saturday"
    SUNDAY = "sunday"

    # Calendar Dates
    DATE = "date"
    EXCEPTION_TYPE = "exception_type"

    # Shapes
    SHAPE_PT_LAT = "shape_pt_lat"
    SHAPE_PT_LON = "shape_pt_lon"
    SHAPE_PT_SEQUENCE = "shape_pt_sequence"

    # Fare Attributes
    FARE_ID = "fare_id"
    PRICE = "price"
    CURRENCY_TYPE = "currency_type"
    PAYMENT_METHOD = "payment_method"
    TRANSFERS = "transfers"
    TRANSFER_DURATION = "transfer_duration"

    # Fare Rules
    ORIGIN_ID = "origin_id"
    DESTINATION_ID = "destination_id"
    CONTAINS_ID = "contains_id"

    # Frequencies
    END_TIME = "end_time"
    HEADWAY_SECS = "headway_secs"
    EXACT_TIMES = "exact_times"

    # Transfers
    FROM_STOP_ID = "from_stop_id"
    TO_STOP_ID = "to_stop_id"
    TRANSFER_TYPE = "transfer_type"
    MIN_TRANSFER_TIME = "min_transfer_time"

    # Feed Info
    FEED_PUBLISHER_NAME = "feed_publisher_name"
    FEED_PUBLISHER_URL = "feed_publisher_url"
    FEED_LANG = "feed_lang"
    FEED_START_DATE = "feed_start_date"
    FEED_END_DATE = "feed_end_date"
    FEED_VERSION = "feed_version"

    # Derived columns
    DOWNLOADED_AT = "downloaded_at"
    VALID_FROM = "valid_from"
    VALID_TO = "valid_to"


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
    # Static Columns ----
    # Agency
    Columns.AGENCY_ID: pa.string(),
    Columns.AGENCY_NAME: pa.string(),
    Columns.AGENCY_URL: pa.string(),
    Columns.AGENCY_TIMEZONE: pa.string(),
    Columns.AGENCY_LANG: pa.string(),
    Columns.AGENCY_PHONE: pa.string(),
    Columns.AGENCY_FARE_URL: pa.string(),
    Columns.AGENCY_EMAIL: pa.string(),
    # Stops
    Columns.STOP_CODE: pa.string(),
    Columns.STOP_NAME: pa.string(),
    Columns.STOP_DESC: pa.string(),
    Columns.STOP_LAT: pa.float64(),
    Columns.STOP_LON: pa.float64(),
    Columns.ZONE_ID: pa.string(),
    Columns.STOP_URL: pa.string(),
    Columns.LOCATION_TYPE: pa.int32(),
    Columns.PARENT_STATION: pa.string(),
    Columns.STOP_TIMEZONE: pa.string(),
    Columns.PLATFORM_CODE: pa.string(),
    Columns.WHEELCHAIR_BOARDING: pa.int32(),
    Columns.END_DATE: pa.string(),
    # Routes
    Columns.ROUTE_SHORT_NAME: pa.string(),
    Columns.ROUTE_LONG_NAME: pa.string(),
    Columns.ROUTE_DESC: pa.string(),
    Columns.ROUTE_TYPE: pa.int32(),
    Columns.ROUTE_URL: pa.string(),
    Columns.ROUTE_COLOR: pa.string(),
    Columns.ROUTE_TEXT_COLOR: pa.string(),
    Columns.ROUTE_SORT_ORDER: pa.int32(),
    Columns.CONTRACT_ID: pa.string(),
    # Trips
    Columns.SERVICE_ID: pa.string(),
    Columns.TRIP_HEADSIGN: pa.string(),
    Columns.TRIP_SHORT_NAME: pa.string(),
    Columns.BLOCK_ID: pa.string(),
    Columns.SHAPE_ID: pa.string(),
    Columns.WHEELCHAIR_ACCESSIBLE: pa.int32(),
    Columns.BIKES_ALLOWED: pa.int32(),
    # Stop Times
    Columns.STOP_HEADSIGN: pa.string(),
    Columns.PICKUP_TYPE: pa.int32(),
    Columns.DROP_OFF_TYPE: pa.int32(),
    Columns.SHAPE_DIST_TRAVELED: pa.float64(),
    Columns.TIMEPOINT: pa.int32(),
    Columns.ARRIVAL_TIME_STR: pa.string(),
    Columns.DEPARTURE_TIME_STR: pa.string(),
    # Calendar
    Columns.MONDAY: pa.string(),
    Columns.TUESDAY: pa.string(),
    Columns.WEDNESDAY: pa.string(),
    Columns.THURSDAY: pa.string(),
    Columns.FRIDAY: pa.string(),
    Columns.SATURDAY: pa.string(),
    Columns.SUNDAY: pa.string(),
    # Calendar Dates
    Columns.DATE: pa.string(),
    Columns.EXCEPTION_TYPE: pa.int32(),
    # Shapes
    Columns.SHAPE_PT_LAT: pa.float64(),
    Columns.SHAPE_PT_LON: pa.float64(),
    Columns.SHAPE_PT_SEQUENCE: pa.int32(),
    # Fare Attributes
    Columns.FARE_ID: pa.string(),
    Columns.PRICE: pa.float64(),
    Columns.CURRENCY_TYPE: pa.string(),
    Columns.PAYMENT_METHOD: pa.int32(),
    Columns.TRANSFERS: pa.int32(),
    Columns.TRANSFER_DURATION: pa.int32(),
    # Fare Rules
    Columns.ORIGIN_ID: pa.string(),
    Columns.DESTINATION_ID: pa.string(),
    Columns.CONTAINS_ID: pa.string(),
    # Frequencies
    Columns.END_TIME: pa.string(),
    Columns.HEADWAY_SECS: pa.int32(),
    Columns.EXACT_TIMES: pa.int32(),
    # Transfers
    Columns.FROM_STOP_ID: pa.string(),
    Columns.TO_STOP_ID: pa.string(),
    Columns.TRANSFER_TYPE: pa.int32(),
    Columns.MIN_TRANSFER_TIME: pa.int32(),
    # Feed Info
    Columns.FEED_PUBLISHER_NAME: pa.string(),
    Columns.FEED_PUBLISHER_URL: pa.string(),
    Columns.FEED_LANG: pa.string(),
    Columns.FEED_START_DATE: pa.string(),
    Columns.FEED_END_DATE: pa.string(),
    Columns.FEED_VERSION: pa.string(),
    # Derived Column
    Columns.DOWNLOADED_AT: pa.timestamp("s", tz="+00:00"),
    Columns.VALID_FROM: pa.string(),
    Columns.VALID_TO: pa.string(),
}


def make_schema(
    columns: list[Columns],
    metadata: dict[str, str] | None = None,
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
    metadata_json = encode_metadata_dict(metadata) if metadata else None
    return pa.schema(fields, metadata=metadata_json)
