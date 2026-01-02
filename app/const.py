"""Column name definitions for GTFS entity schemas."""

from enum import StrEnum


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
