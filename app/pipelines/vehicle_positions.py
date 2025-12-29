"""Pipeline for ingesting Auckland Transport vehicle positions data."""

from datetime import datetime, timezone
from typing import Any

from google.transit import gtfs_realtime_pb2

from app.pipelines.base import BaseRealtimePipeline
from app.schemas.vehicle_positions import VehiclePositionSchema


class VehiclePositionsPipeline(BaseRealtimePipeline):
    """Ingestion pipeline for Auckland Transport vehicle positions feed.

    Extracts vehicle location data from GTFS-Realtime protobuf feed and
    writes to partitioned Parquet files.
    """

    url = "https://api.at.govt.nz/realtime/legacy/vehiclelocations"
    table_name = "vehicle_positions"
    table_schema = VehiclePositionSchema

    def normalise(
        self, feed: gtfs_realtime_pb2.FeedMessage, poll_time: datetime
    ) -> list[dict[str, Any]]:
        """Convert FeedMessage entities into normalised dictionaries.

        Note: String fields use `or None` to convert empty strings to None
        (protobuf returns "" for unset strings). Numeric fields do not use
        this pattern as 0 is a valid value (e.g., bearing=0 means north).
        """
        rows: list[dict[str, Any]] = []

        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue

            v = entity.vehicle
            row = {
                # Timestamps
                VehiclePositionSchema.POLL_TIME: poll_time,
                VehiclePositionSchema.FEED_TIMESTAMP: datetime.fromtimestamp(
                    v.timestamp, tz=timezone.utc
                ),
                # Vehicle details
                VehiclePositionSchema.VEHICLE_ID: v.vehicle.id or None,
                VehiclePositionSchema.LABEL: v.vehicle.label or None,
                VehiclePositionSchema.LICENSE_PLATE: (
                    v.vehicle.license_plate or None
                ),
                # Trip/route info
                VehiclePositionSchema.TRIP_ID: v.trip.trip_id or None,
                VehiclePositionSchema.ROUTE_ID: v.trip.route_id or None,
                VehiclePositionSchema.DIRECTION_ID: v.trip.direction_id,
                VehiclePositionSchema.SCHEDULE_RELATIONSHIP: (
                    v.trip.schedule_relationship
                ),
                VehiclePositionSchema.START_DATE: v.trip.start_date or None,
                VehiclePositionSchema.START_TIME: v.trip.start_time or None,
                # Position data
                VehiclePositionSchema.LATITUDE: v.position.latitude,
                VehiclePositionSchema.LONGITUDE: v.position.longitude,
                VehiclePositionSchema.BEARING: v.position.bearing,
                VehiclePositionSchema.SPEED: v.position.speed,
                VehiclePositionSchema.ODOMETER: v.position.odometer,
                VehiclePositionSchema.OCCUPANCY_STATUS: v.occupancy_status,
                VehiclePositionSchema.ENTITY_IS_DELETED: entity.is_deleted,
            }
            rows.append(row)

        return rows
