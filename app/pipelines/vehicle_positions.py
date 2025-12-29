"""Pipeline for ingesting Auckland Transport vehicle positions data."""

from datetime import datetime, timezone
from typing import Any

from google.transit import gtfs_realtime_pb2

from app.pipelines.base import BaseRealtimePipeline
from app.schemas.vehicle_positions import VehiclePositionColumns


class VehiclePositionsPipeline(BaseRealtimePipeline):
    """Ingestion pipeline for Auckland Transport vehicle positions feed.

    Extracts vehicle location data from GTFS-Realtime protobuf feed and
    writes to partitioned Parquet files.
    """

    url = "https://api.at.govt.nz/realtime/legacy/vehiclelocations"
    table_name = "vehicle_positions"
    columns = VehiclePositionColumns

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
                VehiclePositionColumns.POLL_TIME: poll_time,
                VehiclePositionColumns.FEED_TIMESTAMP: datetime.fromtimestamp(
                    v.timestamp, tz=timezone.utc
                ),
                # Vehicle details
                VehiclePositionColumns.VEHICLE_ID: v.vehicle.id or None,
                VehiclePositionColumns.LABEL: v.vehicle.label or None,
                VehiclePositionColumns.LICENSE_PLATE: (
                    v.vehicle.license_plate or None
                ),
                # Trip/route info
                VehiclePositionColumns.TRIP_ID: v.trip.trip_id or None,
                VehiclePositionColumns.ROUTE_ID: v.trip.route_id or None,
                VehiclePositionColumns.DIRECTION_ID: v.trip.direction_id,
                VehiclePositionColumns.SCHEDULE_RELATIONSHIP: (
                    v.trip.schedule_relationship
                ),
                VehiclePositionColumns.START_DATE: v.trip.start_date or None,
                VehiclePositionColumns.START_TIME: v.trip.start_time or None,
                # Position data
                VehiclePositionColumns.LATITUDE: v.position.latitude,
                VehiclePositionColumns.LONGITUDE: v.position.longitude,
                VehiclePositionColumns.BEARING: v.position.bearing,
                VehiclePositionColumns.SPEED: v.position.speed,
                VehiclePositionColumns.ODOMETER: v.position.odometer,
                VehiclePositionColumns.OCCUPANCY_STATUS: v.occupancy_status,
                VehiclePositionColumns.ENTITY_IS_DELETED: entity.is_deleted,
            }
            rows.append(row)

        return rows
