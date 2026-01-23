"""Combined feed ingestion for GTFS-Realtime data.

Uses Auckland Transport's Combined Feed API endpoint to fetch all
realtime data in a single request, reducing API calls to stay within
rate limits (~3.4 calls/minute).
"""

import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from logging import Logger
from pathlib import Path
from typing import Any, ClassVar

import pyarrow as pa
import requests
from deltalake import write_deltalake
from google.transit import gtfs_realtime_pb2
from requests.models import Response

from app.columns import Columns, make_schema, REALTIME_FIELD_TYPES
from app.config import (
    AT_API_KEY,
    DATA_PATH,
    STALE_THRESHOLD_MINUTES,
)


@dataclass
class FetchResult:
    """Successful fetch result.

    Attributes:
        feed: Decoded protobuf feed.
        poll_time: Timestamp when fetch was initiated.
    """

    feed: gtfs_realtime_pb2.FeedMessage
    poll_time: datetime


@dataclass
class IngestResult:
    """Result of a combined ingest operation.

    Attributes:
        vehicle_positions_rows: Rows written for vehicle positions.
        trip_updates_rows: Rows written for trip updates (trip-level).
        stop_time_updates_rows: Rows written for stop time updates.
    """

    vehicle_positions_rows: int
    trip_updates_rows: int
    stop_time_updates_rows: int

    @property
    def total_rows(self) -> int:
        """Total rows written across all entity types."""
        return (
            self.vehicle_positions_rows
            + self.trip_updates_rows
            + self.stop_time_updates_rows
        )


class CombinedFeedFetcher:
    """Fetches the combined GTFS-Realtime feed from Auckland Transport.

    Handles HTTP requests, MD5-based deduplication, and protobuf decoding.
    Designed to be called by an external scheduler (e.g., APScheduler).

    The combined feed endpoint returns all entity types (vehicle positions,
    trip updates, alerts) in a single response, allowing multiple Ingest
    subclasses to process the same feed without additional API calls.
    """

    url: ClassVar[str] = "https://api.at.govt.nz/realtime/legacy/"
    headers: ClassVar[dict[str, str]] = {
        "Ocp-Apim-Subscription-Key": AT_API_KEY,
        "Accept": "application/x-protobuf",
    }

    def __init__(self) -> None:
        self.log = logging.getLogger(f"{self.__class__.__name__}")
        self._last_md5: str | None = None

    def fetch(self) -> FetchResult | None:
        """Fetch and decode the combined GTFS-Realtime feed.

        Returns:
            FetchResult: On successful fetch with new data.
            None: If feed unchanged (MD5 match).

        Raises:
            requests.RequestException: On HTTP errors.
            DecodeError: On protobuf parsing errors.
        """
        poll_time: datetime = datetime.now(timezone.utc)

        resp: Response = requests.get(url=self.url, headers=self.headers)
        resp.raise_for_status()

        raw_bytes: bytes | Any = resp.content

        # Skip identical feed payloads
        md5: str = hashlib.md5(raw_bytes, usedforsecurity=False).hexdigest()
        if md5 == self._last_md5:
            self.log.info("Feed unchanged (MD5 match); skipping")
            return None

        self._last_md5 = md5

        feed = self.decode_feed(raw_bytes)
        self.log.info("%d protobuf entities decoded", len(feed.entity))

        return FetchResult(feed=feed, poll_time=poll_time)

    def decode_feed(self, raw_bytes: bytes) -> gtfs_realtime_pb2.FeedMessage:
        """Decode raw GTFS-Realtime protobuf bytes into a FeedMessage."""
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(raw_bytes)
        return feed


class Ingest(ABC):
    """Base class for processing and writing GTFS-Realtime entities.

    Subclasses define entity-specific configuration (schema, partition
    columns, write path) and normalisation logic.

    Required class attributes:
        schema: pa.Schema - PyArrow schema for the entity
        partition_cols: list[Columns] - Columns to partition by
        write_path: Path - Output path for Delta Lake table

    Required methods:
        normalise(feed) -> list[dict]
    """

    partition_cols: ClassVar[list[Columns]]
    schema: ClassVar[pa.Schema]
    write_path: ClassVar[Path]

    def __init__(self) -> None:
        self.log = logging.getLogger(f"{self.__class__.__name__}")
        self.poll_time: datetime | None = None

    def ingest(
        self,
        feed: gtfs_realtime_pb2.FeedMessage,
        poll_time: datetime,
    ) -> int | None:
        self.poll_time = poll_time
        try:
            rows: list[dict[str, Any]] = self.normalise(feed)
            self.log.info("%d rows normalised", len(rows))
        except Exception:
            self.log.exception("Unexpected error in normalise")
            return None

        # Create table
        table = pa.Table.from_pylist(rows, schema=self.schema)

        # Write data
        self.write_data(table, self.partition_cols, path=self.write_path)
        self.log.info("%d rows written to Delta Lake", len(table))
        return len(rows)

    def write_data(
        self, data: pa.Table, partition_cols: list[str], path: Path | str
    ) -> None:
        """Write table to Delta Lake with partitioning."""
        write_deltalake(
            table_or_uri=path,
            data=data,
            partition_by=partition_cols,
            mode="append",
        )

    @abstractmethod
    def normalise(
        self,
        feed: gtfs_realtime_pb2.FeedMessage,
    ) -> list[dict[str, Any]]: ...


class VehiclePositions(Ingest):
    """Ingester for GTFS-Realtime vehicle position data.

    Captures real-time vehicle locations including coordinates, bearing,
    speed, and trip information from the Auckland Transport API.

    Partitioned by (start_date, route_id) for efficient queries by
    operational day and route.
    """

    partition_cols: list[Columns] = [
        Columns.START_DATE,
        Columns.ROUTE_ID,
    ]
    schema = make_schema(
        columns=[
            # Timestamps
            Columns.POLL_TIME,
            Columns.FEED_TIMESTAMP,
            # Vehicle details
            Columns.VEHICLE_ID,
            Columns.LABEL,
            Columns.LICENSE_PLATE,
            # Trip/route info
            Columns.TRIP_ID,
            Columns.ROUTE_ID,
            Columns.DIRECTION_ID,
            Columns.SCHEDULE_RELATIONSHIP,
            Columns.START_DATE,
            Columns.START_TIME,
            # Position data
            Columns.LATITUDE,
            Columns.LONGITUDE,
            Columns.BEARING,
            Columns.SPEED,
            Columns.ODOMETER,
            Columns.OCCUPANCY_STATUS,
            Columns.ENTITY_IS_DELETED,
        ],
        field_types=REALTIME_FIELD_TYPES,
        metadata={
            "entity": "vehicle_positions",
            "version": "1",
            "partition_columns": partition_cols,

        },
    )
    write_path: Path = DATA_PATH / "vehicle_positions"

    def normalise(
        self,
        feed: gtfs_realtime_pb2.FeedMessage,
    ) -> list[dict[str, Any]]:
        """Parse vehicle position entities from protobuf feed.

        String fields use `or None` to convert empty strings to None
        (protobuf returns "" for unset strings). Numeric fields do not
        use this pattern as 0 is a valid value (e.g., bearing=0 means
        north).

        start_date and route_id use fallback values ("UNKNOWN", "NA")
        since they are partition columns and cannot be NULL.
        """
        rows: list[dict[str, Any]] = []
        poll_timestamp: int = int(self.poll_time.timestamp())
        stale_threshold_seconds: int = STALE_THRESHOLD_MINUTES * 60

        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue

            e = entity.vehicle

            # Skip entities with stale timestamps to avoid partition sprawl
            if abs(poll_timestamp - e.timestamp) > stale_threshold_seconds:
                continue

            # Vehicles without trips (deadheading, starting service) have no
            # start_date. Use "UNKNOWN" as fallback for partition column.
            start_date: str = e.trip.start_date or "UNKNOWN"

            row: dict[Columns, datetime | Any | None] = {
                Columns.POLL_TIME: self.poll_time,
                Columns.FEED_TIMESTAMP: e.timestamp,
                Columns.VEHICLE_ID: e.vehicle.id or None,
                Columns.LABEL: e.vehicle.label or None,
                Columns.LICENSE_PLATE: e.vehicle.license_plate or None,
                Columns.TRIP_ID: e.trip.trip_id or None,
                Columns.ROUTE_ID: e.trip.route_id or "NA",
                Columns.DIRECTION_ID: e.trip.direction_id,
                Columns.SCHEDULE_RELATIONSHIP: e.trip.schedule_relationship,
                Columns.START_DATE: start_date,
                Columns.START_TIME: e.trip.start_time or None,
                Columns.LATITUDE: e.position.latitude,
                Columns.LONGITUDE: e.position.longitude,
                Columns.BEARING: e.position.bearing,
                Columns.SPEED: e.position.speed,
                Columns.ODOMETER: e.position.odometer,
                Columns.OCCUPANCY_STATUS: e.occupancy_status,
                Columns.ENTITY_IS_DELETED: entity.is_deleted,
            }
            rows.append(row)

        return rows


class TripUpdates(Ingest):
    """Ingester for GTFS-Realtime trip-level data.

    Captures trip status information including schedule_relationship
    (for tracking cancellations) and vehicle assignment. One row per
    trip per feed observation.

    Partitioned by (start_date, route_id) for efficient queries by
    operational day and route.
    """

    partition_cols: list[Columns] = [
        Columns.START_DATE,
        Columns.ROUTE_ID,
    ]
    schema = make_schema(
        columns=[
            # Timestamps
            Columns.POLL_TIME,
            Columns.FEED_TIMESTAMP,
            # Trip/route info
            Columns.TRIP_ID,
            Columns.ROUTE_ID,
            Columns.DIRECTION_ID,
            Columns.SCHEDULE_RELATIONSHIP,
            Columns.START_DATE,
            Columns.START_TIME,
            # Vehicle details
            Columns.VEHICLE_ID,
            Columns.LABEL,
            Columns.LICENSE_PLATE,
            Columns.ENTITY_IS_DELETED,
        ],
        field_types=REALTIME_FIELD_TYPES,
        metadata={
            "entity": "trip_updates",
            "version": "1",
            "partition_columns": partition_cols,
        },
    )
    write_path: Path = DATA_PATH / "trip_updates"

    def normalise(
        self,
        feed: gtfs_realtime_pb2.FeedMessage,
    ) -> list[dict[str, Any]]:
        """Parse trip-level data from protobuf feed.

        Creates one row per trip. This captures trip status (especially
        cancellations via schedule_relationship) without duplicating
        data across every stop.

        start_date and route_id use fallback values ("UNKNOWN", "NA")
        since they are partition columns and cannot be NULL.
        """
        rows: list[dict[str, Any]] = []
        poll_timestamp: int = int(self.poll_time.timestamp())
        stale_threshold_seconds: int = STALE_THRESHOLD_MINUTES * 60

        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            e = entity.trip_update

            # Skip entities with stale timestamps to avoid partition sprawl
            if abs(poll_timestamp - e.timestamp) > stale_threshold_seconds:
                continue

            # Trip updates should normally have start_date, but use "UNKNOWN"
            # as fallback for partition column if missing.
            start_date: str = e.trip.start_date or "UNKNOWN"

            row: dict[Columns, datetime | Any | None] = {
                Columns.POLL_TIME: self.poll_time,
                Columns.FEED_TIMESTAMP: e.timestamp,
                Columns.TRIP_ID: e.trip.trip_id or None,
                Columns.ROUTE_ID: e.trip.route_id or "NA",
                Columns.DIRECTION_ID: e.trip.direction_id,
                Columns.SCHEDULE_RELATIONSHIP: e.trip.schedule_relationship,
                Columns.START_DATE: start_date,
                Columns.START_TIME: e.trip.start_time or None,
                Columns.VEHICLE_ID: e.vehicle.id or None,
                Columns.LABEL: e.vehicle.label or None,
                Columns.LICENSE_PLATE: e.vehicle.license_plate or None,
                Columns.ENTITY_IS_DELETED: entity.is_deleted,
            }
            rows.append(row)

        return rows


class StopTimeUpdates(Ingest):
    """Ingester for GTFS-Realtime stop time update data.

    Captures arrival/departure predictions and actuals for each stop
    along a trip. One row per stop per trip per observation.

    Partitioned by (start_date, route_id) for efficient queries by
    operational day and route.
    """

    partition_cols: list[Columns] = [
        Columns.START_DATE,
        Columns.ROUTE_ID,
    ]
    schema = make_schema(
        [
            # Timestamps
            Columns.POLL_TIME,
            Columns.FEED_TIMESTAMP,
            # FK columns (for joining with trip_updates)
            Columns.TRIP_ID,
            Columns.START_DATE,
            Columns.ROUTE_ID,
            # Stop update data
            Columns.STOP_SEQUENCE,
            Columns.STOP_ID,
            Columns.STOP_SCHEDULE_RELATIONSHIP,
            Columns.ARRIVAL_DELAY,
            Columns.ARRIVAL_TIME,
            Columns.ARRIVAL_UNCERTAINTY,
            Columns.DEPARTURE_DELAY,
            Columns.DEPARTURE_TIME,
            Columns.DEPARTURE_UNCERTAINTY,
        ],
        field_types=REALTIME_FIELD_TYPES,
        metadata={
            "entity": "stop_time_updates",
            "version": "1",
            "partition_columns": partition_cols,
        },
    )
    write_path: Path = DATA_PATH / "stop_time_updates"

    def normalise(
        self,
        feed: gtfs_realtime_pb2.FeedMessage,
    ) -> list[dict[str, Any]]:
        """Parse stop time updates from protobuf feed.

        Creates one row per stop_time_update. Only includes rows with
        confirmed (non-predicted) arrival or departure times.

        start_date and route_id use fallback values ("UNKNOWN", "NA")
        since they are partition columns and cannot be NULL.
        """
        rows: list[dict[str, Any]] = []
        poll_timestamp: int = int(self.poll_time.timestamp())
        stale_threshold_seconds: int = STALE_THRESHOLD_MINUTES * 60

        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            e = entity.trip_update

            # Skip entities with stale timestamps to avoid partition sprawl
            if abs(poll_timestamp - e.timestamp) > stale_threshold_seconds:
                continue

            start_date: str = e.trip.start_date or "UNKNOWN"
            route_id: str = e.trip.route_id or "NA"

            for stu in e.stop_time_update:
                # Only record confirmed times (uncertainty = 0)
                has_confirmed_arrival: bool = (
                    stu.arrival.time > 0 and stu.arrival.uncertainty == 0
                )
                has_confirmed_departure: bool = (
                    stu.departure.time > 0 and stu.departure.uncertainty == 0
                )

                if has_confirmed_arrival or has_confirmed_departure:
                    row: dict[Columns, datetime | Any | None] = {
                        Columns.POLL_TIME: self.poll_time,
                        Columns.FEED_TIMESTAMP: e.timestamp,
                        Columns.TRIP_ID: e.trip.trip_id or None,
                        Columns.START_DATE: start_date,
                        Columns.ROUTE_ID: route_id,
                        Columns.STOP_SEQUENCE: stu.stop_sequence,
                        Columns.STOP_ID: stu.stop_id or None,
                        Columns.STOP_SCHEDULE_RELATIONSHIP: (
                            stu.schedule_relationship
                        ),
                        Columns.ARRIVAL_DELAY: stu.arrival.delay,
                        Columns.ARRIVAL_TIME: stu.arrival.time or None,
                        Columns.ARRIVAL_UNCERTAINTY: stu.arrival.uncertainty,
                        Columns.DEPARTURE_DELAY: stu.departure.delay,
                        Columns.DEPARTURE_TIME: stu.departure.time or None,
                        Columns.DEPARTURE_UNCERTAINTY: stu.departure.uncertainty,
                    }
                    rows.append(row)

        return rows


combined_feed = CombinedFeedFetcher()
vehicle_positions = VehiclePositions()
trip_updates = TripUpdates()
stop_time_updates = StopTimeUpdates()


log: Logger = logging.getLogger(__name__)


def combined_ingest() -> IngestResult | None:
    """Fetch combined feed and ingest all entity types.

    Exceptions propagate to the caller (APScheduler handles them and
    continues to the next tick). Add explicit exception handling when
    Sentry is integrated.

    Returns:
        IngestResult: Row counts on success.
        None: If feed unchanged (skipped).
    """
    result: FetchResult | None = combined_feed.fetch()

    if result is None:
        log.debug("Feed unchanged; skipping ingest")
        return None

    vp_rows: int = vehicle_positions.ingest(result.feed, result.poll_time) or 0
    tu_rows: int = trip_updates.ingest(result.feed, result.poll_time) or 0
    stu_rows: int = (
        stop_time_updates.ingest(result.feed, result.poll_time) or 0
    )

    return IngestResult(
        vehicle_positions_rows=vp_rows,
        trip_updates_rows=tu_rows,
        stop_time_updates_rows=stu_rows,
    )


if __name__ == "__main__":
    combined_ingest()
