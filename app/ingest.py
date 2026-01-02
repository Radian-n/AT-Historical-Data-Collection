"""Ingestion classes for GTFS-Realtime data."""

import hashlib
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, ClassVar

import pyarrow as pa
import pyarrow.compute as pc
import requests
from deltalake import write_deltalake
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

from app.columns import Columns, make_schema
from app.config import (
    AT_API_KEY,
    DATA_PATH,
)
from app.utils import byte_string_array


class Ingester(ABC):
    """GTFS-Realtime ingestion base class.

    A reusable pipeline that fetches, parses, and writes GTFS-Realtime
    data. Subclasses define entity-specific configuration and logic.

    Designed to be called by an external scheduler (e.g., APScheduler).
    Call run() to execute a single fetch-parse-write cycle.

    Required class attributes:
        url: str - HTTP endpoint for the GTFS-Realtime feed
        schema: pa.Schema - PyArrow schema for the entity
        partition_cols: list[str] - Columns to partition by
        write_path: Path - Output path for Delta Lake table

    Required methods:
        normalise(feed) -> list[dict]

    Optional overrides:
        headers: dict[str, str] - HTTP headers (defaults to AT API)
        add_derived_columns(table) -> pa.Table
    """

    url: ClassVar[str]
    schema: ClassVar[pa.Schema]
    partition_cols: ClassVar[list[str]]
    write_path: ClassVar[Path]

    headers: ClassVar[dict[str, str]] = {
        "Ocp-Apim-Subscription-Key": AT_API_KEY,
        "Accept": "application/x-protobuf",
    }

    def __init__(self) -> None:
        self.log = logging.getLogger(f"{self.__class__.__name__}")
        self.poll_time: datetime | None = None

        # Runtime state
        self._last_md5: str | None = None

    def run(self) -> int | None:
        """Execute a single fetch-parse-write cycle.

        Designed to be called by an external scheduler.

        Returns:
            int: Number of rows written (0 if skipped due to unchanged
                feed)
            None: On error
        """
        self.poll_time = datetime.now(timezone.utc)

        try:
            resp = requests.get(url=self.url, headers=self.headers)
            resp.raise_for_status()
        except requests.RequestException:
            self.log.exception("HTTP fetch failed")
            return None

        raw_bytes = resp.content

        # Skip identical feed payloads
        md5 = hashlib.md5(raw_bytes, usedforsecurity=False).hexdigest()
        if md5 == self._last_md5:
            self.log.info("Feed unchanged (MD5 match); skipping")
            return 0

        self._last_md5 = md5

        try:
            feed = self.decode_feed(raw_bytes)
            self.log.info("%d protobuf entities decoded", len(feed.entity))
        except DecodeError:
            self.log.exception("Failed to parse protobuf message")
            return None

        try:
            rows = self.normalise(feed)
            self.log.info("%d rows normalised", len(rows))
        except Exception:
            self.log.exception("Unexpected error in normalise")
            return None

        # Create table
        table = pa.Table.from_pylist(rows, schema=self.schema)
        table = self.add_derived_columns(table)

        # Write data
        self.write_data(table, self.partition_cols, path=self.write_path)
        self.log.info("%d rows written to Delta Lake", len(table))
        return len(rows)

    def decode_feed(self, raw_bytes: bytes) -> gtfs_realtime_pb2.FeedMessage:
        """Decode raw GTFS-Realtime protobuf bytes into a FeedMessage."""
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(raw_bytes)
        return feed

    def add_derived_columns(self, table: pa.Table) -> pa.Table:
        """Derive date and hour columns from feed timestamp."""
        ts = table[Columns.FEED_TIMESTAMP]
        return table.append_column(
            Columns.FEED_DATE, pc.strftime(ts, format="%Y-%m-%d")
        ).append_column(Columns.FEED_HOUR, pc.hour(ts))

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
        self, feed: gtfs_realtime_pb2.FeedMessage
    ) -> list[dict[str, Any]]:
        """Parse entities from protobuf feed into row dictionaries."""
        ...


class VehiclePositions(Ingester):
    """Ingester for GTFS-Realtime vehicle position data.

    Captures real-time vehicle locations including coordinates, bearing,
    speed, and trip information from the Auckland Transport API.
    """

    url = "https://api.at.govt.nz/realtime/legacy/vehiclelocations"
    partition_cols = [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID]
    schema = make_schema(
        [
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
        metadata={
            b"entity": b"vehicle_positions",
            b"version": b"1",
            b"partition_columns": byte_string_array(partition_cols),
        },
    )
    write_path = DATA_PATH / "vehicle_positions"

    def normalise(
        self,
        feed: gtfs_realtime_pb2.FeedMessage,
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

            e = entity.vehicle
            row = {
                Columns.POLL_TIME: self.poll_time,
                Columns.FEED_TIMESTAMP: e.timestamp,
                Columns.VEHICLE_ID: e.vehicle.id or None,
                Columns.LABEL: e.vehicle.label or None,
                Columns.LICENSE_PLATE: e.vehicle.license_plate or None,
                Columns.TRIP_ID: e.trip.trip_id or None,
                Columns.ROUTE_ID: e.trip.route_id or None,
                Columns.DIRECTION_ID: e.trip.direction_id,
                Columns.SCHEDULE_RELATIONSHIP: e.trip.schedule_relationship,
                Columns.START_DATE: e.trip.start_date or None,
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


class TripUpdates(Ingester):
    """Ingester for GTFS-Realtime trip updates data.

    Captures real-time trip updates including stops, arrival and
    departure times and delays data from the Auckland Transport API.
    """

    url = "https://api.at.govt.nz/realtime/legacy/tripupdates"
    partition_cols = [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID]
    schema = make_schema(
        [
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
            Columns.DELAY,  # TODO: Remove
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
        metadata={
            b"entity": b"trip_updates",
            b"version": b"1",
            b"partition_columns": byte_string_array(partition_cols),
        },
    )
    write_path = DATA_PATH / "trip_updates"

    def normalise(
        self,
        feed: gtfs_realtime_pb2.FeedMessage,
    ) -> list[dict[str, Any]]:
        """Parse trip update entities from protobuf feed.

        String fields use `or None` to convert empty strings to None
        (protobuf returns "" for unset strings). Numeric fields do not
        use this pattern as 0 is a valid value.

        Creates one row per stop_time_update. Trips with no stop updates
        (e.g., cancelled trips) produce a single row with null stop fields.
        """
        rows: list[dict[str, Any]] = []

        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            e = entity.trip_update

            # Base row data shared by all stop updates
            base = {
                Columns.POLL_TIME: self.poll_time,
                Columns.FEED_TIMESTAMP: e.timestamp,
                Columns.VEHICLE_ID: e.vehicle.id or None,
                Columns.LABEL: e.vehicle.label or None,
                Columns.LICENSE_PLATE: e.vehicle.license_plate or None,
                Columns.TRIP_ID: e.trip.trip_id or None,
                Columns.ROUTE_ID: e.trip.route_id or None,
                Columns.DIRECTION_ID: e.trip.direction_id,
                Columns.SCHEDULE_RELATIONSHIP: e.trip.schedule_relationship,
                Columns.START_DATE: e.trip.start_date or None,
                Columns.START_TIME: e.trip.start_time or None,
                Columns.DELAY: e.delay,  # TODO: Remove
                Columns.ENTITY_IS_DELETED: entity.is_deleted,
            }

            if e.stop_time_update:
                # Only record the stop time if it was close to the feed timestamp
                for stu in e.stop_time_update:
                    is_current = (
                        stu.arrival.time > 0  # has arrival time
                        and abs(e.timestamp - stu.arrival.time) <= 30 # Recent arrival
                    ) or (
                        stu.departure.time > 0 # has departure time
                        and abs(e.timestamp - stu.departure.time) <= 30 # Recent departure
                    )

                    if is_current:
                        row = {
                            **base,
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
                            Columns.DEPARTURE_UNCERTAINTY: (
                                stu.departure.uncertainty
                            ),
                        }
                        rows.append(row)
            else:
                # Trip with no stop updates (e.g., cancelled trip)
                row = {
                    **base,
                    Columns.STOP_SEQUENCE: None,
                    Columns.STOP_ID: None,
                    Columns.STOP_SCHEDULE_RELATIONSHIP: None,
                    Columns.ARRIVAL_DELAY: None,
                    Columns.ARRIVAL_TIME: None,
                    Columns.ARRIVAL_UNCERTAINTY: None,
                    Columns.DEPARTURE_DELAY: None,
                    Columns.DEPARTURE_TIME: None,
                    Columns.DEPARTURE_UNCERTAINTY: None,
                }
                rows.append(row)

        return rows
