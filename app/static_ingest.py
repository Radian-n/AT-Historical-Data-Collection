"""GTFS Static data ingestion with versioning support.

Downloads GTFS static data from Auckland Transport and stores versioned
snapshots in Delta Lake tables. Uses HTTP conditional requests
(If-Modified-Since, If-None-Match) to efficiently detect changes without
downloading the full ZIP file every time.
"""

import hashlib
import json
import logging
import os
from abc import ABC
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from logging import Logger
from pathlib import Path
from typing import Any, ClassVar
from zipfile import ZipFile

import pyarrow as pa
import pyarrow.csv as csv
import requests
from deltalake import write_deltalake
from requests.models import Response

from app.columns import STATIC_FIELD_TYPES, Columns, make_schema
from app.config import DATA_PATH, GTFS_STATIC_URL
from app.storage import get_storage_options, join_path


@dataclass
class FeedInfo:
    """Feed validity information from feed_info.txt.

    Attributes:
        feed_start_date: Start date of feed validity (YYYYMMDD string).
        feed_end_date: End date of feed validity (YYYYMMDD string).
        feed_version: Version identifier from the feed publisher.
    """

    feed_start_date: str
    feed_end_date: str
    feed_version: str


@dataclass
class StaticFetchMetadata:
    """HTTP metadata from the last successful GTFS static fetch.

    Attributes:
        etag: ETag header value from last response.
        last_modified: Last-Modified header value from last response.
        content_hash: MD5 hash of the last downloaded content.
        download_time: Timestamp of last successful download.
    """

    etag: str | None
    last_modified: str | None
    content_hash: str
    download_time: datetime

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "etag": self.etag,
            "last_modified": self.last_modified,
            "content_hash": self.content_hash,
            "download_time": self.download_time.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StaticFetchMetadata":
        """Create from dictionary loaded from JSON."""
        return cls(
            etag=data["etag"],
            last_modified=data["last_modified"],
            content_hash=data["content_hash"],
            download_time=datetime.fromisoformat(data["download_time"]),
        )


@dataclass
class StaticFetchResult:
    """Successful GTFS static fetch result.

    Attributes:
        zip_file: Opened ZipFile object containing GTFS CSV files.
        feed_info: Feed validity and version info from feed_info.txt.
        download_time: Timestamp when download was initiated.
        metadata: HTTP metadata to save after successful ingestion.
    """

    zip_file: ZipFile
    feed_info: FeedInfo
    download_time: datetime
    metadata: StaticFetchMetadata


def _parse_feed_info(zip_file: ZipFile) -> FeedInfo:
    """Parse feed_info.txt from GTFS zip to get validity dates.

    Args:
        zip_file: Opened ZipFile containing GTFS CSV files.

    Returns:
        FeedInfo with feed_start_date, feed_end_date, and feed_version.

    Raises:
        KeyError: If feed_info.txt not found in zip.
        ValueError: If required fields are missing from feed_info.txt.
    """
    with zip_file.open("feed_info.txt") as f:
        table = csv.read_csv(f)

    if len(table) == 0:
        raise ValueError("feed_info.txt is empty")

    # Get first row values
    row = {col: table[col][0].as_py() for col in table.column_names}

    # Date fields may be parsed as integers (YYYYMMDD format), convert to str
    feed_start_date: str | None = (
        str(row["feed_start_date"]) if row.get("feed_start_date") else None
    )
    feed_end_date: str | None = (
        str(row["feed_end_date"]) if row.get("feed_end_date") else None
    )
    # feed_version is typically a string, but convert just in case
    feed_version_raw = row.get("feed_version")
    feed_version: str | None = (
        str(feed_version_raw) if feed_version_raw is not None else None
    )

    if not feed_start_date or not feed_end_date:
        raise ValueError(
            "feed_info.txt missing feed_start_date or feed_end_date"
        )

    return FeedInfo(
        feed_start_date=feed_start_date,
        feed_end_date=feed_end_date,
        feed_version=feed_version or "",
    )


class GTFSStaticFetcher:
    """Fetches GTFS static data from Auckland Transport.

    Uses HTTP conditional requests (If-Modified-Since, If-None-Match) to
    avoid downloading unchanged files. Stores metadata about the last
    successful fetch to enable conditional requests.
    """

    url: ClassVar[str] = GTFS_STATIC_URL
    metadata_file: ClassVar[str] = join_path(
        DATA_PATH, "static", "static_metadata.json"
    )

    def __init__(self) -> None:
        self.log = logging.getLogger(f"{self.__class__.__name__}")
        self._metadata: StaticFetchMetadata | None = self._load_metadata()

    def _load_metadata(self) -> StaticFetchMetadata | None:
        """Load metadata from the last successful fetch."""
        if not os.path.exists(self.metadata_file):
            return None

        try:
            with open(self.metadata_file, "r") as f:
                data = json.load(f)
            return StaticFetchMetadata.from_dict(data)
        except Exception:
            self.log.exception("Failed to load metadata; ignoring")
            return None

    def _save_metadata(self, metadata: StaticFetchMetadata) -> None:
        """Save metadata for future conditional requests."""
        os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)
        with open(self.metadata_file, "w") as f:
            json.dump(metadata.to_dict(), f, indent=2)

    def fetch(self) -> StaticFetchResult | None:
        """Fetch GTFS static data using conditional HTTP requests.

        Returns:
            StaticFetchResult: On successful fetch with new/changed data.
            None: If server returns 304 Not Modified (unchanged).

        Raises:
            requests.RequestException: On HTTP errors.
        """
        download_time: datetime = datetime.now(timezone.utc)

        # Build conditional request headers
        headers: dict[str, str] = {}
        if self._metadata:
            if self._metadata.etag:
                headers["If-None-Match"] = self._metadata.etag
            if self._metadata.last_modified:
                headers["If-Modified-Since"] = self._metadata.last_modified

        # Make request
        resp: Response = requests.get(
            url=self.url, headers=headers, timeout=30
        )

        # Handle 304 Not Modified
        if resp.status_code == 304:
            self.log.info("GTFS static unchanged (304 Not Modified)")
            return None

        # Handle other status codes
        resp.raise_for_status()

        # Download successful - process content
        raw_bytes: bytes = resp.content
        content_hash: str = hashlib.md5(
            raw_bytes, usedforsecurity=False
        ).hexdigest()

        # Check if content actually changed (belt and suspenders approach)
        if self._metadata and content_hash == self._metadata.content_hash:
            self.log.info(
                "GTFS static unchanged (MD5 match despite 200 response)"
            )
            return None

        # Content has changed - prepare metadata (saved after successful ingest)
        new_metadata = StaticFetchMetadata(
            etag=resp.headers.get("ETag"),
            last_modified=resp.headers.get("Last-Modified"),
            content_hash=content_hash,
            download_time=download_time,
        )

        # Open zip and parse feed_info.txt for validity dates
        zip_file = ZipFile(BytesIO(raw_bytes))
        feed_info: FeedInfo = _parse_feed_info(zip_file)

        self.log.info(
            "GTFS static downloaded (%d bytes, %d files, version=%s, "
            "valid %s to %s)",
            len(raw_bytes),
            len(zip_file.namelist()),
            feed_info.feed_version,
            feed_info.feed_start_date,
            feed_info.feed_end_date,
        )

        return StaticFetchResult(
            zip_file=zip_file,
            feed_info=feed_info,
            download_time=download_time,
            metadata=new_metadata,
        )

    def commit_metadata(self, metadata: StaticFetchMetadata) -> None:
        """Save metadata after successful ingestion.

        Call this after all files have been successfully ingested to
        persist the metadata. This ensures that if ingestion fails,
        subsequent runs will re-download the data.
        """
        self._save_metadata(metadata)
        self._metadata = metadata
        self.log.info("Metadata committed after successful ingestion")


class StaticDataIngest(ABC):
    """Base class for processing GTFS static CSV files.

    Subclasses define file-specific configuration (schema, partition
    columns) and any custom transformations.

    Required class attributes:
        name: str - Entity name (used to derive csv_filename and write_path)
        schema: pa.Schema - PyArrow schema for the table
        partition_cols: list[str] - Columns to partition by

    Instance attributes (set in __init__):
        csv_filename: str - Name of CSV file in GTFS zip (name + ".txt")
        write_path: Path - Output path for Delta Lake table
    """

    name: ClassVar[str]
    partition_cols: ClassVar[list[str]]
    schema: ClassVar[pa.Schema]

    def __init__(self) -> None:
        self.log: Logger = logging.getLogger(f"{self.__class__.__name__}")
        self.csv_filename: str = self.name + ".txt"
        self.write_path: Path = join_path(DATA_PATH, "static", self.name)

    def ingest(
        self,
        zip_file: ZipFile,
        feed_info: FeedInfo,
        download_time: datetime,
    ) -> int:
        """Read CSV from zip and write to Delta Lake with versioning.

        Uses feed_start_date and feed_end_date from feed_info.txt as the
        validity period. If data already exists for this date range, it
        will be overwritten (corrections replace previous data).

        Args:
            zip_file: Opened ZipFile containing GTFS CSV files.
            feed_info: Feed validity and version info from feed_info.txt.
            download_time: Timestamp when the zip was downloaded.

        Returns:
            Number of rows written.

        Raises:
            KeyError: If csv_filename not found in zip.
        """
        # Read CSV from zip
        with zip_file.open(self.csv_filename) as f:
            table = csv.read_csv(f)
            self.log.info(
                "%d rows read from %s", len(table), self.csv_filename
            )

        # Add version tracking columns
        table = self.add_version_columns(table, feed_info, download_time)

        # Apply any custom transformations
        table = self.transform(table)

        # Ensure schema matches
        table = table.cast(self.schema)

        # Write to Delta Lake (overwrites existing data for same date range)
        self.write_data(
            table,
            self.partition_cols,
            path=self.write_path,
            feed_info=feed_info,
        )
        self.log.info(
            "%d rows written to Delta Lake at %s",
            len(table),
            self.write_path,
        )

        return len(table)

    def add_version_columns(
        self,
        table: pa.Table,
        feed_info: FeedInfo,
        download_time: datetime,
    ) -> pa.Table:
        """Add version tracking columns to the table.

        Args:
            table: Original table from CSV.
            feed_info: Feed validity and version info.
            download_time: Timestamp of download (unused, kept for interface).

        Returns:
            Table with version columns appended:
            - valid_from: Feed start date (from feed_info.txt)
            - valid_to: Feed end date (from feed_info.txt)
        """
        n_rows = len(table)

        valid_from = pa.array(
            [feed_info.feed_start_date] * n_rows, type=pa.string()
        )
        valid_to = pa.array(
            [feed_info.feed_end_date] * n_rows, type=pa.string()
        )

        return table.append_column("valid_from", valid_from).append_column(
            "valid_to", valid_to
        )

    def transform(self, table: pa.Table) -> pa.Table:
        """Apply custom transformations to the table.

        Override in subclasses to add file-specific logic (e.g., type
        conversions, derived columns, filtering).

        Args:
            table: Table with version columns added.

        Returns:
            Transformed table.
        """
        return table

    def write_data(
        self,
        data: pa.Table,
        partition_cols: list[str],
        path: Path | str,
        feed_info: FeedInfo,
    ) -> None:
        """Write table to Delta Lake, overwriting existing data for same dates.

        If data exists for the same (valid_from, valid_to) date range, it
        will be replaced. This handles corrections from AT where they
        re-publish data for the same validity period.

        Args:
            data: Table to write.
            partition_cols: Columns to partition by.
            path: Output path for Delta Lake table.
            feed_info: Feed validity info for overwrite predicate.
        """
        # Build predicate for overwriting existing data with same date range
        predicate: str = (
            f"valid_from = '{feed_info.feed_start_date}' "
            f"AND valid_to = '{feed_info.feed_end_date}'"
        )

        # Check if table exists to determine write mode
        table_path = Path(path)
        if os.path.exists(table_path):
            # Table exists - overwrite matching date range
            write_deltalake(
                table_or_uri=str(path),
                data=data,
                partition_by=partition_cols,
                mode="overwrite",
                predicate=predicate,
            )
        else:
            # New table - append (creates table)
            write_deltalake(
                table_or_uri=str(path),
                data=data,
                partition_by=partition_cols,
                mode="append",
            )


class AgencyData(StaticDataIngest):
    """GTFS agency.txt - Transit agency information."""

    name = "agency"
    partition_cols: list[str] = [Columns.VALID_FROM]

    schema: pa.Schema = make_schema(
        columns=[
            Columns.AGENCY_ID,
            Columns.AGENCY_NAME,
            Columns.AGENCY_URL,
            Columns.AGENCY_TIMEZONE,
            Columns.AGENCY_LANG,
            Columns.AGENCY_PHONE,
            Columns.AGENCY_FARE_URL,
            Columns.AGENCY_EMAIL,
            Columns.VALID_FROM,
            Columns.VALID_TO,
        ],
        field_types=STATIC_FIELD_TYPES,
        metadata={
            "entity": name,
            "version": "1",
            "partition_columns": partition_cols,
        },
    )


class StopsData(StaticDataIngest):
    """GTFS stops.txt - Stop/station locations."""

    name = "stops"
    partition_cols: list[str] = [Columns.VALID_FROM]

    schema: pa.Schema = make_schema(
        columns=[
            Columns.STOP_ID,
            Columns.STOP_CODE,
            Columns.STOP_NAME,
            Columns.STOP_DESC,
            Columns.STOP_LAT,
            Columns.STOP_LON,
            Columns.ZONE_ID,
            Columns.STOP_URL,
            Columns.LOCATION_TYPE,
            Columns.PARENT_STATION,
            Columns.STOP_TIMEZONE,
            Columns.PLATFORM_CODE,
            Columns.WHEELCHAIR_BOARDING,
            Columns.START_DATE,
            Columns.END_DATE,
            Columns.VALID_FROM,
            Columns.VALID_TO,
        ],
        field_types=STATIC_FIELD_TYPES,
        metadata={
            "entity": name,
            "version": "1",
            "partition_columns": partition_cols,
        },
    )


class RoutesData(StaticDataIngest):
    """GTFS routes.txt - Transit route definitions."""

    name = "routes"
    partition_cols: list[str] = [Columns.VALID_FROM]

    schema: pa.Schema = make_schema(
        columns=[
            Columns.ROUTE_ID,
            Columns.AGENCY_ID,
            Columns.ROUTE_SHORT_NAME,
            Columns.ROUTE_LONG_NAME,
            Columns.ROUTE_DESC,
            Columns.ROUTE_TYPE,
            Columns.ROUTE_URL,
            Columns.ROUTE_COLOR,
            Columns.ROUTE_TEXT_COLOR,
            Columns.ROUTE_SORT_ORDER,
            Columns.CONTRACT_ID,
            Columns.VALID_FROM,
            Columns.VALID_TO,
        ],
        field_types=STATIC_FIELD_TYPES,
        metadata={
            "entity": name,
            "version": "1",
            "partition_columns": partition_cols,
        },
    )


class TripsData(StaticDataIngest):
    """GTFS trips.txt - Individual trip definitions."""

    name = "trips"
    partition_cols: list[str] = [Columns.VALID_FROM]

    schema: pa.Schema = make_schema(
        columns=[
            Columns.ROUTE_ID,
            Columns.SERVICE_ID,
            Columns.TRIP_ID,
            Columns.TRIP_HEADSIGN,
            Columns.TRIP_SHORT_NAME,
            Columns.DIRECTION_ID,
            Columns.BLOCK_ID,
            Columns.SHAPE_ID,
            Columns.WHEELCHAIR_ACCESSIBLE,
            Columns.BIKES_ALLOWED,
            Columns.VALID_FROM,
            Columns.VALID_TO,
        ],
        field_types=STATIC_FIELD_TYPES,
        metadata={
            "entity": name,
            "version": "1",
            "partition_columns": partition_cols,
        },
    )


class StopTimesData(StaticDataIngest):
    """GTFS stop_times.txt - Stop times for each trip."""

    name = "stop_times"
    partition_cols: list[str] = [Columns.VALID_FROM]

    schema: pa.Schema = make_schema(
        columns=[
            Columns.TRIP_ID,
            Columns.ARRIVAL_TIME,
            Columns.DEPARTURE_TIME,
            Columns.STOP_ID,
            Columns.STOP_SEQUENCE,
            Columns.STOP_HEADSIGN,
            Columns.PICKUP_TYPE,
            Columns.DROP_OFF_TYPE,
            Columns.SHAPE_DIST_TRAVELED,
            Columns.TIMEPOINT,
            Columns.VALID_FROM,
            Columns.VALID_TO,
        ],
        field_types=STATIC_FIELD_TYPES,
        metadata={
            "entity": name,
            "version": "1",
            "partition_columns": partition_cols,
        },
    )


class CalendarData(StaticDataIngest):
    """GTFS calendar.txt - Service day schedules."""

    name = "calendar"
    partition_cols: list[str] = [Columns.VALID_FROM]

    schema: pa.Schema = make_schema(
        columns=[
            Columns.SERVICE_ID,
            Columns.MONDAY,
            Columns.TUESDAY,
            Columns.WEDNESDAY,
            Columns.THURSDAY,
            Columns.FRIDAY,
            Columns.SATURDAY,
            Columns.SUNDAY,
            Columns.START_DATE,
            Columns.END_DATE,
            Columns.VALID_FROM,
            Columns.VALID_TO,
        ],
        field_types=STATIC_FIELD_TYPES,
        metadata={
            "entity": name,
            "version": "1",
            "partition_columns": partition_cols,
        },
    )


class CalendarDatesData(StaticDataIngest):
    """GTFS calendar_dates.txt - Service exceptions."""

    name = "calendar_dates"
    partition_cols: list[str] = [Columns.VALID_FROM]

    schema: pa.Schema = make_schema(
        columns=[
            Columns.SERVICE_ID,
            Columns.DATE,
            Columns.EXCEPTION_TYPE,
            Columns.VALID_FROM,
            Columns.VALID_TO,
        ],
        field_types=STATIC_FIELD_TYPES,
        metadata={
            "entity": name,
            "version": "1",
            "partition_columns": partition_cols,
        },
    )


class ShapesData(StaticDataIngest):
    """GTFS shapes.txt - Vehicle travel path geometry."""

    name = "shapes"
    partition_cols: list[str] = [Columns.VALID_FROM]

    schema: pa.Schema = make_schema(
        columns=[
            Columns.SHAPE_ID,
            Columns.SHAPE_PT_LAT,
            Columns.SHAPE_PT_LON,
            Columns.SHAPE_PT_SEQUENCE,
            Columns.SHAPE_DIST_TRAVELED,
            Columns.VALID_FROM,
            Columns.VALID_TO,
        ],
        field_types=STATIC_FIELD_TYPES,
        metadata={
            "entity": name,
            "version": "1",
            "partition_columns": partition_cols,
        },
    )


class FareAttributesData(StaticDataIngest):
    """GTFS fare_attributes.txt - Fare information."""

    name = "fare_attributes"
    partition_cols: list[str] = [Columns.VALID_FROM]

    schema: pa.Schema = make_schema(
        columns=[
            Columns.FARE_ID,
            Columns.PRICE,
            Columns.CURRENCY_TYPE,
            Columns.PAYMENT_METHOD,
            Columns.TRANSFERS,
            Columns.AGENCY_ID,
            Columns.TRANSFER_DURATION,
            Columns.VALID_FROM,
            Columns.VALID_TO,
        ],
        field_types=STATIC_FIELD_TYPES,
        metadata={
            "entity": name,
            "version": "1",
            "partition_columns": partition_cols,
        },
    )


class FareRulesData(StaticDataIngest):
    """GTFS fare_rules.txt - Fare rules for routes."""

    name = "fare_rules"
    partition_cols: list[str] = [Columns.VALID_FROM]

    schema: pa.Schema = make_schema(
        columns=[
            Columns.FARE_ID,
            Columns.ROUTE_ID,
            Columns.ORIGIN_ID,
            Columns.DESTINATION_ID,
            Columns.CONTAINS_ID,
            Columns.VALID_FROM,
            Columns.VALID_TO,
        ],
        field_types=STATIC_FIELD_TYPES,
        metadata={
            "entity": name,
            "version": "1",
            "partition_columns": partition_cols,
        },
    )


class FrequenciesData(StaticDataIngest):
    """GTFS frequencies.txt - Headway-based service."""

    name = "frequencies"
    partition_cols: list[str] = [Columns.VALID_FROM]

    schema: pa.Schema = make_schema(
        columns=[
            Columns.TRIP_ID,
            Columns.START_TIME,
            Columns.END_TIME,
            Columns.HEADWAY_SECS,
            Columns.EXACT_TIMES,
            Columns.VALID_FROM,
            Columns.VALID_TO,
        ],
        field_types=STATIC_FIELD_TYPES,
        metadata={
            "entity": name,
            "version": "1",
            "partition_columns": partition_cols,
        },
    )


class TransfersData(StaticDataIngest):
    """GTFS transfers.txt - Transfer rules between stops/routes."""

    name = "transfers"
    partition_cols: list[str] = [Columns.VALID_FROM]

    schema: pa.Schema = make_schema(
        columns=[
            Columns.FROM_STOP_ID,
            Columns.TO_STOP_ID,
            Columns.TRANSFER_TYPE,
            Columns.MIN_TRANSFER_TIME,
            Columns.VALID_FROM,
            Columns.VALID_TO,
        ],
        field_types=STATIC_FIELD_TYPES,
        metadata={
            "entity": name,
            "version": "1",
            "partition_columns": partition_cols,
        },
    )


class FeedInfoData(StaticDataIngest):
    """GTFS feed_info.txt - Dataset metadata.

    Note: feed_info.txt already contains feed_start_date, feed_end_date,
    and feed_version columns. We only add valid_from, valid_to (copies of
    the feed dates for consistency) and downloaded_at for this table.
    """

    name = "feed_info"
    partition_cols: list[str] = [Columns.VALID_FROM]

    schema: pa.Schema = make_schema(
        columns=[
            Columns.FEED_PUBLISHER_NAME,
            Columns.FEED_PUBLISHER_URL,
            Columns.FEED_LANG,
            Columns.FEED_START_DATE,
            Columns.FEED_END_DATE,
            Columns.FEED_VERSION,
            Columns.VALID_FROM,
            Columns.VALID_TO,
            Columns.DOWNLOADED_AT,
        ],
        field_types=STATIC_FIELD_TYPES,
        metadata={
            "entity": name,
            "version": "1",
            "partition_columns": partition_cols,
        },
    )

    def add_version_columns(
        self,
        table: pa.Table,
        feed_info: FeedInfo,
        download_time: datetime,
    ) -> pa.Table:
        """Add version columns, excluding feed_version (already in CSV)."""
        n_rows = len(table)

        valid_from = pa.array(
            [feed_info.feed_start_date] * n_rows, type=pa.string()
        )
        valid_to = pa.array(
            [feed_info.feed_end_date] * n_rows, type=pa.string()
        )
        downloaded_at = pa.array(
            [download_time] * n_rows, type=pa.timestamp("s", tz="+00:00")
        )

        return (
            table.append_column("valid_from", valid_from)
            .append_column("valid_to", valid_to)
            .append_column("downloaded_at", downloaded_at)
        )


# Ingester instances for all GTFS static files
STATIC_INGESTERS: list[StaticDataIngest] = [
    AgencyData(),
    StopsData(),
    RoutesData(),
    TripsData(),
    StopTimesData(),
    CalendarData(),
    CalendarDatesData(),
    ShapesData(),
    FareAttributesData(),
    FareRulesData(),
    FrequenciesData(),
    TransfersData(),
    FeedInfoData(),
]

# Singleton instance
static_fetcher = GTFSStaticFetcher()


log: Logger = logging.getLogger(__name__)


def static_ingest() -> dict[str, int] | None:
    """Fetch GTFS static data and ingest all CSV files.

    Metadata is only saved after all files are successfully ingested.
    If any file fails, subsequent runs will re-download the data.

    Returns:
        Dict mapping filename to row count on success.
        None: If data unchanged (skipped).
    """
    result: StaticFetchResult | None = static_fetcher.fetch()

    if result is None:
        log.debug("GTFS static unchanged; skipping ingest")
        return None

    row_counts: dict[str, int] = {}
    had_errors: bool = False

    # Process each CSV file
    for ingester in STATIC_INGESTERS:
        try:
            rows: int = ingester.ingest(
                result.zip_file,
                result.feed_info,
                result.download_time,
            )
            row_counts[ingester.csv_filename] = rows
        except Exception:
            log.exception("Failed to ingest %s", ingester.csv_filename)
            had_errors = True

    # Only commit metadata if all files succeeded
    # This ensures failed ingests will retry on the next run
    if had_errors:
        log.warning(
            "Ingestion had errors; metadata NOT saved. "
            "Next run will re-download."
        )
    else:
        static_fetcher.commit_metadata(result.metadata)

    log.info(
        "GTFS static ingest complete: %d files, %d total rows",
        len(row_counts),
        sum(row_counts.values()),
    )

    return row_counts


if __name__ == "__main__":
    static_ingest()
