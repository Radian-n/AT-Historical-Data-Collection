"""Daily cleanup: deduplication and compaction of Delta Lake tables.

Runs daily to deduplicate and compact the small parquet files
written during that day into fewer, larger files.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from logging import Logger
from pathlib import Path

import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from app.columns import (
    STOP_TIME_UPDATES_DEDUPE_KEYS,
    TRIP_UPDATES_DEDUPE_KEYS,
    VEHICLE_POSITIONS_DEDUPE_KEYS,
    Columns,
)
from app.config import DATA_PATH, Tables

log: Logger = logging.getLogger("DailyCleanup")


@dataclass
class TargetPartition:
    """Target partition for cleanup operations.

    Attributes:
        start_date: Date string in YYYYMMDD format (NZ operational day).
        predicate: SQL predicate for partition filtering.
    """

    start_date: str
    predicate: str


def _get_target_partition(now: datetime) -> TargetPartition:
    """Calculate the target partition for cleanup (previous day).

    Uses NZ timezone to determine the operational day boundary.
    Cleanup runs after midnight UTC, targeting the previous NZ day.

    Args:
        now: Current time (UTC).

    Returns:
        TargetPartition with start_date and SQL predicate.
    """
    # Target the previous day's data
    target: datetime = now.replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    # GTFS start_date format is YYYYMMDD (no dashes)
    start_date: str = target.strftime("%Y%m%d")
    predicate: str = f"{Columns.START_DATE} = '{start_date}'"
    return TargetPartition(start_date, predicate)


def _write_and_vacuum(
    local_path: Path,
    data: pa.Table,
    partition: TargetPartition,
    partition_cols: list[str],
    table_name: str,
) -> None:
    """Write data to Delta Lake and vacuum old files.

    Args:
        local_path: Path to the Delta Lake table.
        data: PyArrow table to write.
        partition: Target partition info.
        partition_cols: Columns to partition by.
        table_name: Name of the table (for logging).
    """
    # Rewrite partition with processed data (also compacts small files)
    write_deltalake(
        local_path,
        data,
        mode="overwrite",
        predicate=partition.predicate,
        partition_by=partition_cols,
    )

    # Reload table to see the new commit before vacuuming
    dt = DeltaTable(local_path)

    # Vacuum old file versions (physically delete unreferenced files)
    vacuumed_files = dt.vacuum(
        retention_hours=0, dry_run=False, enforce_retention_duration=False
    )

    log.info(
        "Cleaned %s %s: %d rows, %d files vacuumed",
        table_name,
        partition.start_date,
        data.num_rows,
        len(vacuumed_files),
    )


def cleanup_day(
    table_name: str,
    dedupe_keys: tuple[Columns, ...],
    now: datetime | None = None,
    data_path: Path | None = None,
) -> None:
    """Deduplicate and compact a completed day's data.

    Targets the previous day's partition, deduplicates rows using the
    specified keys (keeping one row per unique key combination), rewrites
    the partition as a single compacted file, and vacuums old versions.

    Args:
        table_name: Name of the Delta Lake table (e.g., "vehicle_positions").
        dedupe_keys: Columns that uniquely identify a record.
        now: Current time (for testability). Defaults to UTC now.
        data_path: Base path for tables (for testability). Defaults to
            DATA_PATH from config.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    if data_path is None:
        data_path = DATA_PATH

    local_path: Path = data_path / table_name

    if not local_path.exists():
        log.warning("Table %s does not exist, skipping cleanup", table_name)
        return

    # Load table and read partition columns from metadata
    dt = DeltaTable(local_path)
    partition_cols: list[str] = dt.metadata().partition_columns

    partition: TargetPartition = _get_target_partition(now)
    key_cols: str = ", ".join(str(k) for k in dedupe_keys)

    # Deduplication query using DuckDB:
    #
    # 1. delta_scan() reads the Delta Lake table with predicate pushdown
    # 2. WHERE clause filters to only the target day's partition
    # 3. ROW_NUMBER() assigns a sequence within each group of duplicates
    #    (grouped by dedupe_keys). No ORDER BY is needed because rows with
    #    identical dedupe keys have identical data - duplicates arise from
    #    the AT API returning stale data across multiple polls.
    # 4. Outer WHERE rn = 1 keeps exactly one row per unique key combination
    # 5. EXCLUDE (rn) removes the helper column from the final result
    query: str = f"""
        SELECT * EXCLUDE (rn) FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY {key_cols}) as rn
            FROM delta_scan('{local_path}')
            WHERE {Columns.START_DATE} = '{partition.start_date}'
        )
        WHERE rn = 1
    """

    deduped: pa.Table = duckdb.sql(query).fetch_arrow_table()

    if deduped.num_rows == 0:
        log.info(
            "No data for %s %s, skipping",
            table_name,
            partition.start_date,
        )
        return

    _write_and_vacuum(
        local_path, deduped, partition, partition_cols, table_name
    )


def cleanup_stop_time_updates_day(
    now: datetime | None = None,
    data_path: Path | None = None,
) -> None:
    """Merge arrival/departure rows and compact stop time updates.

    Unlike vehicle positions which just deduplicates, stop time updates
    require merging arrival and departure rows for the same stop. A vehicle
    arriving and departing at a stop may be recorded as separate rows
    (different feed_timestamps), and this function merges them into one row.

    The merge logic:
    - Groups by (trip_id, start_date, stop_sequence, stop_id)
    - Takes arrival_* fields from rows where arrival_time IS NOT NULL
      AND arrival_uncertainty = 0 (confirmed, not predicted)
    - Takes departure_* fields from rows where departure_time IS NOT NULL
      AND departure_uncertainty = 0 (confirmed, not predicted)
    - Takes the latest feed_timestamp and poll_time for shared fields

    The uncertainty filter excludes prediction data. A row may be included
    during ingest because it has valid departure data, but its arrival data
    could be a prediction (uncertainty != 0). The filter ensures only
    confirmed times are retained in the merged output.

    Args:
        now: Current time (for testability). Defaults to UTC now.
        data_path: Base path for tables (for testability). Defaults to
            DATA_PATH from config.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    if data_path is None:
        data_path = DATA_PATH

    local_path: Path = data_path / Tables.STOP_TIME_UPDATES

    if not local_path.exists():
        log.warning(
            "Table %s does not exist, skipping cleanup",
            Tables.STOP_TIME_UPDATES,
        )
        return

    # Load table and read partition columns from metadata
    dt = DeltaTable(local_path)
    partition_cols: list[str] = dt.metadata().partition_columns

    partition: TargetPartition = _get_target_partition(now)

    # Merge query: combines arrival and departure rows for the same stop.
    #
    # Arrival fields are taken from rows where arrival_time IS NOT NULL
    # and arrival_uncertainty = 0 (excludes predictions).
    # Departure fields are taken from rows where departure_time IS NOT NULL
    # and departure_uncertainty = 0 (excludes predictions).
    # For rows that have both (rare), both will be populated from same row.
    # For shared fields, we take the latest values (MAX).
    #
    # Column order matches StopTimeUpdates schema in ingest.py.
    query: str = f"""
        SELECT
            -- Timestamps
            MAX({Columns.POLL_TIME}) as {Columns.POLL_TIME},
            MAX({Columns.FEED_TIMESTAMP}) as {Columns.FEED_TIMESTAMP},
            -- FK columns
            {Columns.TRIP_ID},
            {Columns.START_DATE},
            MAX({Columns.ROUTE_ID}) as {Columns.ROUTE_ID},
            -- Stop update data
            {Columns.STOP_SEQUENCE},
            {Columns.STOP_ID},
            MAX({Columns.STOP_SCHEDULE_RELATIONSHIP})
                as {Columns.STOP_SCHEDULE_RELATIONSHIP},
            MAX(CASE WHEN {Columns.ARRIVAL_TIME} IS NOT NULL
                      AND {Columns.ARRIVAL_UNCERTAINTY} = 0
                THEN {Columns.ARRIVAL_DELAY} END) as {Columns.ARRIVAL_DELAY},
            MAX(CASE WHEN {Columns.ARRIVAL_TIME} IS NOT NULL
                      AND {Columns.ARRIVAL_UNCERTAINTY} = 0
                THEN {Columns.ARRIVAL_TIME} END) as {Columns.ARRIVAL_TIME},
            MAX(CASE WHEN {Columns.ARRIVAL_TIME} IS NOT NULL
                      AND {Columns.ARRIVAL_UNCERTAINTY} = 0
                THEN {Columns.ARRIVAL_UNCERTAINTY} END)
                as {Columns.ARRIVAL_UNCERTAINTY},
            MAX(CASE WHEN {Columns.DEPARTURE_TIME} IS NOT NULL
                      AND {Columns.DEPARTURE_UNCERTAINTY} = 0
                THEN {Columns.DEPARTURE_DELAY} END) as {Columns.DEPARTURE_DELAY},
            MAX(CASE WHEN {Columns.DEPARTURE_TIME} IS NOT NULL
                      AND {Columns.DEPARTURE_UNCERTAINTY} = 0
                THEN {Columns.DEPARTURE_TIME} END) as {Columns.DEPARTURE_TIME},
            MAX(CASE WHEN {Columns.DEPARTURE_TIME} IS NOT NULL
                      AND {Columns.DEPARTURE_UNCERTAINTY} = 0
                THEN {Columns.DEPARTURE_UNCERTAINTY} END)
                as {Columns.DEPARTURE_UNCERTAINTY}
        FROM delta_scan('{local_path}')
        WHERE {Columns.START_DATE} = '{partition.start_date}'
        GROUP BY {Columns.TRIP_ID}, {Columns.START_DATE},
                 {Columns.STOP_SEQUENCE}, {Columns.STOP_ID}
    """

    merged: pa.Table = duckdb.sql(query).fetch_arrow_table()

    if merged.num_rows == 0:
        log.info(
            "No data for %s %s, skipping",
            Tables.STOP_TIME_UPDATES,
            partition.start_date,
        )
        return

    _write_and_vacuum(
        local_path, merged, partition, partition_cols, Tables.STOP_TIME_UPDATES
    )


def cleanup_trip_updates_day(
    now: datetime | None = None,
    data_path: Path | None = None,
) -> None:
    """Deduplicate trip updates, keeping only the latest observation per trip.

    For the trip_updates table, we want to keep one row per (trip_id,
    start_date) with the latest feed_timestamp. This preserves the final
    known state of each trip (especially important for cancellations).

    Args:
        now: Current time (for testability). Defaults to UTC now.
        data_path: Base path for tables (for testability). Defaults to
            DATA_PATH from config.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    if data_path is None:
        data_path = DATA_PATH

    local_path: Path = data_path / Tables.TRIP_UPDATES

    if not local_path.exists():
        log.warning(
            "Table %s does not exist, skipping cleanup", Tables.TRIP_UPDATES
        )
        return

    # Load table and read partition columns from metadata
    dt = DeltaTable(local_path)
    partition_cols: list[str] = dt.metadata().partition_columns

    partition: TargetPartition = _get_target_partition(now)
    key_cols: str = ", ".join(str(k) for k in TRIP_UPDATES_DEDUPE_KEYS)

    # Keep only the latest observation per (trip_id, start_date).
    # ORDER BY feed_timestamp DESC ensures we keep the most recent state.
    query: str = f"""
        SELECT * EXCLUDE (rn) FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY {key_cols}
                    ORDER BY {Columns.FEED_TIMESTAMP} DESC
                ) as rn
            FROM delta_scan('{local_path}')
            WHERE {Columns.START_DATE} = '{partition.start_date}'
        )
        WHERE rn = 1
    """

    deduped: pa.Table = duckdb.sql(query).fetch_arrow_table()

    if deduped.num_rows == 0:
        log.info(
            "No data for %s %s, skipping",
            Tables.TRIP_UPDATES,
            partition.start_date,
        )
        return

    _write_and_vacuum(
        local_path, deduped, partition, partition_cols, Tables.TRIP_UPDATES
    )


def cleanup_vehicle_positions(
    now: datetime | None = None, data_path: Path | None = None
) -> None:
    """Cleanup vehicle_positions table for the previous day."""
    cleanup_day(
        table_name=Tables.VEHICLE_POSITIONS,
        dedupe_keys=VEHICLE_POSITIONS_DEDUPE_KEYS,
        now=now,
        data_path=data_path,
    )


def cleanup_trip_updates(
    now: datetime | None = None, data_path: Path | None = None
) -> None:
    """Cleanup trip_updates table for the previous day."""
    cleanup_trip_updates_day(now=now, data_path=data_path)


def cleanup_stop_time_updates(
    now: datetime | None = None, data_path: Path | None = None
) -> None:
    """Cleanup stop_time_updates table for the previous day."""
    cleanup_stop_time_updates_day(now=now, data_path=data_path)


def cleanup_all(now: datetime | None = None) -> None:
    """Cleanup all tables for the previous day."""
    cleanup_vehicle_positions(now=now)
    cleanup_trip_updates(now=now)
    cleanup_stop_time_updates(now=now)
