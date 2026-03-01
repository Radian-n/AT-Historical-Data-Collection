"""Hourly compaction and retention cleanup for raw Delta Lake tables.

Runs hourly to compact small parquet files using Delta OPTIMIZE and
remove partitions older than the retention period.
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from logging import Logger
from pathlib import Path

from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from app.config import RAW_PATH, RAW_RETENTION_DAYS, Tables
from app.storage import join_path

log: Logger = logging.getLogger("Compaction")


def compact_table(
    table_name: str,
    raw_path: Path | None = None,
) -> None:
    """Run Delta OPTIMIZE on a raw table.

    Compacts small parquet files into larger ones for better query
    performance and reduced file count. Also vacuums to physically
    remove unreferenced files.

    Args:
        table_name: Name of the Delta Lake table to compact.
        raw_path: Base path for raw tables (for testability). Defaults to
            RAW_PATH from config.
    """
    if raw_path is None:
        raw_path = RAW_PATH

    table_path: str = join_path(raw_path, table_name)

    if not os.path.exists(table_path):
        log.info("Table %s does not exist, skipping compaction", table_name)
        return

    try:
        dt = DeltaTable(table_path)
        result = dt.optimize.compact()
        log.info(
            "Compacted %s: %d files added, %d files removed",
            table_name,
            result["numFilesAdded"],
            result["numFilesRemoved"],
        )

        # Vacuum to physically remove unreferenced files
        vacuumed_files = dt.vacuum(
            retention_hours=0,
            dry_run=False,
            enforce_retention_duration=False,
        )
        if vacuumed_files:
            log.info(
                "Vacuumed %d files from %s", len(vacuumed_files), table_name
            )
    except TableNotFoundError:
        log.info("Table %s not found, skipping compaction", table_name)


def cleanup_old_partitions(
    table_name: str,
    now: datetime | None = None,
    raw_path: Path | None = None,
    retention_days: int | None = None,
) -> None:
    """Delete raw partitions older than retention period.

    Removes data older than the retention period and vacuums the
    unreferenced files.

    Args:
        table_name: Name of the Delta Lake table to clean.
        now: Current time (for testability). Defaults to UTC now.
        raw_path: Base path for raw tables (for testability). Defaults to
            RAW_PATH from config.
        retention_days: Days to retain data (for testability). Defaults to
            RAW_RETENTION_DAYS from config.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    if raw_path is None:
        raw_path = RAW_PATH
    if retention_days is None:
        retention_days = RAW_RETENTION_DAYS

    table_path: str = join_path(raw_path, table_name)

    if not os.path.exists(table_path):
        log.info(
            "Table %s does not exist, skipping retention cleanup", table_name
        )
        return

    try:
        dt = DeltaTable(table_path)
    except TableNotFoundError:
        log.info("Table %s not found, skipping retention cleanup", table_name)
        return

    # Calculate cutoff date (YYYYMMDD format)
    cutoff: str = (now - timedelta(days=retention_days)).strftime("%Y%m%d")
    predicate: str = f"start_date < '{cutoff}'"

    # Delete old partitions
    deleted_rows: dict = dt.delete(predicate)

    if deleted_rows.get("num_deleted_rows", 0) > 0:
        log.info(
            "Deleted %d rows from %s (start_date < %s)",
            deleted_rows["num_deleted_rows"],
            table_name,
            cutoff,
        )

        # Vacuum to physically remove unreferenced files
        vacuumed_files = dt.vacuum(
            retention_hours=0,
            dry_run=False,
            enforce_retention_duration=False,
        )
        log.info("Vacuumed %d files from %s", len(vacuumed_files), table_name)


def compact_all(
    now: datetime | None = None,
    raw_path: Path | None = None,
) -> None:
    """Compact all raw tables and cleanup old partitions.

    Runs compaction and retention cleanup on all realtime tables:
    - vehicle_positions
    - trip_updates
    - stop_time_updates

    Args:
        now: Current time (for testability). Defaults to UTC now.
        raw_path: Base path for raw tables (for testability). Defaults to
            RAW_PATH from config.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    tables: list[str] = [
        Tables.VEHICLE_POSITIONS,
        Tables.TRIP_UPDATES,
        Tables.STOP_TIME_UPDATES,
    ]

    for table_name in tables:
        compact_table(table_name, raw_path=raw_path)
        cleanup_old_partitions(table_name, now=now, raw_path=raw_path)
