"""Hourly cleanup: deduplication and compaction of Delta Lake tables.

Runs after each hour to deduplicate and compact the small parquet files
written during that hour into fewer, larger files.
"""

import logging
from datetime import datetime, timedelta, timezone
from logging import Logger

import duckdb
from deltalake import DeltaTable, write_deltalake

from app.columns import (
    TRIP_UPDATES_DEDUPE_KEYS,
    VEHICLE_POSITIONS_DEDUPE_KEYS,
    Columns,
)
from app.config import DATA_PATH, Tables

log: Logger = logging.getLogger(__name__)


def cleanup_hour(
    table_name: str,
    dedupe_keys: tuple[Columns, ...],
    now: datetime | None = None,
    data_path: str | None = None,
) -> None:
    """Deduplicate and compact a completed hour's data.

    Targets the previous hour's partition, deduplicates rows using the
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

    local_path = data_path / table_name

    if not local_path.exists():
        log.warning("Table %s does not exist, skipping cleanup", table_name)
        return

    # Target the previous hour
    target: datetime = now.replace(
        minute=0, second=0, microsecond=0
    ) - timedelta(hours=1)
    feed_date: str = target.strftime("%Y-%m-%d")
    feed_hour: int = target.hour

    # Build predicate for partition filtering
    predicate: str = f"feed_date = '{feed_date}' AND feed_hour = {feed_hour}"
    key_cols: str = ", ".join(str(k) for k in dedupe_keys)

    # Deduplication query using DuckDB:
    #
    # 1. delta_scan() reads the Delta Lake table with predicate pushdown
    # 2. WHERE clause filters to only the target hour's partition
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
            WHERE feed_date = '{feed_date}' AND feed_hour = {feed_hour}
        )
        WHERE rn = 1
    """

    deduped = duckdb.sql(query).fetch_arrow_table()

    if deduped.num_rows == 0:
        log.info(
            "No data for %s %s hour %d, skipping",
            table_name,
            feed_date,
            feed_hour,
        )
        return

    # Rewrite partition with deduplicated data (also compacts small files)
    write_deltalake(
        local_path,
        deduped,
        mode="overwrite",
        predicate=predicate,
    )

    # Vacuum old file versions (physically delete unreferenced files)
    dt = DeltaTable(local_path)
    dt.vacuum(
        retention_hours=0, dry_run=False, enforce_retention_duration=False
    )

    log.info(
        "Cleaned %s %s hour %d: %d rows",
        table_name,
        feed_date,
        feed_hour,
        deduped.num_rows,
    )


def cleanup_vehicle_positions(
    now: datetime | None = None, data_path: str | None = None
) -> None:
    """Cleanup vehicle_positions table for the previous hour."""
    cleanup_hour(
        table_name=Tables.VEHICLE_POSITIONS,
        dedupe_keys=VEHICLE_POSITIONS_DEDUPE_KEYS,
        now=now,
        data_path=data_path,
    )


def cleanup_trip_updates(
    now: datetime | None = None, data_path: str | None = None
) -> None:
    """Cleanup trip_updates table for the previous hour."""
    cleanup_hour(
        table_name=Tables.TRIP_UPDATES,
        dedupe_keys=TRIP_UPDATES_DEDUPE_KEYS,
        now=now,
        data_path=data_path,
    )


def cleanup_all(now: datetime | None = None) -> None:
    """Cleanup all tables for the previous hour."""
    cleanup_vehicle_positions(now=now)
    cleanup_trip_updates(now=now)
