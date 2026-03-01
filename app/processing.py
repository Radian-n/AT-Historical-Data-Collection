"""Daily processing: transform raw data into deduplicated processed tables.

Runs daily to process the previous day's raw data, deduplicating and
transforming it into the processed layer.
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from logging import Logger
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa
from deltalake import write_deltalake

from app.columns import (
    TRIP_UPDATES_DEDUPE_KEYS,
    VEHICLE_POSITIONS_DEDUPE_KEYS,
    Columns,
)
from app.config import PROCESSED_DATA_PATH, RAW_DATA_PATH, Tables
from app.storage import get_storage_options
from app.utils import join_path

log: Logger = logging.getLogger("Processing")

NZ_TZ: ZoneInfo = ZoneInfo("Pacific/Auckland")


def _get_target_date(now: datetime) -> str:
    """Calculate the target date for processing (previous NZ day).

    Converts current time to NZ timezone and returns yesterday's date.
    This correctly handles daylight saving transitions (NZST/NZDT).

    Args:
        now: Current time (should be timezone-aware, typically UTC).

    Returns:
        Date string in YYYYMMDD format for yesterday in NZ time.
    """
    nz_now: datetime = now.astimezone(NZ_TZ)
    yesterday_nz: datetime = nz_now - timedelta(days=1)
    return yesterday_nz.strftime("%Y%m%d")


def _write_processed(
    data: pa.Table,
    table_name: str,
    start_date: str,
    processed_path: str,
) -> None:
    """Write processed data to Delta Lake.

    Overwrites the target partition with the processed data.

    Args:
        data: PyArrow table to write.
        table_name: Name of the output table.
        start_date: Target date partition (YYYYMMDD).
        processed_path: str to processed tables directory.
    """
    output_path: str = join_path(processed_path, table_name)
    partition_cols: list[str] = [Columns.START_DATE, Columns.ROUTE_ID]
    predicate: str = f"{Columns.START_DATE} = '{start_date}'"

    write_deltalake(
        str(output_path),
        data,
        mode="overwrite",
        predicate=predicate,
        partition_by=partition_cols,
        storage_options=get_storage_options(),
    )

    log.info(
        "Processed %s %s: %d rows written",
        table_name,
        start_date,
        data.num_rows,
    )


def process_vehicle_positions(
    now: datetime | None = None,
    raw_path: str | None = None,
    processed_path: str | None = None,
) -> None:
    """Process raw vehicle positions into deduplicated output.

    Deduplicates by (vehicle_id, feed_timestamp), keeping one row per
    unique combination.

    Args:
        now: Current time (for testability). Defaults to UTC now.
        raw_path: Base path for raw tables (for testability).
        processed_path: Base path for processed tables (for testability).
    """
    if now is None:
        now = datetime.now(timezone.utc)
    if raw_path is None:
        raw_path = RAW_DATA_PATH
    if processed_path is None:
        processed_path = PROCESSED_DATA_PATH

    table_name: str = Tables.VEHICLE_POSITIONS
    raw_table_path: str = join_path(raw_path, table_name)

    if not os.path.exists(raw_table_path):
        log.warning(
            "Raw table %s does not exist, skipping processing", table_name
        )
        return

    start_date: str = _get_target_date(now)
    key_cols: str = ", ".join(str(k) for k in VEHICLE_POSITIONS_DEDUPE_KEYS)

    # Deduplication query: keep one row per (vehicle_id, feed_timestamp)
    query: str = f"""
        SELECT * EXCLUDE (rn) FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY {key_cols}) as rn
            FROM delta_scan('{raw_table_path}')
            WHERE {Columns.START_DATE} = '{start_date}'
        )
        WHERE rn = 1
    """

    deduped: pa.Table = duckdb.sql(query).fetch_arrow_table()

    if deduped.num_rows == 0:
        log.info("No data for %s %s, skipping", table_name, start_date)
        return

    _write_processed(deduped, table_name, start_date, processed_path)


def process_trip_updates(
    now: datetime | None = None,
    raw_path: str | None = None,
    processed_path: str | None = None,
) -> None:
    """Process raw trip updates into latest-state output.

    Keeps only the latest observation per (trip_id, start_date), ordered
    by feed_timestamp DESC. This preserves the final known state of each
    trip (especially important for cancellations).

    Args:
        now: Current time (for testability). Defaults to UTC now.
        raw_path: Base path for raw tables (for testability).
        processed_path: Base path for processed tables (for testability).
    """
    if now is None:
        now = datetime.now(timezone.utc)
    if raw_path is None:
        raw_path = RAW_DATA_PATH
    if processed_path is None:
        processed_path = PROCESSED_DATA_PATH

    table_name: str = Tables.TRIP_UPDATES
    raw_table_path: str = join_path(raw_path, table_name)

    if not os.path.exists(raw_table_path):
        log.warning(
            "Raw table %s does not exist, skipping processing", table_name
        )
        return

    start_date: str = _get_target_date(now)
    key_cols: str = ", ".join(str(k) for k in TRIP_UPDATES_DEDUPE_KEYS)

    # Keep only the latest observation per (trip_id, start_date)
    query: str = f"""
        SELECT * EXCLUDE (rn) FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY {key_cols}
                    ORDER BY {Columns.FEED_TIMESTAMP} DESC
                ) as rn
            FROM delta_scan('{raw_table_path}')
            WHERE {Columns.START_DATE} = '{start_date}'
        )
        WHERE rn = 1
    """

    deduped: pa.Table = duckdb.sql(query).fetch_arrow_table()

    if deduped.num_rows == 0:
        log.info("No data for %s %s, skipping", table_name, start_date)
        return

    _write_processed(deduped, table_name, start_date, processed_path)


def process_stop_time_events(
    now: datetime | None = None,
    raw_path: str | None = None,
    processed_path: str | None = None,
) -> None:
    """Process raw stop time updates into merged stop time events.

    Filters out predictions (uncertainty != 0) and merges arrival and
    departure rows for the same stop into a single event row.

    The merge logic:
    - Groups by (trip_id, start_date, stop_sequence, stop_id)
    - Takes arrival_* fields from rows where arrival_time IS NOT NULL
      AND arrival_uncertainty = 0 (confirmed, not predicted)
    - Takes departure_* fields from rows where departure_time IS NOT NULL
      AND departure_uncertainty = 0 (confirmed, not predicted)
    - Takes the latest feed_timestamp and poll_time for shared fields

    Args:
        now: Current time (for testability). Defaults to UTC now.
        raw_path: Base path for raw tables (for testability).
        processed_path: Base path for processed tables (for testability).
    """
    if now is None:
        now = datetime.now(timezone.utc)
    if raw_path is None:
        raw_path = RAW_DATA_PATH
    if processed_path is None:
        processed_path = PROCESSED_DATA_PATH

    raw_table_name: str = Tables.STOP_TIME_UPDATES
    output_table_name: str = Tables.STOP_TIME_EVENTS
    raw_table_path: str = join_path(raw_path, raw_table_name)

    if not os.path.exists(raw_table_path):
        log.warning(
            "Raw table %s does not exist, skipping processing",
            raw_table_name,
        )
        return

    start_date: str = _get_target_date(now)

    # Merge query: combines arrival and departure rows for the same stop.
    # Only includes confirmed times (uncertainty = 0).
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
        FROM delta_scan('{raw_table_path}')
        WHERE {Columns.START_DATE} = '{start_date}'
        GROUP BY {Columns.TRIP_ID}, {Columns.START_DATE},
                 {Columns.STOP_SEQUENCE}, {Columns.STOP_ID}
    """

    merged: pa.Table = duckdb.sql(query).fetch_arrow_table()

    if merged.num_rows == 0:
        log.info("No data for %s %s, skipping", raw_table_name, start_date)
        return

    _write_processed(merged, output_table_name, start_date, processed_path)


def process_all(
    now: datetime | None = None,
    raw_path: str | None = None,
    processed_path: str | None = None,
) -> None:
    """Process all raw tables into the processed layer.

    Runs daily processing for:
    - vehicle_positions: Deduplicate by (vehicle_id, feed_timestamp)
    - trip_updates: Keep latest state per (trip_id, start_date)
    - stop_time_events: Filter predictions, merge arrival/departure

    Args:
        now: Current time (for testability). Defaults to UTC now.
        raw_path: Base path for raw tables (for testability).
        processed_path: Base path for processed tables (for testability).
    """
    if now is None:
        now = datetime.now(timezone.utc)

    process_vehicle_positions(
        now=now, raw_path=raw_path, processed_path=processed_path
    )
    process_trip_updates(
        now=now, raw_path=raw_path, processed_path=processed_path
    )
    process_stop_time_events(
        now=now, raw_path=raw_path, processed_path=processed_path
    )
