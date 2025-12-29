"""Utility functions for GTFS data processing."""

import pyarrow as pa
import pyarrow.compute as pc


def derive_feed_partitions(table: pa.Table, timestamp_col: str) -> pa.Table:
    """Add date and hour columns to a table for file partitioning.

    Args:
        table: PyArrow table containing timestamp data.
        timestamp_col: Name of the timestamp column to derive from.

    Returns:
        Table with added 'feed_date' and 'feed_hour' columns.
    """
    ts = table[timestamp_col]

    # Extract date and hour (vectorized)
    feed_date = pc.strftime(ts, format="%Y-%m-%d")
    feed_hour = pc.hour(ts)

    table_partitions = table.append_column(
        "feed_date", feed_date
    ).append_column("feed_hour", feed_hour)

    return table_partitions
