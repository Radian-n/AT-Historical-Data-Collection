import pyarrow.compute as pc


def derive_feed_partitions(table, timestamp_col="feed_timestamp"):
    """Add date and hour columns into table for file partitioning"""
    ts = table[timestamp_col]

    # Extract date and hour (vectorized)
    feed_date = pc.strftime(ts, format="%Y-%m-%d")
    feed_hour = pc.hour(ts)

    table_partitions = table.append_column(
        "feed_date", feed_date
    ).append_column("feed_hour", feed_hour)

    return table_partitions
