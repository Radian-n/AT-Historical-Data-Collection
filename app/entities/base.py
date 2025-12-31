"""Base entity class for GTFS data tables."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, ClassVar

import pyarrow as pa
from google.transit import gtfs_realtime_pb2


class BaseEntity(ABC):
    """Base class for GTFS entity definitions.

    Subclasses define the complete specification for a GTFS entity:
    - URL and table name for the feed
    - Schema mapping columns to PyArrow types
    - How to parse protobuf feed data (normalise)
    - How to derive computed columns (add_derived_columns)
    - Partitioning and deduplication configuration

    Required class attributes:
        URL: str - HTTP endpoint for the GTFS-Realtime feed
        TABLE_NAME: str - name for output directory and checkpoint files
        _schema: dict - mapping of Columns to pa.DataType

    Required methods:
        - normalise(feed, poll_time) -> list[dict]
        - dedupe_keys() -> list[str]
        - partition_cols() -> list[str]
        - pa_schema() -> pa.Schema

    Optional overrides:
        - add_derived_columns(table) -> pa.Table
    """

    URL: str
    TABLE_NAME: str
    _schema: ClassVar[dict[Any, pa.DataType]]

    @classmethod
    @abstractmethod
    def normalise(
        cls,
        feed: gtfs_realtime_pb2.FeedMessage,
        poll_time: datetime,
    ) -> list[dict[str, Any]]:
        """Parse protobuf feed into normalised row dictionaries."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def dedupe_keys(cls) -> list[str]:
        """Return columns used as keys to deduplicate data."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def partition_cols(cls) -> list[str]:
        """Return columns used to partition parquet files."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def pa_schema(cls) -> pa.Schema:
        """Return the PyArrow schema for the table."""
        raise NotImplementedError

    @classmethod
    def add_derived_columns(cls, table: pa.Table) -> pa.Table:
        """Add any derived columns needed before partitioning.

        Override if your schema has columns computed from existing data
        (e.g., extracting date/hour from a timestamp).
        """
        return table
