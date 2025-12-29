"""Base schema class for GTFS data tables."""

from enum import StrEnum

import pyarrow as pa


class BaseColumns(StrEnum):
    """Base class for column enums defining table schemas.

    Subclasses must define column names as class attributes and implement
    the abstract class methods: dedupe_keys(), partition_cols(), schema().
    """

    @classmethod
    def names(cls) -> list[str]:
        """Return ordered list of column names."""
        return [c.value for c in cls]

    @classmethod
    def dedupe_keys(cls) -> list[str]:
        """Return the columns used as keys to de-duplicate data."""
        raise NotImplementedError(
            f"{cls.__name__} must define dedupe_keys() method"
        )

    @classmethod
    def partition_cols(cls) -> list[str]:
        """Return the columns used to partition parquet data writes."""
        raise NotImplementedError(
            f"{cls.__name__} must define partition_cols() method"
        )

    @classmethod
    def schema(cls) -> pa.Schema:
        """Return the PyArrow schema for the table."""
        raise NotImplementedError(
            f"{cls.__name__} must define schema() method"
        )
