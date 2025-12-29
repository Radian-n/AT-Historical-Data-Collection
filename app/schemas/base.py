from abc import abstractmethod
from enum import StrEnum

import pyarrow as pa


class BaseColumns(StrEnum):
    @classmethod
    def names(cls) -> list[str]:
        """Ordered list of column names"""
        return [c.value for c in cls]

    @classmethod
    def dedupe_keys(cls):
        """The columns used as keys to de-duplicate data"""
        raise NotImplementedError(f"{cls.__name__} must define dedupe_keys() method")

    @classmethod
    def partition_cols(cls) -> list[str]:
        """The columns used to partition the parquet data writes"""
        raise NotImplementedError(f"{cls.__name__} must define partition_cols() method")

    @classmethod
    def schema(cls) -> pa.schema:
        """The parquet schema"""
        raise NotImplementedError(f"{cls.__name__} must define schema() method")
