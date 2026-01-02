"""Utility functions for data transformation."""

import json


def list_to_json_bytes(x: list) -> bytes:
    """Convert a list to a UTF-8 encoded JSON byte string.

    Used for storing array data in parquet files where the column
    type is binary.

    Args:
        x: A list to be serialized.

    Returns:
        UTF-8 encoded JSON representation of the list.
    """
    return json.dumps(x).encode("utf-8")
