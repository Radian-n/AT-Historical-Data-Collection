"""Utility functions for data transformation."""

import json


def encode_metadata_dict(metadata: dict[str, object]) -> dict[str, bytes]:
    """Convert metadata values to JSON-encoded UTF-8 bytes."""
    out = {}
    for key, value in metadata.items():
        out[str(key)] = json.dumps(value).encode("utf-8")
    return out
