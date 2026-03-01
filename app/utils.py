"""Utility functions for data transformation."""

import json
from pathlib import Path


def encode_metadata_dict(metadata: dict[str, object]) -> dict[str, bytes]:
    """Convert metadata values to JSON-encoded UTF-8 bytes."""
    out = {}
    for key, value in metadata.items():
        out[str(key)] = json.dumps(value).encode("utf-8")
    return out


def join_path(base: str, *parts: str) -> str:
    """Join path segments for either local or S3 paths."""
    if base.startswith("s3://"):
        return "/".join([base.rstrip("/"), *parts])
    return str(Path(base, *parts))
