"""Storage configuration for Delta Lake operations."""

import os
from pathlib import Path

from app.config import (
    R2_ACCESS_KEY_ID,
    R2_ACCOUNT_ID,
    R2_SECRET_ACCESS_KEY,
)


def get_storage_options() -> dict | None:
    """Return storage options for Delta Lake operations."""
    if not R2_ACCOUNT_ID:
        return None
    return {
        "AWS_ACCESS_KEY_ID": R2_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": R2_SECRET_ACCESS_KEY,
        "AWS_ENDPOINT_URL": f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        "AWS_REGION": "auto",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",  # Safe for single writer
    }
