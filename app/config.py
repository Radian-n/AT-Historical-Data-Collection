"""Configuration settings for the Auckland Transport data collection app."""

import os
from pathlib import Path
from typing import Final

from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────────────────────────────────────
# API
# ──────────────────────────────────────────────────────────────────────────────

AT_API_KEY: Final[str] = os.getenv("AT_API_KEY", "")
if not AT_API_KEY:
    raise RuntimeError("AT_API_KEY not set")

AT_API_HEADERS: Final[dict[str, str]] = {
    "Ocp-Apim-Subscription-Key": AT_API_KEY,
    "Accept": "application/x-protobuf",
}

# ──────────────────────────────────────────────────────────────────────────────
# Polling
# ──────────────────────────────────────────────────────────────────────────────

# How often to fetch data from the API.
POLL_INTERVAL_SECONDS: Final[int] = 30

# ──────────────────────────────────────────────────────────────────────────────
# Data staleness and flush timing
# ──────────────────────────────────────────────────────────────────────────────
# These settings control how old data can be before it's rejected, and when
# hour buffers are flushed to parquet.
#
# Background:
# - The AT API can return data with timestamps up to ~15 minutes in the past.
#   e.g., a response at 05:14:30 may contain data timestamped before 05:00.
# - Data is buffered by hour and flushed to partitioned parquet files.
# - If old data arrives after its hour has been flushed, it would append to
#   an existing partition, causing duplicates.
#
# Solution:
# - MAX_DATA_AGE_MINS: Reject data older than this (the "staleness" threshold)
# - FLUSH_BUFFER_MINS: Additional wait time after max_data_age before flushing
# - Flush delay is derived as: MAX_DATA_AGE_MINS + FLUSH_BUFFER_MINS
#
# This guarantees flush_delay > max_data_age, so by the time an hour is
# flushed, any data for that hour will be older than max_data_age and
# rejected at ingestion.

# Maximum age (in minutes) of data to accept into the buffer.
# Records older than this are discarded as too stale.
MAX_DATA_AGE_MINS: Final[float] = 15.0

# Additional minutes to wait after MAX_DATA_AGE_MINS before flushing.
# Provides a safety margin for processing delays and clock skew.
FLUSH_BUFFER_MINS: Final[float] = 5.0

# ──────────────────────────────────────────────────────────────────────────────
# Storage
# ──────────────────────────────────────────────────────────────────────────────

# Root directory for parquet output.
DATA_ROOT: Final[Path] = Path(os.getenv("DATA_ROOT", "data"))

# Root directory for buffer checkpoint files.
BUFFER_CHECKPOINT_ROOT: Final[Path] = Path(
    os.getenv("BUFFER_CHECKPOINT_ROOT", "buffer_checkpoint")
)
