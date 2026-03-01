"""Configuration settings for the Auckland Transport data collection app."""

import os
from enum import StrEnum
from typing import Final

from dotenv import load_dotenv

from app.utils import join_path

load_dotenv()


# =============================================================================
# Sentry
# =============================================================================
SENTRY_DSN: Final[str | None] = os.getenv("SENTRY_DSN")
SENTRY_ENVIRONMENT: Final[str | None] = os.getenv("SENTRY_ENVIRONMENT")

# =============================================================================
# API Configuration
# =============================================================================

AT_API_KEY: Final[str] = os.getenv("AT_API_KEY", "")
if not AT_API_KEY:
    raise RuntimeError("AT_API_KEY not set")

GTFS_STATIC_URL: Final[str] = os.getenv(
    "GTFS_STATIC_URL", "https://gtfs.at.govt.nz/gtfs.zip"
)

# =============================================================================
# R2 Configuration
# =============================================================================

R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.getenv("R2_BUCKET", "")

# =============================================================================
# Storage Paths
# =============================================================================

# Raw and metadata stored locally
LOCAL_DATA_PATH = os.getenv("LOCAL_DATA_PATH", "data")
METADATA_PATH: Final[str] = join_path(LOCAL_DATA_PATH, "metadata")
RAW_DATA_PATH: Final[str] = join_path(LOCAL_DATA_PATH, "raw")

# Processed and static data stored to remote
if R2_ACCOUNT_ID and R2_BUCKET:
    REMOTE_DATA_PATH = f"s3://{R2_BUCKET}"
else:
    REMOTE_DATA_PATH = join_path(LOCAL_DATA_PATH, "processed")
PROCESSED_DATA_PATH: Final[str] = join_path(REMOTE_DATA_PATH, "realtime")
STATIC_DATA_PATH: Final[str] = join_path(REMOTE_DATA_PATH, "static")

# =============================================================================
# Scheduling
# =============================================================================

# How long to wait before hitting the API again.
POLL_INTERVAL_SECONDS: Final[float] = int(
    os.getenv("POLL_INTERVAL_SECONDS", "30")
)

# Minute of the hour to run compaction (Delta OPTIMIZE + retention cleanup).
# AT API can return data up to ~15 minutes old, so a response at 05:14:30
# may still contain data from the 04:00 hour. Running at minute 20 provides
# a buffer to ensure all late-arriving data has been ingested before compaction.
COMPACTION_MINUTE: Final[int] = 20

# Hour of day (NZ time) to run daily processing (raw -> processed).
# Default 4:00 AM NZT gives ~4 hours buffer after end of NZ operational day.
# Allows any trips that started just before midnight to end before processing.
# APScheduler handles daylight saving (NZST/NZDT) automatically.
PROCESSING_HOUR_NZT: Final[int] = int(os.getenv("PROCESSING_HOUR_NZT", "4"))

# Hour of day (NZ time) to check for GTFS static data updates.
# Default 3:00 AM NZT. APScheduler handles daylight saving automatically.
STATIC_INGEST_HOUR_NZT: Final[int] = int(
    os.getenv("STATIC_INGEST_HOUR_NZT", "3")
)

# =============================================================================
# Data Lifecycle
# =============================================================================

# Maximum age (minutes) for feed_timestamp relative to poll_time.
# Entities older than this are skipped to avoid partition sprawl.
# This value must be less than COMPACTION_MINUTE.
STALE_THRESHOLD_MINUTES: Final[int] = 15

if STALE_THRESHOLD_MINUTES >= COMPACTION_MINUTE:
    raise ValueError(
        f"STALE_THRESHOLD_MINUTES ({STALE_THRESHOLD_MINUTES}) must be less "
        f"than COMPACTION_MINUTE ({COMPACTION_MINUTE}) to avoid partition "
        "sprawl from late-arriving data."
    )

# Number of days to retain raw data before deletion.
RAW_RETENTION_DAYS: Final[int] = int(os.getenv("RAW_RETENTION_DAYS", "2"))

# =============================================================================
# Table Names
# =============================================================================


class Tables(StrEnum):
    VEHICLE_POSITIONS = "vehicle_positions"
    TRIP_UPDATES = "trip_updates"
    STOP_TIME_UPDATES = "stop_time_updates"
    # Processed layer table (merged arrival/departure events)
    STOP_TIME_EVENTS = "stop_time_events"
