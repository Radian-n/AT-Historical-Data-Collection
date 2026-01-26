"""Configuration settings for the Auckland Transport data collection app."""

import os
from enum import StrEnum
from pathlib import Path
from typing import Final

from dotenv import load_dotenv

load_dotenv()

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
# Storage Paths
# =============================================================================

DATA_PATH: Final[Path] = Path(os.getenv("DATA_PATH", "data"))
RAW_PATH: Final[Path] = DATA_PATH / "raw"
PROCESSED_PATH: Final[Path] = DATA_PATH / "processed"

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
RAW_RETENTION_DAYS: Final[int] = int(os.getenv("RAW_RETENTION_DAYS", "7"))

# =============================================================================
# Table Names
# =============================================================================


class Tables(StrEnum):
    VEHICLE_POSITIONS = "vehicle_positions"
    TRIP_UPDATES = "trip_updates"
    STOP_TIME_UPDATES = "stop_time_updates"
    # Processed layer table (merged arrival/departure events)
    STOP_TIME_EVENTS = "stop_time_events"
