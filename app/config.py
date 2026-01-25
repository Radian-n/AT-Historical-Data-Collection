"""Configuration settings for the Auckland Transport data collection app."""

import os
from enum import StrEnum
from pathlib import Path
from typing import Final

from dotenv import load_dotenv

load_dotenv()

# How long to wait before hitting the API again
POLL_INTERVAL_SECONDS: Final[float] = int(
    os.getenv("POLL_INTERVAL_SECONDS", "30")
)

# Maximum age (minutes) for feed_timestamp relative to poll_time.
# Entities older than this are skipped to avoid partition sprawl.
# This value should be less than COMPACTION_MINUTE.
STALE_THRESHOLD_MINUTES: Final[int] = 15

# Minute of the hour to run compaction (Delta OPTIMIZE + retention cleanup).
# AT API can return data up to ~15 minutes old, so a response at 05:14:30
# may still contain data from the 04:00 hour. Running at minute 20 provides
# a buffer to ensure all late-arriving data has been ingested before compaction.
COMPACTION_MINUTE: Final[int] = 20

# Number of days to retain raw data before deletion.
RAW_RETENTION_DAYS: Final[int] = int(os.getenv("RAW_RETENTION_DAYS", "7"))

# Hour of day (UTC) to run daily processing (raw -> processed).
# Default 12:00 UTC provides a 12-hour buffer after end of NZ operational day.
PROCESSING_DELAY_HOURS: Final[int] = int(
    os.getenv("PROCESSING_DELAY_HOURS", "12")
)

# Hour of day (UTC) to check for GTFS static data updates.
# Default 15:00 UTC = 3:00am NZST (4:00am NZDT during daylight saving).
STATIC_INGEST_HOUR: Final[int] = int(os.getenv("STATIC_INGEST_HOUR", "15"))

# API
AT_API_KEY: Final[str] = os.getenv("AT_API_KEY", "")
if not AT_API_KEY:
    raise RuntimeError("AT_API_KEY not set")

GTFS_STATIC_URL = "https://gtfs.at.govt.nz/gtfs.zip"

# Storage
DATA_PATH: Final[Path] = Path("data")
RAW_PATH: Final[Path] = DATA_PATH / "raw"
PROCESSED_PATH: Final[Path] = DATA_PATH / "processed"


class Tables(StrEnum):
    VEHICLE_POSITIONS = "vehicle_positions"
    TRIP_UPDATES = "trip_updates"
    STOP_TIME_UPDATES = "stop_time_updates"
    # Processed layer table (merged arrival/departure events)
    STOP_TIME_EVENTS = "stop_time_events"
