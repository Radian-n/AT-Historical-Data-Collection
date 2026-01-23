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
# This value should be less than CLEANUP_MINUTE.
STALE_THRESHOLD_MINUTES: Final[int] = 15

# Minute of the hour to run cleanup (dedupe + compact).
# AT API can return data up to ~15 minutes old, so a response at 05:14:30
# may still contain data from the 04:00 hour. Running at minute 20 provides
# a buffer to ensure all late-arriving data has been ingested before cleanup.
CLEANUP_MINUTE: Final[int] = 20

# API
AT_API_KEY: Final[str] = os.getenv("AT_API_KEY", "")
if not AT_API_KEY:
    raise RuntimeError("AT_API_KEY not set")


# Storage
DATA_PATH: Final[Path] = Path("data")


class Tables(StrEnum):
    VEHICLE_POSITIONS = "vehicle_positions"
    TRIP_UPDATES = "trip_updates"
    STOP_TIME_UPDATES = "stop_time_updates"
