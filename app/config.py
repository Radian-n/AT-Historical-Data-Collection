"""Configuration settings for the Auckland Transport data collection app."""

from enum import StrEnum
import os
from pathlib import Path
from typing import Final

from dotenv import load_dotenv

load_dotenv()

# How long to wait before hitting the API again
POLL_INTERVAL_SECONDS: Final[float] = int(
    os.getenv("POLL_INTERVAL_SECONDS", "30")
)

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
