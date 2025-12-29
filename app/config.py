"""Configuration settings for the Auckland Transport data collection app."""

import os
from pathlib import Path
from typing import Final

from dotenv import load_dotenv

load_dotenv()

# How long to wait before hitting the API again
POLL_INTERVAL_SECONDS: Final[float] = float(
    os.getenv("POLL_INTERVAL_SECONDS", "30.0")
)

# How many minutes to wait after the end of the hour before writing data.
# AT API can include data for ~15 minutes.
# Therefore, a response at 05:14:30 may still contain information from <5am.
# TODO: Check if this can be lower now that we're doing incremental dedupes
SAFE_DELAY_MINS: Final[float] = float(os.getenv("SAFE_DELAY_MINS", "16.0"))

# API
AT_API_KEY: Final[str] = os.getenv("AT_API_KEY", "")
if not AT_API_KEY:
    raise RuntimeError("AT_API_KEY not set")

AT_API_HEADERS: Final[dict[str, str]] = {
    "Ocp-Apim-Subscription-Key": AT_API_KEY,
    "Accept": "application/x-protobuf",
}

# Storage
DATA_ROOT: Final[Path] = Path("data")
BUFFER_CHECKPOINT_ROOT: Final[Path] = Path("buffer_checkpoint")
