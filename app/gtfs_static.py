"""GTFS static data fetcher for Auckland Transport.

This module handles downloading and caching of GTFS static schedule data.
"""

import logging
import zipfile

import requests
from requests.models import Response

# TODO: Move to config.py
GTFS_STATIC_URL = "https://gtfs.at.govt.nz/gtfs.zip"


class GTFSStatic:
    """Fetches and manages GTFS static schedule data.

    Uses conditional HTTP requests (If-Modified-Since, If-None-Match) to
    avoid re-downloading unchanged data.
    """

    url: str = GTFS_STATIC_URL
    headers: dict[str, str] = {}
    last_modified: str | None = None
    etag: str | None = None

    def __init__(self) -> None:
        """Initialise the GTFS static fetcher."""
        self.log = logging.getLogger(self.__class__.__name__)
        self._set_headers()

    def _set_headers(self) -> None:
        """Set HTTP headers for conditional requests."""
        # TODO: Implement header setup
        pass

    def run_forever(self) -> None:
        """Run the fetcher in a loop, checking for updates."""
        self._run_once()
        # TODO: Add loop with sleep interval

    def _run_once(self) -> None:
        """Perform a single fetch attempt."""
        if self.last_modified:
            self.headers["If-Modified-Since"] = self.last_modified

        if self.etag:
            self.headers["If-None-Match"] = self.etag

        resp: Response = requests.get(
            self.url, headers=self.headers, stream=True, timeout=30
        )

        if resp.status_code == 304:
            self.log.info("No update available.")
            return

        resp.raise_for_status()

        # TODO: Process and save the zip file
        _ = zipfile  # Placeholder for future use

        self.last_modified = resp.headers.get("Last-Modified")
        self.etag = resp.headers.get("ETag")

    def _write(self) -> None:
        """Write the downloaded data to storage."""
        # TODO: Implement write logic
        pass
