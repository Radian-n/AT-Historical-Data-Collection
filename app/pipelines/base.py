"""Base pipeline class for GTFS-Realtime data ingestion."""

import hashlib
import logging
import pickle
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

from app.config import (
    AT_API_HEADERS,
    BUFFER_CHECKPOINT_ROOT,
    DATA_ROOT,
    POLL_INTERVAL_SECONDS,
    SAFE_DELAY_MINS,
)
from app.schemas.base import BaseTableSchema
from app.utils import derive_feed_partitions


class BaseRealtimePipeline(ABC):
    """
    Base class for GTFS-Realtime ingestion pipelines.

    Responsibilities:
        - Poll HTTP endpoint for protobuf feed
        - Skip identical payloads using MD5
        - Normalise feed into dictionaries (subclass-defined)
        - Every poll, add non-duplicate data to buffer
        - Write-ahead log (WAL) durability
        - Hourly partitioned Parquet writes

    Subclasses must define:
        - url: str - HTTP endpoint
        - table_name: str - Name for output directory and checkpoint files
        - table_schema: type[BaseTableSchema] - Schema with pa_schema(),
          dedupe_keys(), partition_cols()
        - normalise(feed, poll_time) -> list[dict]

    Subclasses may override:
        - headers: dict | None - HTTP headers (default: AT_API_HEADERS)
        - poll_interval: float - Seconds between polls (default: POLL_INTERVAL_SECONDS)
        - safe_delay_mins: timedelta - Wait after hour ends before writing (default: 16 mins)
    """

    # ──────────────────────────────────────────────────────────────────────────
    # Subclass must define these
    # ──────────────────────────────────────────────────────────────────────────
    url: str
    table_name: str
    table_schema: type[BaseTableSchema]

    # ──────────────────────────────────────────────────────────────────────────
    # Subclass may override these
    # ──────────────────────────────────────────────────────────────────────────
    headers: dict | None = AT_API_HEADERS
    poll_interval: float = POLL_INTERVAL_SECONDS
    safe_delay_mins: timedelta = timedelta(minutes=SAFE_DELAY_MINS)

    def __init__(self) -> None:
        self.log = logging.getLogger(self.__class__.__name__)

        # Derived from table schema
        self._schema: pa.Schema = self.table_schema.pa_schema()
        self._partition_cols: list[str] = self.table_schema.partition_cols()
        self._dedupe_keys: list[str] = self.table_schema.dedupe_keys()
        self._verify_dedupe_keys(self._dedupe_keys, self._schema.names)

        # Runtime state
        self._last_md5: str | None = None
        self._checkpoint_dir = BUFFER_CHECKPOINT_ROOT / self.table_name
        self._hour_buffer_path = self._checkpoint_dir / "hour_buffers.pickle"
        self._hour_seen_keys_path = (
            self._checkpoint_dir / "hour_seen_keys.pickle"
        )
        self._hour_buffers, self._hour_seen_keys = self._init_buffers()

    @abstractmethod
    def normalise(
        self, feed: gtfs_realtime_pb2.FeedMessage, poll_time: datetime
    ) -> list[dict[str, Any]]:
        """Convert decoded feed entities into normalised row dictionaries.

        Method must also convert any timestamp columns required in further
        processing steps into the correct datetime format (UTC).
        """
        raise NotImplementedError

    def run_forever(self) -> None:
        """Run the pipeline indefinitely."""
        while True:
            try:
                self._run_once()
                self.log.info(
                    "Pipeline run successful. Waiting %ds...",
                    self.poll_interval,
                )
            except Exception:
                # Handle any exceptions without stopping the loop
                self.log.exception("Unexpected error")
            time.sleep(self.poll_interval)

    def _run_once(self) -> None:
        """Run a single fetch, normalise, buffer write sequence."""
        poll_time = datetime.now(timezone.utc)

        try:
            resp = requests.get(url=self.url, headers=self.headers)
            resp.raise_for_status()
        except requests.RequestException:
            self.log.exception("HTTP fetch failed")
            return

        raw_bytes = resp.content

        # Skip identical feed payloads
        md5 = hashlib.md5(raw_bytes).hexdigest()
        if md5 == self._last_md5:
            self.log.info("Feed unchanged (MD5 match); skipping")
            return

        self._last_md5 = md5

        try:
            feed = self._decode_feed(raw_bytes)
            self.log.info("%d protobuf entities decoded", len(feed.entity))
        except DecodeError:
            self.log.exception("Failed to parse protobuf message")
            # Try again with the next data intake
            return

        try:
            rows = self.normalise(feed, poll_time)
            self.log.info("%d rows normalised", len(rows))
        except Exception:
            self.log.exception("Unexpected error in self.normalise")
            return

        self._add_to_buffers(rows)
        self.log.info(
            "Hour buffer sizes: %s", self._buffer_print(self._hour_buffers)
        )
        self.log.info(
            "Hour seen key buffer sizes: %s",
            self._buffer_print(self._hour_seen_keys),
        )

        # Store the buffer state to disk
        self._save_buffer_checkpoint()

        self._flush_ready_hours()

    def _decode_feed(self, raw_bytes: bytes) -> gtfs_realtime_pb2.FeedMessage:
        """Decode raw GTFS-Realtime protobuf bytes into a FeedMessage."""
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(raw_bytes)
        return feed

    def _dedupe(
        self, rows: list[dict[str, Any]], key_names: list[str]
    ) -> list[dict[str, Any]]:
        """Deduplicate rows in a list based on key_names."""
        seen = set()
        out = []
        for row in rows:
            key = tuple(row.get(col) for col in key_names)
            if None in key:
                raise RuntimeError("`key` includes a None value.")
            if key not in seen:
                seen.add(key)
                out.append(row)
        return out

    def _write_parquet(self, rows: list[dict[str, Any]]) -> None:
        """Write a parquet file partitioned by hour and date."""
        table = pa.Table.from_pylist(rows, schema=self._schema)
        table = derive_feed_partitions(table, self.table_schema.FEED_TIMESTAMP)
        pq.write_to_dataset(
            table,
            root_path=DATA_ROOT / self.table_name,
            partition_cols=self._partition_cols,
            compression="zstd",
        )
        self.log.info("%d rows written to parquet partition", len(table))

    def _verify_dedupe_keys(
        self, dedupe_keys: list[str], columns: list[str]
    ) -> None:
        """Ensure all dedupe keys exist in schema."""
        missing_keys: set[str] = set(dedupe_keys) - set(columns)
        if missing_keys:
            raise ValueError(
                f"The following dedupe_key_columns are missing from schema: {missing_keys}"
            )
        self.log.info("Initialised")

    # ──────────────────────────────────────────────────────────────────────────
    # Buffer methods
    # ──────────────────────────────────────────────────────────────────────────

    def _init_buffers(
        self,
    ) -> tuple[
        dict[datetime, list[dict[str, Any]]],
        dict[datetime, set[tuple[Any, ...]]],
    ]:
        """Initialise hour buffers, loading from checkpoint if available."""
        hour_buffers, hour_seen_keys = self._load_buffer_checkpoint()
        if hour_buffers is not None:
            return hour_buffers, hour_seen_keys
        return defaultdict(list), defaultdict(set)

    def _load_buffer_checkpoint(
        self,
    ) -> tuple[
        dict[datetime, list[dict[str, Any]]] | None,
        dict[datetime, set[tuple[Any, ...]]],
    ]:
        """Load buffer state from pickle checkpoint files.

        Returns:
            Tuple of (hour_buffers, hour_seen_keys). If no checkpoint exists,
            returns (None, empty defaultdict).
        """
        if not self._hour_buffer_path.exists():
            self.log.info("No buffer checkpoint found, starting fresh")
            return None, defaultdict(set)

        with open(self._hour_buffer_path, "rb") as f:
            hour_buffers = pickle.load(f)
        self.log.info(
            "Loaded hour buffers from checkpoint: %s",
            self._buffer_print(hour_buffers),
        )

        # Load seen keys if available, otherwise rebuild from buffers
        if self._hour_seen_keys_path.exists():
            with open(self._hour_seen_keys_path, "rb") as f:
                hour_seen_keys = pickle.load(f)
            self.log.info(
                "Loaded hour seen keys from checkpoint: %s",
                self._buffer_print(hour_seen_keys),
            )
        else:
            hour_seen_keys = self._rebuild_seen_keys(hour_buffers)
            self.log.info(
                "Rebuilt hour seen keys from hour buffers: %s",
                self._buffer_print(hour_seen_keys),
            )

        return hour_buffers, hour_seen_keys

    def _rebuild_seen_keys(
        self, hour_buffers: dict[datetime, list[dict[str, Any]]]
    ) -> dict[datetime, set[tuple[Any, ...]]]:
        """Rebuild seen keys set from hour buffers."""
        seen_keys = defaultdict(set)
        for hour, rows in hour_buffers.items():
            for row in rows:
                key = tuple(row[col] for col in self._dedupe_keys)
                seen_keys[hour].add(key)
        return seen_keys

    def _add_to_buffers(self, rows: list[dict[str, Any]]) -> None:
        """Add row data to buffer dict, using timestamp hour as key."""
        added_rows = 0
        for row in rows:
            ts = row[self.table_schema.FEED_TIMESTAMP]
            hour_key = ts.replace(minute=0, second=0, microsecond=0)

            key = tuple(row[col] for col in self._dedupe_keys)

            if key not in self._hour_seen_keys[hour_key]:
                self._hour_seen_keys[hour_key].add(key)
                self._hour_buffers[hour_key].append(row)
                added_rows += 1
        self.log.info("%d rows added to buffer(s)", added_rows)

    def _flush_ready_hours(self) -> None:
        """Flush hour partitions that are past the safe delay."""
        for hour_key in self._get_ready_hours():
            self._flush_hour(hour_key)

    def _get_ready_hours(self) -> list[datetime]:
        """Return hour keys that are safe to write.

        An hour is ready when the current time is past the hour's end
        plus the safe delay margin.
        """
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=1) - self.safe_delay_mins
        return [
            hour_key
            for hour_key in sorted(self._hour_buffers.keys())
            if hour_key < cutoff
        ]

    def _flush_hour(self, hour_key: datetime) -> None:
        """Dedupe, write to parquet, and clear buffer for one hour."""
        # Final de-duplication of data. TODO: Should be able to remove
        deduped_buffer = self._dedupe(
            self._hour_buffers[hour_key], self._dedupe_keys
        )
        self.log.info(
            "%d rows de-duplicated from hour buffer: %s",
            len(self._hour_buffers[hour_key]) - len(deduped_buffer),
            hour_key,
        )

        # Write data to partitioned parquet file
        self._write_parquet(deduped_buffer)

        # Clear written hour from buffer and seen keys
        del self._hour_buffers[hour_key]
        del self._hour_seen_keys[hour_key]

    def _save_buffer_checkpoint(self) -> None:
        """Persist buffer state to pickle checkpoint files."""
        self._checkpoint_dir.mkdir(parents=True, exist_ok=True)

        with open(self._hour_buffer_path, "wb") as f:
            pickle.dump(self._hour_buffers, f)

        with open(self._hour_seen_keys_path, "wb") as f:
            pickle.dump(self._hour_seen_keys, f)

        self.log.debug("Buffer checkpoint saved")

    def _buffer_print(self, buffer) -> str:
        """The length of each hour in the buffer"""
        return ", ".join(
            [f"{k:%Y-%m-%d %H}hr: {len(v)}" for k, v in buffer.items()]
        )
