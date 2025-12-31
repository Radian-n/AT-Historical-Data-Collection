"""Tests for buffer management logic."""

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import patch

import pytest

from app.entities.vehicle_positions import VehiclePositionEntity
from app.pipeline import RealtimePipeline

pytestmark = pytest.mark.unit


def make_row(
    vehicle_id: str,
    feed_timestamp: datetime,
    poll_time: datetime | None = None,
) -> dict[str, Any]:
    """Create a row dict for buffer testing."""
    if poll_time is None:
        poll_time = feed_timestamp + timedelta(seconds=5)

    return {
        VehiclePositionEntity.POLL_TIME: poll_time,
        VehiclePositionEntity.FEED_TIMESTAMP: feed_timestamp,
        VehiclePositionEntity.VEHICLE_ID: vehicle_id,
        VehiclePositionEntity.LABEL: "Bus",
        VehiclePositionEntity.LICENSE_PLATE: "ABC123",
        VehiclePositionEntity.TRIP_ID: "trip_1",
        VehiclePositionEntity.ROUTE_ID: "route_1",
        VehiclePositionEntity.DIRECTION_ID: 0,
        VehiclePositionEntity.SCHEDULE_RELATIONSHIP: 0,
        VehiclePositionEntity.START_DATE: "20241215",
        VehiclePositionEntity.START_TIME: "10:00:00",
        VehiclePositionEntity.LATITUDE: -36.85,
        VehiclePositionEntity.LONGITUDE: 174.76,
        VehiclePositionEntity.BEARING: 90.0,
        VehiclePositionEntity.SPEED: 10.0,
        VehiclePositionEntity.ODOMETER: 5000.0,
        VehiclePositionEntity.OCCUPANCY_STATUS: 1,
        VehiclePositionEntity.ENTITY_IS_DELETED: False,
    }


@pytest.fixture
def pipeline(tmp_path) -> RealtimePipeline:
    """Create a pipeline with mocked checkpoint paths."""
    # Patch checkpoint paths to use tmp_path
    with patch("app.pipeline.BUFFER_CHECKPOINT_ROOT", tmp_path):
        p = RealtimePipeline(
            entity=VehiclePositionEntity,
            headers=None,
            max_data_age=timedelta(minutes=10),
            flush_delay=timedelta(minutes=5),
        )
    return p


class TestAddToBuffers:
    """Tests for RealtimePipeline._add_to_buffers()."""

    def test_add_row_to_empty_buffer(
        self, pipeline: RealtimePipeline
    ) -> None:
        """Row is added to empty buffer."""
        now = datetime(2024, 12, 15, 10, 35, 0, tzinfo=timezone.utc)
        ts = datetime(2024, 12, 15, 10, 30, 0, tzinfo=timezone.utc)
        row = make_row("v1", ts)

        pipeline._add_to_buffers([row], now=now)

        hour_key = ts.replace(minute=0, second=0, microsecond=0)
        assert hour_key in pipeline._hour_buffers
        assert len(pipeline._hour_buffers[hour_key]) == 1

    def test_add_multiple_rows_same_hour(
        self, pipeline: RealtimePipeline
    ) -> None:
        """Multiple rows in same hour are grouped together."""
        now = datetime(2024, 12, 15, 10, 35, 0, tzinfo=timezone.utc)
        ts1 = datetime(2024, 12, 15, 10, 30, 0, tzinfo=timezone.utc)
        ts2 = datetime(2024, 12, 15, 10, 31, 0, tzinfo=timezone.utc)
        rows = [
            make_row("v1", ts1),
            make_row("v2", ts2),
        ]

        pipeline._add_to_buffers(rows, now=now)

        hour_key = ts1.replace(minute=0, second=0, microsecond=0)
        assert len(pipeline._hour_buffers[hour_key]) == 2

    def test_duplicate_row_rejected(self, pipeline: RealtimePipeline) -> None:
        """Duplicate rows (same key) are not added twice."""
        now = datetime(2024, 12, 15, 10, 35, 0, tzinfo=timezone.utc)
        ts = datetime(2024, 12, 15, 10, 30, 0, tzinfo=timezone.utc)
        row = make_row("v1", ts)

        pipeline._add_to_buffers([row], now=now)
        pipeline._add_to_buffers([row], now=now)  # Add again

        hour_key = ts.replace(minute=0, second=0, microsecond=0)
        assert len(pipeline._hour_buffers[hour_key]) == 1

    def test_stale_row_rejected(self, pipeline: RealtimePipeline) -> None:
        """Rows older than max_data_age are discarded."""
        now = datetime(2024, 12, 15, 10, 35, 0, tzinfo=timezone.utc)
        # Row is 20 minutes old, max_data_age is 10 minutes
        stale_ts = datetime(2024, 12, 15, 10, 15, 0, tzinfo=timezone.utc)
        row = make_row("v1", stale_ts)

        pipeline._add_to_buffers([row], now=now)

        # Buffer should be empty (stale row discarded)
        assert len(pipeline._hour_buffers) == 0

    def test_rows_grouped_by_hour(self, pipeline: RealtimePipeline) -> None:
        """Rows from different hours go to different buffers."""
        # Use timestamps within max_data_age (10 min) of "now"
        now = datetime(2024, 12, 15, 11, 5, 0, tzinfo=timezone.utc)
        ts_10 = datetime(2024, 12, 15, 10, 58, 0, tzinfo=timezone.utc)
        ts_11 = datetime(2024, 12, 15, 11, 2, 0, tzinfo=timezone.utc)
        rows = [
            make_row("v1", ts_10),
            make_row("v2", ts_11),
        ]

        pipeline._add_to_buffers(rows, now=now)

        hour_10 = datetime(2024, 12, 15, 10, 0, 0, tzinfo=timezone.utc)
        hour_11 = datetime(2024, 12, 15, 11, 0, 0, tzinfo=timezone.utc)

        assert len(pipeline._hour_buffers[hour_10]) == 1
        assert len(pipeline._hour_buffers[hour_11]) == 1


class TestGetReadyHours:
    """Tests for RealtimePipeline._get_ready_hours()."""

    def test_old_hour_is_ready(self, pipeline: RealtimePipeline) -> None:
        """Hour past cutoff is returned as ready."""
        # Set up: hour 09:00 with data
        hour_9 = datetime(2024, 12, 15, 9, 0, 0, tzinfo=timezone.utc)
        pipeline._hour_buffers[hour_9] = [{"test": "data"}]

        # Current time: 11:10 (1 hour + 5 min delay past hour 10)
        now = datetime(2024, 12, 15, 11, 10, 0, tzinfo=timezone.utc)

        ready = pipeline._get_ready_hours(now=now)

        assert hour_9 in ready

    def test_recent_hour_not_ready(self, pipeline: RealtimePipeline) -> None:
        """Current hour is not returned as ready."""
        # Set up: current hour with data
        hour_11 = datetime(2024, 12, 15, 11, 0, 0, tzinfo=timezone.utc)
        pipeline._hour_buffers[hour_11] = [{"test": "data"}]

        # Current time: 11:10 (same hour)
        now = datetime(2024, 12, 15, 11, 10, 0, tzinfo=timezone.utc)

        ready = pipeline._get_ready_hours(now=now)

        assert hour_11 not in ready

    def test_empty_buffer_returns_empty(
        self, pipeline: RealtimePipeline
    ) -> None:
        """Empty buffer returns empty ready list."""
        now = datetime(2024, 12, 15, 11, 10, 0, tzinfo=timezone.utc)

        ready = pipeline._get_ready_hours(now=now)

        assert ready == []

    def test_multiple_ready_hours_sorted(
        self, pipeline: RealtimePipeline
    ) -> None:
        """Multiple ready hours are returned sorted."""
        hour_8 = datetime(2024, 12, 15, 8, 0, 0, tzinfo=timezone.utc)
        hour_9 = datetime(2024, 12, 15, 9, 0, 0, tzinfo=timezone.utc)
        pipeline._hour_buffers[hour_9] = [{"test": "data"}]
        pipeline._hour_buffers[hour_8] = [{"test": "data"}]

        # Current time: 11:10
        now = datetime(2024, 12, 15, 11, 10, 0, tzinfo=timezone.utc)

        ready = pipeline._get_ready_hours(now=now)

        assert ready == [hour_8, hour_9]


class TestRebuildSeenKeys:
    """Tests for RealtimePipeline._rebuild_seen_keys()."""

    def test_rebuild_from_buffered_rows(self) -> None:
        """Seen keys are rebuilt from buffered rows."""
        hour = datetime(2024, 12, 15, 10, 0, 0, tzinfo=timezone.utc)
        ts = datetime(2024, 12, 15, 10, 30, 0, tzinfo=timezone.utc)
        row = make_row("v1", ts)
        hour_buffers = {hour: [row]}
        dedupe_keys = VehiclePositionEntity.dedupe_keys()

        seen_keys = RealtimePipeline._rebuild_seen_keys(
            hour_buffers, dedupe_keys
        )

        expected_key = ("v1", ts)
        assert expected_key in seen_keys[hour]

    def test_rebuild_empty_buffers(self) -> None:
        """Empty buffers produce empty seen keys."""
        dedupe_keys = VehiclePositionEntity.dedupe_keys()

        seen_keys = RealtimePipeline._rebuild_seen_keys({}, dedupe_keys)

        assert len(seen_keys) == 0

    def test_rebuild_multiple_rows(self) -> None:
        """Multiple rows produce multiple seen keys."""
        hour = datetime(2024, 12, 15, 10, 0, 0, tzinfo=timezone.utc)
        ts1 = datetime(2024, 12, 15, 10, 30, 0, tzinfo=timezone.utc)
        ts2 = datetime(2024, 12, 15, 10, 31, 0, tzinfo=timezone.utc)
        rows = [make_row("v1", ts1), make_row("v2", ts2)]
        hour_buffers = {hour: rows}
        dedupe_keys = VehiclePositionEntity.dedupe_keys()

        seen_keys = RealtimePipeline._rebuild_seen_keys(
            hour_buffers, dedupe_keys
        )

        assert len(seen_keys[hour]) == 2
