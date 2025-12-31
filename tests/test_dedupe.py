"""Tests for deduplication logic."""

from datetime import datetime, timezone
from typing import Any

import pytest

from app.entities.vehicle_positions import VehiclePositionEntity
from app.pipeline import RealtimePipeline

pytestmark = pytest.mark.unit


def make_row(
    vehicle_id: str,
    feed_timestamp: datetime,
    label: str = "Bus",
) -> dict[str, Any]:
    """Create a minimal row dict for testing."""
    return {
        VehiclePositionEntity.VEHICLE_ID: vehicle_id,
        VehiclePositionEntity.FEED_TIMESTAMP: feed_timestamp,
        VehiclePositionEntity.LABEL: label,
    }


class TestDedupe:
    """Tests for RealtimePipeline._dedupe()."""

    def test_no_duplicates(self) -> None:
        """All unique rows are returned."""
        ts = datetime(2024, 12, 15, 10, 0, 0, tzinfo=timezone.utc)
        rows = [
            make_row("v1", ts),
            make_row("v2", ts),
            make_row("v3", ts),
        ]
        key_cols = [
            VehiclePositionEntity.VEHICLE_ID,
            VehiclePositionEntity.FEED_TIMESTAMP,
        ]

        result = RealtimePipeline._dedupe(rows, key_cols)

        assert len(result) == 3

    def test_exact_duplicates(self) -> None:
        """Exact duplicate rows are deduplicated."""
        ts = datetime(2024, 12, 15, 10, 0, 0, tzinfo=timezone.utc)
        rows = [
            make_row("v1", ts, "Bus A"),
            make_row("v1", ts, "Bus A"),
        ]
        key_cols = [
            VehiclePositionEntity.VEHICLE_ID,
            VehiclePositionEntity.FEED_TIMESTAMP,
        ]

        result = RealtimePipeline._dedupe(rows, key_cols)

        assert len(result) == 1

    def test_same_key_different_data(self) -> None:
        """Rows with same key but different data keep first occurrence."""
        ts = datetime(2024, 12, 15, 10, 0, 0, tzinfo=timezone.utc)
        rows = [
            make_row("v1", ts, "First Label"),
            make_row("v1", ts, "Second Label"),
        ]
        key_cols = [
            VehiclePositionEntity.VEHICLE_ID,
            VehiclePositionEntity.FEED_TIMESTAMP,
        ]

        result = RealtimePipeline._dedupe(rows, key_cols)

        assert len(result) == 1
        assert result[0][VehiclePositionEntity.LABEL] == "First Label"

    def test_empty_list(self) -> None:
        """Empty input returns empty output."""
        key_cols = [
            VehiclePositionEntity.VEHICLE_ID,
            VehiclePositionEntity.FEED_TIMESTAMP,
        ]

        result = RealtimePipeline._dedupe([], key_cols)

        assert result == []

    def test_single_row(self) -> None:
        """Single row is returned unchanged."""
        ts = datetime(2024, 12, 15, 10, 0, 0, tzinfo=timezone.utc)
        rows = [make_row("v1", ts)]
        key_cols = [
            VehiclePositionEntity.VEHICLE_ID,
            VehiclePositionEntity.FEED_TIMESTAMP,
        ]

        result = RealtimePipeline._dedupe(rows, key_cols)

        assert len(result) == 1
        assert result[0][VehiclePositionEntity.VEHICLE_ID] == "v1"

    def test_none_in_key_raises_error(self) -> None:
        """None value in key column raises RuntimeError."""
        ts = datetime(2024, 12, 15, 10, 0, 0, tzinfo=timezone.utc)
        rows = [
            make_row("v1", ts),
            {
                VehiclePositionEntity.VEHICLE_ID: None,
                VehiclePositionEntity.FEED_TIMESTAMP: ts,
                VehiclePositionEntity.LABEL: "Bus",
            },
        ]
        key_cols = [
            VehiclePositionEntity.VEHICLE_ID,
            VehiclePositionEntity.FEED_TIMESTAMP,
        ]

        with pytest.raises(RuntimeError, match="None value"):
            RealtimePipeline._dedupe(rows, key_cols)

    def test_different_timestamps_not_deduplicated(self) -> None:
        """Same vehicle at different times are separate records."""
        ts1 = datetime(2024, 12, 15, 10, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2024, 12, 15, 10, 0, 30, tzinfo=timezone.utc)
        rows = [
            make_row("v1", ts1),
            make_row("v1", ts2),
        ]
        key_cols = [
            VehiclePositionEntity.VEHICLE_ID,
            VehiclePositionEntity.FEED_TIMESTAMP,
        ]

        result = RealtimePipeline._dedupe(rows, key_cols)

        assert len(result) == 2

    def test_preserves_order(self) -> None:
        """First occurrence is kept, order is preserved."""
        ts = datetime(2024, 12, 15, 10, 0, 0, tzinfo=timezone.utc)
        rows = [
            make_row("v3", ts),
            make_row("v1", ts),
            make_row("v2", ts),
            make_row("v1", ts),  # duplicate
        ]
        key_cols = [
            VehiclePositionEntity.VEHICLE_ID,
            VehiclePositionEntity.FEED_TIMESTAMP,
        ]

        result = RealtimePipeline._dedupe(rows, key_cols)

        assert len(result) == 3
        assert result[0][VehiclePositionEntity.VEHICLE_ID] == "v3"
        assert result[1][VehiclePositionEntity.VEHICLE_ID] == "v1"
        assert result[2][VehiclePositionEntity.VEHICLE_ID] == "v2"
