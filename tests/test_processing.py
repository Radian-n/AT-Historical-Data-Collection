"""Tests for the processing module."""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pytest
from conftest import (
    STOP_TIME_UPDATES_TEST_SCHEMA,
    TRIP_UPDATES_TEST_SCHEMA,
    VEHICLE_POSITIONS_TEST_SCHEMA,
    create_test_delta_table,
)

from app.columns import Columns
from app.config import Tables
from app.processing import (
    _get_target_date,
    process_all,
    process_stop_time_events,
    process_trip_updates,
    process_vehicle_positions,
)

pytestmark = pytest.mark.unit


class TestGetTargetDate:
    """Unit tests for _get_target_date helper function."""

    def test_returns_previous_day(self) -> None:
        """Should return the previous day's date."""
        now = datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
        result = _get_target_date(now)
        assert result == "20260101"

    def test_new_year_boundary(self) -> None:
        """At Jan 1, should return Dec 31 of previous year."""
        now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = _get_target_date(now)
        assert result == "20251231"

    def test_month_boundary(self) -> None:
        """At Feb 1, should return Jan 31."""
        now = datetime(2026, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = _get_target_date(now)
        assert result == "20260131"


class TestProcessVehiclePositions:
    """Tests for process_vehicle_positions function."""

    def test_deduplicates_vehicle_positions(
        self,
        tmp_path: Path,
        sample_vehicle_positions_data: list[dict],
    ) -> None:
        """Processing should remove duplicate rows based on dedupe keys."""
        raw_path: Path = tmp_path / "raw"
        processed_path: Path = tmp_path / "processed"
        raw_table_path: Path = raw_path / Tables.VEHICLE_POSITIONS
        processed_table_path: Path = processed_path / Tables.VEHICLE_POSITIONS

        create_test_delta_table(
            str(raw_table_path),
            sample_vehicle_positions_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        # Verify 4 rows in raw
        raw_count = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{raw_table_path}')"
        ).fetchone()[0]
        assert raw_count == 4

        # Run processing for Jan 1 (now = Jan 2)
        process_vehicle_positions(
            now=datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
            raw_path=raw_path,
            processed_path=processed_path,
        )

        # Verify 2 rows in processed (duplicates removed)
        processed_count = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{processed_table_path}')"
        ).fetchone()[0]
        assert processed_count == 2

    def test_skips_nonexistent_table(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Should log warning and skip if raw table doesn't exist."""
        raw_path: Path = tmp_path / "raw"
        processed_path: Path = tmp_path / "processed"

        with caplog.at_level(logging.WARNING):
            process_vehicle_positions(
                now=datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
                raw_path=raw_path,
                processed_path=processed_path,
            )

        assert "does not exist, skipping processing" in caplog.text

    def test_skips_empty_partition(
        self,
        tmp_path: Path,
        sample_vehicle_positions_data: list[dict],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Should skip if no data for target day."""
        raw_path: Path = tmp_path / "raw"
        processed_path: Path = tmp_path / "processed"
        raw_table_path: Path = raw_path / Tables.VEHICLE_POSITIONS

        # Create table with data for Jan 1
        create_test_delta_table(
            str(raw_table_path),
            sample_vehicle_positions_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        # Try to process Dec 31 (no data exists) - now = Jan 1
        with caplog.at_level(logging.INFO):
            process_vehicle_positions(
                now=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                raw_path=raw_path,
                processed_path=processed_path,
            )

        assert "No data for" in caplog.text

    def test_preserves_unique_rows(
        self,
        tmp_path: Path,
    ) -> None:
        """Should not remove rows with unique dedupe keys."""
        raw_path: Path = tmp_path / "raw"
        processed_path: Path = tmp_path / "processed"
        raw_table_path: Path = raw_path / Tables.VEHICLE_POSITIONS
        processed_table_path: Path = processed_path / Tables.VEHICLE_POSITIONS

        # Create data with all unique keys
        base_ts = datetime(2026, 1, 1, 10, 30, 0, tzinfo=timezone.utc)
        unique_data: list[dict[Columns, datetime | float | int | str]] = [
            {
                Columns.POLL_TIME: base_ts,
                Columns.FEED_TIMESTAMP: datetime(
                    2026, 1, 1, 10, 25, i, tzinfo=timezone.utc
                ),
                Columns.VEHICLE_ID: f"vehicle_{i}",
                Columns.ROUTE_ID: "route_1",
                Columns.START_DATE: "20260101",
                Columns.LATITUDE: -36.8485,
                Columns.LONGITUDE: 174.7633,
            }
            for i in range(5)
        ]

        create_test_delta_table(
            str(raw_table_path),
            unique_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        process_vehicle_positions(
            now=datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
            raw_path=raw_path,
            processed_path=processed_path,
        )

        # All 5 unique rows should be in processed
        count = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{processed_table_path}')"
        ).fetchone()[0]
        assert count == 5


class TestProcessTripUpdates:
    """Tests for process_trip_updates function."""

    def test_keeps_latest_observation(
        self,
        tmp_path: Path,
    ) -> None:
        """Should keep only the latest observation per trip."""
        raw_path: Path = tmp_path / "raw"
        processed_path: Path = tmp_path / "processed"
        raw_table_path: Path = raw_path / Tables.TRIP_UPDATES
        processed_table_path: Path = processed_path / Tables.TRIP_UPDATES

        # Create data with multiple observations of the same trip
        data: list[dict[str, Any]] = [
            {
                Columns.POLL_TIME: datetime(
                    2026, 1, 1, 10, 30, 0, tzinfo=timezone.utc
                ),
                Columns.FEED_TIMESTAMP: datetime(
                    2026, 1, 1, 10, 25, 0, tzinfo=timezone.utc
                ),
                Columns.TRIP_ID: "trip_A",
                Columns.ROUTE_ID: "route_1",
                Columns.DIRECTION_ID: 0,
                Columns.SCHEDULE_RELATIONSHIP: 0,  # SCHEDULED
                Columns.START_DATE: "20260101",
                Columns.START_TIME: "10:00:00",
                Columns.VEHICLE_ID: "vehicle_1",
                Columns.LABEL: None,
                Columns.LICENSE_PLATE: None,
                Columns.ENTITY_IS_DELETED: False,
            },
            # Later observation - trip was cancelled
            {
                Columns.POLL_TIME: datetime(
                    2026, 1, 1, 11, 0, 0, tzinfo=timezone.utc
                ),
                Columns.FEED_TIMESTAMP: datetime(
                    2026, 1, 1, 10, 55, 0, tzinfo=timezone.utc
                ),
                Columns.TRIP_ID: "trip_A",
                Columns.ROUTE_ID: "route_1",
                Columns.DIRECTION_ID: 0,
                Columns.SCHEDULE_RELATIONSHIP: 3,  # CANCELED
                Columns.START_DATE: "20260101",
                Columns.START_TIME: "10:00:00",
                Columns.VEHICLE_ID: "vehicle_1",
                Columns.LABEL: None,
                Columns.LICENSE_PLATE: None,
                Columns.ENTITY_IS_DELETED: False,
            },
        ]

        create_test_delta_table(
            str(raw_table_path),
            data,
            TRIP_UPDATES_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        process_trip_updates(
            now=datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
            raw_path=raw_path,
            processed_path=processed_path,
        )

        # Should have 1 row (latest observation)
        count = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{processed_table_path}')"
        ).fetchone()[0]
        assert count == 1

        # Verify it's the cancelled state (later observation)
        result = duckdb.sql(
            f"""
            SELECT schedule_relationship
            FROM delta_scan('{processed_table_path}')
            """
        ).fetchone()
        assert result[0] == 3  # CANCELED


class TestProcessStopTimeEvents:
    """Tests for process_stop_time_events function."""

    def test_merges_arrival_departure(
        self,
        tmp_path: Path,
        sample_stop_time_updates_data: list[dict],
    ) -> None:
        """Processing should merge arrival and departure rows for same stop."""
        raw_path: Path = tmp_path / "raw"
        processed_path: Path = tmp_path / "processed"
        raw_table_path: Path = raw_path / Tables.STOP_TIME_UPDATES
        processed_table_path: Path = processed_path / Tables.STOP_TIME_EVENTS

        create_test_delta_table(
            str(raw_table_path),
            sample_stop_time_updates_data,
            STOP_TIME_UPDATES_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        # Verify 4 rows in raw
        raw_count = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{raw_table_path}')"
        ).fetchone()[0]
        assert raw_count == 4

        process_stop_time_events(
            now=datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
            raw_path=raw_path,
            processed_path=processed_path,
        )

        # Verify 2 merged rows in processed
        processed_count = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{processed_table_path}')"
        ).fetchone()[0]
        assert processed_count == 2

        # Verify merged data
        merged_rows = duckdb.sql(
            f"""
            SELECT stop_sequence, arrival_delay,
                   arrival_time IS NOT NULL as has_arrival,
                   departure_delay,
                   departure_time IS NOT NULL as has_departure
            FROM delta_scan('{processed_table_path}')
            ORDER BY stop_sequence
            """
        ).fetchall()

        # Stop 1: should have both arrival and departure
        assert merged_rows[0][0] == 1  # stop_sequence
        assert merged_rows[0][1] == 30  # arrival_delay
        assert merged_rows[0][2] is True  # has_arrival
        assert merged_rows[0][3] == 45  # departure_delay
        assert merged_rows[0][4] is True  # has_departure

    def test_excludes_predictions(
        self,
        tmp_path: Path,
    ) -> None:
        """Predictions (uncertainty != 0) should be excluded."""
        raw_path: Path = tmp_path / "raw"
        processed_path: Path = tmp_path / "processed"
        raw_table_path: Path = raw_path / Tables.STOP_TIME_UPDATES
        processed_table_path: Path = processed_path / Tables.STOP_TIME_EVENTS

        base_ts = datetime(2026, 1, 1, 10, 25, 0, tzinfo=timezone.utc)
        data: list[dict[str, Any]] = [
            {
                Columns.POLL_TIME: base_ts,
                Columns.FEED_TIMESTAMP: base_ts,
                Columns.TRIP_ID: "trip_A",
                Columns.START_DATE: "20260101",
                Columns.ROUTE_ID: "route_1",
                Columns.STOP_SEQUENCE: 1,
                Columns.STOP_ID: "stop_100",
                Columns.STOP_SCHEDULE_RELATIONSHIP: 0,
                # Arrival is a prediction
                Columns.ARRIVAL_DELAY: 30,
                Columns.ARRIVAL_TIME: base_ts,
                Columns.ARRIVAL_UNCERTAINTY: 120,  # Predicted
                # Departure is confirmed
                Columns.DEPARTURE_DELAY: 45,
                Columns.DEPARTURE_TIME: base_ts,
                Columns.DEPARTURE_UNCERTAINTY: 0,
            },
        ]

        create_test_delta_table(
            str(raw_table_path),
            data,
            STOP_TIME_UPDATES_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        process_stop_time_events(
            now=datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
            raw_path=raw_path,
            processed_path=processed_path,
        )

        result = duckdb.sql(
            f"""
            SELECT arrival_delay, arrival_time IS NOT NULL as has_arrival,
                   departure_delay, departure_time IS NOT NULL as has_departure
            FROM delta_scan('{processed_table_path}')
            """
        ).fetchone()

        # Arrival should be NULL (prediction excluded)
        assert result[0] is None  # arrival_delay
        assert result[1] is False  # has_arrival

        # Departure should be preserved
        assert result[2] == 45  # departure_delay
        assert result[3] is True  # has_departure

    def test_writes_to_stop_time_events_table(
        self,
        tmp_path: Path,
        sample_stop_time_updates_data: list[dict],
    ) -> None:
        """Should write to stop_time_events (not stop_time_updates)."""
        raw_path: Path = tmp_path / "raw"
        processed_path: Path = tmp_path / "processed"
        raw_table_path: Path = raw_path / Tables.STOP_TIME_UPDATES
        stop_time_events_path: Path = processed_path / Tables.STOP_TIME_EVENTS

        create_test_delta_table(
            str(raw_table_path),
            sample_stop_time_updates_data,
            STOP_TIME_UPDATES_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        process_stop_time_events(
            now=datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
            raw_path=raw_path,
            processed_path=processed_path,
        )

        # stop_time_events should exist
        assert stop_time_events_path.exists()

        # stop_time_updates in processed should NOT exist
        stu_processed_path: Path = processed_path / Tables.STOP_TIME_UPDATES
        assert not stu_processed_path.exists()


class TestProcessAll:
    """Tests for process_all function."""

    def test_processes_all_tables(
        self,
        tmp_path: Path,
        sample_vehicle_positions_data: list[dict],
        sample_stop_time_updates_data: list[dict],
    ) -> None:
        """Should process all realtime tables."""
        raw_path: Path = tmp_path / "raw"
        processed_path: Path = tmp_path / "processed"

        # Create raw vehicle_positions
        vp_raw_path: Path = raw_path / Tables.VEHICLE_POSITIONS
        create_test_delta_table(
            str(vp_raw_path),
            sample_vehicle_positions_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        # Create raw stop_time_updates
        stu_raw_path: Path = raw_path / Tables.STOP_TIME_UPDATES
        create_test_delta_table(
            str(stu_raw_path),
            sample_stop_time_updates_data,
            STOP_TIME_UPDATES_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        process_all(
            now=datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
            raw_path=raw_path,
            processed_path=processed_path,
        )

        # Check vehicle_positions was processed
        vp_processed_path: Path = processed_path / Tables.VEHICLE_POSITIONS
        assert vp_processed_path.exists()
        vp_count = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{vp_processed_path}')"
        ).fetchone()[0]
        assert vp_count == 2  # Deduplicated from 4

        # Check stop_time_events was created
        ste_processed_path: Path = processed_path / Tables.STOP_TIME_EVENTS
        assert ste_processed_path.exists()
        ste_count = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{ste_processed_path}')"
        ).fetchone()[0]
        assert ste_count == 2  # Merged from 4

    def test_handles_missing_tables_gracefully(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Should handle missing raw tables without error."""
        raw_path: Path = tmp_path / "raw"
        processed_path: Path = tmp_path / "processed"

        with caplog.at_level(logging.WARNING):
            process_all(
                now=datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
                raw_path=raw_path,
                processed_path=processed_path,
            )

        # Should log warnings about missing tables
        assert "does not exist" in caplog.text
