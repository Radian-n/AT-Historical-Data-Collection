"""Tests for the cleanup module."""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pytest
from conftest import (
    STOP_TIME_UPDATES_TEST_SCHEMA,
    VEHICLE_POSITIONS_TEST_SCHEMA,
    create_test_delta_table,
)

from app.cleanup import (
    _get_target_partition,
    cleanup_day,
    cleanup_stop_time_updates_day,
    cleanup_vehicle_positions,
)
from app.columns import VEHICLE_POSITIONS_DEDUPE_KEYS, Columns
from app.config import Tables

pytestmark = pytest.mark.unit


class TestGetTargetPartition:
    """Unit tests for _get_target_partition helper function."""

    def test_returns_previous_day(self) -> None:
        """Should return the previous day's partition."""
        now = datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc)
        result = _get_target_partition(now)

        assert result.start_date == "20260101"
        assert "start_date = '20260101'" in result.predicate

    def test_new_year_boundary(self) -> None:
        """At Jan 1, should return Dec 31 of previous year."""
        now = datetime(2026, 1, 1, 0, 20, 0, tzinfo=timezone.utc)
        result = _get_target_partition(now)

        assert result.start_date == "20251231"

    def test_month_boundary(self) -> None:
        """At Feb 1, should return Jan 31."""
        now = datetime(2026, 2, 1, 10, 20, 0, tzinfo=timezone.utc)
        result = _get_target_partition(now)

        assert result.start_date == "20260131"


class TestCleanupDay:
    """Tests for cleanup_day function."""

    def test_deduplicates_vehicle_positions(
        self,
        tmp_path: Path,
        sample_vehicle_positions_data: list[dict],
    ) -> None:
        """Cleanup should remove duplicate rows based on dedupe keys."""
        table_path: Path = tmp_path / Tables.VEHICLE_POSITIONS

        create_test_delta_table(
            str(table_path),
            sample_vehicle_positions_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        # Verify 4 rows before cleanup
        before = duckdb.sql(
            f"SELECT COUNT(*) as cnt FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert before == 4

        # Run cleanup for Jan 1 (now = Jan 2, so previous day = Jan 1)
        cleanup_day(
            table_name=Tables.VEHICLE_POSITIONS,
            dedupe_keys=VEHICLE_POSITIONS_DEDUPE_KEYS,
            now=datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        # Verify 2 rows after cleanup (duplicates removed)
        after = duckdb.sql(
            f"SELECT COUNT(*) as cnt FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert after == 2

    def test_skips_nonexistent_table(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Cleanup should log warning and skip if table doesn't exist."""
        cleanup_day(
            table_name="nonexistent_table",
            dedupe_keys=VEHICLE_POSITIONS_DEDUPE_KEYS,
            now=datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        assert "does not exist, skipping cleanup" in caplog.text

    def test_skips_empty_partition(
        self,
        tmp_path: Path,
        sample_vehicle_positions_data: list[dict],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Cleanup should log info and skip if no data for target day."""
        table_path: Path = tmp_path / Tables.VEHICLE_POSITIONS

        # Create table with data for Jan 1
        create_test_delta_table(
            str(table_path),
            sample_vehicle_positions_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        # Try to clean Dec 31 (no data exists) - now = Jan 1
        with caplog.at_level(logging.INFO):
            cleanup_day(
                table_name=Tables.VEHICLE_POSITIONS,
                dedupe_keys=VEHICLE_POSITIONS_DEDUPE_KEYS,
                now=datetime(2026, 1, 1, 10, 20, 0, tzinfo=timezone.utc),
                data_path=tmp_path,
            )

        assert "No data for" in caplog.text

    def test_compacts_parquet_files(
        self,
        tmp_path: Path,
    ) -> None:
        """Cleanup should reduce the number of parquet files."""
        table_path: Path = tmp_path / Tables.VEHICLE_POSITIONS

        # Create data with single route to keep all files in one partition
        base_ts = datetime(2026, 1, 1, 10, 30, 0, tzinfo=timezone.utc)
        feed_ts = datetime(2026, 1, 1, 10, 25, 0, tzinfo=timezone.utc)
        single_partition_data: list[
            dict[Columns, datetime | float | int | str]
        ] = [
            {
                Columns.POLL_TIME: base_ts,
                Columns.FEED_TIMESTAMP: feed_ts,
                Columns.VEHICLE_ID: f"vehicle_{i}",
                Columns.ROUTE_ID: "route_1",
                Columns.START_DATE: "20260101",
                Columns.LATITUDE: -36.8485,
                Columns.LONGITUDE: 174.7633,
            }
            for i in range(4)
        ]

        create_test_delta_table(
            str(table_path),
            single_partition_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        # Count parquet files before cleanup (should be 2 from batched writes)
        parquet_files_before: list[Path] = list(table_path.rglob("*.parquet"))
        assert len(parquet_files_before) == 2, (
            "Test setup should create 2 files"
        )

        cleanup_day(
            table_name=Tables.VEHICLE_POSITIONS,
            dedupe_keys=VEHICLE_POSITIONS_DEDUPE_KEYS,
            now=datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        # After cleanup + vacuum, should have 1 file (compacted)
        parquet_files_after: list[Path] = list(table_path.rglob("*.parquet"))
        assert len(parquet_files_after) == 1

    def test_preserves_unique_rows(
        self,
        tmp_path: Path,
    ) -> None:
        """Cleanup should not remove rows with unique dedupe keys."""
        table_path: Path = tmp_path / Tables.VEHICLE_POSITIONS

        # Create data with all unique keys (no duplicates)
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
            str(table_path),
            unique_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        # Run cleanup
        cleanup_day(
            table_name=Tables.VEHICLE_POSITIONS,
            dedupe_keys=VEHICLE_POSITIONS_DEDUPE_KEYS,
            now=datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        # All 5 unique rows should remain
        after = duckdb.sql(
            f"SELECT COUNT(*) as cnt FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert after == 5

    def test_only_cleans_target_day(
        self,
        tmp_path: Path,
    ) -> None:
        """Cleanup should only process the target day."""
        table_path: Path = tmp_path / Tables.VEHICLE_POSITIONS

        # Create data spanning Jan 1, Jan 2, and Jan 3
        base_ts = datetime(2026, 1, 2, 10, 30, 0, tzinfo=timezone.utc)
        multi_day_data: list[Any] = []
        for day in [1, 2, 3]:
            for i in range(2):
                # Create duplicates within each day
                for _ in range(2):
                    multi_day_data.append(
                        {
                            Columns.POLL_TIME: base_ts,
                            Columns.FEED_TIMESTAMP: datetime(
                                2026, 1, day, 10, 25, 0, tzinfo=timezone.utc
                            ),
                            Columns.VEHICLE_ID: f"vehicle_{i}",
                            Columns.ROUTE_ID: "route_1",
                            Columns.START_DATE: f"2026010{day}",
                            Columns.LATITUDE: -36.8485,
                            Columns.LONGITUDE: 174.7633,
                        }
                    )

        create_test_delta_table(
            str(table_path),
            multi_day_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        # 12 rows total: 3 days × 2 vehicles × 2 duplicates
        before = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert before == 12

        # Clean Jan 2 only (now = Jan 3)
        cleanup_day(
            table_name=Tables.VEHICLE_POSITIONS,
            dedupe_keys=VEHICLE_POSITIONS_DEDUPE_KEYS,
            now=datetime(2026, 1, 3, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        # Jan 2 should be deduped (4 → 2), Jan 1 and Jan 3 unchanged (4 each)
        # Total: 4 + 2 + 4 = 10
        after = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert after == 10

        # Verify Jan 2 specifically has 2 rows
        jan2_count = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}') "
            f"WHERE start_date = '20260102'"
        ).fetchone()[0]
        assert jan2_count == 2

    def test_multiple_route_partitions(
        self,
        tmp_path: Path,
    ) -> None:
        """Cleanup should handle data across many route_id partitions."""
        table_path: Path = tmp_path / Tables.VEHICLE_POSITIONS

        # Create data with 5 different routes, each with duplicates
        base_ts = datetime(2026, 1, 1, 10, 30, 0, tzinfo=timezone.utc)
        feed_ts = datetime(2026, 1, 1, 10, 25, 0, tzinfo=timezone.utc)
        multi_route_data: list[Any] = []
        for route_num in range(5):
            for _ in range(2):  # 2 duplicates per route
                multi_route_data.append(
                    {
                        Columns.POLL_TIME: base_ts,
                        Columns.FEED_TIMESTAMP: feed_ts,
                        Columns.VEHICLE_ID: f"vehicle_{route_num}",
                        Columns.ROUTE_ID: f"route_{route_num}",
                        Columns.START_DATE: "20260101",
                        Columns.LATITUDE: -36.8485,
                        Columns.LONGITUDE: 174.7633,
                    }
                )

        create_test_delta_table(
            str(table_path),
            multi_route_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        # 10 rows: 5 routes × 2 duplicates
        before = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert before == 10

        cleanup_day(
            table_name=Tables.VEHICLE_POSITIONS,
            dedupe_keys=VEHICLE_POSITIONS_DEDUPE_KEYS,
            now=datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        # Should have 5 rows (1 per route after deduplication)
        after = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert after == 5

        # Verify each route has exactly 1 row
        route_counts: list[tuple[Any, ...]] = duckdb.sql(
            f"SELECT route_id, COUNT(*) as cnt FROM delta_scan('{table_path}') "
            f"GROUP BY route_id"
        ).fetchall()
        assert len(route_counts) == 5
        for _, count in route_counts:
            assert count == 1

    def test_is_idempotent(
        self,
        tmp_path: Path,
    ) -> None:
        """Running cleanup twice on the same day should produce same result."""
        table_path: Path = tmp_path / Tables.VEHICLE_POSITIONS

        base_ts = datetime(2026, 1, 1, 10, 30, 0, tzinfo=timezone.utc)
        feed_ts = datetime(2026, 1, 1, 10, 25, 0, tzinfo=timezone.utc)
        data: list[dict[Columns, datetime | float | int | str]] = [
            {
                Columns.POLL_TIME: base_ts,
                Columns.FEED_TIMESTAMP: feed_ts,
                Columns.VEHICLE_ID: "vehicle_A",
                Columns.ROUTE_ID: "route_1",
                Columns.START_DATE: "20260101",
                Columns.LATITUDE: -36.8485,
                Columns.LONGITUDE: 174.7633,
            },
            # Duplicate
            {
                Columns.POLL_TIME: datetime(
                    2026, 1, 1, 10, 30, 30, tzinfo=timezone.utc
                ),
                Columns.FEED_TIMESTAMP: feed_ts,
                Columns.VEHICLE_ID: "vehicle_A",
                Columns.ROUTE_ID: "route_1",
                Columns.START_DATE: "20260101",
                Columns.LATITUDE: -36.8485,
                Columns.LONGITUDE: 174.7633,
            },
        ]

        create_test_delta_table(
            str(table_path),
            data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        now = datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc)

        # First cleanup
        cleanup_day(
            table_name=Tables.VEHICLE_POSITIONS,
            dedupe_keys=VEHICLE_POSITIONS_DEDUPE_KEYS,
            now=now,
            data_path=tmp_path,
        )

        after_first = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        files_after_first: int = len(list(table_path.rglob("*.parquet")))

        # Second cleanup (should not error and should produce same result)
        cleanup_day(
            table_name=Tables.VEHICLE_POSITIONS,
            dedupe_keys=VEHICLE_POSITIONS_DEDUPE_KEYS,
            now=now,
            data_path=tmp_path,
        )

        after_second = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        files_after_second: int = len(list(table_path.rglob("*.parquet")))

        # Same row count and file count after both cleanups
        assert after_first == after_second == 1
        assert files_after_first == files_after_second == 1


class TestCleanupVehiclePositions:
    """Tests for cleanup_vehicle_positions wrapper function."""

    def test_uses_correct_dedupe_keys(
        self,
        tmp_path: Path,
        sample_vehicle_positions_data: list[dict],
    ) -> None:
        """cleanup_vehicle_positions should use vehicle_id + feed_timestamp."""
        table_path: Path = tmp_path / Tables.VEHICLE_POSITIONS

        create_test_delta_table(
            str(table_path),
            sample_vehicle_positions_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        cleanup_vehicle_positions(
            now=datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        # Should deduplicate based on vehicle_id + feed_timestamp
        after = duckdb.sql(
            f"SELECT COUNT(*) as cnt FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert after == 2


class TestCleanupStopTimeUpdatesDay:
    """Tests for cleanup_stop_time_updates_day function."""

    def test_merges_arrival_departure(
        self,
        tmp_path: Path,
        sample_stop_time_updates_data: list[dict],
    ) -> None:
        """Cleanup should merge arrival and departure rows for same stop."""
        table_path: Path = tmp_path / Tables.STOP_TIME_UPDATES

        create_test_delta_table(
            str(table_path),
            sample_stop_time_updates_data,
            STOP_TIME_UPDATES_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        # Verify 4 rows before cleanup
        before = duckdb.sql(
            f"SELECT COUNT(*) as cnt FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert before == 4

        # Run cleanup
        cleanup_stop_time_updates_day(
            now=datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        # Verify 2 merged rows after cleanup
        after = duckdb.sql(
            f"SELECT COUNT(*) as cnt FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert after == 2

        # Verify both arrival and departure data are present in merged rows
        merged_rows = duckdb.sql(
            f"""
            SELECT stop_sequence, arrival_delay,
                   arrival_time IS NOT NULL as has_arrival,
                   departure_delay,
                   departure_time IS NOT NULL as has_departure
            FROM delta_scan('{table_path}')
            ORDER BY stop_sequence
            """
        ).fetchall()

        # Stop 1: should have both arrival and departure
        assert merged_rows[0][0] == 1  # stop_sequence
        assert merged_rows[0][1] == 30  # arrival_delay
        assert merged_rows[0][2] is True  # has_arrival
        assert merged_rows[0][3] == 45  # departure_delay
        assert merged_rows[0][4] is True  # has_departure

        # Stop 2: should have both arrival and departure
        assert merged_rows[1][0] == 2  # stop_sequence
        assert merged_rows[1][1] == 60  # arrival_delay
        assert merged_rows[1][2] is True  # has_arrival
        assert merged_rows[1][3] == 75  # departure_delay
        assert merged_rows[1][4] is True  # has_departure

    def test_skips_nonexistent_table(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Cleanup should log warning and skip if table doesn't exist."""
        cleanup_stop_time_updates_day(
            now=datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        assert "does not exist, skipping cleanup" in caplog.text

    def test_skips_empty_partition(
        self,
        tmp_path: Path,
        sample_stop_time_updates_data: list[dict],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Cleanup should log info and skip if no data for target day."""
        table_path: Path = tmp_path / Tables.STOP_TIME_UPDATES

        # Create table with data for Jan 1
        create_test_delta_table(
            str(table_path),
            sample_stop_time_updates_data,
            STOP_TIME_UPDATES_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        # Try to clean Dec 31 (no data exists) - now = Jan 1
        with caplog.at_level(logging.INFO):
            cleanup_stop_time_updates_day(
                now=datetime(2026, 1, 1, 10, 20, 0, tzinfo=timezone.utc),
                data_path=tmp_path,
            )

        assert "No data for" in caplog.text

    def test_excludes_arrival_predictions(
        self,
        tmp_path: Path,
    ) -> None:
        """Arrival data with uncertainty != 0 should be excluded."""
        table_path: Path = tmp_path / Tables.STOP_TIME_UPDATES

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
                # Arrival is a prediction (uncertainty != 0)
                Columns.ARRIVAL_DELAY: 30,
                Columns.ARRIVAL_TIME: base_ts,
                Columns.ARRIVAL_UNCERTAINTY: 120,  # Predicted
                # Departure is confirmed (uncertainty = 0)
                Columns.DEPARTURE_DELAY: 45,
                Columns.DEPARTURE_TIME: base_ts,
                Columns.DEPARTURE_UNCERTAINTY: 0,
            },
        ]

        create_test_delta_table(
            str(table_path),
            data,
            STOP_TIME_UPDATES_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        cleanup_stop_time_updates_day(
            now=datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        result = duckdb.sql(
            f"""
            SELECT arrival_delay, arrival_time IS NOT NULL as has_arrival,
                   arrival_uncertainty,
                   departure_delay, departure_time IS NOT NULL as has_departure,
                   departure_uncertainty
            FROM delta_scan('{table_path}')
            """
        ).fetchone()

        # Arrival should be NULL (prediction excluded)
        assert result[0] is None  # arrival_delay
        assert result[1] is False  # has_arrival
        assert result[2] is None  # arrival_uncertainty

        # Departure should be preserved (confirmed)
        assert result[3] == 45  # departure_delay
        assert result[4] is True  # has_departure
        assert result[5] == 0  # departure_uncertainty

    def test_excludes_departure_predictions(
        self,
        tmp_path: Path,
    ) -> None:
        """Departure data with uncertainty != 0 should be excluded."""
        table_path: Path = tmp_path / Tables.STOP_TIME_UPDATES

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
                # Arrival is confirmed (uncertainty = 0)
                Columns.ARRIVAL_DELAY: 30,
                Columns.ARRIVAL_TIME: base_ts,
                Columns.ARRIVAL_UNCERTAINTY: 0,
                # Departure is a prediction (uncertainty != 0)
                Columns.DEPARTURE_DELAY: 45,
                Columns.DEPARTURE_TIME: base_ts,
                Columns.DEPARTURE_UNCERTAINTY: 60,  # Predicted
            },
        ]

        create_test_delta_table(
            str(table_path),
            data,
            STOP_TIME_UPDATES_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        cleanup_stop_time_updates_day(
            now=datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        result = duckdb.sql(
            f"""
            SELECT arrival_delay, arrival_time IS NOT NULL as has_arrival,
                   arrival_uncertainty,
                   departure_delay, departure_time IS NOT NULL as has_departure,
                   departure_uncertainty
            FROM delta_scan('{table_path}')
            """
        ).fetchone()

        # Arrival should be preserved (confirmed)
        assert result[0] == 30  # arrival_delay
        assert result[1] is True  # has_arrival
        assert result[2] == 0  # arrival_uncertainty

        # Departure should be NULL (prediction excluded)
        assert result[3] is None  # departure_delay
        assert result[4] is False  # has_departure
        assert result[5] is None  # departure_uncertainty

    def test_is_idempotent(
        self,
        tmp_path: Path,
        sample_stop_time_updates_data: list[dict],
    ) -> None:
        """Running cleanup twice on the same day should produce same result."""
        table_path: Path = tmp_path / Tables.STOP_TIME_UPDATES

        create_test_delta_table(
            str(table_path),
            sample_stop_time_updates_data,
            STOP_TIME_UPDATES_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        now = datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc)

        # First cleanup
        cleanup_stop_time_updates_day(now=now, data_path=tmp_path)

        after_first = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        files_after_first: int = len(list(table_path.rglob("*.parquet")))

        # Second cleanup (should not error and should produce same result)
        cleanup_stop_time_updates_day(now=now, data_path=tmp_path)

        after_second = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        files_after_second: int = len(list(table_path.rglob("*.parquet")))

        # Same row count and file count after both cleanups
        # 4 input rows merge to 2 rows (2 stops with arrival+departure each)
        assert after_first == after_second == 2
        assert files_after_first == files_after_second == 1
