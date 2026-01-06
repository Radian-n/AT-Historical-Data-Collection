"""Tests for the cleanup module."""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pytest
from conftest import (
    TRIP_UPDATES_TEST_SCHEMA,
    VEHICLE_POSITIONS_TEST_SCHEMA,
    create_test_delta_table,
)

from app.cleanup import (
    _get_target_partition,
    cleanup_hour,
    cleanup_trip_updates_hour,
    cleanup_vehicle_positions,
)
from app.columns import VEHICLE_POSITIONS_DEDUPE_KEYS, Columns
from app.config import Tables

pytestmark = pytest.mark.unit


class TestGetTargetPartition:
    """Unit tests for _get_target_partition helper function."""

    def test_returns_previous_hour(self) -> None:
        """Should return the previous hour's partition."""
        now = datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc)
        result = _get_target_partition(now)

        assert result.feed_date == "2026-01-02"
        assert result.feed_hour == 10
        assert "feed_date = '2026-01-02'" in result.predicate
        assert "feed_hour = 10" in result.predicate

    def test_midnight_boundary(self) -> None:
        """At 00:20, should return hour 23 of the previous day."""
        now = datetime(2026, 1, 2, 0, 20, 0, tzinfo=timezone.utc)
        result = _get_target_partition(now)

        assert result.feed_date == "2026-01-01"
        assert result.feed_hour == 23
        assert "feed_date = '2026-01-01'" in result.predicate
        assert "feed_hour = 23" in result.predicate

    def test_new_year_boundary(self) -> None:
        """At 00:20 on Jan 1, should return hour 23 of Dec 31 previous year."""
        now = datetime(2026, 1, 1, 0, 20, 0, tzinfo=timezone.utc)
        result = _get_target_partition(now)

        assert result.feed_date == "2025-12-31"
        assert result.feed_hour == 23

    def test_ignores_minutes_and_seconds(self) -> None:
        """Should truncate to hour boundary before subtracting."""
        # 11:59:59 should still target hour 10, not hour 11
        now = datetime(2026, 1, 2, 11, 59, 59, tzinfo=timezone.utc)
        result = _get_target_partition(now)

        assert result.feed_date == "2026-01-02"
        assert result.feed_hour == 10

    def test_exactly_on_hour(self) -> None:
        """At exactly 11:00:00, should return hour 10."""
        now = datetime(2026, 1, 2, 11, 0, 0, tzinfo=timezone.utc)
        result = _get_target_partition(now)

        assert result.feed_date == "2026-01-02"
        assert result.feed_hour == 10


class TestCleanupHour:
    """Tests for cleanup_hour function."""

    def test_deduplicates_vehicle_positions(
        self,
        tmp_path: Path,
        sample_vehicle_positions_data: list[dict],
    ) -> None:
        """Cleanup should remove duplicate rows based on dedupe keys.

        Creates a table with 4 rows containing 2 unique (vehicle_id,
        feed_timestamp) combinations, each duplicated twice. After cleanup,
        verifies only 2 rows remain.
        """
        table_path: Path = tmp_path / Tables.VEHICLE_POSITIONS

        # Create test table with 4 rows (2 duplicates)
        create_test_delta_table(
            str(table_path),
            sample_vehicle_positions_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID],
        )

        # Verify 4 rows before cleanup
        before = duckdb.sql(
            f"SELECT COUNT(*) as cnt FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert before == 4

        # Run cleanup for hour 10 (now = 11:20, so previous hour = 10)
        cleanup_hour(
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
        """Cleanup should log warning and skip if table doesn't exist.

        Attempts cleanup on a nonexistent table and verifies a warning is
        logged without raising an exception.
        """
        cleanup_hour(
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
        """Cleanup should log info and skip if no data for target hour.

        Creates a table with data for hour 10, then attempts cleanup for
        hour 9 (which has no data). Verifies an info message is logged
        and no changes are made.
        """
        table_path: Path = tmp_path / Tables.VEHICLE_POSITIONS

        # Create table with data for hour 10
        create_test_delta_table(
            str(table_path),
            sample_vehicle_positions_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID],
        )

        # Try to clean hour 9 (no data exists)
        with caplog.at_level(logging.INFO):
            cleanup_hour(
                table_name=Tables.VEHICLE_POSITIONS,
                dedupe_keys=VEHICLE_POSITIONS_DEDUPE_KEYS,
                now=datetime(2026, 1, 2, 10, 20, 0, tzinfo=timezone.utc),
                data_path=tmp_path,
            )

        assert "No data for" in caplog.text

    def test_compacts_parquet_files(
        self,
        tmp_path: Path,
    ) -> None:
        """Cleanup should reduce the number of parquet files.

        Creates a table with data written in two batches (creating 2 parquet
        files). After cleanup, verifies only 1 compacted file remains and
        the old files have been vacuumed.
        """
        table_path: Path = tmp_path / Tables.VEHICLE_POSITIONS

        # Create data with single route to keep all files in one partition
        base_ts = datetime(2026, 1, 2, 10, 30, 0, tzinfo=timezone.utc)
        feed_ts = datetime(2026, 1, 2, 10, 25, 0, tzinfo=timezone.utc)
        single_partition_data: list[
            dict[Columns, datetime | float | int | str]
        ] = [
            {
                Columns.POLL_TIME: base_ts,
                Columns.FEED_TIMESTAMP: feed_ts,
                Columns.VEHICLE_ID: f"vehicle_{i}",
                Columns.ROUTE_ID: "route_1",
                Columns.LATITUDE: -36.8485,
                Columns.LONGITUDE: 174.7633,
                Columns.FEED_DATE: "2026-01-02",
                Columns.FEED_HOUR: 10,
            }
            for i in range(4)
        ]

        create_test_delta_table(
            str(table_path),
            single_partition_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID],
        )

        # Count parquet files before cleanup (should be 2 from batched writes)
        parquet_files_before: list[Path] = list(table_path.rglob("*.parquet"))
        assert len(parquet_files_before) == 2, (
            "Test setup should create 2 files"
        )

        cleanup_hour(
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
        """Cleanup should not remove rows with unique dedupe keys.

        Creates a table with 5 rows, each having a unique (vehicle_id,
        feed_timestamp) combination. After cleanup, verifies all 5 rows
        are preserved.
        """
        table_path: Path = tmp_path / Tables.VEHICLE_POSITIONS

        # Create data with all unique keys (no duplicates)
        base_ts = datetime(2026, 1, 2, 10, 30, 0, tzinfo=timezone.utc)
        unique_data: list[dict[Columns, datetime | float | int | str]] = [
            {
                Columns.POLL_TIME: base_ts,
                Columns.FEED_TIMESTAMP: datetime(
                    2026, 1, 2, 10, 25, i, tzinfo=timezone.utc
                ),
                Columns.VEHICLE_ID: f"vehicle_{i}",
                Columns.ROUTE_ID: "route_1",
                Columns.LATITUDE: -36.8485,
                Columns.LONGITUDE: 174.7633,
                Columns.FEED_DATE: "2026-01-02",
                Columns.FEED_HOUR: 10,
            }
            for i in range(5)
        ]

        create_test_delta_table(
            str(table_path),
            unique_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID],
        )

        # Run cleanup
        cleanup_hour(
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

    def test_only_cleans_target_hour(
        self,
        tmp_path: Path,
    ) -> None:
        """Cleanup should only process the target hour.

        Creates data spanning hours 9, 10, and 11, each with duplicates.
        Runs cleanup for hour 10 only. Verifies hour 10 is deduplicated
        while hours 9 and 11 retain their duplicates.
        """
        table_path: Path = tmp_path / Tables.VEHICLE_POSITIONS

        # Create data spanning hours 9, 10, and 11
        base_ts = datetime(2026, 1, 2, 10, 30, 0, tzinfo=timezone.utc)
        multi_hour_data: list[Any] = []
        for hour in [9, 10, 11]:
            for i in range(2):
                # Create duplicates within each hour
                for _ in range(2):
                    multi_hour_data.append(
                        {
                            Columns.POLL_TIME: base_ts,
                            Columns.FEED_TIMESTAMP: datetime(
                                2026, 1, 2, hour, 25, 0, tzinfo=timezone.utc
                            ),
                            Columns.VEHICLE_ID: f"vehicle_{i}",
                            Columns.ROUTE_ID: "route_1",
                            Columns.LATITUDE: -36.8485,
                            Columns.LONGITUDE: 174.7633,
                            Columns.FEED_DATE: "2026-01-02",
                            Columns.FEED_HOUR: hour,
                        }
                    )

        create_test_delta_table(
            str(table_path),
            multi_hour_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID],
        )

        # 12 rows total: 3 hours × 2 vehicles × 2 duplicates
        before = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert before == 12

        # Clean hour 10 only (now = 11:20)
        cleanup_hour(
            table_name=Tables.VEHICLE_POSITIONS,
            dedupe_keys=VEHICLE_POSITIONS_DEDUPE_KEYS,
            now=datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        # Hour 10 should be deduped (4 → 2), hours 9 and 11 unchanged (4 each)
        # Total: 2 + 4 + 4 = 10
        after = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert after == 10

        # Verify hour 10 specifically has 2 rows
        hour_10_count = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}') WHERE feed_hour = 10"
        ).fetchone()[0]
        assert hour_10_count == 2

    def test_multiple_route_partitions(
        self,
        tmp_path: Path,
    ) -> None:
        """Cleanup should handle data across many route_id partitions.

        Creates data with 5 different routes, each with duplicates. After
        cleanup, verifies each route has exactly 1 row and all 5 partition
        directories are preserved.
        """
        table_path: Path = tmp_path / Tables.VEHICLE_POSITIONS

        # Create data with 5 different routes, each with duplicates
        base_ts = datetime(2026, 1, 2, 10, 30, 0, tzinfo=timezone.utc)
        feed_ts = datetime(2026, 1, 2, 10, 25, 0, tzinfo=timezone.utc)
        multi_route_data: list[Any] = []
        for route_num in range(5):
            for _ in range(2):  # 2 duplicates per route
                multi_route_data.append(
                    {
                        Columns.POLL_TIME: base_ts,
                        Columns.FEED_TIMESTAMP: feed_ts,
                        Columns.VEHICLE_ID: f"vehicle_{route_num}",
                        Columns.ROUTE_ID: f"route_{route_num}",
                        Columns.LATITUDE: -36.8485,
                        Columns.LONGITUDE: 174.7633,
                        Columns.FEED_DATE: "2026-01-02",
                        Columns.FEED_HOUR: 10,
                    }
                )

        create_test_delta_table(
            str(table_path),
            multi_route_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID],
        )

        # 10 rows: 5 routes × 2 duplicates
        before = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert before == 10

        cleanup_hour(
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

        # Verify partition directories still exist for all 5 routes
        hour_partition: Path = (
            table_path / "feed_date=2026-01-02" / "feed_hour=10"
        )
        route_partitions: list[Path] = [
            d for d in hour_partition.iterdir() if d.is_dir()
        ]
        route_names: set[str] = {d.name for d in route_partitions}
        expected_routes: set[str] = {f"route_id=route_{i}" for i in range(5)}
        assert route_names == expected_routes

    def test_is_idempotent(
        self,
        tmp_path: Path,
    ) -> None:
        """Running cleanup twice on the same hour should produce same result.

        Creates a table with duplicates, runs cleanup twice on the same hour.
        Verifies no errors occur and the row count and file count are
        identical after both runs.
        """
        table_path: Path = tmp_path / Tables.VEHICLE_POSITIONS

        base_ts = datetime(2026, 1, 2, 10, 30, 0, tzinfo=timezone.utc)
        feed_ts = datetime(2026, 1, 2, 10, 25, 0, tzinfo=timezone.utc)
        data: list[dict[Columns, datetime | float | int | str]] = [
            {
                Columns.POLL_TIME: base_ts,
                Columns.FEED_TIMESTAMP: feed_ts,
                Columns.VEHICLE_ID: "vehicle_A",
                Columns.ROUTE_ID: "route_1",
                Columns.LATITUDE: -36.8485,
                Columns.LONGITUDE: 174.7633,
                Columns.FEED_DATE: "2026-01-02",
                Columns.FEED_HOUR: 10,
            },
            # Duplicate
            {
                Columns.POLL_TIME: datetime(
                    2026, 1, 2, 10, 30, 30, tzinfo=timezone.utc
                ),
                Columns.FEED_TIMESTAMP: feed_ts,
                Columns.VEHICLE_ID: "vehicle_A",
                Columns.ROUTE_ID: "route_1",
                Columns.LATITUDE: -36.8485,
                Columns.LONGITUDE: 174.7633,
                Columns.FEED_DATE: "2026-01-02",
                Columns.FEED_HOUR: 10,
            },
        ]

        create_test_delta_table(
            str(table_path),
            data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID],
        )

        now = datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc)

        # First cleanup
        cleanup_hour(
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
        cleanup_hour(
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
        """cleanup_vehicle_positions should use vehicle_id + feed_timestamp.

        Verifies the wrapper function correctly uses the dedupe keys defined
        in VEHICLE_POSITIONS_DEDUPE_KEYS rather than requiring them to be
        passed explicitly.
        """
        table_path: Path = tmp_path / Tables.VEHICLE_POSITIONS

        create_test_delta_table(
            str(table_path),
            sample_vehicle_positions_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID],
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


class TestCleanupTripUpdatesHour:
    """Tests for cleanup_trip_updates_hour function."""

    def test_merges_arrival_departure(
        self,
        tmp_path: Path,
        sample_trip_updates_data: list[dict],
    ) -> None:
        """Cleanup should merge arrival and departure rows for same stop.

        Creates a table with 4 rows: 2 stops, each with separate arrival and
        departure rows. After cleanup, verifies 2 merged rows remain with
        both arrival and departure data combined.
        """
        table_path: Path = tmp_path / Tables.TRIP_UPDATES

        create_test_delta_table(
            str(table_path),
            sample_trip_updates_data,
            TRIP_UPDATES_TEST_SCHEMA,
            [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID],
        )

        # Verify 4 rows before cleanup
        before = duckdb.sql(
            f"SELECT COUNT(*) as cnt FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert before == 4

        # Run cleanup
        cleanup_trip_updates_hour(
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
        cleanup_trip_updates_hour(
            now=datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        assert "does not exist, skipping cleanup" in caplog.text

    def test_skips_empty_partition(
        self,
        tmp_path: Path,
        sample_trip_updates_data: list[dict],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Cleanup should log info and skip if no data for target hour."""
        table_path: Path = tmp_path / Tables.TRIP_UPDATES

        # Create table with data for hour 10
        create_test_delta_table(
            str(table_path),
            sample_trip_updates_data,
            TRIP_UPDATES_TEST_SCHEMA,
            [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID],
        )

        # Try to clean hour 9 (no data exists)
        with caplog.at_level(logging.INFO):
            cleanup_trip_updates_hour(
                now=datetime(2026, 1, 2, 10, 20, 0, tzinfo=timezone.utc),
                data_path=tmp_path,
            )

        assert "No data for" in caplog.text

    def test_compacts_parquet_files(
        self,
        tmp_path: Path,
    ) -> None:
        """Cleanup should reduce the number of parquet files.

        Creates a table with data written in two batches (creating 2 parquet
        files). After cleanup, verifies only 1 compacted file remains.
        """
        table_path: Path = tmp_path / Tables.TRIP_UPDATES

        base_ts = datetime(2026, 1, 2, 10, 25, 0, tzinfo=timezone.utc)
        base: dict[str, Any] = {
            Columns.VEHICLE_ID: "vehicle_1",
            Columns.LABEL: "BUS001",
            Columns.LICENSE_PLATE: "ABC123",
            Columns.ROUTE_ID: "route_1",
            Columns.DIRECTION_ID: 0,
            Columns.SCHEDULE_RELATIONSHIP: 0,
            Columns.START_TIME: "10:00:00",
            Columns.DELAY: 60,
            Columns.STOP_SCHEDULE_RELATIONSHIP: 0,
            Columns.ENTITY_IS_DELETED: False,
            Columns.FEED_DATE: "2026-01-02",
            Columns.FEED_HOUR: 10,
        }

        # Create 4 unique stop updates (no merging needed, just compaction)
        data: list[dict[str, Any]] = [
            {
                **base,
                Columns.POLL_TIME: base_ts,
                Columns.FEED_TIMESTAMP: base_ts,
                Columns.TRIP_ID: "trip_A",
                Columns.START_DATE: "20260102",
                Columns.STOP_SEQUENCE: i,
                Columns.STOP_ID: f"stop_{i}",
                Columns.ARRIVAL_DELAY: 30,
                Columns.ARRIVAL_TIME: base_ts,
                Columns.ARRIVAL_UNCERTAINTY: 0,
                Columns.DEPARTURE_DELAY: 45,
                Columns.DEPARTURE_TIME: base_ts,
                Columns.DEPARTURE_UNCERTAINTY: 0,
            }
            for i in range(4)
        ]

        create_test_delta_table(
            str(table_path),
            data,
            TRIP_UPDATES_TEST_SCHEMA,
            [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID],
        )

        # Count parquet files before cleanup (should be 2 from batched writes)
        parquet_files_before: list[Path] = list(table_path.rglob("*.parquet"))
        assert len(parquet_files_before) == 2

        cleanup_trip_updates_hour(
            now=datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        # After cleanup + vacuum, should have 1 file (compacted)
        parquet_files_after: list[Path] = list(table_path.rglob("*.parquet"))
        assert len(parquet_files_after) == 1

    def test_excludes_arrival_predictions(
        self,
        tmp_path: Path,
    ) -> None:
        """Arrival data with uncertainty != 0 should be excluded (set to NULL).

        Creates a row with valid departure (uncertainty=0) but predicted
        arrival (uncertainty!=0). After cleanup, arrival fields should be
        NULL while departure fields are preserved.
        """
        table_path: Path = tmp_path / Tables.TRIP_UPDATES

        base_ts = datetime(2026, 1, 2, 10, 25, 0, tzinfo=timezone.utc)
        data: list[dict[str, Any]] = [
            {
                Columns.POLL_TIME: base_ts,
                Columns.FEED_TIMESTAMP: base_ts,
                Columns.VEHICLE_ID: "vehicle_1",
                Columns.LABEL: "BUS001",
                Columns.LICENSE_PLATE: "ABC123",
                Columns.TRIP_ID: "trip_A",
                Columns.ROUTE_ID: "route_1",
                Columns.DIRECTION_ID: 0,
                Columns.SCHEDULE_RELATIONSHIP: 0,
                Columns.START_DATE: "20260102",
                Columns.START_TIME: "10:00:00",
                Columns.DELAY: 60,
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
                Columns.ENTITY_IS_DELETED: False,
                Columns.FEED_DATE: "2026-01-02",
                Columns.FEED_HOUR: 10,
            },
        ]

        create_test_delta_table(
            str(table_path),
            data,
            TRIP_UPDATES_TEST_SCHEMA,
            [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID],
        )

        cleanup_trip_updates_hour(
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
        """Departure data with uncertainty != 0 should be excluded.

        Creates a row with valid arrival (uncertainty=0) but predicted
        departure (uncertainty!=0). After cleanup, departure fields should be
        NULL while arrival fields are preserved.
        """
        table_path: Path = tmp_path / Tables.TRIP_UPDATES

        base_ts = datetime(2026, 1, 2, 10, 25, 0, tzinfo=timezone.utc)
        data: list[dict[str, Any]] = [
            {
                Columns.POLL_TIME: base_ts,
                Columns.FEED_TIMESTAMP: base_ts,
                Columns.VEHICLE_ID: "vehicle_1",
                Columns.LABEL: "BUS001",
                Columns.LICENSE_PLATE: "ABC123",
                Columns.TRIP_ID: "trip_A",
                Columns.ROUTE_ID: "route_1",
                Columns.DIRECTION_ID: 0,
                Columns.SCHEDULE_RELATIONSHIP: 0,
                Columns.START_DATE: "20260102",
                Columns.START_TIME: "10:00:00",
                Columns.DELAY: 60,
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
                Columns.ENTITY_IS_DELETED: False,
                Columns.FEED_DATE: "2026-01-02",
                Columns.FEED_HOUR: 10,
            },
        ]

        create_test_delta_table(
            str(table_path),
            data,
            TRIP_UPDATES_TEST_SCHEMA,
            [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID],
        )

        cleanup_trip_updates_hour(
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

    def test_deduplicates_identical_rows(
        self,
        tmp_path: Path,
    ) -> None:
        """Cleanup should remove duplicate rows for the same stop.

        Creates a table with identical rows (same trip_id, start_date,
        stop_sequence, stop_id). After cleanup, verifies duplicates are
        merged into a single row.
        """
        table_path: Path = tmp_path / Tables.TRIP_UPDATES

        base_ts = datetime(2026, 1, 2, 10, 25, 0, tzinfo=timezone.utc)
        base: dict[str, Any] = {
            Columns.POLL_TIME: base_ts,
            Columns.FEED_TIMESTAMP: base_ts,
            Columns.VEHICLE_ID: "vehicle_1",
            Columns.LABEL: "BUS001",
            Columns.LICENSE_PLATE: "ABC123",
            Columns.TRIP_ID: "trip_A",
            Columns.ROUTE_ID: "route_1",
            Columns.DIRECTION_ID: 0,
            Columns.SCHEDULE_RELATIONSHIP: 0,
            Columns.START_DATE: "20260102",
            Columns.START_TIME: "10:00:00",
            Columns.DELAY: 60,
            Columns.STOP_SEQUENCE: 1,
            Columns.STOP_ID: "stop_100",
            Columns.STOP_SCHEDULE_RELATIONSHIP: 0,
            Columns.ARRIVAL_DELAY: 30,
            Columns.ARRIVAL_TIME: base_ts,
            Columns.ARRIVAL_UNCERTAINTY: 0,
            Columns.DEPARTURE_DELAY: 45,
            Columns.DEPARTURE_TIME: base_ts,
            Columns.DEPARTURE_UNCERTAINTY: 0,
            Columns.ENTITY_IS_DELETED: False,
            Columns.FEED_DATE: "2026-01-02",
            Columns.FEED_HOUR: 10,
        }

        # Create 4 identical rows (duplicates from multiple polls)
        data: list[dict[str, Any]] = [base.copy() for _ in range(4)]

        create_test_delta_table(
            str(table_path),
            data,
            TRIP_UPDATES_TEST_SCHEMA,
            [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID],
        )

        # Verify 4 rows before cleanup
        before = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert before == 4

        cleanup_trip_updates_hour(
            now=datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        # Should have 1 row after deduplication
        after = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert after == 1

    def test_multiple_route_partitions(
        self,
        tmp_path: Path,
    ) -> None:
        """Cleanup should handle data across many route_id partitions.

        Creates data with 3 different routes, each with duplicate rows.
        After cleanup, verifies each route has exactly 1 row.
        """
        table_path: Path = tmp_path / Tables.TRIP_UPDATES

        base_ts = datetime(2026, 1, 2, 10, 25, 0, tzinfo=timezone.utc)

        data: list[dict[str, Any]] = []
        for route_num in range(3):
            # Create 2 duplicates per route
            for _ in range(2):
                data.append(
                    {
                        Columns.POLL_TIME: base_ts,
                        Columns.FEED_TIMESTAMP: base_ts,
                        Columns.VEHICLE_ID: f"vehicle_{route_num}",
                        Columns.LABEL: "BUS001",
                        Columns.LICENSE_PLATE: "ABC123",
                        Columns.TRIP_ID: f"trip_{route_num}",
                        Columns.ROUTE_ID: f"route_{route_num}",
                        Columns.DIRECTION_ID: 0,
                        Columns.SCHEDULE_RELATIONSHIP: 0,
                        Columns.START_DATE: "20260102",
                        Columns.START_TIME: "10:00:00",
                        Columns.DELAY: 60,
                        Columns.STOP_SEQUENCE: 1,
                        Columns.STOP_ID: "stop_100",
                        Columns.STOP_SCHEDULE_RELATIONSHIP: 0,
                        Columns.ARRIVAL_DELAY: 30,
                        Columns.ARRIVAL_TIME: base_ts,
                        Columns.ARRIVAL_UNCERTAINTY: 0,
                        Columns.DEPARTURE_DELAY: 45,
                        Columns.DEPARTURE_TIME: base_ts,
                        Columns.DEPARTURE_UNCERTAINTY: 0,
                        Columns.ENTITY_IS_DELETED: False,
                        Columns.FEED_DATE: "2026-01-02",
                        Columns.FEED_HOUR: 10,
                    }
                )

        create_test_delta_table(
            str(table_path),
            data,
            TRIP_UPDATES_TEST_SCHEMA,
            [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID],
        )

        # 6 rows: 3 routes × 2 duplicates
        before = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert before == 6

        cleanup_trip_updates_hour(
            now=datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        # Should have 3 rows (1 per route after deduplication)
        after = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert after == 3

        # Verify each route has exactly 1 row
        route_counts: list[tuple[Any, ...]] = duckdb.sql(
            f"SELECT route_id, COUNT(*) as cnt FROM delta_scan('{table_path}') "
            f"GROUP BY route_id"
        ).fetchall()
        assert len(route_counts) == 3
        for _, count in route_counts:
            assert count == 1

    def test_preserves_unique_rows(
        self,
        tmp_path: Path,
    ) -> None:
        """Cleanup should not remove rows with unique group keys.

        Creates a table with 4 rows, each having a unique (trip_id,
        start_date, stop_sequence, stop_id) combination. After cleanup,
        verifies all 4 rows are preserved.
        """
        table_path: Path = tmp_path / Tables.TRIP_UPDATES

        base_ts = datetime(2026, 1, 2, 10, 25, 0, tzinfo=timezone.utc)

        # Create 4 unique stop updates (different stop_sequence)
        data: list[dict[str, Any]] = [
            {
                Columns.POLL_TIME: base_ts,
                Columns.FEED_TIMESTAMP: base_ts,
                Columns.VEHICLE_ID: "vehicle_1",
                Columns.LABEL: "BUS001",
                Columns.LICENSE_PLATE: "ABC123",
                Columns.TRIP_ID: "trip_A",
                Columns.ROUTE_ID: "route_1",
                Columns.DIRECTION_ID: 0,
                Columns.SCHEDULE_RELATIONSHIP: 0,
                Columns.START_DATE: "20260102",
                Columns.START_TIME: "10:00:00",
                Columns.DELAY: 60,
                Columns.STOP_SEQUENCE: i,
                Columns.STOP_ID: f"stop_{i}",
                Columns.STOP_SCHEDULE_RELATIONSHIP: 0,
                Columns.ARRIVAL_DELAY: 30,
                Columns.ARRIVAL_TIME: base_ts,
                Columns.ARRIVAL_UNCERTAINTY: 0,
                Columns.DEPARTURE_DELAY: 45,
                Columns.DEPARTURE_TIME: base_ts,
                Columns.DEPARTURE_UNCERTAINTY: 0,
                Columns.ENTITY_IS_DELETED: False,
                Columns.FEED_DATE: "2026-01-02",
                Columns.FEED_HOUR: 10,
            }
            for i in range(4)
        ]

        create_test_delta_table(
            str(table_path),
            data,
            TRIP_UPDATES_TEST_SCHEMA,
            [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID],
        )

        cleanup_trip_updates_hour(
            now=datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        # All 4 unique rows should remain
        after = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert after == 4

    def test_is_idempotent(
        self,
        tmp_path: Path,
        sample_trip_updates_data: list[dict],
    ) -> None:
        """Running cleanup twice on the same hour should produce same result.

        Creates a table with arrival/departure rows, runs cleanup twice.
        Verifies no errors occur and the row count and file count are
        identical after both runs.
        """
        table_path: Path = tmp_path / Tables.TRIP_UPDATES

        create_test_delta_table(
            str(table_path),
            sample_trip_updates_data,
            TRIP_UPDATES_TEST_SCHEMA,
            [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID],
        )

        now = datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc)

        # First cleanup
        cleanup_trip_updates_hour(now=now, data_path=tmp_path)

        after_first = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        files_after_first: int = len(list(table_path.rglob("*.parquet")))

        # Second cleanup (should not error and should produce same result)
        cleanup_trip_updates_hour(now=now, data_path=tmp_path)

        after_second = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        files_after_second: int = len(list(table_path.rglob("*.parquet")))

        # Same row count and file count after both cleanups
        # 4 input rows merge to 2 rows (2 stops with arrival+departure each)
        assert after_first == after_second == 2
        assert files_after_first == files_after_second == 1
