"""Tests for the cleanup module."""

import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from app.cleanup import cleanup_hour, cleanup_vehicle_positions
from app.columns import Columns
from app.config import Tables

from conftest import (
    VEHICLE_POSITIONS_TEST_SCHEMA,
    TRIP_UPDATES_TEST_SCHEMA,
    VEHICLE_POSITIONS_DEDUPE_KEYS,
    TRIP_UPDATES_DEDUPE_KEYS,
    create_test_delta_table,
)

pytestmark = pytest.mark.unit


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
        table_path = tmp_path / Tables.VEHICLE_POSITIONS

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

    def test_deduplicates_trip_updates(
        self,
        tmp_path: Path,
        sample_trip_updates_data: list[dict],
    ) -> None:
        """Cleanup should remove duplicate rows for trip updates.

        Creates a table with 4 rows containing 2 unique (trip_id, start_date,
        stop_sequence, feed_timestamp) combinations. After cleanup, verifies
        only 2 rows remain.
        """
        table_path = tmp_path / Tables.TRIP_UPDATES

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
        cleanup_hour(
            table_name=Tables.TRIP_UPDATES,
            dedupe_keys=TRIP_UPDATES_DEDUPE_KEYS,
            now=datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        # Verify 2 rows after cleanup
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
        table_path = tmp_path / Tables.VEHICLE_POSITIONS

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
        table_path = tmp_path / Tables.VEHICLE_POSITIONS

        # Create data with single route to keep all files in one partition
        base_ts = datetime(2026, 1, 2, 10, 30, 0, tzinfo=timezone.utc)
        feed_ts = datetime(2026, 1, 2, 10, 25, 0, tzinfo=timezone.utc)
        single_partition_data = [
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
        parquet_files_before = list(table_path.rglob("*.parquet"))
        assert len(parquet_files_before) == 2, "Test setup should create 2 files"

        cleanup_hour(
            table_name=Tables.VEHICLE_POSITIONS,
            dedupe_keys=VEHICLE_POSITIONS_DEDUPE_KEYS,
            now=datetime(2026, 1, 2, 11, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        # After cleanup + vacuum, should have 1 file (compacted)
        parquet_files_after = list(table_path.rglob("*.parquet"))
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
        table_path = tmp_path / Tables.VEHICLE_POSITIONS

        # Create data with all unique keys (no duplicates)
        base_ts = datetime(2026, 1, 2, 10, 30, 0, tzinfo=timezone.utc)
        unique_data = [
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


class TestCleanupEdgeCases:
    """Edge case tests for cleanup_hour function."""

    def test_only_cleans_target_hour(
        self,
        tmp_path: Path,
    ) -> None:
        """Cleanup should only process the target hour.

        Creates data spanning hours 9, 10, and 11, each with duplicates.
        Runs cleanup for hour 10 only. Verifies hour 10 is deduplicated
        while hours 9 and 11 retain their duplicates.
        """
        table_path = tmp_path / Tables.VEHICLE_POSITIONS

        # Create data spanning hours 9, 10, and 11
        base_ts = datetime(2026, 1, 2, 10, 30, 0, tzinfo=timezone.utc)
        multi_hour_data = []
        for hour in [9, 10, 11]:
            for i in range(2):
                # Create duplicates within each hour
                for _ in range(2):
                    multi_hour_data.append({
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
                    })

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
            f"SELECT COUNT(*) FROM delta_scan('{table_path}') "
            f"WHERE feed_hour = 10"
        ).fetchone()[0]
        assert hour_10_count == 2

    def test_midnight_boundary(
        self,
        tmp_path: Path,
    ) -> None:
        """Cleanup at 00:20 should process hour 23 of the previous day.

        Creates data for hour 23 on Jan 1st with duplicates. Runs cleanup
        at 00:20 on Jan 2nd. Verifies the date boundary is handled correctly
        and hour 23 of Jan 1st is deduplicated.
        """
        table_path = tmp_path / Tables.VEHICLE_POSITIONS

        # Create data for hour 23 on Jan 1st
        hour_23_data = [
            {
                Columns.POLL_TIME: datetime(
                    2026, 1, 1, 23, 30, 0, tzinfo=timezone.utc
                ),
                Columns.FEED_TIMESTAMP: datetime(
                    2026, 1, 1, 23, 25, 0, tzinfo=timezone.utc
                ),
                Columns.VEHICLE_ID: "vehicle_A",
                Columns.ROUTE_ID: "route_1",
                Columns.LATITUDE: -36.8485,
                Columns.LONGITUDE: 174.7633,
                Columns.FEED_DATE: "2026-01-01",
                Columns.FEED_HOUR: 23,
            },
            # Duplicate
            {
                Columns.POLL_TIME: datetime(
                    2026, 1, 1, 23, 30, 30, tzinfo=timezone.utc
                ),
                Columns.FEED_TIMESTAMP: datetime(
                    2026, 1, 1, 23, 25, 0, tzinfo=timezone.utc
                ),
                Columns.VEHICLE_ID: "vehicle_A",
                Columns.ROUTE_ID: "route_1",
                Columns.LATITUDE: -36.8485,
                Columns.LONGITUDE: 174.7633,
                Columns.FEED_DATE: "2026-01-01",
                Columns.FEED_HOUR: 23,
            },
        ]

        create_test_delta_table(
            str(table_path),
            hour_23_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.FEED_DATE, Columns.FEED_HOUR, Columns.ROUTE_ID],
        )

        # Cleanup at 00:20 on Jan 2nd should target hour 23 of Jan 1st
        cleanup_hour(
            table_name=Tables.VEHICLE_POSITIONS,
            dedupe_keys=VEHICLE_POSITIONS_DEDUPE_KEYS,
            now=datetime(2026, 1, 2, 0, 20, 0, tzinfo=timezone.utc),
            data_path=tmp_path,
        )

        # Should deduplicate: 2 rows → 1 row
        after = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert after == 1

    def test_multiple_route_partitions(
        self,
        tmp_path: Path,
    ) -> None:
        """Cleanup should handle data across many route_id partitions.

        Creates data with 5 different routes, each with duplicates. After
        cleanup, verifies each route has exactly 1 row and all 5 partition
        directories are preserved.
        """
        table_path = tmp_path / Tables.VEHICLE_POSITIONS

        # Create data with 5 different routes, each with duplicates
        base_ts = datetime(2026, 1, 2, 10, 30, 0, tzinfo=timezone.utc)
        feed_ts = datetime(2026, 1, 2, 10, 25, 0, tzinfo=timezone.utc)
        multi_route_data = []
        for route_num in range(5):
            for _ in range(2):  # 2 duplicates per route
                multi_route_data.append({
                    Columns.POLL_TIME: base_ts,
                    Columns.FEED_TIMESTAMP: feed_ts,
                    Columns.VEHICLE_ID: f"vehicle_{route_num}",
                    Columns.ROUTE_ID: f"route_{route_num}",
                    Columns.LATITUDE: -36.8485,
                    Columns.LONGITUDE: 174.7633,
                    Columns.FEED_DATE: "2026-01-02",
                    Columns.FEED_HOUR: 10,
                })

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
        route_counts = duckdb.sql(
            f"SELECT route_id, COUNT(*) as cnt FROM delta_scan('{table_path}') "
            f"GROUP BY route_id"
        ).fetchall()
        assert len(route_counts) == 5
        for _, count in route_counts:
            assert count == 1

        # Verify partition directories still exist for all 5 routes
        hour_partition = table_path / "feed_date=2026-01-02" / "feed_hour=10"
        route_partitions = [d for d in hour_partition.iterdir() if d.is_dir()]
        route_names = {d.name for d in route_partitions}
        expected_routes = {f"route_id=route_{i}" for i in range(5)}
        assert route_names == expected_routes

    def test_cleanup_is_idempotent(
        self,
        tmp_path: Path,
    ) -> None:
        """Running cleanup twice on the same hour should produce same result.

        Creates a table with duplicates, runs cleanup twice on the same hour.
        Verifies no errors occur and the row count and file count are
        identical after both runs.
        """
        table_path = tmp_path / Tables.VEHICLE_POSITIONS

        base_ts = datetime(2026, 1, 2, 10, 30, 0, tzinfo=timezone.utc)
        feed_ts = datetime(2026, 1, 2, 10, 25, 0, tzinfo=timezone.utc)
        data = [
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
        files_after_first = len(list(table_path.rglob("*.parquet")))

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
        files_after_second = len(list(table_path.rglob("*.parquet")))

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
        table_path = tmp_path / Tables.VEHICLE_POSITIONS

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
