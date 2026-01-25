"""Tests for the compaction module."""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pytest
from conftest import (
    VEHICLE_POSITIONS_TEST_SCHEMA,
    create_test_delta_table,
)

from app.compaction import (
    cleanup_old_partitions,
    compact_all,
    compact_table,
)
from app.columns import Columns
from app.config import Tables

pytestmark = pytest.mark.unit


class TestCompactTable:
    """Tests for compact_table function."""

    def test_compacts_multiple_files(
        self,
        tmp_path: Path,
    ) -> None:
        """Compaction should reduce the number of parquet files."""
        raw_path: Path = tmp_path / "raw"
        table_path: Path = raw_path / Tables.VEHICLE_POSITIONS

        # Create data with single route to keep all files in one partition
        base_ts = datetime(2026, 1, 1, 10, 30, 0, tzinfo=timezone.utc)
        feed_ts = datetime(2026, 1, 1, 10, 25, 0, tzinfo=timezone.utc)
        data: list[dict[Columns, datetime | float | int | str]] = [
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
            data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        # Count parquet files before compaction (should be 2 from batched writes)
        parquet_files_before: list[Path] = list(table_path.rglob("*.parquet"))
        assert len(parquet_files_before) == 2, (
            "Test setup should create 2 files"
        )

        compact_table(Tables.VEHICLE_POSITIONS, raw_path=raw_path)

        # After compaction, should have 1 file
        parquet_files_after: list[Path] = list(table_path.rglob("*.parquet"))
        assert len(parquet_files_after) == 1

    def test_skips_nonexistent_table(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Compaction should skip if table doesn't exist."""
        raw_path: Path = tmp_path / "raw"

        with caplog.at_level(logging.INFO):
            compact_table("nonexistent_table", raw_path=raw_path)

        assert "does not exist, skipping compaction" in caplog.text

    def test_preserves_row_count(
        self,
        tmp_path: Path,
    ) -> None:
        """Compaction should not change the number of rows."""
        raw_path: Path = tmp_path / "raw"
        table_path: Path = raw_path / Tables.VEHICLE_POSITIONS

        base_ts = datetime(2026, 1, 1, 10, 30, 0, tzinfo=timezone.utc)
        data: list[dict[Columns, datetime | float | int | str]] = [
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
            for i in range(10)
        ]

        create_test_delta_table(
            str(table_path),
            data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        before = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]

        compact_table(Tables.VEHICLE_POSITIONS, raw_path=raw_path)

        after = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]

        assert before == after == 10


class TestCleanupOldPartitions:
    """Tests for cleanup_old_partitions function."""

    def test_deletes_old_partitions(
        self,
        tmp_path: Path,
    ) -> None:
        """Should delete partitions older than retention period."""
        raw_path: Path = tmp_path / "raw"
        table_path: Path = raw_path / Tables.VEHICLE_POSITIONS

        # Create data spanning multiple days
        base_ts = datetime(2026, 1, 10, 10, 30, 0, tzinfo=timezone.utc)
        multi_day_data: list[Any] = []
        for day in [1, 5, 8, 10]:  # Jan 1, 5, 8, 10
            multi_day_data.append(
                {
                    Columns.POLL_TIME: base_ts,
                    Columns.FEED_TIMESTAMP: datetime(
                        2026, 1, day, 10, 25, 0, tzinfo=timezone.utc
                    ),
                    Columns.VEHICLE_ID: f"vehicle_{day}",
                    Columns.ROUTE_ID: "route_1",
                    Columns.START_DATE: f"2026010{day}"
                    if day < 10
                    else f"202601{day}",
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

        # 4 rows before cleanup
        before = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert before == 4

        # Now = Jan 10, retention = 7 days, cutoff = Jan 3
        # Should delete Jan 1, keep Jan 5, 8, 10
        cleanup_old_partitions(
            Tables.VEHICLE_POSITIONS,
            now=datetime(2026, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
            raw_path=raw_path,
            retention_days=7,
        )

        # 3 rows after cleanup (Jan 1 deleted)
        after = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert after == 3

        # Verify Jan 1 was deleted
        jan1_count = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}') "
            f"WHERE start_date = '20260101'"
        ).fetchone()[0]
        assert jan1_count == 0

    def test_skips_nonexistent_table(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Should skip if table doesn't exist."""
        raw_path: Path = tmp_path / "raw"

        with caplog.at_level(logging.INFO):
            cleanup_old_partitions(
                "nonexistent_table",
                now=datetime(2026, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
                raw_path=raw_path,
            )

        assert "does not exist, skipping retention cleanup" in caplog.text

    def test_keeps_recent_partitions(
        self,
        tmp_path: Path,
    ) -> None:
        """Should keep partitions within retention period."""
        raw_path: Path = tmp_path / "raw"
        table_path: Path = raw_path / Tables.VEHICLE_POSITIONS

        # Create recent data (within retention)
        base_ts = datetime(2026, 1, 10, 10, 30, 0, tzinfo=timezone.utc)
        data: list[dict[Columns, datetime | float | int | str]] = [
            {
                Columns.POLL_TIME: base_ts,
                Columns.FEED_TIMESTAMP: base_ts,
                Columns.VEHICLE_ID: "vehicle_1",
                Columns.ROUTE_ID: "route_1",
                Columns.START_DATE: "20260108",  # Within 7-day retention
                Columns.LATITUDE: -36.8485,
                Columns.LONGITUDE: 174.7633,
            }
        ]

        create_test_delta_table(
            str(table_path),
            data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        cleanup_old_partitions(
            Tables.VEHICLE_POSITIONS,
            now=datetime(2026, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
            raw_path=raw_path,
            retention_days=7,
        )

        # Data should still exist
        count = duckdb.sql(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()[0]
        assert count == 1


class TestCompactAll:
    """Tests for compact_all function."""

    def test_processes_all_tables(
        self,
        tmp_path: Path,
    ) -> None:
        """Should compact and cleanup all realtime tables."""
        raw_path: Path = tmp_path / "raw"

        # Create test data for all three tables
        base_ts = datetime(2026, 1, 1, 10, 30, 0, tzinfo=timezone.utc)
        feed_ts = datetime(2026, 1, 1, 10, 25, 0, tzinfo=timezone.utc)
        base_data: list[dict[Columns, datetime | float | int | str]] = [
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

        # Create vehicle_positions table
        vp_path: Path = raw_path / Tables.VEHICLE_POSITIONS
        create_test_delta_table(
            str(vp_path),
            base_data,
            VEHICLE_POSITIONS_TEST_SCHEMA,
            [Columns.START_DATE, Columns.ROUTE_ID],
        )

        # Verify 2 files before compaction
        files_before: int = len(list(vp_path.rglob("*.parquet")))
        assert files_before == 2

        compact_all(
            now=datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
            raw_path=raw_path,
        )

        # Verify 1 file after compaction
        files_after: int = len(list(vp_path.rglob("*.parquet")))
        assert files_after == 1

    def test_handles_missing_tables_gracefully(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Should handle missing tables without error."""
        raw_path: Path = tmp_path / "raw"

        # Call compact_all with no tables created
        with caplog.at_level(logging.INFO):
            compact_all(
                now=datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
                raw_path=raw_path,
            )

        # Should log messages about missing tables but not raise errors
        assert "does not exist" in caplog.text
