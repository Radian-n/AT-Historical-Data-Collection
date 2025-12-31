"""Tests for entity schema definitions."""

import json

import pyarrow as pa
import pytest

from app.const import Columns
from app.entities.vehicle_positions import VehiclePositionEntity

pytestmark = pytest.mark.unit


class TestVehiclePositionSchema:
    """Tests for VehiclePositionEntity schema methods."""

    def test_schema_has_expected_columns(self) -> None:
        """Schema contains all expected columns."""
        schema = VehiclePositionEntity.pa_schema()

        expected_columns = [
            Columns.POLL_TIME,
            Columns.FEED_TIMESTAMP,
            Columns.VEHICLE_ID,
            Columns.LABEL,
            Columns.LICENSE_PLATE,
            Columns.TRIP_ID,
            Columns.ROUTE_ID,
            Columns.DIRECTION_ID,
            Columns.SCHEDULE_RELATIONSHIP,
            Columns.START_DATE,
            Columns.START_TIME,
            Columns.LATITUDE,
            Columns.LONGITUDE,
            Columns.BEARING,
            Columns.SPEED,
            Columns.ODOMETER,
            Columns.OCCUPANCY_STATUS,
            Columns.ENTITY_IS_DELETED,
        ]

        assert set(schema.names) == set(expected_columns)

    def test_timestamp_columns_have_timezone(self) -> None:
        """Timestamp columns include timezone information."""
        schema = VehiclePositionEntity.pa_schema()

        poll_time_type = schema.field(Columns.POLL_TIME).type
        feed_ts_type = schema.field(Columns.FEED_TIMESTAMP).type

        assert pa.types.is_timestamp(poll_time_type)
        assert pa.types.is_timestamp(feed_ts_type)
        assert poll_time_type.tz == "+00:00"
        assert feed_ts_type.tz == "+00:00"

    def test_string_columns_are_string_type(self) -> None:
        """String columns have string type."""
        schema = VehiclePositionEntity.pa_schema()

        string_cols = [
            Columns.VEHICLE_ID,
            Columns.LABEL,
            Columns.LICENSE_PLATE,
            Columns.TRIP_ID,
            Columns.ROUTE_ID,
            Columns.START_DATE,
            Columns.START_TIME,
        ]

        for col in string_cols:
            assert pa.types.is_string(schema.field(col).type), (
                f"{col} should be string"
            )

    def test_numeric_columns_have_correct_types(self) -> None:
        """Numeric columns have appropriate types."""
        schema = VehiclePositionEntity.pa_schema()

        # Float64 columns (high precision needed)
        assert schema.field(Columns.LATITUDE).type == pa.float64()
        assert schema.field(Columns.LONGITUDE).type == pa.float64()
        assert schema.field(Columns.ODOMETER).type == pa.float64()

        # Float32 columns (lower precision OK)
        assert schema.field(Columns.BEARING).type == pa.float32()
        assert schema.field(Columns.SPEED).type == pa.float32()

        # Integer columns
        assert schema.field(Columns.DIRECTION_ID).type == pa.int32()
        assert schema.field(Columns.SCHEDULE_RELATIONSHIP).type == pa.int32()
        assert schema.field(Columns.OCCUPANCY_STATUS).type == pa.int32()

    def test_bool_column(self) -> None:
        """Boolean column has bool type."""
        schema = VehiclePositionEntity.pa_schema()

        assert schema.field(Columns.ENTITY_IS_DELETED).type == pa.bool_()

    def test_schema_metadata_present(self) -> None:
        """Schema includes expected metadata."""
        schema = VehiclePositionEntity.pa_schema()

        assert schema.metadata is not None
        assert b"entity" in schema.metadata
        assert b"version" in schema.metadata
        assert b"partition_columns" in schema.metadata

    def test_schema_metadata_partition_columns_valid_json(self) -> None:
        """Partition columns metadata is valid JSON list."""
        schema = VehiclePositionEntity.pa_schema()

        partition_json = schema.metadata[b"partition_columns"]
        partition_cols = json.loads(partition_json)

        assert isinstance(partition_cols, list)
        assert all(isinstance(c, str) for c in partition_cols)

    def test_dedupe_keys_exist_in_schema(self) -> None:
        """All dedupe keys are valid schema columns."""
        schema = VehiclePositionEntity.pa_schema()
        dedupe_keys = VehiclePositionEntity.dedupe_keys()

        for key in dedupe_keys:
            assert key in schema.names, f"Dedupe key {key} not in schema"

    def test_partition_cols_defined(self) -> None:
        """Partition columns are defined."""
        partition_cols = VehiclePositionEntity.partition_cols()

        assert len(partition_cols) > 0
        assert Columns.FEED_DATE in partition_cols
        assert Columns.FEED_HOUR in partition_cols
        assert Columns.ROUTE_ID in partition_cols

    def test_can_create_table_from_schema(
        self, sample_rows: list[dict]
    ) -> None:
        """Schema can be used to create a PyArrow table."""
        schema = VehiclePositionEntity.pa_schema()

        table = pa.Table.from_pylist(sample_rows, schema=schema)

        assert table.num_rows == len(sample_rows)
        assert table.schema == schema


class TestAddDerivedColumns:
    """Tests for VehiclePositionEntity.add_derived_columns()."""

    def test_adds_feed_date_column(self, sample_rows: list[dict]) -> None:
        """FEED_DATE column is added."""
        schema = VehiclePositionEntity.pa_schema()
        table = pa.Table.from_pylist(sample_rows, schema=schema)

        result = VehiclePositionEntity.add_derived_columns(table)

        assert Columns.FEED_DATE in result.column_names

    def test_adds_feed_hour_column(self, sample_rows: list[dict]) -> None:
        """FEED_HOUR column is added."""
        schema = VehiclePositionEntity.pa_schema()
        table = pa.Table.from_pylist(sample_rows, schema=schema)

        result = VehiclePositionEntity.add_derived_columns(table)

        assert Columns.FEED_HOUR in result.column_names

    def test_feed_date_format(self, sample_rows: list[dict]) -> None:
        """FEED_DATE is formatted as YYYY-MM-DD."""
        schema = VehiclePositionEntity.pa_schema()
        table = pa.Table.from_pylist(sample_rows, schema=schema)

        result = VehiclePositionEntity.add_derived_columns(table)

        feed_dates = result[Columns.FEED_DATE].to_pylist()
        # sample_rows have timestamps on 2024-12-15
        assert feed_dates[0] == "2024-12-15"

    def test_feed_hour_value(self, sample_rows: list[dict]) -> None:
        """FEED_HOUR extracts the hour (0-23)."""
        schema = VehiclePositionEntity.pa_schema()
        table = pa.Table.from_pylist(sample_rows, schema=schema)

        result = VehiclePositionEntity.add_derived_columns(table)

        feed_hours = result[Columns.FEED_HOUR].to_pylist()
        # sample_rows have timestamps at 10:30
        assert feed_hours[0] == 10

    def test_preserves_original_columns(self, sample_rows: list[dict]) -> None:
        """Original columns are preserved after adding derived columns."""
        schema = VehiclePositionEntity.pa_schema()
        table = pa.Table.from_pylist(sample_rows, schema=schema)
        original_cols = set(table.column_names)

        result = VehiclePositionEntity.add_derived_columns(table)

        assert original_cols.issubset(set(result.column_names))
