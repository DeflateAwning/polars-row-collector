"""Unit tests for PolarsRowCollector configuration parameters."""

from typing import TYPE_CHECKING, Literal

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from polars_row_collector import PolarsRowCollector

if TYPE_CHECKING:
    from polars._typing import SchemaDict


# ============================================================================
# Tests for if_missing_columns parameter
# ============================================================================


def test_missing_columns_set_to_null_default_behavior() -> None:
    """Default behavior should set missing columns to null."""
    schema: SchemaDict = {"a": pl.Int64, "b": pl.String, "c": pl.Float64}
    c = PolarsRowCollector(schema=schema)

    c.add_row({"a": 1})  # missing b, c
    c.add_row({"a": 2, "b": "hello"})  # missing c
    c.add_row({"a": 3, "b": "world", "c": 1.5})  # all present

    df = c.to_df()

    expected = pl.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [None, "hello", "world"],
            "c": [None, None, 1.5],
        },
        schema=schema,
    )

    assert_frame_equal(df, expected)


def test_missing_columns_set_to_null_explicit() -> None:
    """Explicitly setting if_missing_columns to set_missing_to_null."""
    schema: SchemaDict = {"x": pl.Int64, "y": pl.Int64}
    c = PolarsRowCollector(schema=schema, if_missing_columns="set_missing_to_null")

    c.add_row({"x": 10})  # missing y
    c.add_row({"y": 20})  # missing x

    df = c.to_df()

    expected = pl.DataFrame(
        {"x": [10, None], "y": [None, 20]},
        schema=schema,
    )

    assert_frame_equal(df, expected)


def test_missing_columns_raise_with_add_row() -> None:
    """if_missing_columns='raise' should raise ValueError when columns are missing."""
    schema: SchemaDict = {"a": pl.Int64, "b": pl.String}
    c = PolarsRowCollector(schema=schema, if_missing_columns="raise")

    # Adding a complete row should work
    c.add_row({"a": 1, "b": "test"})

    # Adding an incomplete row should raise
    with pytest.raises(ValueError, match="missing columns"):
        c.add_row({"a": 2})  # missing b

    # Verify the error message contains the missing column name
    with pytest.raises(ValueError, match="'b'"):
        c.add_row({"a": 3})


def test_missing_columns_raise_with_add_rows() -> None:
    """if_missing_columns='raise' should raise ValueError in add_rows."""
    schema: SchemaDict = {"a": pl.Int64, "b": pl.String, "c": pl.Float64}
    c = PolarsRowCollector(schema=schema, if_missing_columns="raise")

    # Adding complete rows should work
    c.add_rows([{"a": 1, "b": "x", "c": 1.0}, {"a": 2, "b": "y", "c": 2.0}])

    # Adding rows with missing columns should raise
    with pytest.raises(ValueError, match="missing columns"):
        c.add_rows([{"a": 3, "b": "z"}])  # missing c


def test_missing_columns_raise_lists_all_missing_columns() -> None:
    """Error message should list all missing columns."""
    schema: SchemaDict = {
        "a": pl.Int64,
        "b": pl.String,
        "c": pl.Float64,
        "d": pl.Boolean,
    }
    c = PolarsRowCollector(schema=schema, if_missing_columns="raise")

    with pytest.raises(ValueError) as exc_info:
        c.add_row({"a": 1})  # missing b, c, d

    error_msg = str(exc_info.value)
    assert "'b'" in error_msg
    assert "'c'" in error_msg
    assert "'d'" in error_msg
    assert "3 missing columns" in error_msg or "missing columns" in error_msg


def test_missing_columns_with_no_schema_does_not_raise() -> None:
    """When schema is inferred, missing columns shouldn't cause issues."""
    c = PolarsRowCollector(
        schema="infer_from_first_chunk",
        if_missing_columns="raise",
        collect_chunk_size=2,
    )

    # First row establishes the schema
    c.add_row({"x": 1, "y": 2})
    c.add_row({"x": 3, "y": 4})  # triggers flush and sets schema

    # Now schema is locked, this should raise
    with pytest.raises(ValueError, match="missing columns"):
        c.add_row({"x": 5})  # missing y


# ============================================================================
# Tests for if_extra_columns parameter
# ============================================================================


def test_extra_columns_drop_extra_default_behavior() -> None:
    """Default behavior should drop extra columns silently."""
    schema: SchemaDict = {"a": pl.Int64, "b": pl.String}
    c = PolarsRowCollector(schema=schema)

    c.add_row({"a": 1, "b": "hello", "c": 999})  # c should be dropped
    c.add_row({"a": 2, "b": "world"})

    df = c.to_df()

    expected = pl.DataFrame(
        {"a": [1, 2], "b": ["hello", "world"]},
        schema=schema,
    )

    assert_frame_equal(df, expected)
    assert "c" not in df.columns


def test_extra_columns_drop_extra_explicit() -> None:
    """Explicitly setting if_extra_columns to drop_extra."""
    schema: SchemaDict = {"x": pl.Int64}
    c = PolarsRowCollector(schema=schema, if_extra_columns="drop_extra")

    c.add_row({"x": 1, "y": 2, "z": 3})  # y and z should be dropped

    df = c.to_df()

    expected = pl.DataFrame({"x": [1]}, schema=schema)

    assert_frame_equal(df, expected)
    assert df.columns == ["x"]


def test_extra_columns_raise_with_add_row() -> None:
    """if_extra_columns='raise' should raise ValueError when extra columns present."""
    schema: SchemaDict = {"a": pl.Int64, "b": pl.String}
    c = PolarsRowCollector(schema=schema, if_extra_columns="raise")

    # Adding a row with exact schema should work
    c.add_row({"a": 1, "b": "test"})

    # Adding a row with extra columns should raise
    with pytest.raises(ValueError, match="extra columns"):
        c.add_row({"a": 2, "b": "test", "c": 3.14})  # c is extra


def test_extra_columns_raise_with_add_rows() -> None:
    """if_extra_columns='raise' should raise ValueError in add_rows."""
    schema: SchemaDict = {"a": pl.Int64}
    c = PolarsRowCollector(schema=schema, if_extra_columns="raise")

    # Adding rows with exact schema should work
    c.add_rows([{"a": 1}, {"a": 2}])

    # Adding rows with extra columns should raise
    with pytest.raises(ValueError, match="extra columns"):
        c.add_rows([{"a": 3, "b": "extra"}])


def test_extra_columns_raise_lists_all_extra_columns() -> None:
    """Error message should list all extra columns."""
    schema: SchemaDict = {"a": pl.Int64}
    c = PolarsRowCollector(schema=schema, if_extra_columns="raise")

    with pytest.raises(ValueError) as exc_info:
        c.add_row({"a": 1, "b": 2, "c": 3, "d": 4})  # b, c, d are extra

    error_msg = str(exc_info.value)
    assert "'b'" in error_msg
    assert "'c'" in error_msg
    assert "'d'" in error_msg
    assert "3 extra columns" in error_msg or "extra columns" in error_msg


def test_extra_columns_with_no_schema_does_not_raise() -> None:
    """When schema is inferred, extra columns shouldn't cause issues initially."""
    c = PolarsRowCollector(
        schema="infer_from_first_chunk",
        if_extra_columns="raise",
        collect_chunk_size=2,
    )

    # First chunk establishes the schema
    c.add_row({"x": 1, "y": 2})
    c.add_row({"x": 3, "y": 4})  # triggers flush and sets schema

    # Now schema is locked, extra columns should raise
    with pytest.raises(ValueError, match="extra columns"):
        c.add_row({"x": 5, "y": 6, "z": 7})  # z is extra


# ============================================================================
# Tests for combinations of both parameters
# ============================================================================


def test_both_raise_with_missing_columns() -> None:
    """Test both set to 'raise' when missing columns."""
    schema: SchemaDict = {"a": pl.Int64, "b": pl.String}
    c = PolarsRowCollector(
        schema=schema,
        if_missing_columns="raise",
        if_extra_columns="raise",
    )

    with pytest.raises(ValueError, match="missing columns"):
        c.add_row({"a": 1})  # missing b


def test_both_raise_with_extra_columns() -> None:
    """Test both set to 'raise' when extra columns."""
    schema: SchemaDict = {"a": pl.Int64, "b": pl.String}
    c = PolarsRowCollector(
        schema=schema,
        if_missing_columns="raise",
        if_extra_columns="raise",
    )

    with pytest.raises(ValueError, match="extra columns"):
        c.add_row({"a": 1, "b": "test", "c": 999})  # extra c


def test_both_raise_with_perfect_rows() -> None:
    """Test both set to 'raise' with perfectly matching rows."""
    schema: SchemaDict = {"a": pl.Int64, "b": pl.String}
    c = PolarsRowCollector(
        schema=schema,
        if_missing_columns="raise",
        if_extra_columns="raise",
    )

    # Should work fine with exact schema matches
    c.add_row({"a": 1, "b": "hello"})
    c.add_row({"a": 2, "b": "world"})

    df = c.to_df()

    expected = pl.DataFrame(
        {"a": [1, 2], "b": ["hello", "world"]},
        schema=schema,
    )

    assert_frame_equal(df, expected)


def test_missing_null_extra_drop() -> None:
    """Test if_missing_columns='set_missing_to_null' with if_extra_columns='drop_extra'."""
    schema: SchemaDict = {"a": pl.Int64, "b": pl.String}
    c = PolarsRowCollector(
        schema=schema,
        if_missing_columns="set_missing_to_null",
        if_extra_columns="drop_extra",
    )

    c.add_row({"a": 1})  # missing b, should be null
    c.add_row({"a": 2, "b": "test", "c": 999})  # c should be dropped
    c.add_row({"b": "only_b", "d": 123})  # missing a (null), d dropped

    df = c.to_df()

    expected = pl.DataFrame(
        {"a": [1, 2, None], "b": [None, "test", "only_b"]},
        schema=schema,
    )

    assert_frame_equal(df, expected)


# ============================================================================
# Parameterized tests for configuration combinations
# ============================================================================


@pytest.mark.parametrize(
    "if_missing",
    ["set_missing_to_null", "raise"],
)
@pytest.mark.parametrize(
    "if_extra",
    ["drop_extra", "raise"],
)
@pytest.mark.parametrize("chunk_size", [1, 10, 100])
def test_config_combinations_with_valid_data(
    if_missing: Literal["set_missing_to_null", "raise"],
    if_extra: Literal["drop_extra", "raise"],
    chunk_size: int,
) -> None:
    """Test that all config combinations work with perfectly valid data."""
    schema: SchemaDict = {"a": pl.Int64, "b": pl.String}
    c = PolarsRowCollector(
        schema=schema,
        if_missing_columns=if_missing,
        if_extra_columns=if_extra,
        collect_chunk_size=chunk_size,
    )

    # Add rows that exactly match the schema
    rows = [{"a": i, "b": f"row_{i}"} for i in range(20)]
    c.add_rows(rows)

    df = c.to_df()

    expected = pl.DataFrame(rows, schema=schema)
    assert_frame_equal(df, expected)


@pytest.mark.parametrize("add_method", ["add_row", "add_rows"])
def test_missing_columns_raise_with_both_add_methods(
    add_method: Literal["add_row", "add_rows"],
) -> None:
    """Test that if_missing_columns='raise' works with both add methods."""
    schema: SchemaDict = {"a": pl.Int64, "b": pl.String}
    c = PolarsRowCollector(schema=schema, if_missing_columns="raise")

    with pytest.raises(ValueError, match="missing columns"):
        if add_method == "add_row":
            c.add_row({"a": 1})  # missing b
        else:
            c.add_rows([{"a": 1}])  # missing b


@pytest.mark.parametrize("add_method", ["add_row", "add_rows"])
def test_extra_columns_raise_with_both_add_methods(
    add_method: Literal["add_row", "add_rows"],
) -> None:
    """Test that if_extra_columns='raise' works with both add methods."""
    schema: SchemaDict = {"a": pl.Int64}
    c = PolarsRowCollector(schema=schema, if_extra_columns="raise")

    with pytest.raises(ValueError, match="extra columns"):
        if add_method == "add_row":
            c.add_row({"a": 1, "b": 2})  # extra b
        else:
            c.add_rows([{"a": 1, "b": 2}])  # extra b


# ============================================================================
# Edge cases and special scenarios
# ============================================================================


def test_empty_row_with_raise_configs() -> None:
    """Test adding an empty row when both configs are set to raise."""
    schema: SchemaDict = {"a": pl.Int64, "b": pl.String}
    c = PolarsRowCollector(
        schema=schema,
        if_missing_columns="raise",
        if_extra_columns="raise",
    )

    # Empty row is missing all columns
    with pytest.raises(ValueError, match="missing columns"):
        c.add_row({})


def test_empty_row_with_null_and_drop_configs() -> None:
    """Test adding an empty row when set to null and drop."""
    schema: SchemaDict = {"a": pl.Int64, "b": pl.String}
    c = PolarsRowCollector(
        schema=schema,
        if_missing_columns="set_missing_to_null",
        if_extra_columns="drop_extra",
    )

    c.add_row({})  # Should create a row with all nulls
    c.add_row({"a": 1, "b": "test"})

    df = c.to_df()

    expected = pl.DataFrame(
        {"a": [None, 1], "b": [None, "test"]},
        schema=schema,
    )

    assert_frame_equal(df, expected)


def test_column_order_preserved_with_extra_columns_dropped() -> None:
    """Test that schema column order is preserved when dropping extra columns."""
    schema: SchemaDict = {"z": pl.Int64, "a": pl.String, "m": pl.Float64}
    c = PolarsRowCollector(schema=schema, if_extra_columns="drop_extra")

    # Add row with columns in different order and extra columns
    c.add_row({"m": 1.5, "extra1": 999, "a": "test", "z": 42, "extra2": 888})

    df = c.to_df()

    # Schema order should be preserved (z, a, m)
    assert df.columns == ["z", "a", "m"]
    assert df.schema == schema


def test_multiple_violations_in_sequence() -> None:
    """Test handling multiple violations across different rows."""
    schema: SchemaDict = {"a": pl.Int64, "b": pl.String}
    c = PolarsRowCollector(
        schema=schema,
        if_missing_columns="raise",
        if_extra_columns="raise",
    )

    # First violation: missing column
    with pytest.raises(ValueError, match="missing columns"):
        c.add_row({"a": 1})

    # Second violation: extra column
    with pytest.raises(ValueError, match="extra columns"):
        c.add_row({"a": 1, "b": "test", "c": 999})

    # Valid row should still work
    c.add_row({"a": 1, "b": "test"})

    df = c.to_df()
    assert df.shape == (1, 2)


def test_configs_with_lazyframe_output() -> None:
    """Test that configs work correctly with to_lazyframe."""
    schema: SchemaDict = {"a": pl.Int64, "b": pl.String}
    c = PolarsRowCollector(
        schema=schema,
        if_missing_columns="set_missing_to_null",
        if_extra_columns="drop_extra",
    )

    c.add_row({"a": 1, "extra": 999})  # extra dropped
    c.add_row({"b": "test"})  # a is null

    lf = c.to_lazyframe()
    df = lf.collect()

    expected = pl.DataFrame(
        {"a": [1, None], "b": [None, "test"]},
        schema=schema,
    )

    assert_frame_equal(df, expected)


def test_schema_column_names_are_case_sensitive() -> None:
    """Test that column name matching is case-sensitive."""
    schema: SchemaDict = {"A": pl.Int64, "b": pl.String}
    c = PolarsRowCollector(schema=schema, if_extra_columns="raise")

    # lowercase 'a' should be treated as extra column
    with pytest.raises(ValueError, match="extra columns"):
        c.add_row({"a": 1, "b": "test"})  # 'a' != 'A'
