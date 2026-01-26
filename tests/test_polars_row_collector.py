import os
from typing import TYPE_CHECKING, Literal

import polars as pl
import pytest
from polars.dataframe.frame import DataFrame
from polars.testing import assert_frame_equal

from polars_row_collector import PolarsRowCollector

if TYPE_CHECKING:
    from polars._typing import SchemaDict


def test_empty_collector_returns_empty_df_with_schema_and_cached() -> None:
    schema: SchemaDict = {"a": pl.Int64, "b": pl.String}
    c = PolarsRowCollector(schema=schema)

    df1 = c.to_df()
    assert isinstance(df1, pl.DataFrame)
    assert df1.shape == (0, 2)
    # Polars' .schema returns a dict[str, DataType]
    assert df1.schema == schema

    # Subsequent calls should return the exact same object (cached).
    df2 = c.to_df()
    assert df2 is df1


def test_add_row_preserves_order_and_flushes_on_chunk_size_boundary() -> None:
    c = PolarsRowCollector(collect_chunk_size=3)

    # Add two rows (below threshold).
    c.add_row({"x": 1})
    c.add_row({"x": 2})

    # Add third row -> should trigger a flush.
    c.add_row({"x": 3})

    # Now add one more row (stays in accumulator).
    c.add_row({"x": 4})

    df = c.to_df()
    assert df.shape == (4, 1)
    assert df.columns == ["x"]
    assert df.get_column("x").to_list() == [1, 2, 3, 4]


def test_add_rows_can_trigger_flush_and_preserves_order() -> None:
    c = PolarsRowCollector(collect_chunk_size=5)

    first_batch = [{"x": i} for i in range(5)]  # meets threshold exactly -> flush
    c.add_rows(first_batch)

    second_batch = [{"x": 5}, {"x": 6}]  # stays in accumulator
    c.add_rows(second_batch)

    df = c.to_df()
    assert df.shape == (7, 1)
    assert df.get_column("x").to_list() == list(range(7))


def test_schema_is_respected_when_creating_frames() -> None:
    schema: SchemaDict = {"a": pl.Int64, "b": pl.String}
    c = PolarsRowCollector(schema=schema, collect_chunk_size=2)

    c.add_row({"a": 1, "b": "z"})
    c.add_row({"a": 2, "b": "y"})  # triggers flush
    c.add_row({"a": 3, "b": "x"})  # remains in accumulator

    df: DataFrame = c.to_df()
    assert df.schema == schema
    assert df.select(pl.col("a").sum()).item() == 6  # pyright: ignore[reportUnknownMemberType]
    assert df.select(pl.col("b").str.join(delimiter="")).item() == "zyx"  # pyright: ignore[reportUnknownMemberType]


def test_to_df_caches_even_with_rechunk_flag_changes() -> None:
    c = PolarsRowCollector(collect_chunk_size=2)
    c.add_rows(  # Will flush once (2) and keep 3 in accumulator.
        [{"x": i} for i in range(5)]
    )
    df1 = c.to_df(rechunk=True)

    # After first materialization, further calls should return the same object
    # (regardless of rechunk arg).
    df2 = c.to_df(rechunk=False)
    assert df2 is df1
    assert df1.shape == (5, 1)
    assert df1.get_column("x").to_list() == [0, 1, 2, 3, 4]


def test_adding_after_finalization_does_not_change_result() -> None:
    c = PolarsRowCollector(collect_chunk_size=2)
    c.add_row({"x": 1})
    df1 = c.to_df()

    # Adding rows after finalization should not mutate the finalized DataFrame.
    with pytest.raises(
        RuntimeError, match="Cannot add rows to a finalized PolarsRowCollector."
    ):
        c.add_row({"x": 2})

    with pytest.raises(
        RuntimeError, match="Cannot add rows to a finalized PolarsRowCollector."
    ):
        c.add_rows([{"x": 3}, {"x": 4}])

    df2 = c.to_df()
    assert df2 is df1

    # The finalized DataFrame should still reflect only the rows present at first
    # finalization.
    assert df1.get_column("x").to_list() == [1]


@pytest.mark.parametrize("chunk_size", [1, 10, 1000, 5000, 25_000, 150_000])
@pytest.mark.parametrize("rechunk", [True, False], ids=["rechunk", "no_rechunk"])
@pytest.mark.parametrize(
    "include_schema", [True, False], ids=["with_schema", "without_schema"]
)
@pytest.mark.parametrize("add_rows_method", ["add_row", "add_rows"])
@pytest.mark.parametrize("collect_method", ["dataframe", "lazyframe"])
@pytest.mark.parametrize("number_of_rows", [1, 100_000])
def test_giant_grid(
    chunk_size: int,
    rechunk: bool,
    include_schema: bool,
    add_rows_method: Literal["add_row", "add_rows"],
    collect_method: Literal["dataframe", "lazyframe"],
    number_of_rows: int,
) -> None:
    """Test basically every combination of parameters with a large number of rows."""
    if bool(os.getenv("CI")) and (number_of_rows >= 100_000) and (chunk_size <= 100):
        pytest.skip("Skipping very large test on CI with small chunk size for speed.")

    schema: SchemaDict = {"x": pl.Int64, "y": pl.String}
    schema_arg: SchemaDict | Literal["infer_from_first_chunk"] = (
        schema if include_schema else "infer_from_first_chunk"
    )
    c = PolarsRowCollector(collect_chunk_size=chunk_size, schema=schema_arg)

    rows = [{"x": i, "y": f"Row {i}"} for i in range(number_of_rows)]

    # Add the rows using the specified method.
    if add_rows_method == "add_row":
        for row in rows:
            c.add_row(row)
    elif add_rows_method == "add_rows":
        c.add_rows(rows)

    # Done adding rows. Collect it.
    if collect_method == "dataframe":
        df = c.to_df(rechunk=rechunk)
    elif collect_method == "lazyframe":
        df = c.to_lazyframe(rechunk=rechunk).collect()

    assert df.shape == (number_of_rows, 2)
    assert df.columns == ["x", "y"]

    # Verify that all values are present and in order.
    assert df["x"].to_list() == list(range(number_of_rows))

    # Verify schema if provided.
    if include_schema:
        assert df.schema == schema

    # Verify that the DataFrame matches one created directly from the rows.
    df_expected = pl.DataFrame(rows, schema=schema, infer_schema_length=None)
    assert_frame_equal(df, df_expected)


@pytest.mark.parametrize("chunk_size", [1, 10, 1000, 5000, 25_000, 150_000])
@pytest.mark.parametrize("rechunk", [True, False])
@pytest.mark.parametrize("collect_method", ["dataframe", "lazyframe"])
@pytest.mark.parametrize("number_of_row_groups", [1, 5_100, 25_000])
def test_missing_columns_are_filled_with_nulls_when_schema_is_set(
    chunk_size: int,
    rechunk: bool,
    # include_schema: bool,
    # add_rows_method: Literal["add_row", "add_rows"],
    collect_method: Literal["dataframe", "lazyframe"],
    number_of_row_groups: int,
) -> None:
    if (
        bool(os.getenv("CI"))
        and (number_of_row_groups >= 25_000)
        and (chunk_size <= 100)
    ):
        pytest.skip("Skipping very large test on CI with small chunk size for speed.")

    schema: SchemaDict = {"a": pl.Int64, "b": pl.String, "c": pl.Float64}
    c = PolarsRowCollector(schema=schema, collect_chunk_size=chunk_size)

    # Add rows missing some columns.
    for _ in range(number_of_row_groups):
        c.add_row({"a": 1})  # missing b, c
        c.add_row({"b": "hello"})  # missing a, c
        c.add_row({"c": 3.14})  # missing a, b
        c.add_row({"a": 2, "c": 2.71})  # missing b

    # Done adding rows. Collect it.
    if collect_method == "dataframe":
        df = c.to_df(rechunk=rechunk)
    elif collect_method == "lazyframe":
        df = c.to_lazyframe(rechunk=rechunk).collect()

    # DataFrame should use the full schema in correct order
    assert df.schema == schema
    assert df.columns == ["a", "b", "c"]

    # Expected values with nulls for missing fields
    expected = pl.concat(
        [
            pl.DataFrame(
                {
                    "a": [1, None, None, 2],
                    "b": [None, "hello", None, None],
                    "c": [None, None, 3.14, 2.71],
                },
                schema=schema,
            )
        ]
        * number_of_row_groups
    )

    assert_frame_equal(df, expected)
