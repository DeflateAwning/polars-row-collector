"""Facade to collect rows one-by-one into a Polars DataFrame (in the least-bad way)."""

from collections.abc import Iterable, Sequence
from typing import Any, Literal

import polars as pl
from polars._typing import PolarsDataType, SchemaDict


class PolarsRowCollector:
    """Facade to collect rows into a Polars DataFrame in a memory-efficient manner.

    Strongly recommended to provide an explicit schema. If no schema is provided,
    the schema will be inferred from the first chunk of rows added.

    Example:
        ```python
        collector = PolarsRowCollector(schema={"col1": pl.Int64, "col2": pl.Float64})

        for item in items:
            row = {
                "col1": item.value1,
                "col2": item.value2,
            }
            collector.add_row(row)

        df = collector.to_df()
        ```

    """

    def __init__(
        self,
        # TODO: Add schema modes: "concat_diagonal_strict", "concat_diagonal_relaxed"
        schema: SchemaDict
        | Literal["infer_from_first_chunk"] = "infer_from_first_chunk",
        *,
        collect_chunk_size: int = 25_000,
        maintain_insert_order: bool = False,
        if_missing_columns: Literal[
            "set_missing_to_null", "raise"
        ] = "set_missing_to_null",
        if_extra_columns: Literal["drop_extra", "raise"] = "drop_extra",
    ) -> None:
        """Facade to collect rows into a Polars DataFrame in a memory-efficient manner.

        Strongly recommended to provide an explicit schema. If no schema is provided,
        the schema will be inferred from the first chunk of rows added.

        Example:
            ```python
            collector = PolarsRowCollector(schema={"col1": pl.Int64, "col2": pl.Float64})

            for item in items:
                row = {
                    "col1": item.value1,
                    "col2": item.value2,
                }
                collector.add_row(row)

            df = collector.to_df()
            ```

        Args:
            schema: The schema to use for the DataFrame, or `"infer_from_first_chunk"`
                to infer from the first chunk. It is strongly recommended to provide an explicit schema.
            collect_chunk_size: Number of rows to accumulate before converting to a DataFrame chunk.
            maintain_insert_order: Whether to maintain the order of inserted rows.
                Note: While `PolarsRowCollector` currently always returns the rows in order,
                the presence of this argument allows for future optimizations where the rows
                are NOT returned in order. Explicitly set to `True` if you want to maintain the
                current behaviour forever.
            if_missing_columns: How to handle missing columns in input rows (`"set_missing_to_null"`
                or `"raise"`).
                Note: `"set_missing_to_null"` is currently slightly more performant.
            if_extra_columns: How to handle extra columns in input rows (`"drop_extra"` or `"raise"`).
                Note: `"drop_extra"` is currently slightly more performant.

        """
        if collect_chunk_size <= 0:
            msg = "collect_chunk_size must be a positive integer."
            raise ValueError(msg)

        self.collect_chunk_size: int = collect_chunk_size
        self._if_missing_columns: Literal["set_missing_to_null", "raise"] = (
            if_missing_columns
        )
        self._if_extra_columns: Literal["drop_extra", "raise"] = if_extra_columns

        # These two internal-tracking schema variables must be assigned together.
        # They are separate because certain non-Python types must be parsed as a similar equivalent
        # type, and then must be converted to a normal type after (e.g., Enum parsed as String then cast).
        self._pl_storage_schema: dict[str, PolarsDataType] | None = None
        self._pl_parse_schema: dict[str, PolarsDataType] | None = None
        self._pl_parse_and_storage_schemas_same: bool
        self._set_new_schema(pl_storage_schema=schema)

        self._accumulated_rows: list[dict[str, Any]] = []
        self._collected_dfs: list[pl.DataFrame] = []
        self._final_df: pl.DataFrame | None = None
        self._is_finalized: bool = False

    def _set_new_schema(
        self, pl_storage_schema: SchemaDict | Literal["infer_from_first_chunk"]
    ) -> None:
        """Set the internal schema for storage and parsing.

        Args:
            pl_storage_schema: The schema to use for storage, or `"infer_from_first_chunk"`
                to infer from the first chunk.

        """
        if (self._pl_parse_schema is not None) or (self._pl_storage_schema is not None):
            raise RuntimeError(
                "This method is meant to only be run exactly once when setting a new schema."
            )

        match pl_storage_schema:
            case "infer_from_first_chunk":
                self._pl_storage_schema = None
                self._pl_parse_schema = None
                self._pl_parse_and_storage_schemas_same = True
            case dict():  # SchemaDict
                self._pl_storage_schema = pl_storage_schema
                self._pl_parse_schema = _convert_precise_schema_to_python_parse_schema(
                    pl_storage_schema
                )
            case _:
                self._pl_storage_schema = dict(pl_storage_schema)
                self._pl_parse_schema = _convert_precise_schema_to_python_parse_schema(
                    pl_storage_schema
                )

        self._pl_parse_and_storage_schemas_same = (
            self._pl_storage_schema == self._pl_parse_schema
        )

    def add_row(self, row: dict[str, Any]) -> None:
        """Add a row to the collector.

        Note: If extra columns are passed that are not in the schema, they will be silently
        discarded.

        Args:
            row: A dictionary, where keys are column names and values are the corresponding data.

        """
        if self._is_finalized:
            msg = "Cannot add rows to a finalized PolarsRowCollector."
            raise RuntimeError(msg)

        # Validate missing columns.
        if (self._if_missing_columns == "raise") and (
            self._pl_storage_schema is not None
        ):
            missing_columns = sorted(
                set(self._pl_storage_schema.keys()) - set(row.keys())
            )
            if missing_columns:
                msg = (
                    f"Trying to add a row with {len(missing_columns)} missing columns. "
                    'PolarsRowCollector is configured with if_missing_columns="raise". '
                    f"Missing columns: {missing_columns}"
                )
                raise ValueError(msg)
            del missing_columns

        # Validate extra columns.
        if (self._if_extra_columns == "raise") and (
            self._pl_storage_schema is not None
        ):
            extra_columns = sorted(
                set(row.keys()) - set(self._pl_storage_schema.keys())
            )
            if extra_columns:
                msg = (
                    f"Trying to add a row with {len(extra_columns)} extra columns. "
                    'PolarsRowCollector is configured with if_extra_columns="raise". '
                    f"Extra columns: {extra_columns}"
                )
                raise ValueError(msg)
            del extra_columns

        self._accumulated_rows.append(row)
        if len(self._accumulated_rows) >= self.collect_chunk_size:
            self._flush_accumulated_rows()

    def add_rows(
        self, rows: Sequence[dict[str, Any]] | Iterable[dict[str, Any]]
    ) -> None:
        """Add multiple rows to the collector.

        Note: If extra columns are passed that are not in the schema, they will be silently
        discarded.

        Args:
            rows: A sequence or iterable of dictionaries, each representing a row.

        """
        if self._is_finalized:
            msg = "Cannot add rows to a finalized PolarsRowCollector."
            raise RuntimeError(msg)

        # TODO: This function can be optimized to immediately convert to DataFrame if the `rows` is large enough.
        if (self._if_extra_columns == "drop_extra") and (
            self._if_missing_columns == "set_missing_to_null"
        ):
            self._accumulated_rows.extend(rows)
        else:
            # Must use `self.add_row` for now to validate each row individually.
            for row in rows:
                self.add_row(row)

        if len(self._accumulated_rows) >= self.collect_chunk_size:
            self._flush_accumulated_rows()

    def _flush_accumulated_rows(self) -> None:
        if not self._accumulated_rows:
            return

        df = pl.DataFrame(
            self._accumulated_rows,
            schema=self._pl_parse_schema,
            infer_schema_length=None,  # Use all rows, if necessary (when schema=None).
        )
        if (self._pl_storage_schema is not None) and (
            # Performance Optimization: Only cast if the parse vs. storage schemas are different.
            self._pl_parse_and_storage_schemas_same is False
        ):
            df = df.cast(self._pl_storage_schema)  # pyright: ignore[reportArgumentType]

        # Store the schema after the first chunk, and use it on all subsequent chunks. Improves
        # performance. Removing this would allow each chunk to have a different schema, which ends
        # up causing an error during `pl.concat()` operation anyway.
        if self._pl_storage_schema is None:
            self._set_new_schema(pl_storage_schema=df.schema)

        self._collected_dfs.append(df)
        self._accumulated_rows = []

    def to_df(self, *, rechunk: bool = False) -> pl.DataFrame:
        """Convert the collected rows to a Polars DataFrame.

        Finalizes the collector, preventing further rows from being added.

        Args:
            rechunk: Whether to rechunk the resulting DataFrame for contiguous memory.

        Returns:
            A Polars DataFrame containing all collected rows.

        """
        self._flush_accumulated_rows()

        # Note: Store into `self._final_df` so that multiple calls to to_df() return the same
        # DataFrame, without extra computation on subsequent calls.
        if self._final_df is None:
            if self._collected_dfs:
                self._final_df = pl.concat(self._collected_dfs, rechunk=rechunk)
                self._is_finalized = True
            else:
                self._final_df = pl.DataFrame(schema=self._pl_storage_schema)
                self._is_finalized = True

            self._collected_dfs = []
        return self._final_df

    def to_lazyframe(self, *, rechunk: bool = False) -> pl.LazyFrame:
        """Convert the collected rows to a Polars LazyFrame.

        Does NOT finalize the collector, allowing further rows to be added.

        Args:
            rechunk: Whether to rechunk the resulting DataFrame for contiguous memory.

        Returns:
            A Polars LazyFrame containing all collected rows. No guarantees are made about
            how long the LazyFrame remains valid. It is recommended to fetch and consume
            the LazyFrame in the same scope as the `PolarsRowCollector`.

        """
        if self._is_finalized:
            assert self._final_df is not None
            return self._final_df.lazy()

        self._flush_accumulated_rows()
        if self._collected_dfs:
            df = pl.concat(self._collected_dfs, rechunk=rechunk)
            return df.lazy()
        else:
            return pl.DataFrame(schema=self._pl_storage_schema).lazy()


def _convert_precise_type_to_python_parse_type(dtype: PolarsDataType) -> PolarsDataType:
    """Convert a precise intended schema to a schema which can map to Python objects.

    Required because: https://github.com/pola-rs/polars/issues/26282.

    Args:
        dtype: The Polars data type to convert.

    Returns:
        The converted Polars data type suitable for Python parsing.

    """
    if isinstance(dtype, pl.Enum):
        return pl.String
    if isinstance(dtype, pl.Float32) or (dtype == pl.Float32):
        return pl.Float64

    bad_int_types = (pl.UInt8, pl.Int8, pl.UInt16, pl.Int16, pl.UInt32, pl.Int32)
    if isinstance(dtype, bad_int_types) or (dtype in bad_int_types):  # pyright: ignore[reportUnnecessaryContains]
        return pl.Int64

    return dtype


def _convert_precise_schema_to_python_parse_schema(
    schema: SchemaDict,
) -> dict[str, PolarsDataType]:
    """Convert a precise intended schema to a schema which can map to Python objects.

    Required because: https://github.com/pola-rs/polars/issues/26282.

    Args:
        schema: The schema dictionary to convert.

    Returns:
        A dictionary mapping column names to converted Polars data types suitable for Python parsing.

    """
    return {k: _convert_precise_type_to_python_parse_type(v) for k, v in schema.items()}
