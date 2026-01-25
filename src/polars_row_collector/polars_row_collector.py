"""Facade to collect rows one-by-one into a Polars DataFrame (in the least-bad way)."""

from collections.abc import Iterable, Sequence
from typing import Any

import polars as pl
from polars._typing import SchemaDict


class PolarsRowCollector:
    """Facade to collect rows into a Polars DataFrame in a memory-efficient manner.

    Maintains the order the rows were added.

    Strongly recommended to provide a schema for best performance. If no schema is provided,
    the schema will be inferred from the first chunk of rows added. Note that extra columns added
    afterwards that were not in the schema will be silently discarded!

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
        self, schema: SchemaDict | None = None, *, collect_chunk_size: int = 25_000
    ) -> None:
        """Facade to collect rows into a Polars DataFrame in a memory-efficient manner.

        Maintains the order the rows were added.

        Strongly recommended to provide a schema for best performance. If no schema is provided,
        the schema will be inferred from the first chunk of rows added. Note that extra columns added
        afterwards that were not in the schema will be silently discarded!

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
        if collect_chunk_size <= 0:
            msg = "collect_chunk_size must be a positive integer."
            raise ValueError(msg)

        self.collect_chunk_size: int = collect_chunk_size

        self._pl_schema: SchemaDict | None = schema

        self._accumulated_rows: list[dict[str, Any]] = []
        self._collected_dfs: list[pl.DataFrame] = []
        self._final_df: pl.DataFrame | None = None
        self._is_finalized: bool = False

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
            rows: A sequence of dictionaries, each representing a row.

        """
        if self._is_finalized:
            msg = "Cannot add rows to a finalized PolarsRowCollector."
            raise RuntimeError(msg)

        # TODO: This function can be optimized to immediately convert to DataFrame if the `rows` is large enough.
        self._accumulated_rows.extend(rows)
        if len(self._accumulated_rows) >= self.collect_chunk_size:
            self._flush_accumulated_rows()

    def _flush_accumulated_rows(self) -> None:
        if not self._accumulated_rows:
            return

        df = pl.DataFrame(
            self._accumulated_rows,
            schema=self._pl_schema,
            infer_schema_length=None,  # Use all rows, if necessary.
        )

        # Store the schema after the first chunk, and use it on all subsequent chunks. Improves
        # performance. Removing this would allow each chunk to have a different schema, which ends
        # up causing an error during `pl.concat()` operation anyway.
        if self._pl_schema is None:
            self._pl_schema = df.schema

        self._collected_dfs.append(df)
        self._accumulated_rows = []

    def to_df(self, *, rechunk: bool = False) -> pl.DataFrame:
        """Convert the collected rows to a Polars DataFrame.

        Finalizes the collector, preventing further rows from being added.

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
                self._final_df = pl.DataFrame(schema=self._pl_schema)
                self._is_finalized = True

            self._collected_dfs = []
        return self._final_df

    def to_lazyframe(self, *, rechunk: bool = False) -> pl.LazyFrame:
        """Convert the collected rows to a Polars LazyFrame.

        Does NOT finalize the collector, allowing further rows to be added.

        Returns:
            A Polars LazyFrame containing all collected rows.

        """
        if self._is_finalized:
            assert self._final_df is not None
            return self._final_df.lazy()

        self._flush_accumulated_rows()
        if self._collected_dfs:
            df = pl.concat(self._collected_dfs, rechunk=rechunk)
            return df.lazy()
        else:
            return pl.DataFrame(schema=self._pl_schema).lazy()
