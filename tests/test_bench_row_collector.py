from collections.abc import Callable, Iterator
from typing import Any

import polars as pl
import pytest

from polars_row_collector.polars_row_collector import PolarsRowCollector

# uv run pytest tests/test_bench_row_collector.py --benchmark-only


def generate_rows(n: int) -> Iterator[dict[str, Any]]:
    for i in range(n):
        yield {
            "a": i,
            "b": float(i),
            "c": str(i),
        }


@pytest.mark.benchmark(group="row_ingest")
def test_row_collector_ingest_100k(
    benchmark: Callable[[Callable[[], None]], None],
) -> None:
    def run():
        collector = PolarsRowCollector(
            schema={"a": pl.Int64, "b": pl.Float64, "c": pl.String},
            collect_chunk_size=25_000,
        )
        for row in generate_rows(100_000):
            collector.add_row(row)
        _ = collector.to_df()

    benchmark(run)


@pytest.mark.parametrize("rows", [10_000, 50_000, 100_000, 1_000_000, 10_000_000])
@pytest.mark.benchmark(group="row_ingest_scaling")
def test_scaling(benchmark: Callable[[Callable[[], None]], None], rows: int) -> None:
    def run():
        collector = PolarsRowCollector(
            schema={"a": pl.Int64, "b": pl.Float64},
            collect_chunk_size=25_000,
        )
        for i in range(rows):
            collector.add_row({"a": i, "b": i})
        _ = collector.to_df()

    benchmark(run)
