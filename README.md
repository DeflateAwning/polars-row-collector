# polars-row-collector

![PyPI](https://img.shields.io/pypi/v/polars-row-collector)
![Python](https://img.shields.io/pypi/pyversions/polars-row-collector)
![License](https://img.shields.io/github/license/DeflateAwning/polars-row-collector)
![CI](https://img.shields.io/github/actions/workflow/status/DeflateAwning/polars-row-collector/ci.yml?label=tests)
![Docs](https://img.shields.io/badge/docs-online-blue)
![Built for Polars](https://img.shields.io/badge/built%20for-polars-4B8BBE)

Facade to collect rows one-by-one into a Polars DataFrame (in the least-bad way)

* **Docs:** https://DeflateAwning.github.io/polars-row-collector  
* **GitHub:** https://github.com/DeflateAwning/polars-row-collector  
* **PyPI:** https://pypi.org/project/polars-row-collector/

## Getting Started Example

Add the library to your dependencies: `uv add polars_row_collector`

```python
import polars as pl
from polars_row_collector import PolarsRowCollector

collector = PolarsRowCollector(
    # Note: Schema is optional, but recommended.
    schema={"col1": pl.Int64, "col2": pl.Float64}
)

for item in items:
    row = {
        "col1": item.value1,
        "col2": item.value2,
    }
    collector.add_row(row)

df = collector.to_df()
```

You can think of `collector` as filling the same niche as the following alternatives:
    * `list_of_dfs: list[pl.DataFrame]`
    * `list_of_dicts: list[dict[str, Any]]`, then `pl.from_dicts(list_of_dicts)`

## Features

* Highly performant and memory-optimized.
    * **93% lower memory usage** compares to a list-of-dicts approach.
* Optionally supply a schema for the incoming rows.
* Thread-safe (when GIL is enabled - default in Python <= 3.15).
* Configuration arguments for safety vs. performance tradeoffs:
    * Behaviour if there are missing columns: Enforce all columns present or allow missing columns.
    * Behaviour if there are extra columns: Drop silently or raise.
    * Maintain insertion order.

## Example Applications

* Gathering data in a web scraping/parsing tool.
* Gathering/batching incoming log messages or event logs before writing in bulk to some destination.
* Gathering data in a markup/document parsing pipeline (e.g., XML with lots of conditionals).

## Benchmarks

* Benchmark: Collecting 50M rows. Each row has 3 columns.
    * Average Speed: 0.42µs/row for both (consistent).
        * Conclusion: No additional elapsed runtime overhead.
    * **Peak memory usage: 93% decrease** compared to a naive implementation.
        * Baseline (list-of-dicts): 26,011.93 MiB
        * `PolarsRowCollector`: 1,860.16 MiB

### Baseline (list-of-dicts)

```
> COLLECT_MODE=dicts uv run perf_scripts/perf_test_script.py

Collected DataFrame. Current RSS: 26,011.93 MiB | Peak RSS: 26,011.93 MiB
Final overall time per row: 0.42µs/row
```

### `PolarsRowCollector`

```
> COLLECT_MODE=prc uv run perf_scripts/perf_test_script.py

Collected DataFrame. Current RSS: 1,860.16 MiB | Peak RSS: 1,860.16 MiB
Final overall time per row: 0.42µs/row
```

## Future Features

* Intermediate to-disk storage to temporary parquet files to larger-than-memory collections.
* Further optimize appending many rows at once.
* Read the dataframe so-far, in the middle of gathering rows.
* Documentation.

## Disclaimer

As the project's description says, this is the "least-bad way" to accomplish this pattern.

If you _can_ implement your code in such a way that you're not collecting individual rows of a dataframe, you are likely better-off doing it that way (e.g., collecting a `list[pl.DataFrame]`).

However, there are always exceptions to the best practices. In those cases, this library is an ideal choice, and is significantly more memory-efficient than collecting into a `list[dict[str, Any]]` then converting to a DataFrame later.
