import gc
import os
import time
from collections.abc import Generator
from typing import Any, Never

import polars as pl
from tqdm import tqdm

from polars_row_collector.polars_row_collector import PolarsRowCollector

TOTAL_ROWS = int(float(os.getenv("TOTAL_ROWS", "1e6")))
CHUNK_SIZE = 25_000
REPORT_EVERY = 250_000  # rows
DISABLE_GC = False  # flip to True to confirm GC effects


def row_generator() -> Generator[dict[str, int | float | str], Any, Never]:
    i = 0
    while True:
        yield {
            "a": i,
            "b": float(i),
            "c": str(i),
        }
        i += 1


def main():
    if DISABLE_GC:
        gc.disable()
        print("GC disabled")

    collector = PolarsRowCollector(
        schema={
            "a": pl.Int64,
            "b": pl.Float64,
            "c": pl.String,
        },
        collect_chunk_size=CHUNK_SIZE,
    )

    gen = row_generator()

    start = time.perf_counter()
    last_report = start
    last_rows = 0

    for i in tqdm(range(1, TOTAL_ROWS + 1), desc="Ingesting rows"):
        collector.add_row(next(gen))

        if i % REPORT_EVERY == 0:
            now = time.perf_counter()
            dt = now - last_report
            rows = i - last_rows

            print(
                f"\nRows: {i:,} | "  # pyright: ignore[reportImplicitStringConcatenation]
                f"{rows / dt:,.0f} rows/sec | "
                f"{(dt / rows) * 1e6:,.1f} µs/row"
            )

            last_report = now
            last_rows = i

    print("\nFinalizing DataFrame...")
    t0 = time.perf_counter()
    df = collector.to_df()
    t1 = time.perf_counter()

    print(f"Final concat time: {t1 - t0:.2f}s")
    print(df.shape)

    if DISABLE_GC:
        gc.enable()
        _ = gc.collect()


if __name__ == "__main__":
    main()
