import gc
import os
import time
from collections.abc import Generator
from typing import Any, Literal, Never, assert_never

import polars as pl
from tqdm import tqdm

from polars_row_collector.polars_row_collector import PolarsRowCollector

TOTAL_ROWS = int(float(os.getenv("TOTAL_ROWS", "5e7")))
CHUNK_SIZE = int(float(os.getenv("CHUNK_SIZE", "25e3")))
REPORT_EVERY = int(float(os.getenv("REPORT_EVERY", "1e6")))
DISABLE_GC: bool | int = int(os.getenv("DISABLE_GC", "0"))
ENABLE_TQDM: bool | int = int(os.getenv("ENABLE_TQDM", "0"))

# Configure comparing against the list-of-dicts style.
# prc = PolarsRowCollector (this library)
# dicts = list[dict[str, Any]] (baseline)
COLLECT_MODE: str = os.getenv("COLLECT_MODE", "prc")  # "dicts" or "prc"


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
    else:
        print("GC left enabled")

    if COLLECT_MODE.lower() == "dicts":
        list_of_dicts: list[dict[str, Any]] = []
        collect_mode: Literal["dicts", "prc"] = "dicts"
    elif COLLECT_MODE.lower() == "prc":
        collector = PolarsRowCollector(
            schema={
                "a": pl.Int64,
                "b": pl.Float64,
                "c": pl.String,
            },
            collect_chunk_size=CHUNK_SIZE,
        )
        collect_mode = "prc"
    else:
        raise ValueError(f"Invalid COLLECT_MODE: {COLLECT_MODE}.")

    gen = row_generator()

    # Fully-avoid tqdm overhead when disabled.
    if ENABLE_TQDM:
        iterator = tqdm(range(1, TOTAL_ROWS + 1), desc="Ingesting rows")
    else:
        iterator = range(1, TOTAL_ROWS + 1)

    start = time.perf_counter()
    last_report = start
    last_rows = 0

    for i in iterator:
        if collect_mode == "prc":
            collector.add_row(next(gen))  # pyright: ignore[reportPossiblyUnboundVariable]
        elif collect_mode == "dicts":
            list_of_dicts.append(next(gen))  # pyright: ignore[reportPossiblyUnboundVariable]
        else:
            assert_never(collect_mode)

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
    if collect_mode == "prc":
        df = collector.to_df()  # pyright: ignore[reportPossiblyUnboundVariable]
    elif collect_mode == "dicts":
        df = pl.DataFrame(list_of_dicts)  # pyright: ignore[reportPossiblyUnboundVariable]
    else:
        assert_never(collect_mode)

    t1 = time.perf_counter()

    print(f"Final concat time: {t1 - t0:.2f}s")
    print(f"df.shape: {df.shape}")
    print(f"Final overall time: {t1 - start:.2f}s")

    print(f"Final overall time per row: {1e6 * (t1 - start) / TOTAL_ROWS:.6f}µs")

    if DISABLE_GC:
        gc.enable()
        _ = gc.collect()


if __name__ == "__main__":
    main()
