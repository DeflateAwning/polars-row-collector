import polars as pl

from polars_row_collector.polars_row_collector import (
    _convert_precise_type_to_python_parse_type,  # pyright: ignore[reportPrivateUsage]
)


def test_convert_precise_type_to_python_parse_type() -> None:
    assert _convert_precise_type_to_python_parse_type(pl.Float64) == pl.Float64
    assert _convert_precise_type_to_python_parse_type(pl.Float64()) == pl.Float64

    assert _convert_precise_type_to_python_parse_type(pl.Float32) == pl.Float64
    assert _convert_precise_type_to_python_parse_type(pl.Float32()) == pl.Float64

    assert _convert_precise_type_to_python_parse_type(pl.UInt16) == pl.Int64
    assert _convert_precise_type_to_python_parse_type(pl.UInt16()) == pl.Int64

    assert (
        _convert_precise_type_to_python_parse_type(pl.Enum(["cat1", "cat2"]))
        == pl.String
    )
