"""Tests of the Polars library itself."""

import polars as pl
import pytest


@pytest.mark.skip("Bug reported in: https://github.com/pola-rs/polars/issues/26282")
def test_pl_from_dicts_fails_if_invalid_enum_value() -> None:
    """Bug reported in https://github.com/pola-rs/polars/issues/26282.

    Affects the necessity of `_convert_precise_type_to_python_parse_type()` function.
    """
    with pytest.raises(pl.exceptions.SchemaError):
        _ = pl.from_dicts(
            [
                {"id": 10, "status": "draft"},
                {"id": 10, "status": "archived"},
            ],
            schema={"id": pl.Int64, "status": pl.Enum(["draft", "published"])},
            # Implicitly, strict=True
        )
