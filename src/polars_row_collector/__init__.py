"""Facade to collect rows one-by-one into a Polars DataFrame (in the least-bad way)."""

from .polars_row_collector import main

__all__ = ("main",)
