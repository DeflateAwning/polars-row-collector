"""Unit tests for PolarsRowCollector with Enum and Categorical columns."""

from typing import TYPE_CHECKING

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from polars_row_collector import PolarsRowCollector

if TYPE_CHECKING:
    from polars._typing import SchemaDict


# ============================================================================
# Tests for Enum columns
# ============================================================================


def test_enum_column_basic() -> None:
    """Test basic usage with an Enum column."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "status": pl.Enum(["pending", "active", "completed"]),
    }
    c = PolarsRowCollector(schema=schema)

    c.add_row({"id": 1, "status": "pending"})
    c.add_row({"id": 2, "status": "active"})
    c.add_row({"id": 3, "status": "completed"})

    df = c.to_df()

    expected = pl.DataFrame(
        {"id": [1, 2, 3], "status": ["pending", "active", "completed"]},
        schema=schema,
    )

    assert_frame_equal(df, expected)
    assert df.schema["status"] == pl.Enum(["pending", "active", "completed"])


def test_enum_column_with_nulls() -> None:
    """Test Enum column with null values."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "priority": pl.Enum(["low", "medium", "high"]),
    }
    c = PolarsRowCollector(schema=schema, if_missing_columns="set_missing_to_null")

    c.add_row({"id": 1, "priority": "low"})
    c.add_row({"id": 2})  # missing priority, should be null
    c.add_row({"id": 3, "priority": "high"})

    df = c.to_df()

    expected = pl.DataFrame(
        {"id": [1, 2, 3], "priority": ["low", None, "high"]},
        schema=schema,
    )

    assert_frame_equal(df, expected)


@pytest.mark.parametrize("collect_chunk_size", [1, 2, 5])
def test_enum_column_invalid_value_raises(collect_chunk_size: int) -> None:
    """Test that providing a value not in the Enum raises an error."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "status": pl.Enum(["draft", "published"]),
    }
    c = PolarsRowCollector(
        schema=schema,
        collect_chunk_size=collect_chunk_size,
    )

    c.add_row({"id": 1, "status": "draft"})

    with pytest.raises(pl.exceptions.PolarsError):
        # Adding a value not in the enum should raise.
        # Can happen on `add_row` OR on the flush.
        c.add_row(
            {
                "id": 2,
                "status": "archived",  # "archived" value not in enum!
            }
        )
        _ = c.to_df()


def test_enum_column_across_multiple_chunks() -> None:
    """Test Enum column values across multiple chunk flushes."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "color": pl.Enum(["red", "green", "blue"]),
    }
    c = PolarsRowCollector(schema=schema, collect_chunk_size=3)

    # First chunk
    c.add_row({"id": 1, "color": "red"})
    c.add_row({"id": 2, "color": "green"})
    c.add_row({"id": 3, "color": "blue"})  # triggers flush

    # Second chunk (stays in accumulator)
    c.add_row({"id": 4, "color": "red"})
    c.add_row({"id": 5, "color": "blue"})

    df = c.to_df()

    expected = pl.DataFrame(
        {"id": [1, 2, 3, 4, 5], "color": ["red", "green", "blue", "red", "blue"]},
        schema=schema,
    )

    assert_frame_equal(df, expected)
    assert df.schema["color"] == pl.Enum(["red", "green", "blue"])


def test_enum_column_with_large_category_set() -> None:
    """Test Enum with a larger set of categories."""
    categories = [f"category_{i}" for i in range(50)]
    schema: SchemaDict = {
        "id": pl.Int64,
        "category": pl.Enum(categories),
    }
    c = PolarsRowCollector(schema=schema)

    # Add rows using various categories
    c.add_row({"id": 1, "category": "category_0"})
    c.add_row({"id": 2, "category": "category_25"})
    c.add_row({"id": 3, "category": "category_49"})
    c.add_row({"id": 4, "category": "category_10"})

    df = c.to_df()

    assert df.shape == (4, 2)
    assert df.schema["category"] == pl.Enum(categories)
    assert df["category"].to_list() == [
        "category_0",
        "category_25",
        "category_49",
        "category_10",
    ]


def test_enum_preserves_category_order() -> None:
    """Test that Enum preserves the specified category order."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "size": pl.Enum(["small", "medium", "large", "x-large"]),
    }
    c = PolarsRowCollector(schema=schema)

    # Add in random order
    c.add_row({"id": 1, "size": "large"})
    c.add_row({"id": 2, "size": "small"})
    c.add_row({"id": 3, "size": "x-large"})
    c.add_row({"id": 4, "size": "medium"})

    df = c.to_df()

    # The dtype should preserve the original category order
    assert df.schema["size"] == pl.Enum(["small", "medium", "large", "x-large"])


# ============================================================================
# Tests for Categorical columns
# ============================================================================


def test_categorical_column_basic() -> None:
    """Test basic usage with a Categorical column."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "department": pl.Categorical,
    }
    c = PolarsRowCollector(schema=schema)

    c.add_row({"id": 1, "department": "engineering"})
    c.add_row({"id": 2, "department": "sales"})
    c.add_row({"id": 3, "department": "engineering"})
    c.add_row({"id": 4, "department": "marketing"})

    df = c.to_df()

    expected = pl.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "department": ["engineering", "sales", "engineering", "marketing"],
        },
        schema=schema,
    )

    assert_frame_equal(df, expected)
    assert df.schema["department"] == pl.Categorical


def test_categorical_column_with_nulls() -> None:
    """Test Categorical column with null values."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "tag": pl.Categorical,
    }
    c = PolarsRowCollector(schema=schema, if_missing_columns="set_missing_to_null")

    c.add_row({"id": 1, "tag": "important"})
    c.add_row({"id": 2})  # missing tag, should be null
    c.add_row({"id": 3, "tag": "urgent"})
    c.add_row({"id": 4})  # missing tag

    df = c.to_df()

    expected = pl.DataFrame(
        {"id": [1, 2, 3, 4], "tag": ["important", None, "urgent", None]},
        schema=schema,
    )

    assert_frame_equal(df, expected)


def test_categorical_accepts_any_string_value() -> None:
    """Test that Categorical (unlike Enum) accepts any string value."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "label": pl.Categorical,
    }
    c = PolarsRowCollector(schema=schema)

    # Categorical should accept any string without predefined categories
    c.add_row({"id": 1, "label": "alpha"})
    c.add_row({"id": 2, "label": "beta"})
    c.add_row({"id": 3, "label": "gamma"})
    c.add_row({"id": 4, "label": "delta"})
    c.add_row({"id": 5, "label": "alpha"})  # repeat is fine

    df = c.to_df()

    assert df.shape == (5, 2)
    assert df.schema["label"] == pl.Categorical
    assert df["label"].to_list() == ["alpha", "beta", "gamma", "delta", "alpha"]


def test_categorical_column_across_multiple_chunks() -> None:
    """Test Categorical column values across multiple chunk flushes."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "region": pl.Categorical,
    }
    c = PolarsRowCollector(schema=schema, collect_chunk_size=3)

    # First chunk
    c.add_row({"id": 1, "region": "north"})
    c.add_row({"id": 2, "region": "south"})
    c.add_row({"id": 3, "region": "east"})  # triggers flush

    # Second chunk
    c.add_row({"id": 4, "region": "west"})
    c.add_row({"id": 5, "region": "north"})

    df = c.to_df()

    expected = pl.DataFrame(
        {"id": [1, 2, 3, 4, 5], "region": ["north", "south", "east", "west", "north"]},
        schema=schema,
    )

    assert_frame_equal(df, expected)
    assert df.schema["region"] == pl.Categorical


def test_categorical_with_many_unique_values() -> None:
    """Test Categorical column with many unique values."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "user": pl.Categorical,
    }
    c = PolarsRowCollector(schema=schema)

    # Add 100 rows with unique user names
    for i in range(100):
        c.add_row({"id": i, "user": f"user_{i}"})

    df = c.to_df()

    assert df.shape == (100, 2)
    assert df.schema["user"] == pl.Categorical
    assert df["user"].n_unique() == 100


def test_categorical_with_repeated_values() -> None:
    """Test Categorical efficiency with many repeated values."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "country": pl.Categorical,
    }
    c = PolarsRowCollector(schema=schema)

    # Add many rows but only a few unique countries
    countries = ["USA", "Canada", "Mexico", "Brazil"]
    for i in range(1000):
        c.add_row({"id": i, "country": countries[i % len(countries)]})

    df = c.to_df()

    assert df.shape == (1000, 2)
    assert df.schema["country"] == pl.Categorical
    assert df["country"].n_unique() == 4


# ============================================================================
# Tests combining Enum and Categorical
# ============================================================================


def test_both_enum_and_categorical_columns() -> None:
    """Test schema with both Enum and Categorical columns."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "status": pl.Enum(["pending", "approved", "rejected"]),
        "department": pl.Categorical,
    }
    c = PolarsRowCollector(schema=schema)

    c.add_row({"id": 1, "status": "pending", "department": "engineering"})
    c.add_row({"id": 2, "status": "approved", "department": "sales"})
    c.add_row({"id": 3, "status": "rejected", "department": "engineering"})

    df = c.to_df()

    expected = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "status": ["pending", "approved", "rejected"],
            "department": ["engineering", "sales", "engineering"],
        },
        schema=schema,
    )

    assert_frame_equal(df, expected)
    assert df.schema["status"] == pl.Enum(["pending", "approved", "rejected"])
    assert df.schema["department"] == pl.Categorical


def test_multiple_enum_columns() -> None:
    """Test schema with multiple Enum columns."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "priority": pl.Enum(["low", "medium", "high"]),
        "status": pl.Enum(["open", "closed"]),
    }
    c = PolarsRowCollector(schema=schema)

    c.add_row({"id": 1, "priority": "high", "status": "open"})
    c.add_row({"id": 2, "priority": "low", "status": "closed"})
    c.add_row({"id": 3, "priority": "medium", "status": "open"})

    df = c.to_df()

    assert df.shape == (3, 3)
    assert df.schema["priority"] == pl.Enum(["low", "medium", "high"])
    assert df.schema["status"] == pl.Enum(["open", "closed"])


def test_multiple_categorical_columns() -> None:
    """Test schema with multiple Categorical columns."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "city": pl.Categorical,
        "country": pl.Categorical,
    }
    c = PolarsRowCollector(schema=schema)

    c.add_row({"id": 1, "city": "New York", "country": "USA"})
    c.add_row({"id": 2, "city": "Toronto", "country": "Canada"})
    c.add_row({"id": 3, "city": "Los Angeles", "country": "USA"})

    df = c.to_df()

    assert df.shape == (3, 3)
    assert df.schema["city"] == pl.Categorical
    assert df.schema["country"] == pl.Categorical


# ============================================================================
# Edge cases and special scenarios
# ============================================================================


def test_enum_with_empty_string() -> None:
    """Test Enum that includes an empty string as a valid category."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "flag": pl.Enum(["", "yes", "no"]),
    }
    c = PolarsRowCollector(schema=schema)

    c.add_row({"id": 1, "flag": ""})
    c.add_row({"id": 2, "flag": "yes"})
    c.add_row({"id": 3, "flag": "no"})
    c.add_row({"id": 4, "flag": ""})

    df = c.to_df()

    expected = pl.DataFrame(
        {"id": [1, 2, 3, 4], "flag": ["", "yes", "no", ""]},
        schema=schema,
    )

    assert_frame_equal(df, expected)


def test_categorical_with_empty_string() -> None:
    """Test Categorical with empty string values."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "note": pl.Categorical,
    }
    c = PolarsRowCollector(schema=schema)

    c.add_row({"id": 1, "note": ""})
    c.add_row({"id": 2, "note": "important"})
    c.add_row({"id": 3, "note": ""})

    df = c.to_df()

    expected = pl.DataFrame(
        {"id": [1, 2, 3], "note": ["", "important", ""]},
        schema=schema,
    )

    assert_frame_equal(df, expected)


def test_enum_with_special_characters() -> None:
    """Test Enum categories with special characters."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "type": pl.Enum(["user@admin", "user-regular", "guest/temp"]),
    }
    c = PolarsRowCollector(schema=schema)

    c.add_row({"id": 1, "type": "user@admin"})
    c.add_row({"id": 2, "type": "guest/temp"})
    c.add_row({"id": 3, "type": "user-regular"})

    df = c.to_df()

    assert df.shape == (3, 2)
    assert df["type"].to_list() == ["user@admin", "guest/temp", "user-regular"]


def test_categorical_with_special_characters() -> None:
    """Test Categorical with special character values."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "path": pl.Categorical,
    }
    c = PolarsRowCollector(schema=schema)

    c.add_row({"id": 1, "path": "/home/user"})
    c.add_row({"id": 2, "path": "C:\\Windows\\System32"})
    c.add_row({"id": 3, "path": "/var/log/app.log"})

    df = c.to_df()

    assert df.shape == (3, 2)
    assert df.schema["path"] == pl.Categorical


def test_enum_with_unicode_characters() -> None:
    """Test Enum with Unicode characters."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "language": pl.Enum(["English", "Español", "中文", "العربية"]),
    }
    c = PolarsRowCollector(schema=schema)

    c.add_row({"id": 1, "language": "English"})
    c.add_row({"id": 2, "language": "中文"})
    c.add_row({"id": 3, "language": "Español"})
    c.add_row({"id": 4, "language": "العربية"})

    df = c.to_df()

    assert df.shape == (4, 2)
    assert df["language"].to_list() == ["English", "中文", "Español", "العربية"]


def test_categorical_with_unicode_characters() -> None:
    """Test Categorical with Unicode values."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "emoji": pl.Categorical,
    }
    c = PolarsRowCollector(schema=schema)

    c.add_row({"id": 1, "emoji": "😀"})
    c.add_row({"id": 2, "emoji": "🎉"})
    c.add_row({"id": 3, "emoji": "❤️"})

    df = c.to_df()

    assert df.shape == (3, 2)
    assert df["emoji"].to_list() == ["😀", "🎉", "❤️"]


def test_enum_single_category() -> None:
    """Test Enum with only a single category."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "constant": pl.Enum(["always_this"]),
    }
    c = PolarsRowCollector(schema=schema)

    c.add_row({"id": 1, "constant": "always_this"})
    c.add_row({"id": 2, "constant": "always_this"})
    c.add_row({"id": 3, "constant": "always_this"})

    df = c.to_df()

    assert df.shape == (3, 2)
    assert all(df["constant"] == "always_this")


def test_enum_and_categorical_with_lazyframe() -> None:
    """Test that Enum and Categorical work correctly with to_lazyframe."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "status": pl.Enum(["active", "inactive"]),
        "tag": pl.Categorical,
    }
    c = PolarsRowCollector(schema=schema)

    c.add_row({"id": 1, "status": "active", "tag": "important"})
    c.add_row({"id": 2, "status": "inactive", "tag": "normal"})

    lf = c.to_lazyframe()
    df = lf.collect()

    expected = pl.DataFrame(
        {
            "id": [1, 2],
            "status": ["active", "inactive"],
            "tag": ["important", "normal"],
        },
        schema=schema,
    )

    assert_frame_equal(df, expected)
    assert df.schema["status"] == pl.Enum(["active", "inactive"])
    assert df.schema["tag"] == pl.Categorical


def test_enum_case_sensitivity() -> None:
    """Test that Enum categories are case-sensitive."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "status": pl.Enum(["Active", "active", "ACTIVE"]),
    }
    c = PolarsRowCollector(schema=schema, collect_chunk_size=5)

    c.add_row({"id": 1, "status": "Active"})
    c.add_row({"id": 2, "status": "active"})
    c.add_row({"id": 3, "status": "ACTIVE"})

    df = c.to_df()

    assert df.shape == (3, 2)
    # Each case variant is treated as a distinct category
    assert df["status"].to_list() == ["Active", "active", "ACTIVE"]


def test_categorical_case_sensitivity() -> None:
    """Test that Categorical values are case-sensitive."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "name": pl.Categorical,
    }
    c = PolarsRowCollector(schema=schema)

    c.add_row({"id": 1, "name": "Alice"})
    c.add_row({"id": 2, "name": "alice"})
    c.add_row({"id": 3, "name": "ALICE"})

    df = c.to_df()

    assert df.shape == (3, 2)
    assert df["name"].n_unique() == 3  # All three are distinct


# ============================================================================
# Performance and stress tests
# ============================================================================


@pytest.mark.parametrize("chunk_size", [1, 100, 10_000])
@pytest.mark.parametrize("num_rows", [100, 5_000])
def test_enum_performance_with_various_chunk_sizes(
    chunk_size: int, num_rows: int
) -> None:
    """Test Enum column performance across different chunk sizes."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "status": pl.Enum(["pending", "processing", "complete", "failed"]),
    }
    c = PolarsRowCollector(schema=schema, collect_chunk_size=chunk_size)

    statuses = ["pending", "processing", "complete", "failed"]
    for i in range(num_rows):
        c.add_row({"id": i, "status": statuses[i % len(statuses)]})

    df = c.to_df()

    assert df.shape == (num_rows, 2)
    assert df.schema["status"] == pl.Enum(statuses)


@pytest.mark.parametrize("chunk_size", [1, 100, 10_000])
@pytest.mark.parametrize("num_rows", [100, 5_000])
def test_categorical_performance_with_various_chunk_sizes(
    chunk_size: int, num_rows: int
) -> None:
    """Test Categorical column performance across different chunk sizes."""
    schema: SchemaDict = {
        "id": pl.Int64,
        "category": pl.Categorical,
    }
    c = PolarsRowCollector(schema=schema, collect_chunk_size=chunk_size)

    categories = [f"cat_{i % 20}" for i in range(num_rows)]
    for i in range(num_rows):
        c.add_row({"id": i, "category": categories[i]})

    df = c.to_df()

    assert df.shape == (num_rows, 2)
    assert df.schema["category"] == pl.Categorical
