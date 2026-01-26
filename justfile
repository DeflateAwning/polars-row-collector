# Justfile for easy development workflows.
# Note GitHub Actions call uv directly, not this Justfile.

# Show all just options.
default:
    just --list

# Setup uv environment.
install:
    uv sync --all-extras

lint:
    uv run ruff check --fix .
    uv run ruff format .
    uv run basedpyright .

test:
    uv run pytest

upgrade:
    uv sync --upgrade --all-extras --dev

build:
    uv build
