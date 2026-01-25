# Development

## Setting Up uv

This project is set up to use [uv](https://docs.astral.sh/uv/) to manage Python and
dependencies. First, be sure you
[have uv installed](https://docs.astral.sh/uv/getting-started/installation/).

Then
[fork the DeflateAwning/polars-row-collector repo](https://github.com/DeflateAwning/polars-row-collector/fork)
(having your own fork will make it easier to contribute) and
[clone it](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository).

## Basic Developer Workflows

The `Justfile` simply offers shortcuts to `uv` commands for developer convenience.
(For clarity, GitHub Actions don’t use the Makefile and just call `uv` directly.)

See [uv docs](https://docs.astral.sh/uv/) for details.

## IDE setup

If you use VSCode or a fork like Cursor or Windsurf, you can install the following
extensions:

- [Python](https://marketplace.visualstudio.com/items?itemName=ms-python.python)

- [Based Pyright](https://marketplace.visualstudio.com/items?itemName=detachhead.basedpyright)
  for type checking. Note that this extension works with non-Microsoft VSCode forks like
  Cursor.

## Publishing Releases

See [publishing.md](publishing.md) for instructions on publishing to PyPI.

## Documentation

- [uv docs](https://docs.astral.sh/uv/)

- [basedpyright docs](https://docs.basedpyright.com/latest/)

* * *

*This file was built with
[simple-modern-uv](https://github.com/jlevy/simple-modern-uv).*
