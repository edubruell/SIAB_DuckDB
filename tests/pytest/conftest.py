"""
Shared helpers for the Python arm's tests, the counterpart of
tests/testthat/helper-siab.R.

Two kinds of test live here. A **synthetic** test builds a handful of rows in
memory, runs one step over them and compares against hand-computed values; it
always runs and needs no external data. A **reference** test compares the
Python arm's dump against the committed Stata fixture and skips when either
half is absent.

The comparison SQL is deliberately the same SQL the R arm's helper runs, down
to the predicate. The oracle is the Stata fixture and it is shared; only the
dump being tested differs. Writing the comparison a second way would mean two
arms answering to two slightly different questions.

Folders come from environment variables with fallbacks:

  SIAB_FIXTURES  the committed Stata fixtures, default tests/testthat/fixtures
  SIAB_PY_DUMP   the Python dumps that make_py_dumps.py writes. The default
                 sits in the untracked working folder this project keeps its
                 test database in, so a run from the project root sets it
                 for neither script or for both.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Callable, Mapping, Sequence

import duckdb
import polars as pl
import pytest

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent.parent
sys.path.insert(0, str(PROJECT / "python"))


def fixtures_dir() -> Path:
    return Path(os.environ.get("SIAB_FIXTURES", PROJECT / "tests" / "testthat" / "fixtures"))


def dump_dir() -> Path:
    return Path(os.environ.get("SIAB_PY_DUMP", PROJECT / "local_context" / "testdb" / "py_dump"))


def siab_fixture(name: str) -> pl.DataFrame:
    """Read one committed Stata fixture, skipping the test when it is absent."""
    path = fixtures_dir() / f"{name}.parquet"
    if not path.exists():
        pytest.skip(f"No committed fixture at {path}")
    return pl.read_parquet(path)


@pytest.fixture
def reference_query() -> Callable[[str], Callable[[str], pl.DataFrame]]:
    """Hand back a factory that opens the two halves of one step's comparison.

    The returned callable takes a step name, creates a `stata` view over the
    committed fixture and a `py` view over this arm's dump, and gives back a
    function that runs SQL against both.
    """

    connections: list[duckdb.DuckDBPyConnection] = []

    def for_step(step: str) -> Callable[[str], pl.DataFrame]:
        stata_path = fixtures_dir() / f"{step}.parquet"
        py_path = dump_dir() / f"{step}.parquet"
        missing = [str(p) for p in (stata_path, py_path) if not p.exists()]
        if missing:
            pytest.skip("Missing half of the comparison: " + ", ".join(missing))

        con = duckdb.connect()
        connections.append(con)
        con.execute(f"CREATE VIEW stata AS SELECT * FROM read_parquet('{stata_path}')")
        con.execute(f"CREATE VIEW py AS SELECT * FROM read_parquet('{py_path}')")

        def query(sql: str) -> pl.DataFrame:
            return con.execute(sql).pl()

        return query

    yield for_step

    for con in connections:
        con.close()


def siab_key_on(key: Sequence[str] | Mapping[str, str]) -> str:
    """The join condition, allowing a different column name on each side."""
    if isinstance(key, Mapping):
        pairs = key.items()
    else:
        pairs = ((column, column) for column in key)
    return " AND ".join(f"stata.{left} = py.{right}" for left, right in pairs)


def siab_column_diff(query: Callable[[str], pl.DataFrame],
                     column: str,
                     py_column: str | None = None,
                     tolerance: float | None = None,
                     key: Sequence[str] | Mapping[str, str] = ("persnr", "spell", "begepi")
                     ) -> pl.DataFrame:
    """Count the shared rows and the rows where one column disagrees.

    With no tolerance the test is exact, through `IS DISTINCT FROM`, so a null
    on one side and a value on the other counts as a difference. With a
    tolerance the comparison is relative, and a null still has to line up with
    a null.
    """
    py_column = py_column or column

    if tolerance is None:
        predicate = f"stata.{column} IS DISTINCT FROM py.{py_column}"
    else:
        predicate = (
            f"(stata.{column} IS NULL) <> (py.{py_column} IS NULL) OR "
            f"abs(stata.{column} - py.{py_column}) > {tolerance:.10f} * "
            f"greatest(abs(stata.{column}), 1e-12)"
        )

    return query(
        "SELECT count(*) AS shared, "
        f"       count(*) FILTER (WHERE {predicate}) AS differing "
        f"FROM stata JOIN py ON {siab_key_on(key)}"
    )


def assert_column_matches(query, column, py_column=None, tolerance=None,
                          key=("persnr", "spell", "begepi")) -> None:
    """The assertion every reference test makes: rows shared, none differing."""
    result = siab_column_diff(query, column, py_column, tolerance, key)
    shared = result["shared"][0]
    differing = result["differing"][0]
    assert shared > 0, f"{column}: the two halves share no rows at all"
    assert differing == 0, (
        f"{column}: {differing} of {shared} shared rows differ between the "
        f"Stata fixture and the Python dump"
    )


def siab_column_moments(query: Callable[[str], pl.DataFrame],
                        column: str,
                        py_column: str | None = None,
                        key: Sequence[str] | Mapping[str, str] = ("persnr", "spell", "begepi")
                        ) -> pl.DataFrame:
    """The mean and the two outer quartiles of one column on each side.

    What a column carrying a random draw can be compared on. Every imputed
    wage on either side gets its random term from that side's own generator,
    so a column built from one cannot agree row by row; the shape of its
    distribution still has to. The counterpart of the R helper's
    siab_column_moments().
    """
    py_column = py_column or column

    return query(
        "SELECT count(*) AS shared, "
        f"       avg(stata.{column}) AS stata_mean, "
        f"       avg(py.{py_column}) AS py_mean, "
        f"       quantile_cont(stata.{column}, 0.25) AS stata_q25, "
        f"       quantile_cont(py.{py_column}, 0.25) AS py_q25, "
        f"       quantile_cont(stata.{column}, 0.75) AS stata_q75, "
        f"       quantile_cont(py.{py_column}, 0.75) AS py_q75 "
        f"FROM stata JOIN py ON {siab_key_on(key)}"
    )
