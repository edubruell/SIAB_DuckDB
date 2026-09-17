"""
Common helpers for the Python arm.

The counterpart of R/functions/00_common_functions.R. Everything a step needs
that is not the step itself lives here: the folder helper, Stata's float
precision, Stata's ordering of missing values, the logger, and the two
functions that move a table between DuckDB and polars.

DuckDB owns the table between steps, exactly as it does in the R arm. A step
reads the `data` table, builds its work in polars expressions and writes the
table back. `read_table()` and `write_table()` are that boundary, and they are
the only two places a LazyFrame is collected. Keeping the boundary in one pair
of functions is deliberate: swapping the in-memory handover for a Parquet
round-trip, which would make polars stream as well, is a change to these two
functions and to nothing else.

Author(s): Eduard Brüll
Python/polars reimplementation of the original procedures by Heiko Stüber,
Wolfgang Dauth and Johann Eppelsheimer

Version: 1.0
Created: 2026-09-17
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Callable, Iterable

import duckdb
import numpy as np
import polars as pl
import pyreadstat

__all__ = [
    "folder_reference_factory",
    "classifications_dir",
    "stata_float",
    "pl_stata_float",
    "pl_stata_gt",
    "pl_days",
    "step_logger",
    "read_table",
    "write_table",
    "read_stata",
    "assert_empty",
]


# ====================================================================
# 0. General functions
# ====================================================================


def folder_reference_factory(target_folder: str | os.PathLike) -> Callable[..., Path]:
    """Make a folder reference function, as `folder_reference_factory` does in R.

    The returned function joins its arguments onto `target_folder`, and hands
    back an absolute path unchanged, so a caller can pass either.
    """
    if target_folder is None:
        raise ValueError("Please set the path to a target_folder")
    base = Path(target_folder)

    def reference(*parts: str | os.PathLike) -> Path:
        if not parts:
            return base
        first = Path(parts[0])
        if first.is_absolute() or str(parts[0]).startswith("~"):
            return Path(os.path.expanduser(str(first))).joinpath(*parts[1:])
        return base.joinpath(*parts)

    return reference


def classifications_dir() -> Path:
    """The folder holding the shared classification csv files.

    The counterpart of the R arm's `here("classifications", ...)`. The tables
    are language-neutral data and both arms read the same ones, so the folder
    sits above both. `SIAB_CLASSIFICATIONS` overrides it for a run that keeps
    its inputs somewhere else.
    """
    fallback = Path(__file__).resolve().parents[2] / "classifications"
    return Path(os.environ.get("SIAB_CLASSIFICATIONS", fallback))


def stata_float(x):
    """Round a Python or numpy value to Stata's float precision.

    The reference prep generates its lookup values with plain `gen`, which gives
    a Stata float: four bytes, about seven decimal digits. Values read from a
    csv carry more digits than the reference ever had, and those extra digits
    change a comparison for equality.
    """
    return np.float64(np.float32(x))


def pl_stata_float(expr: pl.Expr) -> pl.Expr:
    """Round an expression to Stata's float precision inside the query.

    The database companion of `stata_float()`, for a value the pipeline
    generates rather than one read from a csv. `gen` and `egen` make a float
    unless told otherwise, so a reference step that generates an intermediate
    has already dropped everything past about the seventh digit before it uses
    it. Where such an intermediate feeds a rounding, as in 02_grund154.do,
    carrying the full double through moves the result by a cent.
    """
    return expr.cast(pl.Float32).cast(pl.Float64)


def pl_stata_gt(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Stata's greater-than, with its ordering of missing.

    Stata stores a missing value as a number larger than any other, so `a > b`
    is true when a is missing and b is not, false when b is missing, and false
    when both are. polars propagates null instead, which turns the first two
    cases into a null result. Every port of a `replace ... if x > y` guard has
    to go through this, because the reference relies on the ordering.
    """
    return (
        pl.when(left.is_null() & right.is_null())
        .then(pl.lit(False))
        .when(left.is_null())
        .then(pl.lit(True))
        .when(right.is_null())
        .then(pl.lit(False))
        .otherwise(left > right)
    )


def pl_days(end: pl.Expr, start: pl.Expr) -> pl.Expr:
    """Whole days between two date columns, as Stata's `end - start` gives them.

    Stata stores a date as a day count, so a difference is an integer. polars
    subtracts two Date columns into a Duration, which has to be asked for its
    days before it is arithmetic again.
    """
    return (end - start).dt.total_days().cast(pl.Int32)


def step_logger(name: str, log_file: str | os.PathLike | None = None) -> logging.Logger:
    """A per-step logger that writes to the console and, optionally, to a file.

    The counterpart of the `log_appender()` block every R step opens with. An
    old log file is removed rather than appended to, as the R steps do.
    """
    logger = logging.getLogger(f"siab.{name}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter("%(levelname)s [%(asctime)s] %(message)s",
                                  datefmt="%Y-%m-%d %H:%M:%S")
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    if log_file is not None:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()
        file_handler = logging.FileHandler(path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# ====================================================================
# 1. Database related functions
# ====================================================================


def read_table(connection: duckdb.DuckDBPyConnection, table: str = "data") -> pl.LazyFrame:
    """Hand the DuckDB table to polars as a LazyFrame.

    This is one of the two collection points in the arm. Everything downstream
    of it stays lazy until `write_table()`.
    """
    return connection.sql(f"SELECT * FROM {table}").pl().lazy()


def write_table(connection: duckdb.DuckDBPyConnection,
                frame: pl.LazyFrame,
                table: str = "data") -> duckdb.DuckDBPyConnection:
    """Collect the frame and put it back into DuckDB under `table`.

    The counterpart of `compute_and_overwrite()`. The connection is returned so
    a caller can chain steps, which is what the R arm's pipe does.
    """
    materialised = frame.collect() if isinstance(frame, pl.LazyFrame) else frame
    connection.register("_siab_write", materialised)
    connection.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM _siab_write")
    connection.unregister("_siab_write")
    return connection


def read_stata(path: str | os.PathLike,
               columns: Iterable[str] | None = None,
               rename: dict[str, str] | None = None) -> pl.LazyFrame:
    """Read a Stata file into a polars LazyFrame.

    The counterpart of the R arm's `read.dta13()` calls. Every file the prep
    merges in arrives as a .dta, and polars reads no such thing, so pyreadstat
    reads it into pandas and polars takes it from there. Reading a named subset
    of the columns matters on the yearly establishment panel, where the files
    are wide and the merge wants three columns out of sixty.

    `rename` maps the delivery's names onto the prepared SIAB's: the files key
    on `betnr_siab` and `jahr`, and the pipeline uses `betnr` and `year`.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Stata file not found: {path}")

    usecols = list(columns) if columns is not None else None
    pandas_frame, _ = pyreadstat.read_dta(str(path), usecols=usecols)
    frame = pl.from_pandas(pandas_frame)
    if rename:
        frame = frame.rename({k: v for k, v in rename.items() if k in frame.columns})
    return frame.lazy()


def assert_empty(frame: pl.LazyFrame, predicate: pl.Expr, message: str) -> int:
    """Count the rows a predicate catches and raise when there are any.

    The reference's `assert` statements become this: the count is reported in
    the error, because a bare failure says nothing about how far off the port is.
    """
    offending = frame.filter(predicate).select(pl.len()).collect().item()
    if offending > 0:
        raise ValueError(f"{message}: {offending} rows")
    return offending
