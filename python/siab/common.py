"""
Common helpers for the Python arm.

The counterpart of R/functions/00_common_functions.R. Everything a step needs
that is not the step itself lives here: the folder helper, Stata's float
precision, Stata's ordering of missing values, the logger, and the functions
that move a table between the store and polars.

The steps compute in polars and never see the store. Something has to own the
table between steps, and two things can: a DuckDB database, as in the R arm, or
a folder of Parquet files. `open_store()` picks by the name of the target, and
`read_table()` and `write_table()` work on either. Neither holds the table in
memory. A Parquet store scans the table's own file and sinks the result beside
it. A DuckDB store hands over through a Parquet file, written by DuckDB's own
writer on the way out and streamed by polars on the way in, with
`boundary_dir()` saying where those files go. Peak memory is therefore whatever
one step's own plan needs, not the size of the dataset.

Author(s): Eduard Brüll
Python/polars reimplementation of the original procedures by Heiko Stüber,
Wolfgang Dauth and Johann Eppelsheimer

Version: 1.0
Created: 2026-09-17
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Callable, Iterable

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
    "ParquetStore",
    "open_store",
    "store_description",
    "table_names",
    "drop_table",
    "count_rows",
    "database_path",
    "boundary_dir",
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
# 1. Storage: a DuckDB database or a folder of Parquet files
# ====================================================================
#
# The steps compute in polars and never see the store. All the store has to do
# is hand a table over as a LazyFrame and take one back, which a DuckDB
# database and a folder of Parquet files can both do. `open_store()` picks by
# the target's name, and everything below works on either.
#
# DuckDB is imported where it is used rather than at the top of the module, so
# the Parquet store runs in an environment that has no DuckDB in it.


def _duckdb():
    """Import DuckDB at the point of use, with a message worth reading."""
    try:
        import duckdb
    except ModuleNotFoundError as error:  # pragma: no cover - environment
        raise ModuleNotFoundError(
            "DuckDB is needed for a .duckdb store. Install it, or point the "
            "pipeline at a folder to keep the tables as Parquet files instead."
        ) from error
    return duckdb


class ParquetStore:
    """A folder of Parquet files, one per table, used in place of a database.

    The Python arm's steps are polars from end to end, so nothing in the prep
    needs a database engine: a table is a file, `data.parquet` beside
    `orig.parquet`, and a step scans one and sinks the other. What DuckDB adds
    over this is SQL over the result and one file that both arms of the project
    can open, which is worth having and is not required to run the prep.
    """

    suffix = ".parquet"

    def __init__(self, folder: str | os.PathLike):
        self.folder = Path(folder)
        self.folder.mkdir(parents=True, exist_ok=True)

    def path(self, table: str = "data") -> Path:
        return self.folder / f"{table}{self.suffix}"

    def tables(self) -> list[str]:
        # A `.pending.parquet` file is a write that did not finish. It is not a
        # table and must not be read as one.
        return sorted(path.stem for path in self.folder.glob(f"*{self.suffix}")
                      if not path.stem.endswith(".pending"))

    def drop(self, table: str) -> None:
        self.path(table).unlink(missing_ok=True)

    def close(self) -> None:
        """Nothing to close. Here so a caller can treat both stores alike."""

    def __repr__(self) -> str:
        return f"ParquetStore({str(self.folder)!r})"


def open_store(target: str | os.PathLike):
    """Open the store the pipeline should work in, chosen by the target's name.

    A name ending in `.duckdb`, `.db` or `.ddb` is a DuckDB database and comes
    back as a connection. Anything else is a folder and comes back as a
    `ParquetStore`. The folder is created if it is not there, so a first run
    needs no setup beyond naming a place.
    """
    target = Path(target)
    if target.suffix.lower() in (".duckdb", ".db", ".ddb"):
        target.parent.mkdir(parents=True, exist_ok=True)
        return _duckdb().connect(str(target))
    return ParquetStore(target)


def store_description(store) -> str:
    """One line naming the store, for a log line or an error message."""
    if isinstance(store, ParquetStore):
        return f"the Parquet folder {store.folder}"
    database = database_path(store)
    return f"the DuckDB database {database}" if database else "an in-memory DuckDB database"


def table_names(store) -> list[str]:
    """Every table the store holds."""
    if isinstance(store, ParquetStore):
        return store.tables()
    return [row[0] for row in store.execute("SHOW TABLES").fetchall()]


def drop_table(store, table: str) -> None:
    """Remove one table from the store."""
    if isinstance(store, ParquetStore):
        store.drop(table)
        return
    store.execute(f"DROP TABLE IF EXISTS {table}")


def count_rows(store, table: str = "data") -> int:
    """How many rows the table holds, without reading it into memory."""
    if isinstance(store, ParquetStore):
        return int(pl.scan_parquet(store.path(table)).select(pl.len()).collect().item())
    return int(store.execute(f"SELECT count(*) FROM {table}").fetchone()[0])


def database_path(connection) -> Path | None:
    """Where the connection's own database file sits, or None in memory."""
    row = connection.execute(
        "SELECT path FROM duckdb_databases() WHERE database_name = current_database()"
    ).fetchone()
    if row is None or not row[0]:
        return None
    return Path(row[0])


def boundary_dir(connection) -> Path:
    """The folder the Parquet handover files are written to.

    Beside the database by default, because that is a disk already known to
    hold a dataset of this size, and the files are the dataset. `SIAB_SPILL`
    moves them somewhere else, which is what a database on a small volume or a
    read-only mount needs. An in-memory connection has no folder of its own and
    falls back to the system temporary directory.

    A `ParquetStore` has no handover files at all: its tables are already the
    files the steps read and write.
    """
    override = os.environ.get("SIAB_SPILL")
    if override:
        folder = Path(override)
    else:
        database = database_path(connection)
        folder = (database.parent if database is not None
                  else Path(tempfile.gettempdir())) / "siab_boundary"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def _handover(connection, table: str, direction: str) -> Path:
    """The Parquet file one side of one table's boundary uses.

    The database's own name is in the file name, because two databases in one
    folder share a boundary folder and both arms of this project keep a table
    called `data`. Without it a Python run against one database would overwrite
    the handover of a run against the other.
    """
    database = database_path(connection)
    stem = database.stem if database is not None else "memory"
    return boundary_dir(connection) / f"{stem}__{table}__{direction}.parquet"


def read_table(store, table: str = "data") -> pl.LazyFrame:
    """Hand the stored table to polars as a LazyFrame, without materialising it.

    In a Parquet store the table is a file already and this is a scan of it.
    In a DuckDB store the table is copied to a Parquet file with DuckDB's own
    writer, which never holds more than a row group, and polars scans that.
    Either way what comes back is a plan over a file rather than over memory,
    so a step the streaming engine can run reads the dataset in batches and one
    that cannot still only materialises at the moment it collects.
    """
    if isinstance(store, ParquetStore):
        path = store.path(table)
        if not path.exists():
            raise FileNotFoundError(
                f"No `{table}` table in {store.folder}: {path} does not exist")
        return pl.scan_parquet(path)

    handover = _handover(store, table, "read")
    store.execute(
        f"COPY (SELECT * FROM {table}) TO '{handover}' "
        f"(FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    return pl.scan_parquet(handover)


def write_table(store, frame: pl.LazyFrame, table: str = "data"):
    """Put the frame back into the store under `table`, through a Parquet file.

    The counterpart of `compute_and_overwrite()`. `sink_parquet()` runs the
    plan into a file rather than into memory wherever polars can stream it.

    In a Parquet store the plan usually scans the very file it is replacing, so
    it is written beside the table and moved over it once it is complete. The
    move is atomic on one filesystem, which means an interrupted step leaves
    the table it started from rather than half a table.

    In a DuckDB store the written file is read into the table and deleted. The
    store is returned either way, so a caller can chain steps, which is what
    the R arm's pipe does.
    """
    if isinstance(store, ParquetStore):
        target = store.path(table)
        pending = target.with_suffix(f".pending{ParquetStore.suffix}")
        if isinstance(frame, pl.LazyFrame):
            frame.sink_parquet(pending, compression="zstd")
        else:
            frame.write_parquet(pending, compression="zstd")
        os.replace(pending, target)
        return store

    handover = _handover(store, table, "write")

    if isinstance(frame, pl.LazyFrame):
        frame.sink_parquet(handover, compression="zstd")
    else:
        frame.write_parquet(handover, compression="zstd")

    store.execute(
        f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_parquet('{handover}')"
    )

    # Both files have been read to the end by now: the sink above finished
    # reading the one the frame was scanning, and `CREATE TABLE AS` is eager.
    # They are the size of the dataset, so they do not stay on disk between
    # steps.
    handover.unlink(missing_ok=True)
    _handover(store, table, "read").unlink(missing_ok=True)
    return store


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
