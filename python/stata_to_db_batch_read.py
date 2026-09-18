"""
Read a Stata SIAB into a DuckDB database, one batch of persons at a time.

The counterpart of R/stata_to_db_batch_read.R, and the step that has to run
before main.py: the pipeline opens a database and expects an `orig` table in
it, and this is what writes one.

A SIAB 7523 v2 delivery is a single .dta file of some tens of gigabytes, which
is more than the machine this project targets can hold. It is read in batches
instead, and a batch boundary never falls inside a person: the file arrives
sorted by the person key, so a run of rows with the same key is contiguous and
the split is a row range. The batch number rides along as `pn_batch`, exactly
as the R script does, which gives every later step a coarse partition of the
persons to work with.

Three details the reader has to get right, none of which pyreadstat does on its
own:

  * **The keys.** The 7523 v2 delivery keys on `persnr_siab` and `betnr_siab`;
    the reference, the pipeline and every fixture call them `persnr` and
    `betnr`. They are renamed here. A delivery that already carries the short
    names, such as the 7514 v1, passes through untouched.
  * **The dates.** Stata stores a `%td` column as whole days since 1 January
    1960 and pyreadstat hands them over as that integer. All five of them are
    converted to real dates, or every episode boundary in the prep would be a
    five-figure number.
  * **The storage types.** pyreadstat returns a float column for any Stata
    integer that carries a missing value, so the column type would depend on
    the batch. Each column is cast to what its Stata storage type says it is:
    `byte`, `int` and `long` to a 32-bit integer, `float` and `double` to a
    double. That is the same mapping readstata13 makes on the R side, so both
    arms build the same `orig` table out of the same file.

Two environment variables point at the data, each with a fallback:

  SIAB_RAW_FOLDER  where the raw SIAB delivery sits, default ~/data/siab_raw
  SIAB_DB_FOLDER   where the DuckDB database is written, default ~/data/siab_db

Run it with:

  uv run --project python python python/stata_to_db_batch_read.py

Author(s): Eduard Brüll
Python reimplementation of the R arm's batch reader

Version: 1.0
Created: 2026-09-18
"""

from __future__ import annotations

import datetime as dt
import os
import sys
from pathlib import Path

import duckdb
import polars as pl
import pyreadstat

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from siab.common import folder_reference_factory  # noqa: E402

# The delivery's name for each key, and the pipeline's.
KEY_RENAMES = {"persnr_siab": "persnr", "betnr_siab": "betnr"}

# Stata counts days from 1 January 1960 and polars from 1 January 1970, so a
# Stata day number is this many days off a polars one. It is negative.
STATA_EPOCH_OFFSET = (dt.date(1960, 1, 1) - dt.date(1970, 1, 1)).days

# How many rows a batch aims for, the number the R script passes. A batch is
# whole persons, so the real count overshoots this by the tail of the last
# person in it.
BATCH_SIZE = 3_000_000


def stata_metadata(siab_file: str | os.PathLike):
    """The delivery's column names, storage types and display formats.

    Read off a single row, which costs nothing on a file of any size.
    """
    _, meta = pyreadstat.read_dta(str(siab_file), row_limit=1)
    return meta


def person_key(meta) -> str:
    """Which spelling of the person key this delivery uses."""
    return "persnr_siab" if "persnr_siab" in meta.column_names else "persnr"


def column_types(meta) -> dict[str, pl.DataType]:
    """What each column should end up as, from its Stata storage type.

    A `%td` display format makes a date whatever the storage type says, because
    Stata stores a date as an integer and the format is the only thing that
    marks it as one.
    """
    plan: dict[str, pl.DataType] = {}
    for name in meta.column_names:
        display = meta.original_variable_types[name]
        storage = meta.readstat_variable_types[name]
        if display.startswith("%t") or display.startswith("%d"):
            plan[name] = pl.Date
        elif storage.startswith("int"):
            plan[name] = pl.Int32
        else:
            plan[name] = pl.Float64
    return plan


def batch_bounds(person_column: pl.Series, batch_size: int) -> list[tuple[int, int, int]]:
    """Cut the file into row ranges that never split a person.

    Takes the person key of every row, in file order, and gives back one
    `(batch, offset, length)` per batch, with `offset` counted from zero. The
    arithmetic is the R script's: count the rows per person, run a cumulative
    total over them, and label each person with `floor(total / batch_size) + 1`.
    A person longer than one batch takes a label of its own and the labels in
    between are never used, which is why the batch number is carried rather
    than recomputed.

    The file has to be sorted by the person key for a row range to be a set of
    whole persons, and it is: the delivery arrives that way. `ingest()` checks
    it rather than trusting it, because the failure is silent otherwise.
    """
    if person_column.len() == 0:
        return []

    runs = (
        pl.DataFrame({"persnr": person_column})
        .with_columns(pl.col("persnr").rle_id().alias("run"))
        .group_by("run")
        .agg(pl.len().alias("n"))
        .sort("run")
        .with_columns(pl.col("n").cum_sum().alias("upto"))
        .with_columns((pl.col("upto") // batch_size + 1).alias("batch"))
        .group_by("batch")
        .agg(pl.col("upto").max().alias("last_row"))
        .sort("batch")
    )

    bounds: list[tuple[int, int, int]] = []
    offset = 0
    for batch, last_row in zip(runs["batch"], runs["last_row"]):
        bounds.append((int(batch), offset, int(last_row) - offset))
        offset = int(last_row)
    return bounds


def read_batch(siab_file: str | os.PathLike,
               offset: int,
               length: int,
               plan: dict[str, pl.DataType],
               batch: int) -> pl.DataFrame:
    """Read one row range of the delivery and put it in the pipeline's shape."""
    pandas_frame, _ = pyreadstat.read_dta(str(siab_file),
                                          row_offset=offset,
                                          row_limit=length)
    frame = pl.from_pandas(pandas_frame)

    casts = []
    for name, dtype in plan.items():
        column = pl.col(name)
        # A Stata missing arrives as NaN in a float column, and NaN is not
        # null: casting it to an integer or a date would keep it as a value
        # the reference does not have. This is the one place it can be caught
        # for the whole pipeline.
        if frame.schema[name] in (pl.Float32, pl.Float64):
            column = column.fill_nan(None)
        if dtype == pl.Date:
            column = (column.cast(pl.Int32, strict=False)
                      + STATA_EPOCH_OFFSET).cast(pl.Date)
        else:
            column = column.cast(dtype, strict=False)
        casts.append(column.alias(name))

    return (frame.with_columns(casts)
            .rename({old: new for old, new in KEY_RENAMES.items()
                     if old in frame.columns})
            .with_columns(pl.lit(batch, dtype=pl.Int32).alias("pn_batch")))


def ingest(siab_file: str | os.PathLike,
           db_file: str | os.PathLike,
           batch_size: int = BATCH_SIZE,
           table: str = "orig") -> int:
    """Read a whole SIAB delivery into `table`, replacing what was there.

    Gives back the number of rows written. The table is replaced rather than
    appended to, so running this twice leaves one copy of the data instead of
    two.
    """
    siab_file = Path(siab_file)
    if not siab_file.exists():
        raise FileNotFoundError(f"No SIAB delivery at {siab_file}")

    meta = stata_metadata(siab_file)
    key = person_key(meta)
    plan = column_types(meta)

    print(f"Reading the {key} column of {siab_file.name} to find the batches")
    keys, _ = pyreadstat.read_dta(str(siab_file), usecols=[key])
    person_column = pl.from_pandas(keys)[key]

    # A row range is a set of whole persons only if the persons are contiguous,
    # which they are in a delivery and are not in a file somebody has re-sorted.
    runs = person_column.rle_id().max() + 1
    distinct = person_column.n_unique()
    if runs != distinct:
        raise ValueError(
            f"{siab_file.name} is not sorted by {key}: {runs} runs of the key "
            f"over {distinct} persons. Sort it before reading it in, or a "
            f"batch would hold part of a person.")

    bounds = batch_bounds(person_column, batch_size)
    print(f" -> {person_column.len()} rows, {distinct} persons, "
          f"{len(bounds)} batch(es)")
    del keys, person_column

    connection = duckdb.connect(str(db_file))
    connection.execute(f"DROP TABLE IF EXISTS {table}")

    written = 0
    for position, (batch, offset, length) in enumerate(bounds, start=1):
        print(f"Uploading batch {position}/{len(bounds)} "
              f"(pn_batch {batch}, rows {offset + 1} to {offset + length})")
        frame = read_batch(siab_file, offset, length, plan, batch)
        connection.register("_siab_batch", frame)
        if position == 1:
            connection.execute(
                f"CREATE TABLE {table} AS SELECT * FROM _siab_batch")
        else:
            connection.execute(
                f"INSERT INTO {table} SELECT * FROM _siab_batch")
        connection.unregister("_siab_batch")
        written += frame.height
        del frame

    connection.close()
    print(f"\n{written} rows written to the `{table}` table of {db_file}")
    return written


def main() -> None:
    rawdata = folder_reference_factory(
        os.environ.get("SIAB_RAW_FOLDER", str(Path.home() / "data" / "siab_raw")))
    dbfolder = folder_reference_factory(
        os.environ.get("SIAB_DB_FOLDER", str(Path.home() / "data" / "siab_db")))

    ingest(rawdata("SIAB_7523_v2.dta"), dbfolder("siab.duckdb"))


if __name__ == "__main__":
    main()
