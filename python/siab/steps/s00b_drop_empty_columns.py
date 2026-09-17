"""
00b.) Drop every column that holds nothing but missing values

Generates no variable and changes no value. It removes the columns that are
missing on every row of the table and keeps the rest.

Notes:
  00_master_SIAB.do does this inline rather than in a numbered step, right
after the source restriction and before it generates jahr and age:

    * Remove all variables that contain only missings
    foreach var of varlist _all {
        capture assert missing(`var')
        if !_rc {
            drop `var'
        }
    }

The position is what gives the step its point. Cutting the sources down to the
employment history leaves behind the variables that only the benefit spells
ever filled, and they would otherwise travel through the whole preparation as
columns of nothing.

  What counts as missing. Stata's `missing()` is true for a numeric missing and
for the empty string, because the empty string is how a Stata string variable
spells missing. polars has both an empty string and null, and they are
different values. This port drops a column when every row of it is null, and it
keeps a column of empty strings. The two notions therefore part company on
exactly one case: a string column that is empty on every row is dropped by the
reference and kept here. That is deliberate. An empty string in the data is a
value someone wrote, and the read-in preserves the difference, so throwing the
column away would discard information the reference never had in the first
place.

  The reference gates its optional do-files behind macros. `drop` is the same
kind of switch: it is on by default, as the master's loop is, and setting it to
False leaves the column set alone.

  The step runs on the frame as it stands, so what it drops depends on where it
is called. In the pipeline it is the first step after the source restriction,
which is where the master runs it. It sees `year` and `age`, which the master
generates just after the loop rather than just before it; neither can be
missing throughout once begepi and gebjahr are there, so the column set is the
same either way.

Author(s): Eduard Brüll
Python/polars reimplementation of the original procedure by Wolfgang Dauth,
Johann Eppelsheimer and Heiko Stüber

Version: 1.0
Created: 2026-09-17
"""

from __future__ import annotations

import os

import polars as pl

from siab.common import step_logger

__all__ = ["drop_empty_columns"]


def drop_empty_columns(frame: pl.LazyFrame,
                       drop: bool = True,
                       log_file: str | os.PathLike | None = None) -> pl.LazyFrame:
    if not isinstance(drop, bool):
        raise ValueError("drop has to be True or False")

    log = step_logger("empty_columns", log_file)

    if not drop:
        log.info("drop = False, the column set is left as it is")
        return frame

    # The schema is known without touching a row, so the column names cost
    # nothing.
    columns = frame.collect_schema().names()

    # One aggregation over the frame, collected once: the row count and a null
    # count per column. Knowing which columns hold nothing but nulls cannot be
    # done lazily, because the answer is what decides the shape of the frame
    # this step returns. Everything else here is schema work.
    #
    # An empty string is counted as filled, only null is not. This mirrors the
    # note above on where the port and Stata's missing() part company.
    row_count_name = "_siab_n_rows"
    while row_count_name in columns:
        row_count_name += "_"

    counts = frame.select(
        pl.len().alias(row_count_name),
        *[pl.col(name).null_count().alias(name) for name in columns],
    ).collect()

    n_rows = counts.get_column(row_count_name).item()

    # With no rows nothing witnesses a value, so every column would qualify and
    # the step would empty the schema. An empty table is a broken pipeline, not
    # a table of empty columns, so say so and leave the columns alone.
    if n_rows == 0:
        log.warning("The data table has no rows, so no column is dropped")
        log.info("Empty columns dropped")
        return frame

    empty = [name for name in columns if counts.get_column(name).item() == n_rows]

    if len(empty) == len(columns):
        raise ValueError(
            "Every column of the data table is missing throughout, so the step "
            "would leave no column at all"
        )

    if not empty:
        log.info(f"No column is missing throughout, all {len(columns)} are kept")
        log.info("Empty columns dropped")
        return frame

    frame = frame.drop(empty)

    log.info(
        f"{len(empty)} of {len(columns)} columns are missing on all {n_rows} "
        f"rows and are dropped: {', '.join(empty)}"
    )
    log.info("Empty columns dropped")
    return frame
