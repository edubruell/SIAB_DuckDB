"""
01.) Episode splitting

Port of 01_split_episodes.do. Split episodes that span over one year (only
relevant for the sources LeH and LHG).

Modifies the variables:
  - begepi: splitted version of begepi
  - endepi: splitted version of endepi
  - year:   year (the reference calls it jahr)
  - age:    age, measured in years

Generates the variables:
  - begepi_orig: original version of begepi
  - endepi_orig: original version of endepi

Note: the reference splits only the variables listed above. Other variables,
such as alo_beg and alo_dau, are not split.

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

__all__ = ["split_episodes"]


def split_episodes(frame: pl.LazyFrame,
                   log_file: str | os.PathLike | None = None) -> pl.LazyFrame:
    log = step_logger("split_episodes", log_file)
    log.info("Episode splitting script started")

    # Save the original start and end date of the episode, and count the
    # calendar years the spell spans.
    frame = frame.with_columns(
        begepi_orig=pl.col("begepi"),
        endepi_orig=pl.col("endepi"),
        span_year=pl.col("endepi").dt.year() - pl.col("begepi").dt.year() + 1,
    )

    # `expand span_year` copies a row span_year times. The copies are numbered
    # 1 to span_year, which is the `is_clone` counter of the reference read as
    # a number rather than a flag: the reference reaches the same place by
    # walking the clone group with `_n`, and the counter says directly which
    # calendar year each copy belongs to.
    frame = frame.with_columns(
        year_instance=pl.int_ranges(1, pl.col("span_year") + 1)
    ).explode("year_instance", empty_as_null=False)

    frame = frame.with_columns(
        year_instance=pl.col("begepi").dt.year() - 1 + pl.col("year_instance")
    )

    # Every expression below reads the frame as it was before this call, so
    # begepi and endepi are still the unsplit dates on the right-hand side,
    # exactly as they are in the reference's `replace` statements.
    frame = frame.with_columns(
        # For all clone spells except the last of the clone group: set the end
        # date to 31 December.
        endepi=pl.when(pl.col("year_instance") < pl.col("endepi").dt.year())
        .then(pl.date(pl.col("year_instance"), 12, 31))
        .otherwise(pl.col("endepi")),
        # Set the begin date of all clones to 1 January of the copy's own year.
        begepi=pl.when(pl.col("year_instance") > pl.col("begepi").dt.year())
        .then(pl.date(pl.col("year_instance"), 1, 1))
        .otherwise(pl.col("begepi")),
    ).with_columns(
        # Update the variables 'year' and 'age'.
        year=pl.col("begepi").dt.year(),
    ).with_columns(
        age=pl.col("year") - pl.col("gebjahr"),
    ).drop("year_instance", "span_year")

    log.info("Episode splitting script finished")
    return frame
