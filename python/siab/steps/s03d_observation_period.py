"""
03d.) Restrict the data to the observation period

Generates no variable. It keeps the episodes whose year lies between min_year
and max_year and drops the rest.

Notes:
  00_master_SIAB.do does this inline rather than in a numbered step:
`keep if inrange(jahr,${minYear},${maxYear})`, run after 03_SIAB_bio.do and
before 04_merge_basic_BHP.do. The position is what makes the biographic
variables right: tage_erw, tage_bet and the rest count over the whole history,
so they have to be built before any year is cut away.

  The default span is the whole SIAB 7523 v2 delivery, 1975 to 2023, which
keeps every episode. A narrower period is a user's sample choice, the same way
the reference's minYear and maxYear macros are.

  `year` is the port's name for the reference's `jahr`, and both are built from
begepi. An episode split at a year boundary by 01_split_episodes.do carries the
year of its own start, so the cut works on the split episodes.

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

__all__ = ["restrict_observation_period"]


def restrict_observation_period(frame: pl.LazyFrame,
                                min_year: int = 1975,
                                max_year: int = 2023,
                                log_file: str | os.PathLike | None = None) -> pl.LazyFrame:
    if isinstance(min_year, bool) or isinstance(max_year, bool) \
            or not isinstance(min_year, (int, float)) \
            or not isinstance(max_year, (int, float)):
        raise ValueError("min_year and max_year are single years, as in min_year = 1975")
    if min_year > max_year:
        raise ValueError(f"min_year is after max_year: {min_year} > {max_year}")

    log = step_logger("obs_period", log_file)
    log.info(f"Restricting the data to {min_year} to {max_year}")

    # `keep if inrange(jahr,${minYear},${maxYear})`. Both bounds are inclusive,
    # which is what inrange() does, and inrange() is false for a missing value,
    # so the reference drops an episode with no year. A polars filter drops a
    # row whose predicate is null for the same reason, so the plain comparison
    # already carries Stata's behaviour here. This is not a `replace ... if`
    # guard, where Stata's ordering of missing as a large number would matter.
    #
    # The R arm counts the episodes before and after and logs the difference.
    # Here that would mean collecting the whole upstream chain twice for a log
    # line, so the step stays lazy and logs the period alone. The same applies
    # to the R arm's warning about a period that keeps no episode: there is
    # nothing to count without materialising the frame.
    frame = frame.filter(
        (pl.col("year") >= min_year) & (pl.col("year") <= max_year)
    )

    log.info("Observation period applied")
    return frame
