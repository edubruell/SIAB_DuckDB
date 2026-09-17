"""
05.) Add Marginal Part-Time Income Threshold and flag affected records (1975 - 2014)

Port of 07_wages_marginal.do.

Generates the variables:
  - limit_marginal: Marginal part-time income threshold
  - marginal: 1 if marginal wage, 0 otherwise

Reads tentgelt and east, so it runs after the assessment ceiling step.

Note: Limits for the years 1975 - 2001 are converted from DM to EUR. Based on
an FDZ Arbeitshilfe (http://doku.iab.de/fdz/Bemessungsgrenzen_de_en.xls).

Author(s): Eduard Brüll
Python/polars reimplementation of the original procedure by Wolfgang Dauth and
Johann Eppelsheimer

Version: 1.0
Created: 2026-09-17
"""

from __future__ import annotations

import os

import polars as pl

from siab.common import classifications_dir, pl_stata_gt, stata_float, step_logger

__all__ = ["generate_limit_marginal"]


def generate_limit_marginal(frame: pl.LazyFrame,
                            log_file: str | os.PathLike | None = None) -> pl.LazyFrame:
    log = step_logger("limit_marginal", log_file)
    log.info("Reading limit_marginal values from csv")

    # As in the assessment ceiling step: 07_wages_marginal.do writes the
    # threshold with `gen`, which gives a Stata float, and the csv holds the
    # full double of the DM conversion. stata_float() puts the lookup back on
    # the reference's own precision before anything compares a wage against it,
    # which matters twice over here, because the comparison decides a flag.
    tbl_limit_marginal = pl.read_csv(classifications_dir() / "limit_marginal.csv")
    tbl_limit_marginal = tbl_limit_marginal.with_columns(
        limit_marginal=pl.Series(
            "limit_marginal", stata_float(tbl_limit_marginal["limit_marginal"].to_numpy())
        )
    ).rename({"east": "east_lookup"}).lazy()

    log.info("Generating limit_marginal and marginal dummy in data")

    # Same pre-1992 rule as the assessment ceiling: 07_wages_marginal.do assigns
    # the threshold on the year alone until 1991 and only conditions on east from
    # 1992 on. See siab/steps/s04_wage_assessment_ceiling.py.
    frame = frame.with_columns(
        east_lookup=pl.when(pl.col("year") < 1992)
        .then(pl.lit(0, dtype=pl.Int64))
        .otherwise(pl.col("east").cast(pl.Int64))
    ).join(
        tbl_limit_marginal, on=["east_lookup", "year"], how="left", maintain_order="left"
    ).drop("east_lookup")

    # 07_wages_marginal.do writes `gen byte marginal = 0` and then
    # `replace marginal = 1 if tentgelt <= limit_marginal`, so the flag is never
    # missing. The two missing cases below follow from how Stata orders its
    # missing values, and both are reproduced on purpose because the Stata prep
    # is the reference:
    #
    #  - A missing tentgelt always gives 0. The SIAB codes an absent wage as an
    #    extended missing (.a to .z, never the system missing), and an extended
    #    missing is larger than everything, including the system missing the
    #    do-file starts limit_marginal at. So the comparison is false whatever
    #    the threshold is. Checked on the test data: all 32,848 missing wages at
    #    this step are extended, none is a system missing.
    #  - A missing limit_marginal with a real wage gives 1, because any number
    #    is at or below Stata's missing.
    #
    # The second case is what pl_stata_gt() is for: Stata's `a <= b` is the
    # negation of `a > b`, and with a missing threshold `tentgelt > .` is false,
    # so the flag goes up. The missing-wage case is taken before that, because
    # it is the extended missing that decides it, and pl_stata_gt() knows only
    # the system missing polars has.
    frame = frame.with_columns(
        marginal=pl.when(pl.col("tentgelt").is_null())
        .then(pl.lit(0, dtype=pl.Int32))
        .otherwise(
            (~pl_stata_gt(pl.col("tentgelt"), pl.col("limit_marginal"))).cast(pl.Int32)
        )
    )

    log.info(" ->  limit_marginal and marginal added")
    log.info("Marginal Part-Time Income Threshold and Flag affected records")
    return frame
