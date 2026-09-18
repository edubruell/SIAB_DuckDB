"""
04.) Add contribution assessment ceiling (1975 - 2017)

Port of 06_wages_assessment_ceiling.do.

Generates the variables:
  - east: 1 if workplace in East Germany (Berlin from 1992); 0 if West
  - limit_assess: contribution assessment ceiling

Requires ao_bula, which merge_basic_bhp() brings in from the Basic
Establishment File. The SUF this code was written for carried ao_region
instead, from which ao_bula was derived as floor(ao_region/1000).

Notes:
  In Germany there is a contribution assessment ceiling
  ("Beitragsbemessungsgrenze"). Hence, wages are right-cencored. The generation
  of the variable limit_assess is based on a FDZ-Arbeitshilfe
  (http://doku.iab.de/fdz/Bemessungsgrenzen_de_en.xls). Limits for the years
  1975 - 2001 are converted from DM to EUR.

Author(s): Eduard Brüll
Python/polars reimplementation of the original procedure by Wolfgang Dauth and
Johann Eppelsheimer

Version: 1.0
Created: 2026-09-17
"""

from __future__ import annotations

import os

import polars as pl

from siab.common import classifications_dir, stata_float, step_logger

__all__ = ["generate_limit_assess"]


def generate_limit_assess(frame: pl.LazyFrame,
                          log_file: str | os.PathLike | None = None) -> pl.LazyFrame:
    log = step_logger("wa_ceiling", log_file)
    log.info("Reading limit_assess values from csv")

    # The statutory table is one row per year and Rechtskreis, so it is read in
    # one go and handed to the query as a LazyFrame. stata_float() is what makes
    # it comparable with the reference: 06_wages_assessment_ceiling.do writes
    # the ceiling with `gen`, which gives a Stata float, while the csv carries
    # the full double the DM conversion produced. The extra digits are what an
    # exact comparison against the reference would trip over.
    #
    # The lookup's `east` is renamed to east_lookup for the join, because the
    # column the data joins on is not the east it keeps; see below.
    ceiling = pl.read_csv(classifications_dir() / "wa_ceiling.csv")
    ceiling = ceiling.with_columns(
        limit_assess=pl.Series("limit_assess",
                               stata_float(ceiling["limit_assess"].to_numpy()))
    ).rename({"east": "east_lookup"}).lazy()

    log.info("Generating east and the limit_assess")

    # The branches are in the reference's order read backwards: Stata's three
    # `replace` statements overwrite each other, and the last one wins, so the
    # Berlin-before-1992 rule that 06_wages_assessment_ceiling.do writes last is
    # tested first here.
    #
    # A missing ao_bula falls through every branch and leaves east missing, and
    # so does an unknown state code above 16. A missing year takes Berlin to the
    # East branch, because a missing year is not smaller than 1992 in Stata
    # either; the port records the behaviour rather than endorsing it.
    frame = frame.with_columns(
        east=pl.when((pl.col("ao_bula") == 11) & (pl.col("year") < 1992))
        # West: Berlin until 1991, following 06_wages_assessment_ceiling.do
        .then(pl.lit(0, dtype=pl.Int32))
        # East: Berlin (from 1992), Brandenburg, Mecklenburg-Western Pomerania,
        # Saxony, Saxony-Anhalt, Thuringia
        .when(pl.col("ao_bula").is_in([11, 12, 13, 14, 15, 16]))
        .then(pl.lit(1, dtype=pl.Int32))
        # West: Schleswig-Holstein, Hamburg, Lower Saxony, Bremen, North
        # Rhine-Westphalia, Hesse, Rhineland-Palatinate, Baden-Wuerttemberg,
        # Bavaria, Saarland
        .when(pl.col("ao_bula") < 11)
        .then(pl.lit(0, dtype=pl.Int32))
        .otherwise(pl.lit(None, dtype=pl.Int32))
    )

    # Before 1992 there was one nationwide ceiling, and 06_wages_assessment_ceiling.do
    # assigns it on the year alone. A spell whose federal state is unknown therefore
    # still gets a ceiling in those years, and only loses one from 1992, when the
    # reference starts conditioning on east. Joining on east throughout would drop
    # those pre-1992 rows to missing.
    #
    # east_lookup is cast to the lookup's own integer type, because a frame that
    # arrives with east as a double would otherwise not join at all. The left
    # order is kept so the step is deterministic; nothing downstream relies on
    # it, but a fixture dump compares more easily when it does not move.
    frame = frame.with_columns(
        east_lookup=pl.when(pl.col("year") < 1992)
        .then(pl.lit(0, dtype=pl.Int64))
        .otherwise(pl.col("east").cast(pl.Int64))
    ).join(
        ceiling, on=["east_lookup", "year"], how="left", maintain_order="left"
    ).drop("east_lookup")

    log.info(" -> east and limit_assess added")
    log.info("Wage assesment ceiling file finished")
    return frame
