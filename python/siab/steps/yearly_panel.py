"""
09.) Transfer data set into a yearly panel (using one specific cutoff date)

Port of 16_yearly_panel.do. The step totals the days and the earnings of each
calendar year, keeps the one episode of that year running across the cutoff
date, and trims the four duration counters so they end on that date rather than
at the episode's own end.

Generates the variables:
  - year_days_emp: total days employed per calendar year
  - year_days_benefits: total days benefit recipience per calendar year
  - year_labor_earn: total labor earnings per calendar year

Modifies the variables:
  - tage_bet: days in establishment, trimmed to end at the cutoff date
  - tage_job: days in job, trimmed to end at the cutoff date
  - tage_erw: days in employment, trimmed to end at the cutoff date
  - tage_lst: days with benefit receipt, trimmed to end at the cutoff date

The reference drops seven columns on its way out: nspell, begorig, endorig,
begepi, endepi, begepi_orig and endepi_orig. This port keeps all seven, as the
R arm does. Keeping begepi and endepi is what makes the fixture comparison
checkable at all: the two halves then join on the person and the year alone,
and every surviving episode can be shown to run across the cutoff date. The
year is `jahr` on the Stata side and `year` here.

This is an alternative to build_monthly_panel(), not a step before or after it.
Both start from the output of handle_parallel_episodes(), and the reference
master calls neither: a pipeline calls one or the other.

Author(s): Eduard Brüll
Python/polars reimplementation of the original procedure by Wolfgang Dauth and
Johann Eppelsheimer

Version: 1.0
Created: 2026-09-17
"""

from __future__ import annotations

import os

import polars as pl

from siab.common import pl_days, pl_stata_gt, step_logger

__all__ = ["build_yearly_panel"]

# The trainee statuses the reference excludes from the tage_erw trim. This is
# 03_SIAB_bio.do's list of apprenticeships plus 144, which 16_yearly_panel.do
# adds; siab_bio.py therefore carries its own, shorter list.
AZUBI_ERWSTAT = [102, 121, 122, 141, 144]


def _episode_days() -> pl.Expr:
    """endepi - begepi + 1, the length of an episode in days."""
    return pl_days(pl.col("endepi"), pl.col("begepi")) + 1


def build_yearly_panel(frame: pl.LazyFrame,
                       cutoff_month: int = 6,
                       cutoff_day: int = 30,
                       log_file: str | os.PathLike | None = None) -> pl.LazyFrame:
    log = step_logger("yearly_panel", log_file)

    # -------------------------------------------------------------------
    #  Aggregate employment outcomes: days employed, labor earnings
    # -------------------------------------------------------------------
    log.info("Generating aggregate employment outcomes: days employed, labor earnings ")

    # The reference opens this block with
    #   sort persnr jahr begepi endepi quelle
    #   by persnr jahr begepi endepi: gen dur_emp = ...
    # but the `by` prefix in front of a plain `gen` changes nothing: the two
    # durations are row-wise, and the three totals below are sums over the
    # person-year, which do not depend on the order of the rows either. So the
    # port needs no sort at all, and consequently no decision about where a
    # null would land in one.
    #
    # `gen ... if` leaves a missing where the condition fails and egen total()
    # then reads that missing as a zero, so the two branches are the day count
    # and 0. A null quelle fails every one of these tests in Stata, and a
    # polars `when` sends a null condition down the `otherwise` branch, so the
    # plain comparison already carries Stata's reading here.
    frame = frame.with_columns(
        dur_emp=pl.when(pl.col("quelle") == 1)
        .then(_episode_days())
        .otherwise(0)
        .cast(pl.Int32),
        # 16_yearly_panel.do also allows the legacy code quelle == 16, which
        # does not occur in SIAB 7523 v2.
        dur_benefits=pl.when(
            (pl.col("quelle") == 2) | (pl.col("parallel_benefits") == 1)
        )
        .then(_episode_days())
        .otherwise(0)
        .cast(pl.Int32),
    ).with_columns(
        # by persnr jahr: egen year_days_emp = total(dur_emp)
        year_days_emp=pl.col("dur_emp").sum().over(["persnr", "year"]),
        year_days_benefits=pl.col("dur_benefits").sum().over(["persnr", "year"]),
        # Earnings = wage * duration. egen total() skips a missing wage, and so
        # does a polars sum.
        year_labor_earn=(pl.col("parallel_wage_imp") * pl.col("dur_emp"))
        .sum()
        .over(["persnr", "year"]),
    ).drop("dur_emp", "dur_benefits")

    log.info("->  year_days_emp, year_days_benefits and year_labor_earn generated")

    # Transfer dates into strictly ascending numbers. This is an artefact of the
    # original code being Stata; polars compares dates directly, but the port
    # follows the reference.
    cutoff_num = 100 * int(cutoff_month) + int(cutoff_day)

    # -------------------------------------------------------------------
    #  Keep only episodes that include the cutoff date
    # -------------------------------------------------------------------
    log.info("Keep only episodes that include the cutoff date")

    # keep if begepi_num <= cutoff_num & cutoff_num <= endepi_num
    #
    # Both halves of that test are comparisons against a value that could be
    # missing, and Stata sorts a missing above every number, so the two halves
    # read differently there: a missing begepi_num fails its test and drops the
    # episode, while a missing endepi_num passes its own and keeps it. polars
    # propagates a null instead, and a filter drops the row either way, so the
    # second half has to go through pl_stata_gt() to keep the reference's
    # reading. `a <= b` is `not (a > b)`, which is how the guard is written.
    #
    # In the data as the pipeline hands it over neither date is ever null --
    # begepi and endepi identify an episode by this point -- so the two
    # readings only part company on data that should not reach this step.
    #
    # month() and day() come back as one-byte integers, and 100 * a month past
    # January does not fit in one. Both parts are widened before the
    # multiplication, not after it.
    cutoff = pl.lit(cutoff_num, dtype=pl.Int32)
    frame = frame.with_columns(
        # begin of episodes
        begepi_num=100 * pl.col("begepi").dt.month().cast(pl.Int32)
        + pl.col("begepi").dt.day().cast(pl.Int32),
        # end of episodes
        endepi_num=100 * pl.col("endepi").dt.month().cast(pl.Int32)
        + pl.col("endepi").dt.day().cast(pl.Int32),
    ).filter(
        ~pl_stata_gt(pl.col("begepi_num"), cutoff)
        & ~pl_stata_gt(cutoff, pl.col("endepi_num"))
    ).drop("begepi_num", "endepi_num")

    log.info("->  Only episodes including the cutoff data included")

    # -------------------------------------------------------------------
    #  Adjust durations to end at cutoff date
    # -------------------------------------------------------------------
    log.info("Adjust durations to end at cutoff date")

    # 16_yearly_panel.do uses `replace ... if`, which leaves the rows that do
    # not meet the condition unchanged. The alternative branch therefore has to
    # be the existing value, not a null.
    #
    # None of the four guards compares magnitudes, so none of them needs
    # pl_stata_gt(): they are equality tests and an inlist(), and a null quelle
    # or a null parallel_benefits fails them in Stata exactly as a null
    # condition falls through to `otherwise` here.
    overhang = pl_days(pl.col("endepi"),
                       pl.date(pl.col("year"), int(cutoff_month), int(cutoff_day)))
    is_beh = pl.col("quelle") == 1

    frame = frame.with_columns(overhang=overhang.cast(pl.Int32)).with_columns(
        tage_bet=pl.when(is_beh)
        .then(pl.col("tage_bet") - pl.col("overhang"))
        .otherwise(pl.col("tage_bet")),
        tage_job=pl.when(is_beh)
        .then(pl.col("tage_job") - pl.col("overhang"))
        .otherwise(pl.col("tage_job")),
        # Stata's inlist() returns 0 for a missing argument, so
        # `!inlist(erwstat, ...)` is true where erwstat is missing and the
        # reference trims the counter there too. polars' is_in() returns null
        # for a null instead, which would send the row down the `otherwise`
        # branch and leave the counter untrimmed: 966 rows of the FDZ test
        # data, all quelle 1 with no erwstat. fill_null() puts the Stata
        # reading back, the same way the R arm coalesces the comparison.
        tage_erw=pl.when(
            is_beh & ~pl.col("erwstat").fill_null(-1).is_in(AZUBI_ERWSTAT)
        )
        .then(pl.col("tage_erw") - pl.col("overhang"))
        .otherwise(pl.col("tage_erw")),
        # 16_yearly_panel.do also allows the legacy code quelle == 16, which
        # does not occur in SIAB 7523 v2. The reference joins both conditions
        # in a single `replace`, so a benefit episode that also carries
        # parallel_benefits loses the overhang once; 16_monthly_panel.do splits
        # them into two statements and subtracts twice.
        tage_lst=pl.when(
            (pl.col("quelle") == 2) | (pl.col("parallel_benefits") == 1)
        )
        .then(pl.col("tage_lst") - pl.col("overhang"))
        .otherwise(pl.col("tage_lst")),
    ).drop("overhang")

    log.info("-> Durations adjusted")
    log.info("Yearly panel file finished")
    return frame
