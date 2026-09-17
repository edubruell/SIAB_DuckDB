"""
09b.) Transfer data set into a monthly panel (using one cutoff day per month)

Port of 16_monthly_panel.do. The step totals the days and the earnings of each
calendar year, as the yearly panel does, cuts every episode into one row per
calendar month, keeps the row whose month holds the cutoff day inside the
episode, and trims the four duration counters so they end on that day.

Generates the variables:
  - year_days_emp: total days employed per calendar year
  - year_days_benefits: total days benefit recipience per calendar year
  - year_labor_earn: total labor earnings per calendar year
  - month: first day of the calendar month the row describes
  - month_num: the month of the year, 1 to 12
  - begepi_monthly: episode start, cut to the month
  - endepi_monthly: episode end, cut to the month

Modifies the variables:
  - tage_bet: days in establishment, trimmed to end at the monthly cutoff
  - tage_job: days in job, trimmed to end at the monthly cutoff
  - tage_erw: days in employment, trimmed to end at the monthly cutoff
  - tage_lst: days with benefit receipt, trimmed to end at the monthly cutoff

The reference drops seven columns on its way out: nspell, begorig, endorig,
begepi, endepi, begepi_orig and endepi_orig. This port keeps all seven, as the
R arm does, so the two halves of the fixture comparison join on the person, the
year and the monthly episode start, and every surviving row can be shown to be
one month of its own episode.

The reference builds a second year column out of the month it generates, beside
the `jahr` it inherits from the episode start. 01_split_episodes.do cuts every
episode at the year boundary, so no episode reaches this step spanning two
years and the two can never disagree, which is why the port carries one `year`
column for both.

This is an alternative to build_yearly_panel(), not a step after it. Both start
from the output of handle_parallel_episodes(), and the reference master calls
neither: a pipeline calls one or the other.

Author(s): Eduard Brüll
Python/polars reimplementation of the original procedure by Wolfgang Dauth,
Johann Eppelsheimer and Heiko Stüber

Version: 1.0
Created: 2026-09-17
"""

from __future__ import annotations

import os

import polars as pl

from siab.common import pl_days, step_logger

__all__ = ["build_monthly_panel"]

# The trainee statuses the reference excludes from the tage_erw trim, the same
# list 16_yearly_panel.do uses: 03_SIAB_bio.do's apprenticeships plus 144.
AZUBI_ERWSTAT = [102, 121, 122, 141, 144]


def _episode_days() -> pl.Expr:
    """endepi - begepi + 1, the length of an episode in days."""
    return pl_days(pl.col("endepi"), pl.col("begepi")) + 1


def build_monthly_panel(frame: pl.LazyFrame,
                        cutoff_day: int = 15,
                        log_file: str | os.PathLike | None = None,
                        *,
                        monthly_vars: bool = True) -> pl.LazyFrame:
    if isinstance(cutoff_day, bool) or not isinstance(cutoff_day, (int, float)) \
            or int(cutoff_day) != cutoff_day or not 1 <= cutoff_day <= 31:
        raise ValueError("cutoff_day has to be a single whole number between 1 and 31")
    if not isinstance(monthly_vars, bool):
        raise ValueError("monthly_vars has to be True or False")

    # A whole number, so the date built in the cutoff below gets an integer
    # rather than the decimal a plain 15.0 would carry into the expression.
    cutoff_day = int(cutoff_day)

    log = step_logger("monthly_panel", log_file)

    # -------------------------------------------------------------------
    #  Aggregate employment outcomes: days employed, labor earnings
    # -------------------------------------------------------------------
    # 16_monthly_panel.do puts this block behind `if $monthly_vars == 1`, so
    # the argument carries the switch. The block is the one 16_yearly_panel.do
    # runs unconditionally, and it works on the unexpanded data: the three
    # totals are per person and calendar year, and every row of a year carries
    # the same ones.
    if monthly_vars:
        log.info("Generating aggregate employment outcomes: days employed, labor earnings ")

        # The reference sorts on persnr jahr begepi endepi quelle first, but
        # the `by` prefix in front of a plain `gen` changes nothing: the two
        # durations are row-wise, and the totals below are sums over the
        # person-year, which do not depend on the order of the rows. The port
        # needs no sort, and so makes no decision about where a null lands in
        # one.
        #
        # `gen ... if` leaves a missing where the condition fails and egen
        # total() reads that missing as a zero, so the two branches are the day
        # count and 0. A null quelle fails every one of these tests in Stata,
        # and a polars `when` sends a null condition down the `otherwise`
        # branch, so the plain comparison already carries Stata's reading.
        frame = frame.with_columns(
            dur_emp=pl.when(pl.col("quelle") == 1)
            .then(_episode_days())
            .otherwise(0)
            .cast(pl.Int32),
            # 16_yearly_panel.do also allows the legacy code quelle == 16 here,
            # which 16_monthly_panel.do does not. Neither occurs in SIAB 7523
            # v2, so the two conditions select the same rows.
            dur_benefits=pl.when(
                (pl.col("quelle") == 2) | (pl.col("parallel_benefits") == 1)
            )
            .then(_episode_days())
            .otherwise(0)
            .cast(pl.Int32),
        ).with_columns(
            year_days_emp=pl.col("dur_emp").sum().over(["persnr", "year"]),
            year_days_benefits=pl.col("dur_benefits").sum().over(["persnr", "year"]),
            # Earnings = wage * duration. egen total() skips a missing wage,
            # and so does a polars sum.
            year_labor_earn=(pl.col("parallel_wage_imp") * pl.col("dur_emp"))
            .sum()
            .over(["persnr", "year"]),
        ).drop("dur_emp", "dur_benefits")

        log.info("->  year_days_emp, year_days_benefits and year_labor_earn generated")

    # -------------------------------------------------------------------
    #  One row per episode and calendar month
    # -------------------------------------------------------------------
    log.info("Expanding each episode into one row per calendar month")

    # The reference counts the months of an episode, n-plicates the row with
    # Stata's expand and numbers the copies within persnr, begepi and endepi.
    # That numbering is only unambiguous while those three columns identify a
    # row, which they do after 15_parallel_episodes.do. Stop rather than
    # silently multiply rows if a caller runs this on data that still holds
    # parallel episodes.
    #
    # This is the one place the step leaves the lazy plan: the guard is the R
    # arm's `stop()` and the reference's implicit assumption, and a count
    # cannot be checked without materialising it. Everything else below stays
    # in the plan.
    counts = frame.select(
        rows=pl.len(),
        episodes=pl.struct("persnr", "begepi", "endepi").n_unique(),
    ).collect()
    rows, episodes = counts.item(0, "rows"), counts.item(0, "episodes")
    if rows != episodes:
        raise ValueError(
            f"persnr, begepi and endepi do not identify a row: {rows} rows on "
            f"{episodes} episodes. Run handle_parallel_episodes() first."
        )

    # gen n_months = mofd(endepi) - mofd(begepi) + 1
    # expand n_months
    # by persnr begepi endepi: gen month_counter = _n - 1
    # gen month = mofd(begepi) + month_counter
    #
    # Stata's monthly date is a running count of months since January 1960, so
    # the reference's arithmetic on it is arithmetic on that count. The port
    # counts months the same way, from year 0, and turns the count back into
    # the first day of its month. int_ranges plus explode is the house stand-in
    # for `expand`, as in split_episodes.py, and it numbers the copies in one
    # go, so the `by`-group counter is not needed separately.
    begepi_index = 12 * pl.col("begepi").dt.year() + pl.col("begepi").dt.month() - 1
    endepi_index = 12 * pl.col("endepi").dt.year() + pl.col("endepi").dt.month() - 1

    frame = frame.with_columns(
        month_counter=pl.int_ranges(0, endepi_index - begepi_index + 1)
    ).explode("month_counter", empty_as_null=False)

    frame = frame.with_columns(
        month_index=(begepi_index + pl.col("month_counter")).cast(pl.Int32)
    ).with_columns(
        # `month` is the first day of the calendar month the row describes.
        month=pl.date(pl.col("month_index") // 12, pl.col("month_index") % 12 + 1, 1)
    ).drop("month_counter", "month_index")

    log.info("-> Data expanded to one row per episode and month")

    # -------------------------------------------------------------------
    #  Cut each row to its month and keep the one covering the cutoff day
    # -------------------------------------------------------------------
    log.info("Cutting episodes to their month and keeping the one covering the cutoff day")

    # The reference builds the month's last day out of a table of month lengths
    # and a leap-year test; polars has month_end() for it.
    #
    # The reference hardcodes the 15th. A cutoff day beyond the length of a
    # short month would drop that month entirely, so the day is capped at the
    # month's own last day; at the default of 15 the cap never binds.
    month_end = pl.col("month").dt.month_end()

    frame = frame.with_columns(
        # gen year = year(dofm(month)) / gen month_num = month(dofm(month)).
        # The port keeps the `year` it already carries, because no episode
        # reaching this step spans two calendar years, so the reference's two
        # year columns hold the same value.
        month_num=pl.col("month").dt.month().cast(pl.Int32),
        # Update begepi and endepi to cover only this month
        begepi_monthly=pl.max_horizontal(pl.col("begepi"), pl.col("month")),
        endepi_monthly=pl.min_horizontal(pl.col("endepi"), month_end),
        # The cutoff date of this month
        ref_date=pl.date(
            pl.col("month").dt.year(),
            pl.col("month").dt.month(),
            pl.min_horizontal(pl.lit(cutoff_day, dtype=pl.Int32), month_end.dt.day()),
        ),
    ).filter(
        # keep if inrange(ref15, begepi_monthly, endepi_monthly)
        #
        # inrange() returns 0 as soon as one of its arguments is missing, so it
        # is not one of the guards that leans on Stata sorting a missing above
        # every number, and no pl_stata_gt() is needed: a null bound makes the
        # polars predicate null and the filter drops the row, which is what
        # inrange() does. Both bounds are inclusive on either side.
        (pl.col("ref_date") >= pl.col("begepi_monthly"))
        & (pl.col("ref_date") <= pl.col("endepi_monthly"))
    )

    log.info("-> One row per month kept, begepi_monthly and endepi_monthly generated")

    # -------------------------------------------------------------------
    #  Adjust durations to end at the monthly cutoff
    # -------------------------------------------------------------------
    log.info("Adjust durations to end at the monthly cutoff")

    # 16_monthly_panel.do uses `replace ... if`, which leaves the rows that do
    # not meet the condition unchanged. The alternative branch therefore has to
    # be the existing value, not a null. None of these guards compares
    # magnitudes, so none of them needs pl_stata_gt(): they are equality tests
    # and an inlist(), and a null quelle or a null parallel_benefits fails them
    # in Stata exactly as a null condition falls through to `otherwise` here.
    is_beh = pl.col("quelle") == 1

    frame = frame.with_columns(
        overhang=pl_days(pl.col("endepi_monthly"), pl.col("ref_date")).cast(pl.Int32)
    ).with_columns(
        tage_bet=pl.when(is_beh)
        .then(pl.col("tage_bet") - pl.col("overhang"))
        .otherwise(pl.col("tage_bet")),
        tage_job=pl.when(is_beh)
        .then(pl.col("tage_job") - pl.col("overhang"))
        .otherwise(pl.col("tage_job")),
        # Stata's inlist() returns 0 for a missing argument, so
        # `!inlist(erwstat, ...)` is true where erwstat is missing and the
        # reference trims the counter there too. polars' is_in() returns null
        # for a null instead, which would leave the counter untrimmed.
        # fill_null() puts the Stata reading back, as in build_yearly_panel().
        tage_erw=pl.when(
            is_beh & ~pl.col("erwstat").fill_null(-1).is_in(AZUBI_ERWSTAT)
        )
        .then(pl.col("tage_erw") - pl.col("overhang"))
        .otherwise(pl.col("tage_erw")),
        # 16_monthly_panel.do also allows the legacy code quelle == 16, which
        # does not occur in SIAB 7523 v2.
        tage_lst=pl.when(pl.col("quelle") == 2)
        .then(pl.col("tage_lst") - pl.col("overhang"))
        .otherwise(pl.col("tage_lst")),
    ).with_columns(
        # The reference trims tage_lst in two separate `replace` statements,
        # one on quelle and one on parallel_benefits, where 16_yearly_panel.do
        # uses a single `replace` with both conditions joined by `or`.
        # 15_parallel_episodes.do sets parallel_benefits to 1 for every episode
        # of a person and episode start that includes a benefit episode, the
        # benefit episode itself included, so a surviving quelle 2 row meets
        # both conditions and the reference subtracts the overhang from it
        # twice. The port reproduces that, because the fixture comparison is
        # against the reference as published, not against a corrected version
        # of it.
        tage_lst=pl.when(pl.col("parallel_benefits") == 1)
        .then(pl.col("tage_lst") - pl.col("overhang"))
        .otherwise(pl.col("tage_lst"))
    ).drop("overhang", "ref_date")

    log.info("-> Durations adjusted")
    log.info("Monthly panel file finished")
    return frame
