"""
08.) Treat parallel episodes

Port of 15_parallel_episodes.do. A person can hold several spells that start on
the same day: two jobs at once, or a job beside a benefit receipt. The step
aggregates what the parallel spells say about each other, keeps one of them as
the 'main' episode, and numbers what is left.

Generates the variables:
  - nspell:            non-parallel spell counter
  - parallel_jobs:     number of parallel jobs
  - parallel_wage:     total wage of all parallel employment spells
  - parallel_wage_imp: total imputed wage of all parallel employment spells
  - parallel_benefits: indicator for recipience of UI benefits

Drops the variables:
  - spell
  - level1
  - level2

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

__all__ = ["handle_parallel_episodes"]

GROUP = ["persnr", "begepi"]

# There is no common rule of identifying the main episode. Usually the job with
# the longest duration or the highest wage is defined as the main episode.
# However, it is left to the user to decide which characteristic defines the
# main episode. The reference carries both sorts, one of them commented out;
# here the choice is the `handling` argument, as it is in the R arm.
#
# 15_parallel_episodes.do warns beside its own sort that the order has to be
# unambiguous, because the step then keeps the first row of each person and
# episode start. On the FDZ test data 1,945 of 479,806 groups are tied on
# quelle, tage_bet and wage_imp together, and the kept row there is whatever
# the database happens to return. `spell` closes it: the key persnr, spell,
# begepi is unique, so spell is unique within a person and episode start. The
# reference carries the same last key, as 15_parallel_episodes_tiebreak.patch.
#
# Every entry is (column, descending). The reference's line is a `gsort`, so
# the null placement follows from the direction: see `_null_placement()`.
SORT_ORDERS = {
    # gsort persnr begepi quelle -tage_bet -wage_imp spell
    "tenure": [("persnr", False), ("begepi", False), ("quelle", False),
               ("tage_bet", True), ("wage_imp", True), ("spell", False)],
    # gsort persnr begepi quelle -wage_imp -tage_bet spell
    "wage": [("persnr", False), ("begepi", False), ("quelle", False),
             ("wage_imp", True), ("tage_bet", True), ("spell", False)],
}


def _null_placement(descending: list[bool]) -> list[bool]:
    """Where a missing value goes in a `gsort`, key by key.

    Last, every time, whichever way the key runs. This is worth stating
    because the obvious reasoning gives the wrong answer: Stata stores a
    missing value as a number larger than any other, so `-tage_bet` looks as
    though it should put a missing tenure first. `gsort` does not work that
    way. Its default is `mlast`, which holds missing values back to the end
    regardless of the direction of the sort, and only `gsort -x, mfirst`
    brings them to the front. The reference's line carries no such option:

        gsort persnr begepi quelle -tage_bet -wage_imp spell

    Checked against Stata MP 17 rather than reasoned about, because the two
    readings pick a different main episode on real data. `tage_bet` and
    `wage_imp` are blank outside the employment history, and `wage_imp` can be
    blank on an employment spell whose wage never arrived, so the spells this
    decides between exist in the delivery.

    DuckDB's `default_null_order` is also NULLS_LAST in both directions, so the
    R arm reaches the same place with a bare `desc()` and the two arms agree
    here, each against the same reference.
    """
    return [True for _ in descending]


def handle_parallel_episodes(frame: pl.LazyFrame,
                             handling: str = "wage",
                             log_file: str | os.PathLike | None = None) -> pl.LazyFrame:
    log = step_logger("parallel", log_file)

    # Validate the inputs
    if handling not in SORT_ORDERS:
        raise ValueError("handling must be either set to 'wage' or 'tenure'")

    # ------------------------------------------------------
    # Identify main episode (by sorting data accordingly)
    # ------------------------------------------------------
    if handling != "wage":
        log.info("Job with longest tenure is defined as main episode")
    else:
        log.info("Job with highest wage  is defined as main episode")

    order = SORT_ORDERS[handling]
    sort_by = [column for column, _ in order]
    descending = [desc for _, desc in order]
    nulls_last = _null_placement(descending)

    # ------------------------------------------------------
    # Generate information on parallel jobs
    # (parallel_jobs, parallel_wage, parallel_wage_imp)
    # ------------------------------------------------------
    log.info("Generating info on parallel jobs and keeping only the main spell")

    # `gen tmp_jobs = quelle == 1` is 0, not missing, when quelle is missing:
    # a Stata missing is not equal to 1. fill_null() stands in for that.
    #
    # `gen tmp_wage = tentgelt if quelle == 1` leaves the other rows missing and
    # `egen total()` then counts them as nothing at all, which is why the R arm
    # writes the contribution as 0 straight away. Both readings give the same
    # sum, and the zero is what makes a group with no employment episode come
    # out at 0 rather than missing.
    frame = frame.with_columns(
        tmp_jobs=(pl.col("quelle").fill_null(-1) == 1).cast(pl.Int32),
        tmp_benefits=(pl.col("quelle").fill_null(-1) == 2).cast(pl.Int32),
        tmp_wage=pl.when(
            (pl.col("quelle").fill_null(-1) == 1) & pl.col("tentgelt").is_not_null()
        )
        .then(pl.col("tentgelt"))
        .otherwise(0.0),
        # wage_imp comes from 07_wages_imputation, which the Python arm has not
        # ported yet. It is referenced here exactly as the reference does.
        tmp_wage_imp=pl.when(
            (pl.col("quelle").fill_null(-1) == 1) & pl.col("wage_imp").is_not_null()
        )
        .then(pl.col("wage_imp"))
        .otherwise(0.0),
    )

    # The four aggregates are taken over the whole person-episode group before
    # anything is dropped, so they do not depend on the sort above. Stata keeps
    # the two wage totals as floats, which is why the reference test compares
    # them to about seven digits rather than exactly; the sum is left at full
    # precision here, as it is in the R arm.
    frame = frame.with_columns(
        # count parallel employment episodes
        parallel_jobs=pl.col("tmp_jobs").sum().over(GROUP).cast(pl.Int32),
        # total wage of all parallel employment episodes
        parallel_wage=pl.col("tmp_wage").sum().over(GROUP),
        # total (imputed) wage of all parallel employment episodes
        parallel_wage_imp=pl.col("tmp_wage_imp").sum().over(GROUP),
        # Attach indicator for recipience of UI benefits to all parallel episodes
        parallel_benefits=pl.col("tmp_benefits").max().over(GROUP).cast(pl.Int32),
    )

    # `by persnr begepi: keep if _n == 1` on the sorted data. The sort is the
    # whole selection rule, so it is spelled out as a frame sort rather than a
    # window expression, and the row number then counts the rows of each group
    # in that order.
    frame = frame.sort(
        sort_by, descending=descending, nulls_last=nulls_last, maintain_order=True
    ).filter(
        pl.int_range(pl.len()).over(GROUP) == 0
    )

    # ------------------------------------------------------
    # Generate non-parallel spell counter
    # ------------------------------------------------------
    # `by persnr: gen nspell = _n`, with the data still in the order above. The
    # group keys persnr and begepi lead that order, and one row per episode
    # start is left, so sorting on the two keys alone reproduces it. Both are
    # ascending keys, so a missing one would sort last in Stata.
    frame = frame.sort(GROUP, nulls_last=True, maintain_order=True).with_columns(
        nspell=pl.int_range(1, pl.len() + 1).over("persnr").cast(pl.Int32)
    )

    log.info("-> parallel episodes handled")

    # ------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------
    # `drop spell level1 level2`, then `sort persnr nspell`. level1 and level2
    # are the observation counters 03_SIAB_bio.do built; spell stops meaning
    # anything once the parallel rows it distinguished are gone.
    frame = frame.drop(
        "spell", "level1", "level2",
        "tmp_jobs", "tmp_benefits", "tmp_wage", "tmp_wage_imp",
    ).sort(["persnr", "nspell"], nulls_last=True, maintain_order=True)

    log.info(" ->  Cleanup finished")
    log.info("Parallel episodes file finished")

    return frame
