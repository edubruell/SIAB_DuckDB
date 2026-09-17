"""
01b.) Reallocation of one-time payments (deregistration reason 154)

Port of 02_grund154.do. Spells with grund == 154 carry a one-time payment that
the employer reported separately from the employment spell it belongs to, so
the daily wage on those spells is meaningless and the wage on the employment
spell is too low. Only BEH spells are affected. For the description of the
problem see Frodermann et al. (2021), Sections 5.5.1 and 5.5.12.

Procedure, following the reference step by step:
  a) spell duration (episode_length) and spell earnings (episode_entgelt)
  b) per person, establishment and year, the total earnings of the 154 spells
  c) that total is carried to every spell of the same combination (tentgelt154)
  d) the 154 spells are dropped
  e) per combination, the total duration of the remaining spells (total_length)
  f) the total from b) is spread over those spells in proportion to duration
  g) the daily wage tentgelt is recomputed and rounded to two decimals

Modifies the variable:
  - tentgelt: daily wage, raised by the reallocated one-time payments

Drops the spells with grund == 154.

Author(s): Eduard Brüll
Python/polars reimplementation of the original procedure by Heiko Stüber,
Wolfgang Dauth and Johann Eppelsheimer

Version: 1.0
Created: 2026-09-17
"""

from __future__ import annotations

import os

import polars as pl

from siab.common import assert_empty, pl_days, pl_stata_float, step_logger

__all__ = ["reallocate_one_time_payments"]

GROUP = ["persnr", "betnr", "year"]


def reallocate_one_time_payments(frame: pl.LazyFrame,
                                 log_file: str | os.PathLike | None = None) -> pl.LazyFrame:
    log = step_logger("grund154", log_file)
    log.info("Reallocation of one-time payments started")

    # ==================================================================
    #  a) to d): spell earnings, the group total, and the drop
    # ==================================================================

    # `bysort persnr betnr jahr: egen earnings154 = sum(episode_entgelt) if grund == 154`
    # followed by `egen tentgelt154 = max(earnings154)` is one grouped sum over
    # the 154 spells, carried to every row of the group. Stata's egen sum()
    # counts a missing summand as zero, and so does a polars sum.
    #
    # Both `gen` and `egen` make a float, so episode_entgelt and the group total
    # carry about seven digits and no more. That loss reaches the result: it is
    # what the rounding in g) sees.
    #
    # `drop if grund == 154` keeps rows with a missing grund, because a missing
    # value is not equal to 154 in Stata either. The non-BEH sources have no
    # deregistration reason at all, so this is not a corner case.
    frame = frame.with_columns(
        episode_length=pl_days(pl.col("endepi"), pl.col("begepi")) + 1
    ).with_columns(
        episode_entgelt=pl_stata_float(pl.col("tentgelt") * pl.col("episode_length"))
    ).with_columns(
        tentgelt154=pl_stata_float(
            pl.when(pl.col("grund").is_not_null() & (pl.col("grund") == 154))
            .then(pl.col("episode_entgelt").fill_null(0.0))
            .otherwise(0.0)
            .sum()
            .over(GROUP)
        )
    ).filter(
        pl.col("grund").is_null() | (pl.col("grund") != 154)
    )

    # `assert episode_length >= 1 & episode_length <= 366`. The reference
    # asserts before the drop; here it runs after, which tests the same rows
    # apart from the 154 spells that no longer exist.
    #
    # The assert collects, which is the one place this step leaves the lazy
    # chain. The reference asserts here and so does the port.
    assert_empty(
        frame,
        pl.col("episode_length").is_null()
        | (pl.col("episode_length") < 1)
        | (pl.col("episode_length") > 366),
        "episode_length outside 1 to 366",
    )
    log.info(" ->  episode_length is between 1 and 366 on every spell")

    # ==================================================================
    #  e) to g): spread the total and recompute the daily wage
    # ==================================================================

    # total_length is summed over the spells that survive the drop, so it can
    # only be taken after the filter above. It needs no float cast: it is a sum
    # of day counts, and every value it can take is exact in four bytes.
    #
    # The wage expression is kept in the reference's own form rather than
    # simplified to `tentgelt + tentgelt154 / total_length`: the two are equal
    # in exact arithmetic and need not be equal in the last bit of a double,
    # and this step is compared against the Stata output. The result itself is
    # not cut back to float: tentgelt is a double in the SIAB, so `replace`
    # stores the rounded value at full precision.
    #
    # The rounding is written as `0.01 * round(x / 0.01)`, which is what Stata's
    # round(x,.01) does, rather than as a two-decimal round. The two scale the
    # other way round and land a few times 1e-13 apart, which is invisible in a
    # wage and still enough to keep a spell from comparing equal.
    frame = frame.with_columns(
        tentgelt_orig=pl.col("tentgelt"),
        total_length=pl.col("episode_length").sum().over(GROUP),
    ).with_columns(
        tentgelt=pl.when(pl.col("tentgelt").is_not_null())
        .then(
            0.01
            * (
                (
                    pl.col("episode_entgelt")
                    + pl.col("tentgelt154") * pl.col("episode_length") / pl.col("total_length")
                )
                / pl.col("episode_length")
                / 0.01
            ).round(0)
        )
        .otherwise(pl.col("tentgelt"))
    )

    # `assert tentgelt >= tentgelt_orig if !missing(tentgelt_orig) & !missing(tentgelt)`
    assert_empty(
        frame,
        pl.col("tentgelt_orig").is_not_null()
        & pl.col("tentgelt").is_not_null()
        & (pl.col("tentgelt") < pl.col("tentgelt_orig")),
        "Reallocation lowered tentgelt",
    )
    log.info(" ->  no spell lost wage in the reallocation")

    # ==================================================================
    #  Clean up the working variables
    # ==================================================================

    frame = frame.drop(
        "episode_length", "episode_entgelt", "tentgelt154", "total_length",
        "tentgelt_orig",
    )

    log.info("Reallocation of one-time payments finished")
    return frame
