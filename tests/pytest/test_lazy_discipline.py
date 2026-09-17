"""
The lazy check.

The user's condition on choosing polars was that the code stays lazy and a
check proves it, instead of a habit claiming it. Every step function is fed a
LazyFrame and has to hand a LazyFrame back.

The hole in this check is known and is not a hole in the rule: it does not see
a `collect()` in the middle of a step that re-wraps its result. Two steps do
collect mid-chain, both to run an assert the reference also runs; that is
recorded here so the next reader does not mistake the check for more than it is.
"""

import datetime as dt

import polars as pl
import pytest

from siab.steps import (
    generate_biographic_variables,
    reallocate_one_time_payments,
    split_episodes,
)

STEPS = [split_episodes, reallocate_one_time_payments, generate_biographic_variables]


def one_spell() -> pl.LazyFrame:
    """A single BEH spell, enough for any step to run over without special-casing."""
    return pl.LazyFrame(
        {
            "persnr": [1],
            "spell": [1],
            "betnr": [10],
            "quelle": [1],
            "begepi": [dt.date(2000, 3, 1)],
            "endepi": [dt.date(2000, 8, 31)],
            "begorig": [dt.date(2000, 3, 1)],
            "endorig": [dt.date(2000, 8, 31)],
            "tentgelt": [100.0],
            "erwstat": [101],
            "grund": [30],
            "gebjahr": [1970],
            "year": [2000],
            "age": [30],
        },
        schema_overrides={
            "persnr": pl.Int32, "spell": pl.Int32, "betnr": pl.Int32,
            "quelle": pl.Int32, "erwstat": pl.Int32, "grund": pl.Int32,
            "gebjahr": pl.Int32, "year": pl.Int32, "age": pl.Int32,
        },
    )


@pytest.mark.parametrize("step", STEPS, ids=lambda s: s.__name__)
def test_step_takes_and_returns_a_lazyframe(step):
    result = step(one_spell())
    assert isinstance(result, pl.LazyFrame), (
        f"{step.__name__} returned {type(result).__name__}, not a LazyFrame"
    )


@pytest.mark.parametrize("step", STEPS, ids=lambda s: s.__name__)
def test_step_result_collects(step):
    """A LazyFrame that cannot be collected proves nothing, so collect it once."""
    result = step(one_spell()).collect()
    assert result.height >= 1
