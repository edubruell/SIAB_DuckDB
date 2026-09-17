"""
Synthetic tests for the observation period restriction.

The counterpart of tests/testthat/test-03d_observation_period.R.
00_master_SIAB.do keeps the episodes whose year lies in the observation period,
`keep if inrange(jahr,${minYear},${maxYear})`, and runs it after the biography
step so that the cumulative biographic variables are built over the whole
history first. The port's `year` is the reference's `jahr`.

The bounds are inclusive on both sides, which is what inrange() does.
"""

import polars as pl
import pytest

from siab.steps import restrict_observation_period


def year_frame(years, persnr=None, **extra):
    persnr = persnr if persnr is not None else list(range(1, len(years) + 1))
    return pl.LazyFrame(
        {"persnr": persnr, "year": years, **extra},
        schema_overrides={"persnr": pl.Int32, "year": pl.Int32},
    )


def test_both_bounds_are_inclusive():
    out = restrict_observation_period(
        year_frame([1994, 1995, 2010, 2023, 2024]),
        min_year=1995,
        max_year=2023,
    ).collect().sort("persnr")

    assert out["persnr"].to_list() == [2, 3, 4]


def test_the_full_span_of_the_delivery_keeps_every_episode():
    # This is the default, and the position in the pipeline is only observable
    # when a user narrows the period, so the default has to be a no-op.
    out = restrict_observation_period(
        year_frame([1975, 1999, 2023, 2023])
    ).collect()

    assert out.height == 4


def test_an_episode_with_no_year_is_dropped():
    # Stata's inrange() is false for a missing value, so the reference drops
    # such an episode as well. Every episode reaching this step has a year,
    # because the master builds it from begepi before any step runs, so this is
    # a guard against a hole upstream rather than a case the data produces.
    out = restrict_observation_period(
        year_frame([2000, None, 2001])
    ).collect().sort("persnr")

    assert out["persnr"].to_list() == [1, 3]


def test_a_single_year_period_keeps_that_year_alone():
    out = restrict_observation_period(
        year_frame([1999, 2000, 2001]), min_year=2000, max_year=2000
    ).collect()

    assert out["persnr"].to_list() == [2]


def test_a_reversed_period_is_refused_rather_than_silently_emptying_the_data():
    with pytest.raises(ValueError, match="min_year is after max_year"):
        restrict_observation_period(year_frame([2000, 2001]),
                                    min_year=2010, max_year=2000)


def test_a_bound_that_is_not_a_year_is_refused():
    with pytest.raises(ValueError, match="single years"):
        restrict_observation_period(year_frame([2000, 2001]), min_year="1995")

    with pytest.raises(ValueError, match="single years"):
        restrict_observation_period(year_frame([2000, 2001]), max_year=True)


def test_the_other_columns_survive_the_restriction():
    frame = pl.LazyFrame(
        {
            "persnr": [1, 2, 3],
            "year": [1994, 2000, 2001],
            "tage_erw": [10, 20, 30],
            "tentgelt": [1.5, 2.5, 3.5],
        },
        schema_overrides={"persnr": pl.Int32, "year": pl.Int32,
                          "tage_erw": pl.Int32},
    )

    out = restrict_observation_period(frame, min_year=1995).collect().sort("persnr")

    assert out.columns == ["persnr", "year", "tage_erw", "tentgelt"]
    assert out["tage_erw"].to_list() == [20, 30]
    assert out["tentgelt"].to_list() == [2.5, 3.5]


def test_the_step_hands_a_lazyframe_back():
    assert isinstance(restrict_observation_period(year_frame([2000])), pl.LazyFrame)
