"""
Synthetic tests for the marginal part-time income threshold step.

The step joins the threshold (Geringfuegigkeitsgrenze) on year and Rechtskreis,
then flags a spell as marginal when the daily wage is at or below it. It runs
after the assessment ceiling step, so `east` already exists.

The same cases the R arm's tests/testthat/test-05_wages_marginal.R covers, plus
the two the port's own float and missing-value handling ask for.
"""

import numpy as np
import polars as pl

from siab.common import classifications_dir, stata_float
from siab.steps import generate_limit_marginal


def lookup(file: str, value: str) -> pl.DataFrame:
    """The csv on Stata's float precision, as the step itself reads it."""
    table = pl.read_csv(classifications_dir() / file)
    return table.with_columns(
        pl.Series(value, stata_float(table[value].to_numpy()))
    )


def threshold_value(year: int, east: int) -> float:
    table = lookup("limit_marginal.csv", "limit_marginal")
    return table.filter(
        (pl.col("year") == year) & (pl.col("east") == east)
    )["limit_marginal"][0]


def frame(east_dtype=pl.Int32, **columns) -> pl.LazyFrame:
    return pl.LazyFrame(
        columns,
        schema_overrides={"persnr": pl.Int32, "year": pl.Int32,
                          "east": east_dtype, "tentgelt": pl.Float64},
    )


def test_marginal_is_one_at_or_below_the_threshold_and_zero_above_it():
    limit = threshold_value(2000, 0)

    out = generate_limit_marginal(frame(
        persnr=[1, 2, 3],
        year=[2000, 2000, 2000],
        east=[0, 0, 0],
        tentgelt=[limit - 1, limit, limit + 1],
    )).collect().sort("persnr")

    assert out["marginal"].to_list() == [1, 1, 0]
    assert out["limit_marginal"].to_list() == [limit, limit, limit]


def test_the_flag_is_never_missing_and_follows_statas_missing_value_ordering():
    # 07_wages_marginal.do starts the flag at 0 and raises it to 1 where
    # tentgelt <= limit_marginal. A missing wage is an extended missing in the
    # SIAB and sorts above everything, so it never compares as at or below the
    # threshold and stays 0, even when the threshold is missing too. A missing
    # threshold with a real wage goes the other way and gives 1. Reproduced on
    # purpose; see siab/steps/wages_marginal.py.
    out = generate_limit_marginal(frame(
        persnr=[1, 2, 3, 4],
        year=[2000, 2025, 2025, 2000],
        east=[0, 0, 0, 0],
        tentgelt=[None, 10.0, None, 50.0],
    )).collect().sort("persnr")

    assert out["marginal"].null_count() == 0
    assert out["marginal"].to_list() == [
        0,  # wage missing, threshold known: never at or below it
        1,  # threshold missing, wage real: Stata compares as at or below
        0,  # both missing: the wage's extended missing is the larger one
        0,  # 50 euro a day is above the 2000 threshold
    ]
    # The second and third row are the ones the threshold is missing on; this
    # pins that the two go different ways for the same missing threshold.
    assert out["limit_marginal"][1] is None
    assert out["limit_marginal"][2] is None


def test_a_missing_east_leaves_the_threshold_missing_from_1992_on():
    out = generate_limit_marginal(frame(
        east_dtype=pl.Float64,
        persnr=[1, 2],
        year=[1991, 2000],
        east=[None, None],
        tentgelt=[10.0, 10.0],
    )).collect().sort("persnr")

    # Before 1992 there was one nationwide threshold, and the reference assigns
    # it on the year alone, so an unknown Rechtskreis still gets a value.
    assert out["limit_marginal"][0] is not None
    assert out["limit_marginal"][0] == threshold_value(1991, 0)
    assert out["limit_marginal"][1] is None


def test_east_and_west_get_different_thresholds_in_the_years_the_law_split_them():
    out = generate_limit_marginal(frame(
        persnr=[1, 2, 3, 4],
        year=[1995, 1995, 2000, 2000],
        east=[0, 1, 0, 1],
        tentgelt=[10.0, 10.0, 10.0, 10.0],
    )).collect().sort("persnr")

    assert out["limit_marginal"][0] != out["limit_marginal"][1]  # split in 1995
    assert out["limit_marginal"][2] == out["limit_marginal"][3]  # levelled by 2000
    assert out["limit_marginal"].to_list() == [
        threshold_value(*pair)
        for pair in ((1995, 0), (1995, 1), (2000, 0), (2000, 1))
    ]


def test_an_east_column_arriving_as_a_double_still_joins():
    # The port casts east to the lookup's integer type before the join. Without
    # that cast a frame carrying east as a double would match nothing, which is
    # exactly how the column arrives out of a DuckDB round-trip in the R arm.
    out = generate_limit_marginal(frame(
        east_dtype=pl.Float64,
        persnr=[1, 2],
        year=[2000, 2000],
        east=[0.0, 1.0],
        tentgelt=[10.0, 10.0],
    )).collect().sort("persnr")

    assert out["limit_marginal"].null_count() == 0
    assert out["limit_marginal"].to_list() == [
        threshold_value(2000, 0), threshold_value(2000, 1)
    ]


def test_the_threshold_is_carried_at_statas_float_precision():
    # 07_wages_marginal.do writes the threshold with `gen`, so the reference only
    # ever held a Stata float of it. The csv carries the full double of the DM
    # conversion, and the extra digits would move the flag on a wage that sits
    # on the threshold, not just fail an exact comparison.
    limit_1975 = threshold_value(1975, 0)
    raw = pl.read_csv(classifications_dir() / "limit_marginal.csv")
    raw_1975 = raw.filter(
        (pl.col("year") == 1975) & (pl.col("east") == 0)
    )["limit_marginal"][0]

    assert limit_1975 == float(np.float32(raw_1975))
    assert limit_1975 != raw_1975

    out = generate_limit_marginal(frame(
        persnr=[1],
        year=[1975],
        east=[0],
        tentgelt=[limit_1975],
    )).collect()

    assert out["limit_marginal"][0] == limit_1975
    assert out["marginal"][0] == 1


def test_the_join_adds_no_rows_and_leaves_no_working_column_behind():
    out = generate_limit_marginal(frame(
        persnr=[1, 2, 3, 4, 5],
        year=[2010, 2010, 2010, 2010, 2010],
        east=[0, 0, 1, 1, 0],
        tentgelt=[1.0, 2.0, 3.0, 4.0, 5.0],
    )).collect()

    assert out.height == 5
    assert "east_lookup" not in out.columns
