"""
Synthetic tests for the wage deflation step.

The step joins the consumer price index on year and divides three money
variables by it, following 08_wages_deflation.do:

    wage_defl           = 100 * tentgelt       / cpi
    limit_marginal_defl = 100 * limit_marginal / cpi
    limit_assess_defl   = 100 * limit_assess   / cpi

The index has 2015 as its base, so a 2015 wage has to come back unchanged.

The same cases the R arm's tests/testthat/test-06_wages_deflation.R covers, plus
the one the port's own float handling asks for.
"""

import numpy as np
import polars as pl
import pytest

from siab.common import classifications_dir, stata_float
from siab.steps import deflate_wages


def cpi_table() -> pl.DataFrame:
    """classifications/cpi.csv on Stata's float precision, as the step reads it."""
    table = pl.read_csv(classifications_dir() / "cpi.csv")
    return table.with_columns(pl.Series("cpi", stata_float(table["cpi"].to_numpy())))


def cpi_value(year: int) -> float:
    return cpi_table().filter(pl.col("year") == year)["cpi"][0]


def frame(**columns) -> pl.LazyFrame:
    return pl.LazyFrame(
        columns,
        schema_overrides={"persnr": pl.Int32, "year": pl.Int32,
                          "tentgelt": pl.Float64, "limit_marginal": pl.Float64,
                          "limit_assess": pl.Float64},
    )


def test_the_three_deflated_variables_are_the_nominal_ones_over_the_index():
    years = [1975, 1990, 2015, 2024]
    tentgelt = [30.0, 60.0, 90.0, 120.0]
    limit_marginal = [5.0, 10.0, 15.0, 20.0]
    limit_assess = [50.0, 100.0, 150.0, 200.0]

    out = deflate_wages(frame(
        persnr=[1, 2, 3, 4],
        year=years,
        tentgelt=tentgelt,
        limit_marginal=limit_marginal,
        limit_assess=limit_assess,
    )).collect().sort("persnr")

    index = [cpi_value(y) for y in years]
    assert out["cpi"].to_list() == index
    assert out["wage_defl"].to_list() == [
        100 * w / c for w, c in zip(tentgelt, index)
    ]
    assert out["limit_marginal_defl"].to_list() == [
        100 * w / c for w, c in zip(limit_marginal, index)
    ]
    assert out["limit_assess_defl"].to_list() == [
        100 * w / c for w, c in zip(limit_assess, index)
    ]


def test_2015_is_the_base_year_so_nothing_moves():
    out = deflate_wages(frame(
        persnr=[1],
        year=[2015],
        tentgelt=[123.45],
        limit_marginal=[11.11],
        limit_assess=[222.22],
    )).collect()

    # The index is exactly 100 in the base year and the division is exact, so
    # this is an equality and not an approximation.
    assert out["cpi"][0] == 100.0
    assert out["wage_defl"][0] == 123.45
    assert out["limit_marginal_defl"][0] == 11.11
    assert out["limit_assess_defl"][0] == 222.22


def test_deflating_raises_pre_2015_wages_and_lowers_post_2015_wages():
    out = deflate_wages(frame(
        persnr=[1, 2, 3],
        year=[1980, 2015, 2024],
        tentgelt=[100.0, 100.0, 100.0],
        limit_marginal=[10.0, 10.0, 10.0],
        limit_assess=[200.0, 200.0, 200.0],
    )).collect().sort("persnr")

    assert out["wage_defl"][0] > 100
    assert out["wage_defl"][1] == 100
    assert out["wage_defl"][2] < 100


def test_a_missing_wage_stays_missing_and_the_row_survives():
    out = deflate_wages(frame(
        persnr=[1, 2],
        year=[2000, 2000],
        tentgelt=[None, 50.0],
        limit_marginal=[10.0, 10.0],
        limit_assess=[200.0, 200.0],
    )).collect().sort("persnr")

    assert out.height == 2
    assert out["wage_defl"][0] is None
    assert out["wage_defl"][1] is not None
    # The two statutory limits are unaffected by the missing wage.
    assert out["limit_marginal_defl"].null_count() == 0
    assert out["limit_assess_defl"].null_count() == 0


def test_a_year_the_index_does_not_cover_gives_null_rather_than_dropping_the_row():
    out = deflate_wages(frame(
        persnr=[1, 2],
        year=[1974, 2025],
        tentgelt=[50.0, 50.0],
        limit_marginal=[10.0, 10.0],
        limit_assess=[200.0, 200.0],
    )).collect().sort("persnr")

    assert out.height == 2
    assert out["cpi"].null_count() == 2
    assert out["wage_defl"].null_count() == 2
    assert out["limit_marginal_defl"].null_count() == 2
    assert out["limit_assess_defl"].null_count() == 2


def test_the_index_is_carried_at_statas_float_precision():
    # 08_wages_deflation.do builds the index with `gen`/`replace`, which gives a
    # Stata float, and rebases the years up to 1991 in that same float. The csv
    # holds the rebased double, so the port has to put it back on float before
    # anything divides by it.
    raw = pl.read_csv(classifications_dir() / "cpi.csv")
    raw_1975 = raw.filter(pl.col("year") == 1975)["cpi"][0]

    out = deflate_wages(frame(
        persnr=[1],
        year=[1975],
        tentgelt=[100.0],
        limit_marginal=[10.0],
        limit_assess=[200.0],
    )).collect()

    assert out["cpi"][0] == float(np.float32(raw_1975))
    assert out["cpi"][0] != raw_1975
    assert out["wage_defl"][0] == pytest.approx(100 * 100 / float(np.float32(raw_1975)),
                                                rel=0, abs=0)


def test_the_join_adds_no_rows_and_leaves_the_input_columns_alone():
    columns = dict(
        persnr=[1, 2, 3, 4, 5, 6],
        year=[1980, 1980, 1990, 2000, 2010, 2020],
        tentgelt=[10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        limit_marginal=[10.0] * 6,
        limit_assess=[200.0] * 6,
    )
    given = frame(**columns)
    out = deflate_wages(given).collect().sort("persnr")

    assert out.height == 6
    assert out.select(list(columns)).equals(given.collect().sort("persnr"))
    assert set(out.columns) - set(columns) == {
        "cpi", "wage_defl", "limit_marginal_defl", "limit_assess_defl"
    }
