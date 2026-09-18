"""
Synthetic tests for the contribution assessment ceiling step.

The step builds the Rechtskreis dummy `east` from the establishment's federal
state `ao_bula` and then joins the contribution assessment ceiling
(Beitragsbemessungsgrenze) for that year and Rechtskreis. Berlin is the case
that decides the whole variable: it counts as West up to 1991 and as East from
1992, which is what 06_wages_assessment_ceiling.do does.

The same cases the R arm's tests/testthat/test-04_wage_assesment_ceiling.R
covers, plus the one the port's own float handling asks for.
"""

import numpy as np
import polars as pl
import pytest

from siab.common import classifications_dir, stata_float
from siab.steps import generate_limit_assess


def lookup(file: str, value: str) -> pl.DataFrame:
    """The csv on Stata's float precision, as the step itself reads it.

    The counterpart of the R arm's siab_lookup(); the rounding is what makes a
    comparison for equality mean anything, because the reference only ever held
    seven digits of these figures.
    """
    table = pl.read_csv(classifications_dir() / file)
    return table.with_columns(
        pl.Series(value, stata_float(table[value].to_numpy()))
    )


def ceiling_value(year: int, east: int) -> float:
    table = lookup("wa_ceiling.csv", "limit_assess")
    return table.filter(
        (pl.col("year") == year) & (pl.col("east") == east)
    )["limit_assess"][0]


def frame(**columns) -> pl.LazyFrame:
    return pl.LazyFrame(
        columns,
        schema_overrides={"persnr": pl.Int32, "year": pl.Int32,
                          "ao_bula": pl.Int32},
    )


def test_east_follows_the_rechtskreis_split_with_berlin_changing_side_in_1992():
    out = generate_limit_assess(frame(
        persnr=list(range(1, 9)),
        year=[1991, 1992, 1991, 1991, 2000, 2000, 2000, 2000],
        ao_bula=[11, 11, 1, 12, 9, 16, 10, 13],
    )).collect().sort("persnr")

    assert out["east"].to_list() == [
        0,  # Berlin 1991, still West
        1,  # Berlin 1992, now East
        0,  # Schleswig-Holstein
        1,  # Brandenburg
        0,  # Bavaria
        1,  # Thuringia
        0,  # Saarland
        1,  # Mecklenburg-Western Pomerania
    ]


def test_an_unknown_or_missing_federal_state_gives_a_missing_east():
    out = generate_limit_assess(frame(
        persnr=[1, 2, 3],
        year=[2000, 2000, 2000],
        ao_bula=[None, 17, 99],
    )).collect().sort("persnr")

    assert out["east"].null_count() == out.height
    assert out["limit_assess"].null_count() == out.height


def test_berlin_with_a_missing_year_falls_to_east_as_it_does_in_stata():
    # The first branch, ao_bula == 11 & year < 1992, cannot be true when the
    # year is missing, so the row drops through to the East branch. Stata's
    # `if ao_bula==11 & jahr<1992` behaves the same way, because a missing jahr
    # is not less than 1992. The test records the behaviour rather than
    # endorsing it.
    out = generate_limit_assess(
        frame(persnr=[1], year=[None], ao_bula=[11])
    ).collect()

    assert out["east"][0] == 1


def test_limit_assess_is_the_value_the_statutory_table_holds():
    out = generate_limit_assess(frame(
        persnr=[1, 2, 3, 4],
        year=[1980, 2000, 2000, 2024],
        ao_bula=[9, 9, 14, 14],
    )).collect().sort("persnr")

    expected = [ceiling_value(*pair)
                for pair in ((1980, 0), (2000, 0), (2000, 1), (2024, 1))]
    assert out["limit_assess"].to_list() == expected


def test_the_ceiling_is_carried_at_statas_float_precision():
    # classifications/wa_ceiling.csv holds the 2024 East ceiling as the round
    # 244.26, but 06_wages_assessment_ceiling.do writes it with `gen`, which
    # gives a Stata float. The port has to hand over the neighbouring float, or
    # the exact comparison against the reference fails on every 2024 East row.
    out = generate_limit_assess(
        frame(persnr=[1], year=[2024], ao_bula=[14])
    ).collect()

    assert out["limit_assess"][0] == float(np.float32(244.26))
    assert out["limit_assess"][0] != 244.26
    assert out["limit_assess"][0] == pytest.approx(244.259995, abs=1e-5)


def test_a_year_outside_the_statutory_table_gives_a_missing_ceiling():
    out = generate_limit_assess(frame(
        persnr=[1, 2],
        year=[1974, 2025],
        ao_bula=[9, 9],
    )).collect().sort("persnr")

    assert out.height == 2
    assert out["limit_assess"].null_count() == 2


def test_a_missing_east_still_gets_the_nationwide_ceiling_before_1992():
    # 06_wages_assessment_ceiling.do assigns the ceiling on the year alone up to
    # 1991 and only conditions on east from 1992 on, so a spell whose federal
    # state is unknown keeps a ceiling in the earlier years.
    out = generate_limit_assess(frame(
        persnr=[1, 2],
        year=[1991, 1992],
        ao_bula=[None, None],
    )).collect().sort("persnr")

    assert out["east"].null_count() == 2
    assert out["limit_assess"][0] == ceiling_value(1991, 0)
    assert out["limit_assess"][1] is None


def test_the_join_adds_no_rows_and_leaves_no_working_column_behind():
    out = generate_limit_assess(frame(
        persnr=[1, 2, 3, 4, 5],
        year=[1980, 1991, 1992, 2000, 2024],
        ao_bula=[9, 11, 11, 14, 14],
    )).collect()

    assert out.height == 5
    assert "east_lookup" not in out.columns
