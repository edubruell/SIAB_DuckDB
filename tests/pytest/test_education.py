"""
Synthetic tests for the broad education variable.

The counterpart of tests/testthat/test-03_education.R. The step recodes the
FDZ's imputed training variable `ausbildung_imp` into three broad groups,
following 05_educ_broad.do:
  1  no vocational training and no degree      (ausbildung_imp 1, 3)
  2  vocational training                       (ausbildung_imp 2, 4)
  3  degree from a university or a university
     of applied science                        (ausbildung_imp 5, 6)
Every other value, including a missing one, has to come out null. The input is
the imputed variable, not the raw `ausbildung`: the raw one carries the full
administrative code list and would put most spells in the wrong group.
"""

import polars as pl

from siab.steps import generate_educ_variable


def educ_frame(imputed, **extra):
    return pl.LazyFrame(
        {"persnr": list(range(1, len(imputed) + 1)),
         "ausbildung_imp": imputed, **extra},
        schema_overrides={"persnr": pl.Int32, "ausbildung_imp": pl.Int32},
    )


def test_ausbildung_imp_is_recoded_into_the_three_broad_education_groups():
    out = generate_educ_variable(
        educ_frame([1, 2, 3, 4, 5, 6])
    ).collect().sort("persnr")

    assert out["educ"].to_list() == [1, 2, 1, 2, 3, 3]


def test_a_code_outside_1_to_6_gives_a_missing_educ():
    # ausbildung_imp only takes 1 to 6, so anything else is a data error rather
    # than a category. The reference leaves educ at Stata's missing there.
    out = generate_educ_variable(
        educ_frame([0, 7, 11, 12])
    ).collect().sort("persnr")

    assert out["educ"].to_list() == [None, None, None, None]


def test_a_missing_ausbildung_imp_gives_a_missing_educ():
    out = generate_educ_variable(
        educ_frame([None, 2])
    ).collect().sort("persnr")

    assert out["educ"].to_list() == [None, 2]


def test_the_raw_ausbildung_variable_is_ignored():
    # The two variables disagree on purpose here. Reading the raw one was the
    # R port's original bug, found by the comparison against the Stata
    # reference.
    frame = pl.LazyFrame(
        {"persnr": [1, 2], "ausbildung": [12, 1], "ausbildung_imp": [1, 5]},
        schema_overrides={"persnr": pl.Int32, "ausbildung": pl.Int32,
                          "ausbildung_imp": pl.Int32},
    )

    out = generate_educ_variable(frame).collect().sort("persnr")

    assert out["educ"].to_list() == [1, 3]


def test_educ_is_an_integer_column():
    out = generate_educ_variable(educ_frame([1, 2, 5])).collect()

    assert out.schema["educ"] == pl.Int32


def test_the_step_adds_educ_and_changes_nothing_else():
    frame = pl.LazyFrame(
        {
            "persnr": [1, 2, 3],
            "ausbildung_imp": [1, 2, 5],
            "tentgelt": [50.5, 60.5, 70.5],
            "year": [1999, 2000, 2001],
        },
        schema_overrides={"persnr": pl.Int32, "ausbildung_imp": pl.Int32,
                          "year": pl.Int32},
    )
    before = frame.collect()

    out = generate_educ_variable(frame).collect().sort("persnr")

    assert set(out.columns) == set(before.columns) | {"educ"}
    assert out.select(before.columns).equals(before)


def test_the_step_hands_a_lazyframe_back_so_it_can_be_piped():
    # The R arm's step hands its connection back; here the frame itself travels
    # the pipe, so what has to hold is that a LazyFrame comes back and the next
    # step can go on lazily from it.
    result = generate_educ_variable(educ_frame([2]))

    assert isinstance(result, pl.LazyFrame)
    assert result.filter(pl.col("educ") == 2).collect().height == 1
