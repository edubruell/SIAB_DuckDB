"""
Synthetic tests for the episode splitter.

These build a handful of rows in memory and check hand-computed values, so they
run without the FDZ test data and without Stata. The same cases the R arm's
tests/testthat/test-01_split_episodes.R covers.
"""

import datetime as dt

import polars as pl

from siab.steps import split_episodes


def spell_frame(begepi, endepi, gebjahr=1970):
    return pl.LazyFrame(
        {
            "persnr": [1],
            "spell": [1],
            "begepi": [begepi],
            "endepi": [endepi],
            "gebjahr": [gebjahr],
            "year": [begepi.year],
            "age": [begepi.year - gebjahr],
        },
        schema_overrides={"persnr": pl.Int32, "spell": pl.Int32,
                          "gebjahr": pl.Int32, "year": pl.Int32, "age": pl.Int32},
    )


def test_a_spell_inside_one_year_is_left_alone():
    out = split_episodes(spell_frame(dt.date(1999, 6, 1), dt.date(1999, 12, 1))).collect()

    assert out.height == 1
    assert out["begepi"][0] == dt.date(1999, 6, 1)
    assert out["endepi"][0] == dt.date(1999, 12, 1)
    assert out["begepi_orig"][0] == dt.date(1999, 6, 1)
    assert out["endepi_orig"][0] == dt.date(1999, 12, 1)
    assert out["year"][0] == 1999


def test_a_spell_over_three_years_becomes_three_rows_cut_at_year_ends():
    out = split_episodes(
        spell_frame(dt.date(1999, 6, 1), dt.date(2001, 3, 15))
    ).collect().sort("begepi")

    assert out.height == 3
    assert out["begepi"].to_list() == [
        dt.date(1999, 6, 1), dt.date(2000, 1, 1), dt.date(2001, 1, 1)
    ]
    assert out["endepi"].to_list() == [
        dt.date(1999, 12, 31), dt.date(2000, 12, 31), dt.date(2001, 3, 15)
    ]
    # The original dates ride along on every piece.
    assert out["begepi_orig"].unique().to_list() == [dt.date(1999, 6, 1)]
    assert out["endepi_orig"].unique().to_list() == [dt.date(2001, 3, 15)]
    # year and age are recomputed from the split start date.
    assert out["year"].to_list() == [1999, 2000, 2001]
    assert out["age"].to_list() == [29, 30, 31]


def test_the_pieces_keep_one_spell_number():
    out = split_episodes(
        spell_frame(dt.date(1999, 6, 1), dt.date(2000, 3, 15))
    ).collect()
    assert out["spell"].unique().to_list() == [1]


def test_the_working_columns_are_gone():
    out = split_episodes(
        spell_frame(dt.date(1999, 6, 1), dt.date(2000, 3, 15))
    ).collect()
    assert "span_year" not in out.columns
    assert "year_instance" not in out.columns
