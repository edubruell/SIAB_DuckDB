"""
Synthetic tests for the column dropper.

The counterpart of tests/testthat/test-00b_drop_empty_columns.R. 00_master_SIAB.do
drops every variable that holds only missings, in a loop over `varlist _all`
guarded by `capture assert missing(`var')`, run right after the source
restriction.

The step changes no value, only the column set, so there is nothing to compare
against a Stata fixture and the tests assert the column set directly.

The one place where the port and the reference part company is the empty
string: Stata's missing() is true for it, a polars null is a different value,
and the port drops on null alone. That case has a test of its own.
"""

import polars as pl
import pytest

from siab.steps import drop_empty_columns


def test_a_column_that_is_missing_throughout_is_dropped():
    frame = pl.LazyFrame(
        {
            "persnr": [1, 2, 3],
            "year": [2000, 2001, 2002],
            "nvvz": [None, None, None],
        },
        schema_overrides={"persnr": pl.Int32, "year": pl.Int32, "nvvz": pl.Int32},
    )

    out = drop_empty_columns(frame).collect()

    assert out.columns == ["persnr", "year"]
    assert out["persnr"].to_list() == [1, 2, 3]


def test_one_non_missing_value_is_enough_to_keep_a_column():
    frame = pl.LazyFrame(
        {
            "persnr": [1, 2, 3],
            "grund": [None, None, 154],
            "alonach": [None, None, None],
        },
        schema_overrides={"persnr": pl.Int32, "grund": pl.Int32,
                          "alonach": pl.Float64},
    )

    out = drop_empty_columns(frame).collect()

    assert out.columns == ["persnr", "grund"]
    assert out["grund"].to_list() == [None, None, 154]


def test_a_column_of_empty_strings_is_kept():
    # Stata would drop this one, because missing("") is true for a string
    # variable. In polars an empty string is a value and a null is not, and the
    # port drops on null alone. See the note in the step.
    frame = pl.LazyFrame(
        {
            "persnr": [1, 2, 3],
            "blank": ["", "", ""],
            "gone": [None, None, None],
        },
        schema_overrides={"persnr": pl.Int32, "blank": pl.String,
                          "gone": pl.String},
    )

    out = drop_empty_columns(frame).collect()

    assert out.columns == ["persnr", "blank"]
    assert out["blank"].to_list() == ["", "", ""]


def test_the_switch_keeps_every_column():
    frame = pl.LazyFrame(
        {
            "persnr": [1, 2, 3],
            "nvvz": [None, None, None],
            "blank": [None, None, None],
        },
        schema_overrides={"persnr": pl.Int32, "nvvz": pl.Int32,
                          "blank": pl.String},
    )

    out = drop_empty_columns(frame, False).collect()

    assert out.columns == ["persnr", "nvvz", "blank"]


def test_a_table_with_nothing_to_drop_is_left_alone():
    frame = pl.LazyFrame(
        {"persnr": [1, 2, 3], "tentgelt": [1.5, 2.5, 3.5]},
        schema_overrides={"persnr": pl.Int32},
    )

    out = drop_empty_columns(frame).collect()

    assert out.columns == ["persnr", "tentgelt"]
    assert out["tentgelt"].to_list() == [1.5, 2.5, 3.5]


def test_several_empty_columns_go_in_one_pass_and_the_rest_keep_their_order():
    frame = pl.LazyFrame(
        {
            "persnr": [1, 2],
            "a": [None, None],
            "year": [2000, 2001],
            "b": [None, None],
            "quelle": [1, 2],
            "c": [None, None],
        },
        schema_overrides={"persnr": pl.Int32, "a": pl.Int32, "year": pl.Int32,
                          "b": pl.Float64, "quelle": pl.Int32, "c": pl.String},
    )

    out = drop_empty_columns(frame).collect()

    assert out.columns == ["persnr", "year", "quelle"]


def test_an_empty_table_keeps_its_columns_rather_than_losing_its_schema(tmp_path):
    # With no rows every column would qualify, which would leave no schema at
    # all for the steps downstream. The step refuses and warns instead. The
    # warning goes through the step logger, so it is read back out of a log
    # file rather than caught as a Python warning.
    frame = pl.LazyFrame(
        schema={"persnr": pl.Int32, "year": pl.Int32},
    )
    log_file = tmp_path / "empty_columns.log"

    out = drop_empty_columns(frame, log_file=log_file).collect()

    assert out.columns == ["persnr", "year"]
    assert out.height == 0
    assert "no rows" in log_file.read_text()


def test_a_table_that_is_missing_throughout_is_refused():
    frame = pl.LazyFrame(
        {"a": [None, None], "b": [None, None]},
        schema_overrides={"a": pl.Int32, "b": pl.String},
    )

    with pytest.raises(ValueError, match="would leave no column at all"):
        drop_empty_columns(frame).collect()


def test_a_switch_that_is_not_true_or_false_is_refused():
    frame = pl.LazyFrame(
        {"persnr": [1, 2], "year": [2000, 2001]},
        schema_overrides={"persnr": pl.Int32, "year": pl.Int32},
    )

    with pytest.raises(ValueError, match="drop has to be True or False"):
        drop_empty_columns(frame, None)


def test_the_step_hands_a_lazyframe_back():
    frame = pl.LazyFrame(
        {"persnr": [1, 2], "nvvz": [None, None]},
        schema_overrides={"persnr": pl.Int32, "nvvz": pl.Int32},
    )

    assert isinstance(drop_empty_columns(frame), pl.LazyFrame)
