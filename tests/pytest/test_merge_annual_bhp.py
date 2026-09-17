"""
Synthetic tests for the yearly BHP merge.

These build five tiny establishment files and a handful of SIAB episodes in a
tmp_path, run merge_annual_bhp() over them and check hand-computed values, so
they run without the FDZ test data and without Stata. They cover what the R
arm's tests/testthat/test-reference-11_merge_BHP.R can only assert on the
fixture, and one thing it cannot: Stata's `update` option, in both directions,
on data small enough to read.

`update` without `replace` fills a missing value in the master from the using
file and leaves a non-missing one alone. `besch` is the one column two of the
five files carry, the entry merge runs before the exit merge, so:

  entry missing, exit has a value   -> the exit value arrives
  entry has a value, exit has one   -> the entry value stays
  entry has a value, no exit row    -> the entry value stays
  neither file has a value          -> the column stays missing

A plain left join on the exit merge would overwrite instead of fill and would
fail the second and third of those.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd
import polars as pl
import pyreadstat
import pytest

from siab.steps import merge_annual_bhp

PREFIX = "TEST_BHP"
YEARS = range(1999, 2001)


def write_dta(path, columns: dict) -> None:
    """Write one establishment file the way the delivery ships it."""
    pyreadstat.write_dta(pd.DataFrame(columns), str(path))


def episodes() -> pl.LazyFrame:
    """Five SIAB episodes, each with a different fate in the merge.

    persnr 1  betnr 10, 1999  matches every file; entry leaves besch missing
    persnr 2  betnr 10, 2000  matches every file; entry and exit both hold besch
    persnr 3  betnr 20, 1999  matches the yearly block, outflow and entry
    persnr 4  betnr 20, 2000  the establishment exists, but not in that year
    persnr 5  no betnr         a benefit episode, which no file can reach
    """
    return pl.LazyFrame(
        {
            "persnr": [1, 2, 3, 4, 5],
            "spell": [1, 1, 1, 1, 1],
            "begepi": [dt.date(1999, 3, 1), dt.date(2000, 3, 1),
                       dt.date(1999, 5, 1), dt.date(2000, 5, 1),
                       dt.date(2000, 7, 1)],
            "betnr": [10, 10, 20, 20, None],
            "year": [1999, 2000, 1999, 2000, 2000],
            "quelle": [1, 1, 1, 1, 2],
        },
        schema_overrides={"persnr": pl.Int32, "spell": pl.Int32,
                          "betnr": pl.Int32, "year": pl.Int32,
                          "quelle": pl.Int32},
    )


@pytest.fixture
def bhp_folder(tmp_path):
    """The five files of 11_merge_BHP.do, two calendar years wide."""
    write_dta(tmp_path / f"{PREFIX}_bhp_v1_1999.dta",
              {"betnr_siab": [10, 20], "jahr": [1999, 1999],
               "az_f": [11, 21], "az_reg": [1, 2]})
    # betnr 99 is in no episode: a using row that matches nothing is dropped.
    write_dta(tmp_path / f"{PREFIX}_bhp_v1_2000.dta",
              {"betnr_siab": [10, 99], "jahr": [2000, 2000],
               "az_f": [12, 99], "az_reg": [3, 9]})

    write_dta(tmp_path / f"{PREFIX}_bhp_inflow_v1.dta",
              {"betnr_siab": [10, 10], "jahr": [1999, 2000],
               "ein_ges": [100, 200]})
    write_dta(tmp_path / f"{PREFIX}_bhp_outflow_v1.dta",
              {"betnr_siab": [20], "jahr": [1999], "aus_ges": [50]})

    # besch is the shared column. The first entry row leaves it missing on
    # purpose, so the exit merge has something to fill.
    write_dta(tmp_path / f"{PREFIX}_bhp_entry_v1.dta",
              {"betnr_siab": [10, 10, 20], "jahr": [1999, 2000, 1999],
               "eintritt": [1, 0, 1], "besch": [float("nan"), 5.0, 3.0]})
    write_dta(tmp_path / f"{PREFIX}_bhp_exit_v1.dta",
              {"betnr_siab": [10, 10], "jahr": [1999, 2000],
               "austritt": [0, 1], "besch": [7.0, 9.0]})
    return tmp_path


def merged(folder, years=YEARS, **kwargs) -> pl.DataFrame:
    out = merge_annual_bhp(episodes(), bhp_folder=folder, prefix=PREFIX,
                           years=years, **kwargs)
    assert isinstance(out, pl.LazyFrame), "the step must hand back a LazyFrame"
    return out.collect().sort("persnr")


# ====================================================================
#  What every left join has to leave alone
# ====================================================================


def test_the_merge_keeps_every_episode(bhp_folder):
    out = merged(bhp_folder)
    assert out.height == 5
    assert out["persnr"].to_list() == [1, 2, 3, 4, 5]
    assert out["spell"].to_list() == [1, 1, 1, 1, 1]


def test_a_using_row_that_matches_nothing_is_discarded(bhp_folder):
    """The 2000 block carries an establishment no episode has. `keep(master
    match match_update)` drops it rather than adding a row for it."""
    out = merged(bhp_folder)
    assert out.height == 5
    assert 99.0 not in out["az_f"].to_list()


def test_a_benefit_episode_comes_out_empty(bhp_folder):
    """Only employment episodes carry a betnr, so nothing can reach quelle 2."""
    out = merged(bhp_folder)
    row = out.filter(pl.col("persnr") == 5)
    for column in ("az_f", "az_reg", "ein_ges", "aus_ges",
                   "eintritt", "austritt", "besch"):
        assert row[column][0] is None, f"{column} reached a benefit episode"


def test_an_establishment_outside_its_year_matches_nothing(bhp_folder):
    """betnr 20 exists, but only in 1999, so the 2000 episode stays empty."""
    row = merged(bhp_folder).filter(pl.col("persnr") == 4)
    for column in ("az_f", "az_reg", "ein_ges", "aus_ges",
                   "eintritt", "austritt", "besch"):
        assert row[column][0] is None, f"{column} matched on betnr alone"


# ====================================================================
#  The columns each of the five files brings
# ====================================================================


def test_the_yearly_blocks_join_on_betnr_and_year(bhp_folder):
    out = merged(bhp_folder)
    assert out["az_f"].to_list() == [11.0, 12.0, 21.0, None, None]
    assert out["az_reg"].to_list() == [1.0, 3.0, 2.0, None, None]


def test_the_flow_files_arrive(bhp_folder):
    out = merged(bhp_folder)
    assert out["ein_ges"].to_list() == [100.0, 200.0, None, None, None]
    assert out["aus_ges"].to_list() == [None, None, 50.0, None, None]


def test_the_entry_and_exit_columns_arrive(bhp_folder):
    out = merged(bhp_folder)
    assert out["eintritt"].to_list() == [1.0, 0.0, 1.0, None, None]
    assert out["austritt"].to_list() == [0.0, 1.0, None, None, None]


# ====================================================================
#  Stata's `update`, in both directions
# ====================================================================


def test_update_fills_a_missing_besch_from_the_exit_file(bhp_folder):
    """Entry leaves persnr 1 without a besch; the exit file's 7 fills it."""
    row = merged(bhp_folder).filter(pl.col("persnr") == 1)
    assert row["besch"][0] == 7.0


def test_update_does_not_overwrite_a_besch_the_entry_file_set(bhp_folder):
    """Both files describe persnr 2. Entry runs first, so its 5 stays."""
    row = merged(bhp_folder).filter(pl.col("persnr") == 2)
    assert row["besch"][0] == 5.0, "the exit merge overwrote instead of filling"


def test_a_besch_with_no_exit_row_survives(bhp_folder):
    row = merged(bhp_folder).filter(pl.col("persnr") == 3)
    assert row["besch"][0] == 3.0


def test_besch_stays_missing_where_neither_file_has_one(bhp_folder):
    out = merged(bhp_folder)
    assert out["besch"].to_list() == [7.0, 5.0, 3.0, None, None]


def test_the_exit_file_alone_gives_its_own_besch(bhp_folder):
    """The counterfactual: without the entry merge, besch is the exit value.

    This is what makes the test above a statement about `update` rather than
    about the exit file being empty.
    """
    out = merged(bhp_folder, modules=("exit",))
    assert out["besch"].to_list() == [7.0, 9.0, None, None, None]


def test_the_entry_file_alone_gives_its_own_besch(bhp_folder):
    out = merged(bhp_folder, modules=("entry",))
    assert out["besch"].to_list() == [None, 5.0, 3.0, None, None]


def test_the_module_order_is_the_references_and_not_the_callers(bhp_folder):
    """Entry before exit, however the modules are listed. `update` is not
    symmetric, so the order is part of the answer."""
    out = merged(bhp_folder, modules=("exit", "entry"))
    assert out["besch"].to_list() == [7.0, 5.0, 3.0, None, None]


def test_no_using_suffix_column_survives_the_update(bhp_folder):
    out = merged(bhp_folder)
    assert [c for c in out.columns if c.endswith("_using")] == []


# ====================================================================
#  Refusals and skips
# ====================================================================


def test_no_modules_hands_the_frame_back_untouched(bhp_folder):
    out = merge_annual_bhp(episodes(), bhp_folder=bhp_folder, prefix=PREFIX,
                           years=YEARS, modules=()).collect()
    assert out.columns == episodes().collect_schema().names()
    assert out.height == 5


def test_an_unknown_module_is_refused(bhp_folder):
    with pytest.raises(ValueError, match="Unknown BHP module"):
        merge_annual_bhp(episodes(), bhp_folder=bhp_folder, prefix=PREFIX,
                         years=YEARS, modules=("annual", "nonsense"))


def test_a_missing_folder_argument_is_refused():
    with pytest.raises(ValueError, match="bhp_folder"):
        merge_annual_bhp(episodes(), bhp_folder=None, years=YEARS)


def test_absent_yearly_files_warn_and_skip(tmp_path):
    """The reference's five switches are off by default because the files have
    to be requested separately, so an absent module warns rather than stops."""
    with pytest.warns(UserWarning, match="No yearly BHP files"):
        out = merge_annual_bhp(episodes(), bhp_folder=tmp_path, prefix=PREFIX,
                               years=YEARS, modules=("annual",)).collect()
    assert out.height == 5
    assert "az_f" not in out.columns


def test_an_absent_extension_file_warns_and_skips(bhp_folder):
    (bhp_folder / f"{PREFIX}_bhp_exit_v1.dta").unlink()
    with pytest.warns(UserWarning, match="exit module skipped"):
        out = merge_annual_bhp(episodes(), bhp_folder=bhp_folder,
                               prefix=PREFIX, years=YEARS,
                               modules=("entry", "exit")).collect()
    assert out.height == 5
    # Without the exit merge, besch is whatever entry left behind.
    assert out.sort("persnr")["besch"].to_list() == [None, 5.0, 3.0, None, None]


def test_a_using_file_that_repeats_a_key_is_refused(bhp_folder):
    """The reference merges m:1, which errors on a repeated key; so does this."""
    write_dta(bhp_folder / f"{PREFIX}_bhp_inflow_v1.dta",
              {"betnr_siab": [10, 10], "jahr": [1999, 1999],
               "ein_ges": [100, 101]})
    with pytest.raises(ValueError, match="not unique by betnr and year"):
        merge_annual_bhp(episodes(), bhp_folder=bhp_folder, prefix=PREFIX,
                         years=YEARS, modules=("inflow",)).collect()


def test_the_years_argument_bounds_what_is_read(bhp_folder):
    """Only the 1999 block is asked for, so the 2000 episode stays empty."""
    out = merged(bhp_folder, years=range(1999, 2000), modules=("annual",))
    assert out["az_f"].to_list() == [11.0, None, 21.0, None, None]
