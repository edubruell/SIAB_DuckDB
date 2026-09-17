"""
Synthetic tests for build_monthly_panel().

The step is the port of 16_monthly_panel.do: total the days and the earnings of
each calendar year, cut every episode into one row per calendar month, keep the
row whose month holds the cutoff day inside the episode, and trim the four
duration counters so they end on that day.

These are the Python arm's copy of tests/testthat/test-09b_monthly_panel.R,
which pins the same arithmetic on the same two episodes, with a few cases added
that the R file leaves to its fixture comparison. There is no fixture
comparison on this side yet: the step reads `parallel_wage_imp`, which
handle_parallel_episodes() builds out of the imputed wage, and although the
imputation is ported now, tests/pytest/make_py_dumps.py still writes no
16_monthly_panel.parquet to hold against the committed Stata one. These tables
are the whole of the step's coverage for now.
"""

import datetime as dt

import polars as pl
import pytest

from siab.steps import build_monthly_panel

SCHEMA = {
    "persnr": pl.Int32,
    "year": pl.Int32,
    "quelle": pl.Int32,
    "erwstat": pl.Int32,
    "parallel_benefits": pl.Int32,
    "tage_bet": pl.Int32,
    "tage_job": pl.Int32,
    "tage_erw": pl.Int32,
    "tage_lst": pl.Int32,
}


def episodes(rows=None):
    """One employment episode and one benefit episode, both inside 2000.

    The same two rows tests/testthat/test-09b_monthly_panel.R builds. Person 1
    works from 20 January to 10 April, 82 days; person 2 receives benefits from
    1 March to 31 May, 92 days.
    """
    if rows is None:
        rows = [
            {"persnr": 1, "begepi": dt.date(2000, 1, 20),
             "endepi": dt.date(2000, 4, 10), "quelle": 1, "erwstat": 101,
             "parallel_benefits": 0, "parallel_wage_imp": 100.0,
             "tage_bet": 82, "tage_job": 82, "tage_erw": 82, "tage_lst": 0},
            {"persnr": 2, "begepi": dt.date(2000, 3, 1),
             "endepi": dt.date(2000, 5, 31), "quelle": 2, "erwstat": None,
             "parallel_benefits": 1, "parallel_wage_imp": 0.0,
             "tage_bet": 0, "tage_job": 0, "tage_erw": 0, "tage_lst": 92},
        ]

    default = {
        "persnr": 1,
        "year": 2000,
        "begepi": dt.date(2000, 1, 1),
        "endepi": dt.date(2000, 12, 31),
        "quelle": 1,
        "erwstat": 101,
        "parallel_benefits": 0,
        "parallel_wage_imp": 100.0,
        "tage_bet": 1000,
        "tage_job": 1000,
        "tage_erw": 1000,
        "tage_lst": 1000,
    }
    filled = [{**default, **row} for row in rows]
    return pl.LazyFrame(
        {key: [row[key] for row in filled] for key in default},
        schema_overrides=SCHEMA,
    )


def collected(frame, **kwargs):
    return build_monthly_panel(frame, **kwargs).collect().sort(["persnr", "month"])


# ======================================================================
#  Which months survive
# ======================================================================

def test_an_episode_becomes_one_row_per_month_whose_cutoff_day_it_covers():
    out = collected(episodes())
    employment = out.filter(pl.col("persnr") == 1)

    # Person 1 runs 20 January to 10 April. January and April both fall out:
    # the 15th lies before the episode starts in January and after it ends in
    # April.
    assert employment["month"].to_list() == [dt.date(2000, 2, 1), dt.date(2000, 3, 1)]
    assert employment["month_num"].to_list() == [2, 3]
    assert employment["begepi_monthly"].to_list() == [
        dt.date(2000, 2, 1), dt.date(2000, 3, 1)
    ]
    assert employment["endepi_monthly"].to_list() == [
        dt.date(2000, 2, 29), dt.date(2000, 3, 31)
    ]


def test_the_first_and_last_month_are_cut_to_the_episode_not_to_the_month():
    # At a cutoff of the 5th both end months of person 1's episode survive, and
    # the bounds then show the cut against the episode rather than the month.
    out = collected(episodes(), cutoff_day=5)
    employment = out.filter(pl.col("persnr") == 1)

    assert employment["month_num"].to_list() == [2, 3, 4]
    # January is gone: the 5th lies before the episode starts.
    assert employment["begepi_monthly"][0] == dt.date(2000, 2, 1)
    # April ends where the episode ends, not at the month's end.
    assert employment["endepi_monthly"][-1] == dt.date(2000, 4, 10)


def test_the_starting_month_survives_when_the_cutoff_falls_inside_it():
    out = collected(episodes(), cutoff_day=20)
    employment = out.filter(pl.col("persnr") == 1)

    assert employment["month_num"].to_list() == [1, 2, 3]
    # January starts where the episode starts, not at the month's first day.
    assert employment["begepi_monthly"][0] == dt.date(2000, 1, 20)
    assert employment["endepi_monthly"][0] == dt.date(2000, 1, 31)


def test_a_cutoff_day_longer_than_the_month_is_capped_at_the_month_s_end():
    out = collected(episodes(), cutoff_day=31)

    # February 2000 has 29 days. Without the cap the month would fall out of the
    # panel; with it the row survives and ends on 29 February.
    february = out.filter(
        (pl.col("persnr") == 1) & (pl.col("month") == dt.date(2000, 2, 1))
    )
    assert february.height == 1
    assert february["endepi_monthly"][0] == dt.date(2000, 2, 29)


def test_the_month_and_the_year_agree_with_the_episode():
    out = collected(episodes())

    assert (out["month"].dt.year() == out["year"]).all()
    assert (out["month"].dt.month() == out["month_num"]).all()
    assert (out["begepi_monthly"] >= out["begepi"]).all()
    assert (out["endepi_monthly"] <= out["endepi"]).all()


# ======================================================================
#  The three yearly totals and the monthly_vars switch
# ======================================================================

def test_the_three_yearly_totals_are_the_ones_the_yearly_panel_builds():
    out = collected(episodes())

    # Person 1 works 82 days at an imputed 100, person 2 receives benefits for 92.
    employment = out.filter(pl.col("persnr") == 1)
    benefits = out.filter(pl.col("persnr") == 2)

    assert employment["year_days_emp"].unique().to_list() == [82]
    assert employment["year_labor_earn"].unique().to_list() == [8200.0]
    assert benefits["year_days_benefits"].unique().to_list() == [92]


def test_every_month_of_a_year_carries_the_same_yearly_totals():
    out = collected(episodes())
    employment = out.filter(pl.col("persnr") == 1)

    assert employment.height == 2
    assert employment["year_days_emp"].n_unique() == 1


def test_monthly_vars_false_leaves_the_three_yearly_totals_out():
    out = collected(episodes(), monthly_vars=False)

    for column in ("year_days_emp", "year_days_benefits", "year_labor_earn"):
        assert column not in out.columns


def test_monthly_vars_changes_nothing_but_the_three_totals():
    with_totals = collected(episodes())
    without = collected(episodes(), monthly_vars=False)

    assert without.height == with_totals.height
    shared = [c for c in without.columns if c in with_totals.columns]
    assert with_totals.select(shared).equals(without.select(shared))


# ======================================================================
#  Trimming the four counters
# ======================================================================

def test_the_employment_counters_end_on_the_cutoff_day():
    out = collected(episodes())
    employment = out.filter(pl.col("persnr") == 1)

    # 82 days less the part of the month that runs past the 15th: 14 days in
    # February and 16 in March.
    assert employment["tage_bet"].to_list() == [82 - 14, 82 - 16]
    assert employment["tage_job"].to_list() == employment["tage_bet"].to_list()
    assert employment["tage_erw"].to_list() == employment["tage_bet"].to_list()
    # quelle 1 and no parallel benefit, so the benefit counter is left alone.
    assert employment["tage_lst"].to_list() == [0, 0]


# 16_monthly_panel.do trims tage_lst in two separate `replace` statements, one
# on quelle and one on parallel_benefits, and 15_parallel_episodes.do sets
# parallel_benefits on the benefit episode itself, so a surviving LeH row meets
# both conditions and loses the overhang twice. 16_yearly_panel.do joins the
# two conditions in one statement and subtracts once; test_yearly_panel.py pins
# that side. The port reproduces the reference as published.
def test_a_benefit_episode_loses_the_overhang_from_tage_lst_twice():
    out = collected(episodes())
    benefits = out.filter(pl.col("persnr") == 2)

    # March, April and May, with 16, 15 and 16 days past the 15th.
    assert benefits["month_num"].to_list() == [3, 4, 5]
    assert benefits["tage_lst"].to_list() == [92 - 2 * 16, 92 - 2 * 15, 92 - 2 * 16]


def test_an_employment_episode_beside_a_benefit_loses_the_overhang_once():
    # quelle 1 meets only the parallel_benefits half of the double trim.
    out = collected(episodes([
        {"quelle": 1, "parallel_benefits": 1,
         "begepi": dt.date(2000, 3, 1), "endepi": dt.date(2000, 3, 31)},
    ]))

    assert out.height == 1
    assert out["tage_lst"][0] == 1000 - 16


def test_a_trainee_keeps_the_employment_counter_but_loses_the_other_two():
    out = collected(episodes([
        {"erwstat": 102,
         "begepi": dt.date(2000, 3, 1), "endepi": dt.date(2000, 3, 31)},
    ]))

    assert out["tage_erw"][0] == 1000
    assert out["tage_bet"][0] == 1000 - 16
    assert out["tage_job"][0] == 1000 - 16


# Stata's inlist() returns 0 for a missing argument, so `!inlist(erwstat, ...)`
# is true where erwstat is missing and the reference trims the counter there
# too, as it does in the yearly panel.
def test_an_employment_episode_with_no_status_still_has_its_counter_trimmed():
    out = collected(episodes([
        {"erwstat": None,
         "begepi": dt.date(2000, 3, 1), "endepi": dt.date(2000, 3, 31)},
    ]))

    assert out["tage_erw"][0] == 1000 - 16
    assert out["tage_erw"][0] is not None


def test_no_counter_comes_out_missing_where_it_went_in_with_a_value():
    out = collected(episodes())

    for column in ("tage_bet", "tage_job", "tage_erw", "tage_lst"):
        assert out[column].null_count() == 0, f"{column} came out missing somewhere"


def test_the_working_columns_do_not_survive():
    out = collected(episodes())

    for column in ("month_counter", "month_index", "ref_date", "overhang",
                   "dur_emp", "dur_benefits"):
        assert column not in out.columns


# ======================================================================
#  The guards
# ======================================================================

# The reference numbers the copies of an expanded row within persnr, begepi and
# endepi, which is only unambiguous while those three identify a row, as they do
# after 15_parallel_episodes.do.
def test_the_step_refuses_data_that_still_holds_parallel_episodes():
    doubled = episodes([
        {"persnr": 1, "begepi": dt.date(2000, 1, 20), "endepi": dt.date(2000, 4, 10)},
        {"persnr": 1, "begepi": dt.date(2000, 1, 20), "endepi": dt.date(2000, 4, 10)},
    ])

    with pytest.raises(ValueError, match="do not identify a row"):
        build_monthly_panel(doubled).collect()


@pytest.mark.parametrize("bad", [0, 32, -1, 2.5, "15", True, None])
def test_a_cutoff_day_outside_one_to_thirty_one_is_refused(bad):
    with pytest.raises(ValueError, match="between 1 and 31"):
        build_monthly_panel(episodes(), cutoff_day=bad)


@pytest.mark.parametrize("bad", [1, 0, "TRUE", None])
def test_monthly_vars_has_to_be_a_boolean(bad):
    with pytest.raises(ValueError, match="True or False"):
        build_monthly_panel(episodes(), monthly_vars=bad)


def test_monthly_vars_is_keyword_only():
    # The R arm takes it as the fourth positional argument; this arm does not,
    # so a third positional lands on log_file and the call has to fail.
    with pytest.raises(TypeError):
        build_monthly_panel(episodes(), 15, None, False)
