"""
Synthetic tests for build_yearly_panel().

The step is the port of 16_yearly_panel.do: total the days and the earnings of
each calendar year, keep the one episode of that year running across the cutoff
date, and trim the four duration counters so they end on that date rather than
at the episode's own end.

There is no fixture comparison for this step yet. It reads `parallel_wage_imp`,
which handle_parallel_episodes() builds out of the imputed wage; the imputation
is ported now, but tests/pytest/make_py_dumps.py still writes no
16_yearly_panel.parquet to compare against the committed Stata one. These
hand-built tables are therefore the step's only coverage, and they carry the
cases
tests/testthat/test-reference-16_yearly_panel.R makes against the reference:
one episode per person-year, the yearly totals, the four trimmed counters, and
the missing employment status Stata's inlist() reads as "not one of these".

The reference drops seven columns on its way out -- nspell, begorig, endorig,
begepi, endepi, begepi_orig and endepi_orig -- and the port keeps all seven, so
the episode dates are still here to be checked against.
"""

import datetime as dt

import polars as pl

from siab.steps import build_yearly_panel

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

COUNTERS = ("tage_bet", "tage_job", "tage_erw", "tage_lst")


def episodes(rows):
    """Build a frame out of a list of dicts, filling in what a row leaves out."""
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


# ======================================================================
#  Which episode survives
# ======================================================================

def test_only_the_episode_covering_the_cutoff_date_survives():
    frame = episodes([
        {"begepi": dt.date(2000, 1, 20), "endepi": dt.date(2000, 7, 31)},
        {"begepi": dt.date(2000, 8, 1), "endepi": dt.date(2000, 12, 31)},
    ])
    out = build_yearly_panel(frame).collect()

    assert out.height == 1
    assert out["begepi"][0] == dt.date(2000, 1, 20)
    assert out["endepi"][0] == dt.date(2000, 7, 31)


def test_an_episode_touching_the_cutoff_on_either_bound_is_kept():
    # Both bounds of the reference's `keep if` are inclusive.
    frame = episodes([
        {"persnr": 1, "begepi": dt.date(2000, 6, 30), "endepi": dt.date(2000, 9, 30)},
        {"persnr": 2, "begepi": dt.date(2000, 2, 1), "endepi": dt.date(2000, 6, 30)},
    ])
    out = build_yearly_panel(frame).collect().sort("persnr")

    assert out.height == 2


def test_a_person_year_with_no_episode_over_the_cutoff_drops_out_entirely():
    frame = episodes([
        {"begepi": dt.date(2000, 1, 1), "endepi": dt.date(2000, 3, 31)},
        {"begepi": dt.date(2000, 9, 1), "endepi": dt.date(2000, 12, 31)},
    ])
    out = build_yearly_panel(frame).collect()

    assert out.height == 0


def test_each_calendar_year_keeps_its_own_episode():
    frame = episodes([
        {"year": 2000, "begepi": dt.date(2000, 1, 1), "endepi": dt.date(2000, 12, 31)},
        {"year": 2001, "begepi": dt.date(2001, 1, 1), "endepi": dt.date(2001, 12, 31)},
    ])
    out = build_yearly_panel(frame).collect().sort("year")

    assert out["year"].to_list() == [2000, 2001]


def test_the_cutoff_date_is_the_caller_s_to_move():
    frame = episodes([
        {"begepi": dt.date(2000, 1, 1), "endepi": dt.date(2000, 3, 31)},
        {"begepi": dt.date(2000, 9, 1), "endepi": dt.date(2000, 12, 31)},
    ])
    out = build_yearly_panel(frame, cutoff_month=10, cutoff_day=1).collect()

    assert out.height == 1
    assert out["begepi"][0] == dt.date(2000, 9, 1)


# ======================================================================
#  The three yearly totals
# ======================================================================

def test_the_yearly_totals_run_over_every_episode_of_the_year():
    # 194 days from 20 January to 31 July, 153 from 1 August to 31 December.
    frame = episodes([
        {"begepi": dt.date(2000, 1, 20), "endepi": dt.date(2000, 7, 31),
         "parallel_wage_imp": 100.0},
        {"begepi": dt.date(2000, 8, 1), "endepi": dt.date(2000, 12, 31),
         "parallel_wage_imp": 80.0},
    ])
    out = build_yearly_panel(frame).collect()

    # The totals are computed before the selection, so the surviving row carries
    # the whole year, not only its own episode.
    assert out["year_days_emp"][0] == 194 + 153
    assert out["year_days_benefits"][0] == 0
    assert out["year_labor_earn"][0] == 100.0 * 194 + 80.0 * 153


def test_benefit_days_count_a_benefit_source_or_a_parallel_benefit():
    frame = episodes([
        {"persnr": 1, "quelle": 2, "erwstat": None,
         "begepi": dt.date(2000, 3, 1), "endepi": dt.date(2000, 12, 31)},
        {"persnr": 2, "quelle": 1, "parallel_benefits": 1,
         "begepi": dt.date(2000, 3, 1), "endepi": dt.date(2000, 12, 31)},
    ])
    out = build_yearly_panel(frame).collect().sort("persnr")

    # 306 days from 1 March to 31 December of a leap year.
    assert out["year_days_benefits"].to_list() == [306, 306]
    # Only the second is employment, so only it contributes employment days.
    assert out["year_days_emp"].to_list() == [0, 306]


def test_the_totals_do_not_reach_across_calendar_years():
    frame = episodes([
        {"year": 2000, "begepi": dt.date(2000, 1, 1), "endepi": dt.date(2000, 12, 31)},
        {"year": 2001, "begepi": dt.date(2001, 1, 1), "endepi": dt.date(2001, 12, 31)},
    ])
    out = build_yearly_panel(frame).collect().sort("year")

    # 2000 is a leap year, 2001 is not.
    assert out["year_days_emp"].to_list() == [366, 365]


def test_the_working_duration_columns_do_not_survive():
    frame = episodes([{}])
    out = build_yearly_panel(frame).collect()

    for column in ("dur_emp", "dur_benefits", "begepi_num", "endepi_num", "overhang"):
        assert column not in out.columns


# ======================================================================
#  Trimming the four counters
# ======================================================================

def test_the_counters_are_trimmed_by_the_days_past_the_cutoff():
    # 31 July is 31 days past 30 June.
    frame = episodes([{"begepi": dt.date(2000, 1, 1), "endepi": dt.date(2000, 7, 31)}])
    out = build_yearly_panel(frame).collect()

    assert out["tage_bet"][0] == 1000 - 31
    assert out["tage_job"][0] == 1000 - 31
    assert out["tage_erw"][0] == 1000 - 31
    # quelle 1 and no parallel benefit, so the benefit counter is left alone.
    assert out["tage_lst"][0] == 1000


def test_an_episode_ending_on_the_cutoff_loses_nothing():
    frame = episodes([{"begepi": dt.date(2000, 1, 1), "endepi": dt.date(2000, 6, 30)}])
    out = build_yearly_panel(frame).collect()

    assert out["tage_bet"][0] == 1000


def test_a_benefit_episode_leaves_the_employment_counters_alone():
    frame = episodes([{"quelle": 2, "erwstat": None,
                       "begepi": dt.date(2000, 1, 1), "endepi": dt.date(2000, 12, 31)}])
    out = build_yearly_panel(frame).collect()

    # 31 December is 184 days past 30 June.
    assert out["tage_bet"][0] == 1000
    assert out["tage_job"][0] == 1000
    assert out["tage_erw"][0] == 1000
    assert out["tage_lst"][0] == 1000 - 184


# 16_yearly_panel.do joins the two conditions in one `replace`, so a benefit
# episode that also carries parallel_benefits loses the overhang once.
# 16_monthly_panel.do splits them into two statements and subtracts twice;
# test_monthly_panel.py pins that side of it.
def test_a_benefit_episode_with_a_parallel_benefit_is_trimmed_only_once():
    frame = episodes([{"quelle": 2, "erwstat": None, "parallel_benefits": 1,
                       "begepi": dt.date(2000, 1, 1), "endepi": dt.date(2000, 12, 31)}])
    out = build_yearly_panel(frame).collect()

    assert out["tage_lst"][0] == 1000 - 184


def test_an_employment_episode_beside_a_benefit_loses_the_benefit_overhang_too():
    frame = episodes([{"quelle": 1, "parallel_benefits": 1,
                       "begepi": dt.date(2000, 1, 1), "endepi": dt.date(2000, 12, 31)}])
    out = build_yearly_panel(frame).collect()

    assert out["tage_lst"][0] == 1000 - 184
    assert out["tage_bet"][0] == 1000 - 184


# `!inlist(erwstat, 102, 121, 122, 141, 144)` keeps the trainee statuses out of
# the employment trim.
def test_a_trainee_keeps_the_employment_counter_but_loses_the_other_two():
    frame = episodes([{"erwstat": 102,
                       "begepi": dt.date(2000, 1, 1), "endepi": dt.date(2000, 12, 31)}])
    out = build_yearly_panel(frame).collect()

    assert out["tage_erw"][0] == 1000
    assert out["tage_bet"][0] == 1000 - 184
    assert out["tage_job"][0] == 1000 - 184


# Stata's inlist() returns 0 for a missing argument, so `!inlist(erwstat, ...)`
# is true where erwstat is missing and the reference trims the counter there
# too. polars' is_in() returns null for a null, which would send the row down
# the `otherwise` branch and leave the counter untrimmed: 966 rows of the FDZ
# test data, all quelle 1 with no erwstat.
def test_an_employment_episode_with_no_status_still_has_its_counter_trimmed():
    frame = episodes([{"erwstat": None,
                       "begepi": dt.date(2000, 1, 1), "endepi": dt.date(2000, 12, 31)}])
    out = build_yearly_panel(frame).collect()

    assert out["tage_erw"][0] == 1000 - 184, (
        "a missing erwstat is not one of the trainee statuses in Stata, so the "
        "counter is trimmed"
    )
    assert out["tage_erw"][0] is not None


def test_no_counter_comes_out_missing_where_it_went_in_with_a_value():
    frame = episodes([
        {"persnr": 1, "quelle": 1, "erwstat": None},
        {"persnr": 2, "quelle": 2, "erwstat": None},
        {"persnr": 3, "quelle": 1, "erwstat": 102, "parallel_benefits": 1},
        {"persnr": 4, "quelle": 3, "erwstat": None},
    ])
    out = build_yearly_panel(frame).collect()

    assert out.height == 4
    for column in COUNTERS:
        assert out[column].null_count() == 0, f"{column} came out missing somewhere"


def test_the_trim_follows_the_caller_s_cutoff():
    frame = episodes([{"begepi": dt.date(2000, 1, 1), "endepi": dt.date(2000, 12, 31)}])
    out = build_yearly_panel(frame, cutoff_month=12, cutoff_day=1).collect()

    # 31 December is 30 days past 1 December.
    assert out["tage_bet"][0] == 1000 - 30
