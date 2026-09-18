"""
16_monthly_panel.do against the Python port.

The reference totals days and earnings over each calendar year, cuts every
episode into one row per calendar month, keeps the row whose month contains the
15th inside the episode, and trims the four duration counters so they end on
the 15th rather than at the episode's own end.

It is an alternative to 16_yearly_panel.do, not a step after it: both start
from the parallel-episode data and the reference master calls neither. Both
halves of this comparison therefore come from a second run over that data.

The reference drops seven columns on its way out: nspell, begorig, endorig,
begepi, endepi, begepi_orig and endepi_orig. The port keeps all seven, so the
two halves join on the person, the year and the monthly episode start, and the
year is `jahr` on the Stata side and `year` on the Python side.
"""

import pytest

from conftest import assert_column_matches, siab_column_moments

STEP = "16_monthly_panel"

DISTRIBUTION_TOLERANCE = 0.01

STEP_KEY = {"persnr": "persnr", "jahr": "year", "begepi_monthly": "begepi_monthly"}


def test_both_halves_hold_the_same_person_months(reference_query):
    query = reference_query(STEP)

    counts = query(
        "SELECT (SELECT count(*) FROM stata) AS n_stata,"
        "       (SELECT count(*) FROM py) AS n_py,"
        "       (SELECT count(*) FROM (SELECT DISTINCT persnr, jahr, begepi_monthly"
        "                              FROM stata)) AS distinct_stata,"
        "       (SELECT count(*) FROM (SELECT persnr, jahr, begepi_monthly FROM stata"
        "                              EXCEPT"
        "                              SELECT persnr, year, begepi_monthly FROM py))"
        "         AS only_stata,"
        "       (SELECT count(*) FROM (SELECT persnr, year, begepi_monthly FROM py"
        "                              EXCEPT"
        "                              SELECT persnr, jahr, begepi_monthly FROM stata))"
        "         AS only_py"
    )

    assert counts["n_stata"][0] > 0
    assert counts["n_py"][0] == counts["n_stata"][0]
    assert counts["distinct_stata"][0] == counts["n_stata"][0]
    assert counts["only_stata"][0] == 0, (
        f'{counts["only_stata"][0]} person-months are in the Stata fixture only')
    assert counts["only_py"][0] == 0, (
        f'{counts["only_py"][0]} person-months are in the Python dump only')


@pytest.mark.parametrize("column", ["endepi_monthly", "month_num"])
def test_the_monthly_episode_bounds_and_the_month_itself_match_the_reference(
        reference_query, column):
    assert_column_matches(reference_query(STEP), column, key=STEP_KEY)


def test_the_year_of_the_month_is_the_year_of_the_episode(reference_query):
    # The reference generates a second year column from the month it builds,
    # beside the `jahr` it inherits from the episode start.
    # 01_split_episodes.do cuts every episode at the year boundary, so the two
    # can never disagree, which is why the port carries one column for both.
    query = reference_query(STEP)

    assert_column_matches(query, "year", py_column="year", key=STEP_KEY)

    own = query(
        "SELECT count(*) FILTER (WHERE year IS DISTINCT FROM jahr) AS n FROM stata"
    )["n"][0]
    assert own == 0


@pytest.mark.parametrize("column", ["year_days_emp", "year_days_benefits"])
def test_the_yearly_totals_match_the_reference_exactly(reference_query, column):
    assert_column_matches(reference_query(STEP), column, key=STEP_KEY)


@pytest.mark.parametrize("column", ["tage_bet", "tage_job", "tage_erw", "tage_lst"])
def test_the_four_trimmed_duration_counters_match_the_reference_exactly(
        reference_query, column):
    # 16_monthly_panel.do trims tage_lst in two separate `replace` statements,
    # one on quelle and one on parallel_benefits, where 16_yearly_panel.do joins
    # both conditions in a single one. 15_parallel_episodes.do sets
    # parallel_benefits for every episode of a person and episode start that
    # includes a benefit episode, the benefit episode itself included, so a
    # surviving LeH row meets both conditions and the reference subtracts the
    # overhang from it twice. The port reproduces that, and the exact match on
    # tage_lst here is what proves the reference behaves this way: trimming once
    # would put every such row out by the overhang. test_s09b_monthly_panel.py
    # pins the same arithmetic on a small synthetic table, without a fixture.
    assert_column_matches(reference_query(STEP), column, key=STEP_KEY)


def test_an_episode_with_no_employment_status_still_has_its_counter_trimmed(
        reference_query):
    # The employment counter is trimmed for every employment episode except the
    # trainee statuses, and Stata's inlist() reads a missing erwstat as "not one
    # of these", so an episode with no status recorded is trimmed too. The port
    # has to coalesce the comparison to keep that reading, as
    # build_yearly_panel() does.
    query = reference_query(STEP)

    no_status = query(
        "SELECT count(*) AS n,"
        "       count(*) FILTER (WHERE stata.tage_erw IS NULL) AS stata_null,"
        "       count(*) FILTER (WHERE py.tage_erw IS NULL) AS py_null"
        " FROM stata JOIN py ON stata.persnr = py.persnr"
        "                   AND stata.jahr = py.year"
        "                   AND stata.begepi_monthly = py.begepi_monthly"
        " WHERE stata.quelle = 1 AND stata.erwstat IS NULL"
    )

    assert no_status["n"][0] > 0
    assert no_status["py_null"][0] == no_status["stata_null"][0]


def test_yearly_labour_earnings_agree_in_distribution_within_one_percent(
        reference_query):
    # year_labor_earn multiplies the summed imputed wage by the days employed,
    # so it inherits the random draws behind that wage and is bounded as a
    # distribution, the same way it is at the yearly panel.
    moments = siab_column_moments(reference_query(STEP), "year_labor_earn",
                                  key=STEP_KEY)

    assert moments["shared"][0] > 0
    for statistic in ("mean", "q25", "q75"):
        reference = moments[f"stata_{statistic}"][0]
        port = moments[f"py_{statistic}"][0]
        gap = abs(port - reference) / abs(reference)
        assert gap < DISTRIBUTION_TOLERANCE, (
            f"the {statistic} of year_labor_earn is {gap:.2%} away from the "
            f"reference's {reference:.2f}")


def test_every_surviving_python_row_is_one_month_of_its_episode_and_covers_the_15th(
        reference_query):
    # Every surviving row describes one month of one episode: its bounds lie
    # inside that month, inside the episode, and they straddle the 15th. The
    # Python half still carries the episode dates the reference drops, which is
    # what makes the second half of this checkable at all.
    query = reference_query(STEP)

    outside = query(
        "SELECT count(*) FILTER (WHERE begepi_monthly < month"
        "                          OR endepi_monthly > last_day(month))"
        "         AS beyond_month,"
        "       count(*) FILTER (WHERE begepi_monthly < begepi"
        "                          OR endepi_monthly > endepi)"
        "         AS beyond_episode,"
        "       count(*) FILTER (WHERE day(begepi_monthly) > 15"
        "                          OR day(endepi_monthly) < 15)"
        "         AS missing_the_15th"
        " FROM py"
    )

    assert outside["beyond_month"][0] == 0
    assert outside["beyond_episode"][0] == 0
    assert outside["missing_the_15th"][0] == 0
