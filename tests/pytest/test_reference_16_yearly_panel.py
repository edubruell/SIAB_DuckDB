"""
16_yearly_panel.do against the Python port.

The reference totals days and earnings over each calendar year, keeps the one
episode of that year covering 30 June, and trims the four duration counters so
they end at the cutoff rather than at the episode's own end.

It also drops seven columns on its way out: nspell, begorig, endorig, begepi,
endepi, begepi_orig and endepi_orig. The port keeps all seven, so the two
halves join on the person and the year alone, and the year is `jahr` on the
Stata side and `year` on the Python side.

Both halves come off the parallel-episode data, which make_py_dumps.py keeps
aside so the monthly panel can be built from the same input.
"""

import pytest

from conftest import assert_column_matches, siab_column_moments

STEP = "16_yearly_panel"

DISTRIBUTION_TOLERANCE = 0.01

# The year is called jahr in the fixture and year in the dump.
STEP_KEY = {"persnr": "persnr", "jahr": "year"}


def test_both_halves_keep_one_episode_per_person_and_year_the_same_ones(
        reference_query):
    query = reference_query(STEP)

    counts = query(
        "SELECT (SELECT count(*) FROM stata) AS n_stata,"
        "       (SELECT count(*) FROM py) AS n_py,"
        "       (SELECT count(*) FROM (SELECT DISTINCT persnr, jahr FROM stata))"
        "         AS distinct_stata,"
        "       (SELECT count(*) FROM (SELECT persnr, jahr FROM stata"
        "                              EXCEPT"
        "                              SELECT persnr, year FROM py)) AS only_stata,"
        "       (SELECT count(*) FROM (SELECT persnr, year FROM py"
        "                              EXCEPT"
        "                              SELECT persnr, jahr FROM stata)) AS only_py"
    )

    assert counts["n_stata"][0] > 0
    assert counts["n_py"][0] == counts["n_stata"][0]
    assert counts["distinct_stata"][0] == counts["n_stata"][0]
    assert counts["only_stata"][0] == 0, (
        f'{counts["only_stata"][0]} person-years are in the Stata fixture only')
    assert counts["only_py"][0] == 0, (
        f'{counts["only_py"][0]} person-years are in the Python dump only')


@pytest.mark.parametrize("column", ["year_days_emp", "year_days_benefits"])
def test_the_yearly_totals_match_the_reference_exactly(reference_query, column):
    assert_column_matches(reference_query(STEP), column, key=STEP_KEY)


@pytest.mark.parametrize("column", ["tage_bet", "tage_job", "tage_erw", "tage_lst"])
def test_the_four_trimmed_duration_counters_match_the_reference_exactly(
        reference_query, column):
    assert_column_matches(reference_query(STEP), column, key=STEP_KEY)


def test_an_episode_with_no_employment_status_still_has_its_counter_trimmed(
        reference_query):
    # The employment counter is trimmed for every employment episode except the
    # trainee statuses, and Stata's inlist() reads a missing erwstat as "not one
    # of these", so an episode with no status recorded is trimmed too. SQL's
    # NOT IN returns NULL there, which an unguarded conditional turns into a
    # missing counter: 966 rows of the test data, on the reading the R arm hit
    # before it coalesced the comparison.
    query = reference_query(STEP)

    no_status = query(
        "SELECT count(*) AS n,"
        "       count(*) FILTER (WHERE stata.tage_erw IS NULL) AS stata_null,"
        "       count(*) FILTER (WHERE py.tage_erw IS NULL) AS py_null"
        " FROM stata JOIN py ON stata.persnr = py.persnr AND stata.jahr = py.year"
        " WHERE stata.quelle = 1 AND stata.erwstat IS NULL"
    )

    assert no_status["n"][0] > 0
    assert no_status["py_null"][0] == no_status["stata_null"][0]


def test_yearly_labour_earnings_agree_in_distribution_within_one_percent(
        reference_query):
    # year_labor_earn multiplies the summed imputed wage by the days employed,
    # so it inherits the random draws behind that wage and is bounded as a
    # distribution, the same way parallel_wage_imp is at step 15.
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


def test_every_surviving_python_episode_covers_the_cutoff_date(reference_query):
    # The episodes the step keeps are the ones running across 30 June, so every
    # surviving episode must have started on or before it and ended on or after
    # it. The Python half still carries the episode dates the reference drops,
    # which is what makes this checkable at all.
    query = reference_query(STEP)

    outside = query(
        "SELECT count(*) AS n FROM py"
        " WHERE NOT (100 * month(begepi) + day(begepi) <= 630"
        "            AND 630 <= 100 * month(endepi) + day(endepi))"
    )["n"][0]

    assert outside == 0, f"{outside} surviving episodes do not cover 30 June"
