"""
10_wages_imputation.do against the Python port.

This is the first comparison past 09_restrictions.do. That step cuts the data
down to one project's population and neither port has a counterpart for it, so
make_fixtures.do takes its dump and then continues from the step 08 data. Both
halves of this comparison therefore carry the whole dataset.

10_wages_imputation.do drops every intermediate it builds, so the columns the
two sides have in common are the three the step's own header names: cens, wage
and wage_imp. The first two are arithmetic and are compared row for row. The
third cannot be: the reference draws a random term for every censored wage from
Stata's generator, seeded inside the step, and the port draws from numpy's. The
two draws are different numbers by construction, so wage_imp is compared as a
distribution.

Both sides run both imputation steps: the one on observables from Gartner
(2005) and the extended one whose regressors include leave-one-out mean wages
per worker and per plant.
"""

import pytest

from conftest import assert_column_matches

STEP = "10_wages_imputation"

# Stata float storage, the same tolerance the deflated wage gets, because wage
# is built from wage_defl and the assessment ceiling and stored as a float.
FLOAT_TOLERANCE = 1e-6

# What a distributional comparison is allowed to be off by. Measured on the test
# data the three statistics land 0.03, 0.13 and 0.16 percent apart, so the bound
# has room for the sampling noise of a different draw without being loose enough
# to pass a port that models something else: the R arm measured the same three
# at 1.1, 1.9 and 3.8 percent before its second imputation step was written.
DISTRIBUTION_TOLERANCE = 0.01


def test_key_sets_are_identical(reference_query):
    query = reference_query(STEP)

    only_stata = query(
        "SELECT count(*) AS n FROM ("
        "  SELECT persnr, spell, begepi FROM stata"
        "  EXCEPT SELECT persnr, spell, begepi FROM py)"
    )["n"][0]
    only_py = query(
        "SELECT count(*) AS n FROM ("
        "  SELECT persnr, spell, begepi FROM py"
        "  EXCEPT SELECT persnr, spell, begepi FROM stata)"
    )["n"][0]
    n_stata = query("SELECT count(*) AS n FROM stata")["n"][0]

    assert n_stata > 0
    assert only_stata == 0, f"{only_stata} keys are in the Stata fixture only"
    assert only_py == 0, f"{only_py} keys are in the Python dump only"


def test_cens_matches_the_reference_on_every_row(reference_query):
    # Not restricted to the employment history. The reference generates the flag
    # as 0 for every row, so the port does too, and the two agree off the BeH as
    # well as on it. The spells from 1992 on whose east flag is missing, and
    # whose assessment ceiling is therefore unknown, are the case that made this
    # worth testing: Stata orders missing above every number, so its
    # `wage_defl > limit_assess4` is false there and the flag stays 0.
    assert_column_matches(reference_query(STEP), "cens")


def test_wage_matches_the_reference_to_stata_float_precision(reference_query):
    assert_column_matches(reference_query(STEP), "wage", tolerance=FLOAT_TOLERANCE)


def test_wage_imp_is_imputed_on_exactly_the_same_rows_as_in_the_reference(
        reference_query):
    # The row set is deterministic even though the values are not: which spells
    # get an imputed wage follows from the estimation sample, the carry-through
    # of uncensored wages, and the fallback from the second step to the first.
    query = reference_query(STEP)

    coverage = query(
        "SELECT count(*) FILTER (WHERE stata.wage_imp IS NOT NULL) AS stata_has,"
        "       count(*) FILTER (WHERE py.wage_imp IS NOT NULL) AS py_has,"
        "       count(*) FILTER (WHERE stata.wage_imp IS NOT NULL"
        "                          AND py.wage_imp IS NULL) AS stata_only,"
        "       count(*) FILTER (WHERE stata.wage_imp IS NULL"
        "                          AND py.wage_imp IS NOT NULL) AS py_only"
        " FROM stata JOIN py USING (persnr, spell, begepi)"
    )

    assert coverage["stata_only"][0] == 0, (
        f'{coverage["stata_only"][0]} rows are imputed by the reference and not '
        f"by the port")
    assert coverage["py_only"][0] == 0, (
        f'{coverage["py_only"][0]} rows are imputed by the port and not by the '
        f"reference")
    assert coverage["py_has"][0] == coverage["stata_has"][0]


def test_an_uncensored_wage_is_carried_through_unchanged_on_both_sides(
        reference_query):
    # Nothing is drawn for these rows: both sides set the imputed wage to the
    # observed one, so they agree with the raw wage and with each other.
    query = reference_query(STEP)

    diff = query(
        "SELECT count(*) AS shared,"
        "       count(*) FILTER (WHERE abs(stata.wage_imp - stata.wage)"
        "                          > 1e-5 * stata.wage) AS stata_differs,"
        "       count(*) FILTER (WHERE abs(py.wage_imp - py.wage)"
        "                          > 1e-5 * py.wage) AS py_differs,"
        "       count(*) FILTER (WHERE abs(stata.wage_imp - py.wage_imp)"
        "                          > 1e-5 * stata.wage_imp) AS between"
        " FROM stata JOIN py USING (persnr, spell, begepi)"
        " WHERE stata.cens = 0"
        "   AND stata.wage_imp IS NOT NULL AND py.wage_imp IS NOT NULL"
    )

    assert diff["stata_differs"][0] == 0
    assert diff["py_differs"][0] == 0
    assert diff["between"][0] == 0, (
        f'the two sides differ on {diff["between"][0]} of {diff["shared"][0]} '
        f"uncensored rows")


@pytest.mark.parametrize("statistic", ["median", "p90", "mean"])
def test_the_imputed_wage_distribution_matches_the_reference(reference_query,
                                                             statistic):
    # This is the whole of what a censored wage can be compared on. The
    # reference draws its random term from Stata's generator, seeded inside the
    # step, and the port draws from numpy's, so the two columns are different
    # numbers by construction and only their distributions are comparable.
    query = reference_query(STEP)

    stats = query(
        "SELECT count(*) AS n,"
        "       median(stata.wage_imp) AS stata_median,"
        "       median(py.wage_imp) AS py_median,"
        "       quantile_cont(stata.wage_imp, 0.9) AS stata_p90,"
        "       quantile_cont(py.wage_imp, 0.9) AS py_p90,"
        "       avg(stata.wage_imp) AS stata_mean,"
        "       avg(py.wage_imp) AS py_mean"
        " FROM stata JOIN py USING (persnr, spell, begepi)"
        " WHERE stata.cens = 1"
        "   AND stata.wage_imp IS NOT NULL AND py.wage_imp IS NOT NULL"
    )

    assert stats["n"][0] > 10000

    reference = stats[f"stata_{statistic}"][0]
    port = stats[f"py_{statistic}"][0]
    gap = abs(port - reference) / reference

    assert gap < DISTRIBUTION_TOLERANCE, (
        f"the {statistic} of the imputed wage is {gap:.2%} away from the "
        f"reference's {reference:.2f}, over {stats['n'][0]} censored spells")
