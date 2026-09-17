"""
08_wages_deflation.do against the Python port.

The consumer price index and the three deflated variables are stored by the
reference as Stata `float`, which holds about seven decimal digits, so they get
a relative tolerance of 1e-6. The arithmetic itself is one division and is
expected to agree far more closely than that; the tolerance covers the storage,
not the calculation.

The R arm restricts the wage_defl comparison to rows whose daily wage already
agrees, because 02_grund154.do has no R counterpart. The Python arm has ported
that step, so wage_defl is compared on every shared row, with
test_tentgelt_still_matches() standing guard over the assumption.
"""

import pytest

from conftest import assert_column_matches

STEP = "08_wages_deflation"

# Stata float storage, about seven decimal digits. Not the division's error.
FLOAT_TOLERANCE = 1e-6


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


def test_tentgelt_still_matches(reference_query):
    # The nominal wage wage_defl is built from, exactly.
    assert_column_matches(reference_query(STEP), "tentgelt")


def test_cpi_matches_to_stata_float_precision(reference_query):
    # The port rounds classifications/cpi.csv to float on read-in, which makes
    # 44 of the 50 years agree bit for bit. Six years, 1977, 1983 and 1987 to
    # 1990, land one unit in the last place apart, because the reference
    # computed the index from an expression and the csv's generator arrives at
    # the neighbouring float. The gap is 9e-08 relative, well inside the
    # tolerance, and below anything the float can represent.
    assert_column_matches(reference_query(STEP), "cpi", tolerance=FLOAT_TOLERANCE)


@pytest.mark.parametrize("column", ["limit_marginal_defl", "limit_assess_defl"])
def test_the_deflated_statutory_limits_match_to_float_precision(reference_query, column):
    assert_column_matches(reference_query(STEP), column, tolerance=FLOAT_TOLERANCE)


def test_wage_defl_matches_to_float_precision(reference_query):
    assert_column_matches(reference_query(STEP), "wage_defl",
                          tolerance=FLOAT_TOLERANCE)


def test_the_deflated_wage_is_missing_exactly_where_the_nominal_one_is(
        reference_query):
    # The division carries a missing wage through rather than dropping the row,
    # on both sides. Counted rather than joined, so a dump with the right values
    # on the wrong rows would still be caught.
    query = reference_query(STEP)
    for view in ("stata", "py"):
        wrong = query(
            f"SELECT count(*) AS n FROM {view} "
            "WHERE (tentgelt IS NULL AND cpi IS NOT NULL) <> "
            "      (wage_defl IS NULL AND cpi IS NOT NULL)"
        )["n"][0]
        assert wrong == 0, f"{view}: {wrong} rows deflate a missing wage into a value"
