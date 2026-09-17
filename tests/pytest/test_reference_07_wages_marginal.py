"""
07_wages_marginal.do against the Python port.

limit_marginal is a statutory figure the reference stores as a Stata `float`,
and the port rounds the lookup to the same precision, so it is compared
exactly. So is the flag.

The R arm restricts the flag comparison to rows whose daily wage already
agrees, because 02_grund154.do, which reallocates one-time payments across a
year's spells, has no R counterpart. The Python arm has ported that step, so
the restriction is not needed here and the flag is compared on every shared
row. test_tentgelt_still_matches() is what keeps that honest: if the wage ever
starts to drift, it fails first and says so, rather than letting the flag test
quietly compare against a different wage.
"""

from conftest import assert_column_matches

STEP = "07_wages_marginal"


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
    # The daily wage this step compares against the threshold. It comes out of
    # the ported 02_grund154, and the flag test below relies on it agreeing.
    assert_column_matches(reference_query(STEP), "tentgelt")


def test_limit_marginal_matches(reference_query):
    assert_column_matches(reference_query(STEP), "limit_marginal")


def test_the_marginal_flag_matches(reference_query):
    assert_column_matches(reference_query(STEP), "marginal")


def test_the_flag_is_never_missing_on_either_side(reference_query):
    # 07_wages_marginal.do writes `gen byte marginal = 0` before it replaces
    # anything, so the reference never leaves the flag missing and neither may
    # the port. A column comparison alone would pass on two dumps that were
    # missing in the same places.
    query = reference_query(STEP)
    assert query("SELECT count(*) AS n FROM stata WHERE marginal IS NULL")["n"][0] == 0
    assert query("SELECT count(*) AS n FROM py WHERE marginal IS NULL")["n"][0] == 0


def test_a_missing_wage_is_flagged_zero_on_both_sides(reference_query):
    # The extended-missing ordering the port reproduces by hand: an absent wage
    # never counts as at or below the threshold. This is the case the synthetic
    # test pins in the abstract, checked here against the real reference.
    query = reference_query(STEP)
    for view in ("stata", "py"):
        wrong = query(
            f"SELECT count(*) AS n FROM {view} "
            "WHERE tentgelt IS NULL AND marginal <> 0"
        )["n"][0]
        assert wrong == 0, f"{view}: {wrong} missing wages are not flagged 0"
    # And the case is actually exercised by the test data.
    assert query("SELECT count(*) AS n FROM py WHERE tentgelt IS NULL")["n"][0] > 0
