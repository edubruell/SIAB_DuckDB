"""generate_educ_variable() against 05_educ_broad.do.

educ is an integer recode of the FDZ's imputed training variable, so it is
compared exactly with no tolerance. A null on one side and a value on the other
counts as a difference, which is what makes the missing training code a tested
case rather than an assumed one.
"""

from conftest import assert_column_matches, siab_column_diff

STEP = "05_educ_broad"


def test_educ_matches_the_reference_exactly(reference_query):
    assert_column_matches(reference_query(STEP), "educ")


def test_the_two_halves_share_every_key(reference_query):
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

    assert only_stata == 0, f"{only_stata} keys are in the Stata fixture only"
    assert only_py == 0, f"{only_py} keys are in the Python dump only"


def test_educ_only_ever_holds_1_2_3_or_nothing(reference_query):
    query = reference_query(STEP)

    stray = query(
        "SELECT count(*) AS n FROM py "
        "WHERE educ IS NOT NULL AND educ NOT IN (1, 2, 3)"
    )["n"][0]

    assert stray == 0, f"{stray} rows of the dump carry an educ outside 1, 2, 3"


def test_the_missing_case_is_exercised_and_the_two_sides_agree_on_it(reference_query):
    # The dumps carry the key and the compared column alone, so ausbildung_imp
    # is not there to select on. What can be checked is that the reference
    # leaves educ missing on some rows at all, which is the branch a null
    # training code takes, and that the port leaves it missing on exactly those.
    query = reference_query(STEP)

    counts = query(
        "SELECT count(*) FILTER (WHERE stata.educ IS NULL) AS stata_null, "
        "       count(*) FILTER (WHERE py.educ IS NULL) AS py_null, "
        "       count(*) FILTER (WHERE (stata.educ IS NULL) <> (py.educ IS NULL)) "
        "           AS disagreeing "
        "FROM stata JOIN py ON stata.persnr = py.persnr "
        "                  AND stata.spell = py.spell "
        "                  AND stata.begepi = py.begepi"
    )

    assert counts["stata_null"][0] > 0, (
        "the reference leaves educ missing nowhere, so the missing branch is "
        "never exercised by this comparison"
    )
    assert counts["py_null"][0] == counts["stata_null"][0]
    assert counts["disagreeing"][0] == 0


def test_the_comparison_is_made_over_the_whole_fixture(reference_query):
    query = reference_query(STEP)
    diff = siab_column_diff(query, "educ")
    n_stata = query("SELECT count(*) AS n FROM stata")["n"][0]

    assert diff["shared"][0] == n_stata, (
        "the join loses rows of the fixture, so a difference could hide in them"
    )
