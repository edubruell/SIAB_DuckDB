"""
06_wages_assessment_ceiling.do against the Python port, column by column.

Both columns are compared exactly. east is an integer recode of the federal
state. limit_assess is a statutory euro figure that the reference stores as a
Stata `float`; the port rounds classifications/wa_ceiling.csv to the same
precision on read-in with stata_float(), so the two agree bit for bit.
"""

from conftest import assert_column_matches

STEP = "06_wages_assessment_ceiling"


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


def test_east_matches(reference_query):
    assert_column_matches(reference_query(STEP), "east")


def test_limit_assess_matches(reference_query):
    # Exactly, not to a tolerance: both sides hold the Stata float of the
    # statutory figure, so any difference here is a real one.
    assert_column_matches(reference_query(STEP), "limit_assess")


def test_the_ceiling_is_missing_exactly_where_the_reference_leaves_it_missing(
        reference_query):
    """A stronger statement than the column comparison on its own.

    The column test joins on the key and would pass on a dump that matched row
    for row while carrying a different number of missing ceilings from a
    different set of rows. This counts them on both sides.
    """
    query = reference_query(STEP)
    counts = query(
        "SELECT count(*) FILTER (WHERE limit_assess IS NULL) AS n FROM stata"
    )["n"][0], query(
        "SELECT count(*) FILTER (WHERE limit_assess IS NULL) AS n FROM py"
    )["n"][0]
    assert counts[0] == counts[1]
