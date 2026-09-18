"""13_industries_1digit.do against the Python port, one test per mapping.

The reference maps the time-consistent three-digit industry `w93_3_gen` onto two
one-digit classifications, the Statistisches Bundesamt's and the IAB
establishment panel's, and leaves a code that falls in no range missing.

The two sides reach the columns from different positions in the pipeline. The
reference builds them at step 13, after the AKM merge; the port builds them in
generate_industry_variables(), directly after merge_basic_bhp(), which is the
step that brings w93_3_gen in. Nothing between the two positions touches
w93_3_gen, which is what makes the comparison meaningful, and the fourth test
below is what makes that claim testable.
"""

import pytest

from conftest import assert_column_matches, siab_column_diff

STEP = "13_industries_1digit"

INDUSTRY_COLUMNS = ["industry1_destatis", "industry1_estpanel"]


@pytest.mark.parametrize("column", INDUSTRY_COLUMNS)
def test_industry_column_matches(reference_query, column):
    assert_column_matches(reference_query(STEP), column)


@pytest.mark.parametrize("column", INDUSTRY_COLUMNS)
def test_the_comparison_covers_every_reference_row(reference_query, column):
    """The R arm pins the shared count at the fixture's own size; so does this."""
    query = reference_query(STEP)

    n_stata = query("SELECT count(*) AS n FROM stata")["n"][0]
    shared = siab_column_diff(query, column)["shared"][0]

    assert n_stata > 0
    assert shared == n_stata


def test_an_episode_with_no_industry_carries_no_category_on_either_side(reference_query):
    """An episode with no establishment carries no industry, and on the test data
    that is most of the non-employment spells. Both sides have to leave the two
    categories missing there rather than assign a code."""
    query = reference_query(STEP)

    no_industry = query(
        "SELECT count(*) AS n, "
        "       count(*) FILTER (WHERE stata.industry1_destatis IS NULL) AS stata_destatis, "
        "       count(*) FILTER (WHERE py.industry1_destatis IS NULL) AS py_destatis, "
        "       count(*) FILTER (WHERE stata.industry1_estpanel IS NULL) AS stata_estpanel, "
        "       count(*) FILTER (WHERE py.industry1_estpanel IS NULL) AS py_estpanel "
        "FROM stata JOIN py USING (persnr, spell, begepi) "
        "WHERE stata.w93_3_gen IS NULL"
    )

    n = no_industry["n"][0]
    assert n > 0
    assert no_industry["stata_destatis"][0] == n
    assert no_industry["py_destatis"][0] == n
    assert no_industry["stata_estpanel"][0] == n
    assert no_industry["py_estpanel"][0] == n


def test_w93_3_gen_itself_is_untouched_between_the_two_positions(reference_query):
    assert_column_matches(reference_query(STEP), "w93_3_gen")


def test_no_row_is_lost_or_duplicated_by_the_two_mappings(reference_query):
    query = reference_query(STEP)

    counts = query(
        "SELECT (SELECT count(*) FROM stata) AS n_stata, "
        "       (SELECT count(*) FROM py) AS n_py, "
        "       (SELECT count(*) FROM (SELECT persnr, spell, begepi FROM stata "
        "                              EXCEPT "
        "                              SELECT persnr, spell, begepi FROM py)) AS only_stata, "
        "       (SELECT count(*) FROM (SELECT persnr, spell, begepi FROM py "
        "                              EXCEPT "
        "                              SELECT persnr, spell, begepi FROM stata)) AS only_py"
    )

    assert counts["n_stata"][0] == counts["n_py"][0]
    assert counts["only_stata"][0] == 0
    assert counts["only_py"][0] == 0
