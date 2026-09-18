"""04_merge_basic_BHP.do against the Python port, column by column.

The reference merges the Basic Establishment File onto the SIAB on betnr and
jahr, keeping master and matched rows. Two of its variables are used
downstream: the establishment's federal state ao_bula, which decides the
Rechtskreis, and the generated 3-digit industry w93_3_gen.
"""

import pytest

from conftest import assert_column_matches

STEP = "04_merge_basic_BHP"

MERGED_COLUMNS = ["ao_bula", "w93_3_gen"]


@pytest.mark.parametrize("column", MERGED_COLUMNS)
def test_merged_column_matches(reference_query, column):
    assert_column_matches(reference_query(STEP), column)


def test_the_merge_loses_no_reference_row(reference_query):
    query = reference_query(STEP)

    only_stata = query(
        "SELECT count(*) AS n FROM ("
        "  SELECT persnr, spell, begepi FROM stata"
        "  EXCEPT SELECT persnr, spell, begepi FROM py)"
    )["n"][0]

    assert only_stata == 0, f"{only_stata} keys are in the Stata fixture only"


def test_the_merge_duplicates_no_row(reference_query):
    """A many-to-one merge that went m:m would show up as extra Python rows."""
    query = reference_query(STEP)

    counts = query(
        "SELECT (SELECT count(*) FROM stata) AS n_stata, "
        "       (SELECT count(*) FROM py) AS n_py, "
        "       (SELECT count(*) FROM (SELECT persnr, spell, begepi FROM py "
        "                              EXCEPT "
        "                              SELECT persnr, spell, begepi FROM stata)) AS only_py"
    )

    assert counts["n_stata"][0] > 0
    assert counts["n_py"][0] == counts["n_stata"][0]
    assert counts["only_py"][0] == 0
