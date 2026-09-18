"""02_grund154.do against the Python port."""

from conftest import assert_column_matches

STEP = "02_grund154"


def test_tentgelt_matches(reference_query):
    assert_column_matches(reference_query(STEP), "tentgelt")


def test_no_stata_row_is_missing_from_the_dump(reference_query):
    query = reference_query(STEP)
    orphans = query(
        "SELECT count(*) AS n FROM stata "
        "ANTI JOIN py USING (persnr, spell, begepi)"
    )["n"][0]
    assert orphans == 0, f"{orphans} Stata rows have no counterpart in the dump"
