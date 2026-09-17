"""01_split_episodes.do against the Python port, column by column."""

from conftest import assert_column_matches

STEP = "01_split_episodes"


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


def test_endepi_matches(reference_query):
    assert_column_matches(reference_query(STEP), "endepi")


def test_begepi_orig_matches(reference_query):
    assert_column_matches(reference_query(STEP), "begepi_orig")


def test_endepi_orig_matches(reference_query):
    assert_column_matches(reference_query(STEP), "endepi_orig")


def test_year_matches(reference_query):
    # The reference calls it jahr; the port calls it year.
    assert_column_matches(reference_query(STEP), "jahr", py_column="year")


def test_age_matches(reference_query):
    assert_column_matches(reference_query(STEP), "age")
