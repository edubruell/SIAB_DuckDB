"""
11_merge_BHP.do against the Python port, column by column.

The reference merges five files onto the SIAB, all many-to-one on betnr and
jahr, all keeping master and matched rows: the yearly establishment variable
blocks, one file per calendar year, and the four extension files for worker
inflow, worker outflow, establishment entry and establishment exit.

The port does not reproduce the year-by-year loop. Each yearly file holds
exactly one calendar year and is unique on betnr, so a SIAB episode can match at
most one of the forty-nine, and joining once against all of them stacked gives
the same answer. The key test below is what makes that claim testable rather
than asserted.

The same comparison the R arm runs in tests/testthat/test-reference-11_merge_BHP.R,
against the same committed fixture.
"""

import pytest

from conftest import assert_column_matches

STEP = "11_merge_BHP"

ANNUAL_COLUMNS = ["az_f", "az_reg", "az_azubi", "az_atz", "az_tz",
                  "az_f_vz", "az_f_tz", "az_reg_vz"]

FLOW_COLUMNS = ["ein_ges", "ein_gf", "ein_vz",
                "aus_ges", "aus_gf", "aus_vz"]

ENTRY_EXIT_COLUMNS = ["eintritt", "besch", "besch_vor", "status_vor",
                      "inflow", "austritt", "besch_nach", "status_nach",
                      "outflow"]


def test_key_sets_are_identical(reference_query):
    """A merge that keeps master and matched rows changes no key and no row."""
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
    counts = query(
        "SELECT (SELECT count(*) FROM stata) AS n_stata,"
        "       (SELECT count(*) FROM py) AS n_py"
    )

    assert counts["n_stata"][0] > 0
    assert only_stata == 0, f"{only_stata} keys are in the Stata fixture only"
    assert only_py == 0, f"{only_py} keys are in the Python dump only"
    assert counts["n_py"][0] == counts["n_stata"][0], "the merge changed the row count"


@pytest.mark.parametrize("column", ANNUAL_COLUMNS)
def test_the_yearly_establishment_blocks_match(reference_query, column):
    assert_column_matches(reference_query(STEP), column)


@pytest.mark.parametrize("column", FLOW_COLUMNS)
def test_the_worker_flow_columns_match(reference_query, column):
    assert_column_matches(reference_query(STEP), column)


@pytest.mark.parametrize("column", ENTRY_EXIT_COLUMNS)
def test_the_entry_and_exit_columns_match(reference_query, column):
    assert_column_matches(reference_query(STEP), column)


def test_quelle_matches(reference_query):
    """The column the two tests below read, so it has to line up first."""
    assert_column_matches(reference_query(STEP), "quelle")


def test_besch_is_filled_by_the_exit_merge_and_not_overwritten_by_it(reference_query):
    """`besch` is carried by both the entry file and the exit file, and both
    merges run with `update`, which fills a missing value in the master from the
    using file and leaves a non-missing one alone. Entry runs first, so a row
    that both files describe keeps the entry value. This is the one place in the
    step where the option changes the answer, and a port that used a plain left
    join for the exit merge would overwrite instead of fill."""
    query = reference_query(STEP)

    filled = query(
        "SELECT count(*) FILTER (WHERE besch IS NOT NULL) AS with_besch,"
        "       count(*) FILTER (WHERE eintritt IS NOT NULL) AS with_entry "
        "FROM stata"
    )
    assert filled["with_besch"][0] > filled["with_entry"][0], (
        "the fixture has no row where the exit merge filled besch, so this "
        "comparison cannot tell fill from overwrite"
    )

    assert_column_matches(query, "besch")


def test_only_employment_episodes_match_an_establishment_file(reference_query):
    """quelle 2 and 3 are benefit and job-search episodes: they carry no
    establishment number, so nothing in this step can reach them. A port that
    matched any of them would have joined on the wrong key."""
    query = reference_query(STEP)

    stray = query(
        "SELECT count(*) AS n FROM py WHERE az_f IS NOT NULL AND quelle <> 1"
    )["n"][0]
    assert stray == 0, f"{stray} non-employment episodes carry az_f"

    matched = query(
        "SELECT count(*) AS n FROM py WHERE az_f IS NOT NULL AND quelle = 1"
    )["n"][0]
    assert matched > 0, "no employment episode matched a yearly block at all"
