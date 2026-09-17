"""generate_occupation_variables() against 14_occ_blossfeld.do.

The reference recodes the three-digit occupation code `beruf` into the twelve
Blossfeld classes, and closes the recode with `(else = 99)`. Stata's `else`
covers missing values as well as unmatched ones, so an episode with no
occupation at all leaves the step carrying 99, "not assignable".

The two sides reach the column from different positions in the pipeline. The
reference builds it at step 14, after the AKM merge; the port builds it in
generate_occupation_variables(), immediately after the biography step. Nothing
between the two positions touches `beruf`, which is what makes the comparison
meaningful, and one of the tests below is what makes that claim testable.
"""

import polars as pl

from conftest import assert_column_matches, siab_column_diff
from siab.common import classifications_dir

STEP = "14_occ_blossfeld"

# The row count of the FDZ test data at this step, as the R arm asserts it.
EXPECTED_ROWS = 505050


def test_the_blossfeld_classification_matches_the_reference_exactly(reference_query):
    query = reference_query(STEP)

    diff = siab_column_diff(query, "occ_blo")

    assert diff["shared"][0] == EXPECTED_ROWS
    assert diff["differing"][0] == 0, (
        f"occ_blo differs on {diff['differing'][0]} of {diff['shared'][0]} "
        f"shared rows"
    )


def test_beruf_itself_is_untouched_between_the_two_positions(reference_query):
    assert_column_matches(reference_query(STEP), "beruf")


def test_an_episode_with_no_occupation_carries_99_on_both_sides(reference_query):
    # The reference's `(else = 99)` reaches every episode without an
    # occupation, which on the test data is a fifth of all rows: benefit and
    # job-search spells carry no beruf. A port using a plain left join alone
    # leaves them missing.
    query = reference_query(STEP)

    no_beruf = query(
        "SELECT count(*) AS n, "
        "       count(*) FILTER (WHERE stata.occ_blo = 99) AS stata_99, "
        "       count(*) FILTER (WHERE py.occ_blo = 99) AS py_99 "
        "FROM stata JOIN py ON stata.persnr = py.persnr "
        "                  AND stata.spell = py.spell "
        "                  AND stata.begepi = py.begepi "
        "WHERE stata.beruf IS NULL"
    )

    assert no_beruf["n"][0] > 0
    assert no_beruf["stata_99"][0] == no_beruf["n"][0]
    assert no_beruf["py_99"][0] == no_beruf["n"][0]


def test_occ_blo_is_never_missing_in_the_dump(reference_query):
    query = reference_query(STEP)

    missing = query("SELECT count(*) AS n FROM py WHERE occ_blo IS NULL")["n"][0]

    assert missing == 0, f"{missing} rows of the dump carry no occ_blo at all"


def test_no_row_is_lost_or_duplicated_by_the_two_crosswalk_joins(reference_query):
    query = reference_query(STEP)

    counts = query(
        "SELECT (SELECT count(*) FROM stata) AS n_stata, "
        "       (SELECT count(*) FROM py) AS n_py, "
        "       (SELECT count(*) FROM (SELECT persnr, spell, begepi FROM stata "
        "                              EXCEPT "
        "                              SELECT persnr, spell, begepi FROM py)) "
        "           AS only_stata"
    )

    assert counts["n_stata"][0] == counts["n_py"][0]
    assert counts["only_stata"][0] == 0


def test_the_dump_carries_the_walkover_value_for_every_occupation(reference_query):
    # The reference is one oracle and the crosswalk csv is another. Checking
    # the dump against the csv as well says whether a disagreement, if one ever
    # appears, sits in the join or in the crosswalk itself.
    query = reference_query(STEP)

    walkover = pl.read_csv(classifications_dir() / "walkover_beruf_occblo.csv",
                           null_values=["NA"]).select("beruf", "occ_blo")
    pairs = ", ".join(f"({row[0]}, {row[1]})" for row in walkover.iter_rows())

    wrong = query(
        f"WITH walkover(beruf, occ_blo) AS (VALUES {pairs}) "
        "SELECT count(*) AS n FROM py JOIN walkover USING (beruf) "
        "WHERE py.occ_blo IS DISTINCT FROM walkover.occ_blo"
    )["n"][0]
    assert wrong == 0, f"{wrong} rows disagree with the Blossfeld walkover csv"

    unmatched = query(
        f"WITH walkover(beruf, occ_blo) AS (VALUES {pairs}) "
        "SELECT count(*) AS n, count(*) FILTER (WHERE py.occ_blo = 99) AS as_99 "
        "FROM py WHERE py.beruf IS NULL OR py.beruf NOT IN "
        "  (SELECT beruf FROM walkover)"
    )
    assert unmatched["n"][0] > 0, "the dump holds no beruf outside the walkover"
    assert unmatched["as_99"][0] == unmatched["n"][0], (
        "a beruf the walkover does not cover comes out as something other "
        "than 99"
    )
