"""
12_merge_AKM.do against the Python port.

The reference merges two files onto the SIAB, the establishment wage effects
many-to-one on betnr and the person wage effects many-to-one on persnr, both
keeping master and matched rows. Neither file carries a year: an effect is
estimated once per window, so the five columns of each side arrive together.

NEITHER SIDE OF THIS COMPARISON USES REAL AKM EFFECTS. No FDZ test product
carries them, so the fixture run fabricates both files from the shape the FDZ
methodology report describes, and the Stata reference run and this port then
read the same two fabricated files. The values are normal draws with the
documented dispersion and nothing else, so this file compares which episodes
carry an effect and never a value.

That is enough to compare the step, because the step is a merge: what it decides
is which episode receives which row, and that is settled by the keys. It is not
enough to say anything about AKM effects, wages, or merge rates on the real
delivery, and no test here should be read as doing so.
"""

import pytest

from conftest import assert_column_matches

STEP = "12_merge_AKM"

WINDOWS = ["1985_1992", "1993_2000", "2001_2008", "2009_2016", "2017_2023"]
ESTAB_COLUMNS = [f"feff_{w}" for w in WINDOWS]
PERSON_COLUMNS = [f"peff_{w}" for w in WINDOWS]
EFFECT_COLUMNS = ESTAB_COLUMNS + PERSON_COLUMNS

KEY_ON = ("stata.persnr = py.persnr AND stata.spell = py.spell "
          "AND stata.begepi = py.begepi")


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


@pytest.mark.parametrize("column", EFFECT_COLUMNS)
def test_the_same_episodes_carry_an_effect(reference_query, column):
    """Episode for episode, an effect is present on both sides or on neither.

    The values are fabricated noise, so the comparison is on presence. An
    episode whose establishment or person is outside the estimation's connected
    set keeps the effect missing, and a port that dropped those rows, or filled
    them with zero, would differ here.
    """
    query = reference_query(STEP)

    result = query(
        "SELECT count(*) AS shared,"
        f"      count(*) FILTER (WHERE (stata.{column} IS NULL)"
        f"                          <> (py.{column} IS NULL)) AS differing "
        f"FROM stata JOIN py ON {KEY_ON}"
    )
    shared = result["shared"][0]
    differing = result["differing"][0]

    assert shared > 0, f"{column}: the two halves share no rows at all"
    assert differing == 0, (
        f"{column}: {differing} of {shared} shared rows disagree on whether "
        f"the episode carries an effect"
    )


@pytest.mark.parametrize("column", EFFECT_COLUMNS)
def test_coverage_is_partial_on_both_sides(reference_query, column):
    """Coverage is below 100 per cent by construction in the fabricated files.

    Without this, the presence comparison above would also pass on a column
    that is missing everywhere, or present everywhere, on both sides.
    """
    query = reference_query(STEP)

    counts = query(
        f"SELECT count(*) FILTER (WHERE {column} IS NULL) AS n_null,"
        f"       count(*) FILTER (WHERE {column} IS NOT NULL) AS n_set "
        "FROM py"
    )
    assert counts["n_null"][0] > 0, f"{column} is set on every episode"
    assert counts["n_set"][0] > 0, f"{column} is missing on every episode"


def test_the_effects_are_not_filled_with_zero(reference_query):
    """A non-match stays missing; nothing is imputed at the mean of the window."""
    query = reference_query(STEP)

    for column in EFFECT_COLUMNS:
        zeros = query(
            f"SELECT count(*) AS n FROM py WHERE {column} = 0.0"
        )["n"][0]
        assert zeros == 0, f"{column} carries an exact zero on {zeros} episodes"


def test_the_two_sides_are_merged_on_different_keys(reference_query):
    """The establishment side is joined on betnr and the person side on persnr,
    so the two coverages have to differ. A port that merged both on the same key
    would line up perfectly, here and nowhere in the fixture."""
    query = reference_query(STEP)

    crossed = query(
        "SELECT count(*) AS n FROM py "
        "WHERE (feff_2001_2008 IS NULL) <> (peff_2001_2008 IS NULL)"
    )["n"][0]
    assert crossed > 0, (
        "every episode agrees on the two sides' coverage, which is what a "
        "single-key merge would look like"
    )
