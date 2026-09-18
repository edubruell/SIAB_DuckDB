"""
15_parallel_episodes.do against the Python port.

The reference aggregates over the episodes a person has running at the same
start date, keeps one of them, and numbers what is left. It defines the kept
one by a sort: source first, then longest tenure, then highest imputed wage.
The port takes that rule as handling = "tenure"; its other setting sorts on the
imputed wage first, and because both sides draw their own random terms for a
censored wage, that setting would make the two halves keep different rows.
make_py_dumps.py therefore calls the step with "tenure", as make_r_dumps.R
does, while main.py and siab_main.R both pass "wage".

Both sides carry `spell` as the last sort key, which the reference's own
comment beside the line asks for and which
15_parallel_episodes_tiebreak.patch supplies. Without it 1,945 of the 479,806
person-episode groups are tied on everything the sort looks at and the kept row
is arbitrary on each side separately.

The step drops `spell` on its way out, so the two halves join on the person and
the episode start alone.
"""

import pytest

from conftest import assert_column_matches, siab_column_moments

STEP = "15_parallel_episodes"

# Stata's egen total() stores its result as a float, which carries about seven
# decimal digits, so a summed wage agrees to that and no further.
FLOAT_TOLERANCE = 1e-6

# What a column carrying a random draw is allowed to be off by, the same bound
# wage_imp itself gets at step 10.
DISTRIBUTION_TOLERANCE = 0.01

STEP_KEY = ("persnr", "begepi")


def test_the_two_halves_keep_the_same_set_of_episodes(reference_query):
    query = reference_query(STEP)

    counts = query(
        "SELECT (SELECT count(*) FROM stata) AS n_stata,"
        "       (SELECT count(*) FROM py) AS n_py,"
        "       (SELECT count(*) FROM (SELECT persnr, begepi FROM stata"
        "                              EXCEPT"
        "                              SELECT persnr, begepi FROM py)) AS only_stata,"
        "       (SELECT count(*) FROM (SELECT persnr, begepi FROM py"
        "                              EXCEPT"
        "                              SELECT persnr, begepi FROM stata)) AS only_py"
    )

    assert counts["n_stata"][0] > 0
    assert counts["n_py"][0] == counts["n_stata"][0]
    assert counts["only_stata"][0] == 0, (
        f'{counts["only_stata"][0]} episodes are in the Stata fixture only')
    assert counts["only_py"][0] == 0, (
        f'{counts["only_py"][0]} episodes are in the Python dump only')


# The selection is settled by quelle and tage_bet, so a mismatch on either is a
# mismatch on which row of a parallel group survived, not on a value.
@pytest.mark.parametrize("column", ["quelle", "tage_bet"])
def test_the_same_row_of_each_parallel_group_survives_on_both_sides(
        reference_query, column):
    assert_column_matches(reference_query(STEP), column, key=STEP_KEY)


@pytest.mark.parametrize("column", ["parallel_jobs", "parallel_benefits", "nspell"])
def test_the_counts_and_the_benefit_indicator_match_the_reference_exactly(
        reference_query, column):
    assert_column_matches(reference_query(STEP), column, key=STEP_KEY)


def test_the_summed_reported_wage_matches_to_stata_float_precision(reference_query):
    # parallel_wage sums the reported wage over a group, and that sum is a
    # float on the Stata side.
    assert_column_matches(reference_query(STEP), "parallel_wage",
                          tolerance=FLOAT_TOLERANCE, key=STEP_KEY)


def test_the_summed_imputed_wage_agrees_in_distribution_within_one_percent(
        reference_query):
    # parallel_wage_imp sums the imputed wage instead. Every censored wage on
    # either side carries a random term drawn from that side's own generator,
    # so the column cannot agree row by row and is bounded as a distribution,
    # the same way wage_imp itself is at step 10.
    moments = siab_column_moments(reference_query(STEP), "parallel_wage_imp",
                                  key=STEP_KEY)

    assert moments["shared"][0] > 0
    for statistic in ("mean", "q25", "q75"):
        reference = moments[f"stata_{statistic}"][0]
        port = moments[f"py_{statistic}"][0]
        gap = abs(port - reference) / abs(reference)
        assert gap < DISTRIBUTION_TOLERANCE, (
            f"the {statistic} of parallel_wage_imp is {gap:.2%} away from the "
            f"reference's {reference:.2f}")


def test_a_group_with_no_employment_episode_gets_zero_not_missing(reference_query):
    # The aggregate counts only employment episodes, so a person with none in a
    # group has to come out at zero rather than missing: Stata's egen total()
    # treats a missing contribution as nothing at all.
    query = reference_query(STEP)

    nulls = query(
        "SELECT count(*) FILTER (WHERE stata.parallel_jobs IS NULL) AS stata_null,"
        "       count(*) FILTER (WHERE py.parallel_jobs IS NULL) AS py_null,"
        "       count(*) FILTER (WHERE py.parallel_jobs = 0) AS py_zero"
        " FROM stata JOIN py ON stata.persnr = py.persnr"
        "                   AND stata.begepi = py.begepi"
    )

    assert nulls["stata_null"][0] == 0
    assert nulls["py_null"][0] == 0
    assert nulls["py_zero"][0] > 0
