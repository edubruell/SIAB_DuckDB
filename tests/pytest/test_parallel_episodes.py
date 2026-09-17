"""
Synthetic tests for handle_parallel_episodes().

The step is the port of 15_parallel_episodes.do. It aggregates what a person's
simultaneously starting spells say about each other, keeps one of them as the
main episode, and numbers what is left.

There is no fixture comparison for this step yet. `wage_imp` comes out of
10_wages_imputation.do, which the Python arm has not ported, so
tests/pytest/make_py_dumps.py stops before step 15 and there is no
15_parallel_episodes.parquet on this side to compare against the committed
Stata one. Until that changes these hand-built tables are the only coverage the
step has, so they carry the cases
tests/testthat/test-reference-15_parallel_episodes.R makes against the
reference: which row of a group survives, the four aggregates, and the zero a
benefit-only group has to come out with rather than a missing.

One case is worth naming because the obvious reasoning gets it wrong. Stata
stores a missing value as a number larger than any other, which suggests that
`gsort -wage_imp` should put a spell with no imputed wage first and make it the
main episode. It does not. `gsort` defaults to `mlast`, holding missing values
back to the end whichever way the key runs, and the reference passes no
`mfirst`. Checked against Stata MP 17, because the two readings keep a
different episode on real data. The test below pins the checked behaviour.
"""

import datetime as dt

import polars as pl
import pytest

from siab.steps import handle_parallel_episodes

# The columns the step reads besides the ones each test varies. level1 and
# level2 are 03_SIAB_bio.do's observation counters, which the step drops.
FIXED = {
    "level1": pl.Int32,
    "level2": pl.Int32,
    "quelle": pl.Int32,
    "tage_bet": pl.Int32,
    "persnr": pl.Int32,
    "spell": pl.Int32,
}


def group(spell, quelle, tentgelt, wage_imp, tage_bet, persnr=None, begepi=None):
    """One person's spells, all starting on the same day unless told otherwise."""
    n = len(spell)
    return pl.LazyFrame(
        {
            "persnr": persnr if persnr is not None else [1] * n,
            "spell": spell,
            "level1": [0] * n,
            "level2": list(range(n)),
            "begepi": begepi if begepi is not None else [dt.date(2000, 1, 1)] * n,
            "endepi": [dt.date(2000, 6, 30)] * n,
            "quelle": quelle,
            "tentgelt": tentgelt,
            "wage_imp": wage_imp,
            "tage_bet": tage_bet,
        },
        schema_overrides=FIXED,
    )


# Two jobs starting on the same day, one with the higher wage and the other with
# the longer tenure, so the two settings of `handling` cannot pick the same row.
def two_jobs():
    return group(
        spell=[1, 2],
        quelle=[1, 1],
        tentgelt=[100.0, 50.0],
        wage_imp=[110.0, 60.0],
        tage_bet=[10, 200],
    )


def test_only_one_episode_per_person_and_start_date_survives():
    out = handle_parallel_episodes(two_jobs()).collect()
    assert out.height == 1


def test_the_highest_imputed_wage_wins_under_wage_handling():
    out = handle_parallel_episodes(two_jobs(), handling="wage").collect()
    assert out["wage_imp"][0] == 110.0
    assert out["tage_bet"][0] == 10


def test_the_longest_tenure_wins_under_tenure_handling():
    out = handle_parallel_episodes(two_jobs(), handling="tenure").collect()
    assert out["tage_bet"][0] == 200
    assert out["wage_imp"][0] == 60.0


def test_wage_is_the_default_handling():
    default = handle_parallel_episodes(two_jobs()).collect()
    explicit = handle_parallel_episodes(two_jobs(), handling="wage").collect()
    assert default["tage_bet"][0] == explicit["tage_bet"][0] == 10


def test_an_unknown_handling_is_refused():
    with pytest.raises(ValueError, match="'wage' or 'tenure'"):
        handle_parallel_episodes(two_jobs(), handling="duration")


# `gsort persnr begepi quelle ...` sorts the source ascending, before either
# wage or tenure is looked at, so an employment spell outranks a benefit spell
# however short or badly paid it is.
@pytest.mark.parametrize("handling", ["wage", "tenure"])
def test_the_employment_spell_beats_the_benefit_spell_whatever_the_sort(handling):
    frame = group(
        spell=[1, 2],
        quelle=[2, 1],
        tentgelt=[None, 10.0],
        wage_imp=[None, 10.0],
        tage_bet=[None, 1],
    )
    out = handle_parallel_episodes(frame, handling=handling).collect()

    assert out.height == 1
    assert out["quelle"][0] == 1


# `gsort` holds a missing value back to the end whichever way the key runs,
# because its default is `mlast` and the reference passes no `mfirst`. So a
# spell whose imputed wage never arrived sorts behind every reported one and
# loses, rather than winning on the strength of missing being the largest
# number. `_null_placement()` in the step is what pins polars to that, and this
# is the case that would silently flip if someone rewrote it from first
# principles.
def test_a_missing_imputed_wage_loses_to_every_reported_one():
    frame = group(
        spell=[1, 2],
        quelle=[1, 1],
        tentgelt=[100.0, 50.0],
        wage_imp=[110.0, None],
        tage_bet=[200, 10],
    )
    out = handle_parallel_episodes(frame, handling="wage").collect()

    assert out.height == 1
    assert out["wage_imp"][0] == 110.0, (
        "the spell with no imputed wage sorts last under gsort's mlast "
        "default, so the reported wage wins"
    )
    assert out["tage_bet"][0] == 200
    # The missing contributes nothing to the total, as egen total() has it.
    assert out["parallel_wage_imp"][0] == 110.0


# The same rule on an ascending key. `quelle` leads the sort, and a missing one
# goes last there too, so a spell with no source loses however large its wage or
# tenure. Ascending is the direction where the two readings agree, which is why
# it is the descending case above that had to be checked against Stata.
@pytest.mark.parametrize("handling", ["wage", "tenure"])
def test_a_missing_source_sorts_last_under_the_ascending_key(handling):
    frame = group(
        spell=[1, 2],
        quelle=[None, 1],
        tentgelt=[900.0, 50.0],
        wage_imp=[900.0, 60.0],
        tage_bet=[900, 5],
    )
    out = handle_parallel_episodes(frame, handling=handling).collect()

    assert out.height == 1
    assert out["quelle"][0] == 1, (
        "a missing quelle sorts last, so the spell carrying it goes to the "
        "back whatever its wage or tenure"
    )
    # A missing quelle is neither an employment nor a benefit spell.
    assert out["parallel_jobs"][0] == 1
    assert out["parallel_benefits"][0] == 0
    assert out["parallel_wage"][0] == 50.0


# 15_parallel_episodes_tiebreak.patch adds `spell` as the last sort key, because
# the reference keeps the first row of each group and its own comment asks for
# an unambiguous order. Two spells alike on everything else therefore resolve on
# the lower spell number.
@pytest.mark.parametrize("handling", ["wage", "tenure"])
def test_the_spell_number_breaks_a_tie_on_everything_else(handling):
    # tentgelt is not one of the sort keys, so it can mark the two rows apart
    # without changing which of them wins.
    frame = group(
        spell=[7, 3],
        quelle=[1, 1],
        tentgelt=[100.0, 200.0],
        wage_imp=[110.0, 110.0],
        tage_bet=[42, 42],
    )
    out = handle_parallel_episodes(frame, handling=handling).collect()

    assert out.height == 1
    assert out["tentgelt"][0] == 200.0, (
        "the lower spell number has to win, and it is the second row here"
    )


def test_the_four_aggregates_cover_the_whole_group():
    frame = group(
        spell=[1, 2, 3],
        quelle=[1, 1, 2],
        tentgelt=[100.0, 50.0, None],
        wage_imp=[110.0, 60.0, None],
        tage_bet=[10, 200, None],
    )
    out = handle_parallel_episodes(frame).collect()

    # Only the employment spells count, and both wage totals skip the benefit
    # spell rather than carrying its missing into the sum.
    assert out["parallel_jobs"][0] == 2
    assert out["parallel_wage"][0] == 150.0
    assert out["parallel_wage_imp"][0] == 170.0
    # The benefit spell is dropped by the selection but its indicator is not.
    assert out["parallel_benefits"][0] == 1


# egen total() reads a missing contribution as nothing at all, so a group with
# no employment spell comes out at zero. A null here would propagate into
# year_labor_earn at the panel step and quietly empty it.
def test_a_benefit_only_group_gets_zero_rather_than_missing():
    frame = group(
        spell=[1, 2],
        quelle=[2, 2],
        tentgelt=[None, None],
        wage_imp=[None, None],
        tage_bet=[None, None],
    )
    out = handle_parallel_episodes(frame).collect()

    assert out["parallel_jobs"][0] == 0
    assert out["parallel_wage"][0] == 0.0
    assert out["parallel_wage_imp"][0] == 0.0
    assert out["parallel_wage"][0] is not None
    assert out["parallel_wage_imp"][0] is not None
    assert out["parallel_benefits"][0] == 1


def test_a_group_with_no_benefit_spell_gets_a_zero_indicator():
    out = handle_parallel_episodes(two_jobs()).collect()
    assert out["parallel_benefits"][0] == 0


# The aggregates are taken per person and episode start, so two starts of one
# person do not pool.
def test_the_aggregates_do_not_reach_across_episode_starts():
    frame = group(
        spell=[1, 2, 3],
        quelle=[1, 1, 1],
        tentgelt=[100.0, 50.0, 20.0],
        wage_imp=[110.0, 60.0, 30.0],
        tage_bet=[10, 200, 5],
        begepi=[dt.date(2000, 1, 1), dt.date(2000, 1, 1), dt.date(2001, 1, 1)],
    )
    out = handle_parallel_episodes(frame).collect().sort("begepi")

    assert out.height == 2
    assert out["parallel_jobs"].to_list() == [2, 1]
    assert out["parallel_wage"].to_list() == [150.0, 20.0]


# `by persnr: gen nspell = _n` on data sorted by person and episode start.
def test_nspell_counts_the_surviving_episodes_of_each_person_from_one():
    frame = group(
        spell=[1, 2, 3, 4],
        quelle=[1, 1, 1, 1],
        tentgelt=[100.0, 50.0, 20.0, 10.0],
        wage_imp=[110.0, 60.0, 30.0, 15.0],
        tage_bet=[10, 20, 30, 40],
        persnr=[1, 1, 2, 2],
        begepi=[dt.date(2000, 1, 1), dt.date(2001, 1, 1),
                dt.date(2000, 1, 1), dt.date(2002, 1, 1)],
    )
    out = handle_parallel_episodes(frame).collect().sort(["persnr", "nspell"])

    assert out.height == 4
    assert out["nspell"].to_list() == [1, 2, 1, 2]
    assert out["begepi"].to_list() == [
        dt.date(2000, 1, 1), dt.date(2001, 1, 1),
        dt.date(2000, 1, 1), dt.date(2002, 1, 1),
    ]


def test_the_step_generates_its_five_columns_and_drops_its_three():
    out = handle_parallel_episodes(two_jobs()).collect()

    for column in ("nspell", "parallel_jobs", "parallel_wage",
                   "parallel_wage_imp", "parallel_benefits"):
        assert column in out.columns, f"{column} is not generated"

    for column in ("spell", "level1", "level2"):
        assert column not in out.columns, f"{column} should have been dropped"


def test_no_working_column_survives_the_step():
    out = handle_parallel_episodes(two_jobs()).collect()
    for column in ("tmp_jobs", "tmp_benefits", "tmp_wage", "tmp_wage_imp"):
        assert column not in out.columns
