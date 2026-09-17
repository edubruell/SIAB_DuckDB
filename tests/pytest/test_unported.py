"""
What the Python arm still owes, on the record.

The counterpart of tests/testthat/test-reference-unported.R. Five of the
committed Stata fixtures have no Python comparison, and the reasons are not the
same one:

  09_restrictions      09_restrictions.do cuts the sample to certain groups and
                       takes the test data from 505,050 rows to 83,817. The
                       reference README calls the step project-specific rather
                       than part of the reusable prep, so neither arm ports it;
                       porting it would be a design decision, not a translation.
  10_wages_imputation  the imputation of right-censored wages needs a censored
                       normal regression, which Stata fits with `intreg` and the
                       R arm with `survival::survreg`. No drop-in exists in the
                       Python scientific stack, so the step raises rather than
                       guessing. See siab/steps/s07_wages_imputation.py.
  15_parallel_episodes these three read `wage_imp`, which the imputation would
  16_yearly_panel      produce, so they wait on it: make_py_dumps.py stops after
  16_monthly_panel     12_merge_AKM and writes no dump for them. All three do
                       have synthetic coverage, in test_parallel_episodes.py,
                       test_yearly_panel.py and test_monthly_panel.py.

The tests below keep that gap in the test output rather than in someone's
memory. Two of them are written to fail the day the debt is paid: the
imputation test breaks when the step stops raising, so nobody forgets to delete
it. The fixtures are checked for presence because they are the waiting half of
each comparison: the moment the imputation lands, the dumps can be regenerated
and the comparisons written against these files with nothing else to prepare.
"""

from __future__ import annotations

import polars as pl
import pytest

from conftest import fixtures_dir
from siab import steps
from siab.steps import impute_wages

# The five steps with a committed Stata fixture and no Python comparison.
UNCOMPARED = [
    "09_restrictions",
    "10_wages_imputation",
    "15_parallel_episodes",
    "16_yearly_panel",
    "16_monthly_panel",
]


# ======================================================================
#  10_wages_imputation.do: the one step that is not ported
# ======================================================================

def test_impute_wages_raises_and_says_what_it_is_waiting_for():
    """Delete this test when the imputation is ported; it will fail first."""
    with pytest.raises(NotImplementedError, match="censored normal regression"):
        impute_wages(pl.LazyFrame({"persnr": [1]}))


def test_impute_wages_names_both_references_the_port_has_to_match():
    # Stata's intreg and the R arm's survival::survreg are the two fits the
    # Python replacement has to agree with, so the message carries them rather
    # than leaving the next reader to find them in the do-file.
    with pytest.raises(NotImplementedError) as raised:
        impute_wages(pl.LazyFrame({"persnr": [1]}))

    message = str(raised.value)
    assert "intreg" in message
    assert "survreg" in message


def test_the_unported_step_still_holds_its_place_in_the_pipeline():
    # The placeholder is exported in the reference's order, so the call site is
    # written once and does not move when the body arrives.
    assert "impute_wages" in steps.__all__
    position = steps.__all__.index("impute_wages")
    assert steps.__all__[position - 1] == "deflate_wages"
    assert steps.__all__[position + 1] == "merge_annual_bhp"


# ======================================================================
#  The waiting half of each comparison
# ======================================================================

@pytest.mark.parametrize("fixture", UNCOMPARED)
def test_the_stata_fixture_of_an_uncompared_step_is_committed(fixture):
    path = fixtures_dir() / f"{fixture}.parquet"

    assert path.exists(), (
        f"{path} is missing. It is the oracle half of a comparison that is "
        f"waiting on the Python side; see this file's docstring for why the "
        f"Python half is not there yet."
    )

    # A committed fixture that reads as an empty table would be no oracle at
    # all, and the file being on disk would not show it.
    n_rows = pl.scan_parquet(path).select(pl.len()).collect().item()
    assert n_rows > 0, f"{path} holds no rows"
