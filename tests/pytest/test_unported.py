"""
What the Python arm still owes, on the record.

The counterpart of tests/testthat/test-reference-unported.R. One committed
Stata fixture has no Python comparison:

  09_restrictions      09_restrictions.do cuts the sample to certain groups and
                       takes the test data from 505,050 rows to 83,817. The
                       reference README calls the step project-specific rather
                       than part of the reusable prep, so neither arm ports it;
                       porting it would be a design decision, not a translation.

10_wages_imputation left this list on 2026-09-17, when the step was ported. The
two tests that asserted its `NotImplementedError` were written to fail the day
the debt was paid, and they did; the comparison lives in
test_reference_10_wages_imputation.py now.

15_parallel_episodes, 16_yearly_panel and 16_monthly_panel left it the same
day. All three read `wage_imp`, so they were waiting on the imputation and on
nothing else; once make_py_dumps.py ran past 12_merge_AKM their comparisons
went into test_reference_15_parallel_episodes.py,
test_reference_16_yearly_panel.py and test_reference_16_monthly_panel.py. Every
committed fixture except 09_restrictions is now compared against this arm.

The tests below keep the remaining gap in the test output rather than in
someone's memory. The fixtures are checked for presence because they are the
waiting half of each comparison: the dumps can be regenerated and the
comparisons written against these files with nothing else to prepare.
"""

from __future__ import annotations

import polars as pl
import pytest

from conftest import fixtures_dir
from siab import steps

# The one step with a committed Stata fixture and no Python comparison.
UNCOMPARED = [
    "09_restrictions",
]


# ======================================================================
#  The pipeline's shape
# ======================================================================

def test_the_imputation_holds_its_place_in_the_pipeline():
    # It is exported in the reference's order, between the deflation that makes
    # its dependent variable and the establishment merge that follows it.
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
