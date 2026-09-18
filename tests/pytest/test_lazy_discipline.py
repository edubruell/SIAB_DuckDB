"""
The lazy check.

The user's condition on choosing polars was that the code stays lazy and a
check proves it, instead of a habit claiming it. Every step function is fed a
LazyFrame and has to hand a LazyFrame back.

The hole in this check is known and is not a hole in the rule: it does not see
a `collect()` in the middle of a step that re-wraps its result. Seven steps do
collect mid-chain. Six of them collect a summary rather than the data:
`drop_empty_columns` a null count per column, because which columns it returns
is what the count decides; `generate_educ_variable` a tabulation for the log;
`merge_basic_bhp`, `merge_annual_bhp` and `merge_akm` a uniqueness check on the
using file and a match rate, both of which the reference's `merge` also
computes; `build_monthly_panel` a row count it asserts on.

`impute_wages` is the seventh and the exception: it collects the data itself,
once, at the top of the step. It maximises a censored normal likelihood per
year/education/east cell in numpy, and there is no lazy expression for that. Its
place here is the boundary check every other step gets, which is all this file
ever proved. That is recorded so the next reader does not mistake the check for
more than it is: what it proves is the boundary, not the interior.

What it does prove, for every step in `siab.steps.__all__`:

  * a LazyFrame goes in and a LazyFrame comes back, so no step materialises
    its result and hands a DataFrame to the next one, and
  * the plan it hands back actually runs, because a LazyFrame that cannot be
    collected proves nothing.

Each step reads different columns, so one shared frame will not do. The
registry below holds the minimal frame each step needs, and the last test in
the file fails when a step is added to the package and not to the registry, so
the coverage cannot quietly fall behind the pipeline.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Callable

import pandas as pd
import polars as pl
import pyreadstat
import pytest

import siab.steps as steps
from siab.steps import (
    build_monthly_panel,
    build_yearly_panel,
    deflate_wages,
    drop_empty_columns,
    generate_biographic_variables,
    generate_educ_variable,
    generate_industry_variables,
    generate_limit_assess,
    generate_limit_marginal,
    generate_occupation_variables,
    handle_parallel_episodes,
    impute_wages,
    merge_akm,
    merge_annual_bhp,
    merge_basic_bhp,
    reallocate_one_time_payments,
    restrict_observation_period,
    split_episodes,
)

# Every step in the package is covered. The set is kept because the registry
# check below reads it, and because a step that has to be exempted again should
# have to say so in one named place.
NOT_PORTED: set[str] = set()

# The dtype each column travels through the pipeline with, so that the frames
# below only have to name the columns they need.
SCHEMA: dict[str, pl.DataType] = {
    "persnr": pl.Int32,
    "spell": pl.Int32,
    "betnr": pl.Int32,
    "quelle": pl.Int32,
    "erwstat": pl.Int32,
    "grund": pl.Int32,
    "gebjahr": pl.Int32,
    "year": pl.Int32,
    "age": pl.Int32,
    "beruf": pl.Int32,
    "w93_3_gen": pl.Int32,
    "ausbildung_imp": pl.Int32,
    "ao_bula": pl.Int32,
    "east": pl.Int32,
    "level1": pl.Int32,
    "level2": pl.Int32,
    "tage_bet": pl.Int32,
    "tage_job": pl.Int32,
    "tage_erw": pl.Int32,
    "tage_lst": pl.Int32,
    "parallel_benefits": pl.Int32,
    "frau": pl.Int32,
    "teilzeit": pl.Int32,
    "marginal": pl.Int32,
    "educ": pl.Int32,
    "cpi": pl.Float64,
    "wage_defl": pl.Float64,
    "limit_assess_defl": pl.Float64,
    "tentgelt": pl.Float64,
    "wage_imp": pl.Float64,
    "limit_marginal": pl.Float64,
    "limit_assess": pl.Float64,
    "parallel_wage_imp": pl.Float64,
}


def frame(**columns) -> pl.LazyFrame:
    """A LazyFrame of the named columns, on the pipeline's dtypes."""
    return pl.LazyFrame(
        columns,
        schema_overrides={k: v for k, v in SCHEMA.items() if k in columns},
    )


# ======================================================================
#  The minimal frame each step needs
#
#  A builder takes the test's tmp_path and hands back the frame and the
#  keyword arguments the step needs besides it. Only the steps that read an
#  external Stata file use the tmp_path; the rest ignore it.
# ======================================================================

Builder = Callable[[Path], tuple[pl.LazyFrame, dict]]


def one_spell() -> pl.LazyFrame:
    """A single BEH spell, what the first four steps read between them."""
    return frame(
        persnr=[1],
        spell=[1],
        betnr=[10],
        quelle=[1],
        begepi=[dt.date(2000, 3, 1)],
        endepi=[dt.date(2000, 8, 31)],
        begorig=[dt.date(2000, 3, 1)],
        endorig=[dt.date(2000, 8, 31)],
        tentgelt=[100.0],
        erwstat=[101],
        grund=[30],
        gebjahr=[1970],
        year=[2000],
        age=[30],
    )


def spell_builder(_: Path) -> tuple[pl.LazyFrame, dict]:
    return one_spell(), {}


def observation_period_builder(_: Path) -> tuple[pl.LazyFrame, dict]:
    # `keep if inrange(jahr, minYear, maxYear)` reads the year alone.
    return frame(persnr=[1], year=[2000]), {}


def occupations_builder(_: Path) -> tuple[pl.LazyFrame, dict]:
    # Both crosswalks are joined on `beruf`; 11 is in both of them.
    return frame(persnr=[1], beruf=[11]), {}


def industries_builder(_: Path) -> tuple[pl.LazyFrame, dict]:
    # The two one-digit mappings are ranges over the time-consistent WZ93 code.
    return frame(persnr=[1], w93_3_gen=[101]), {}


def education_builder(_: Path) -> tuple[pl.LazyFrame, dict]:
    return frame(persnr=[1], ausbildung_imp=[2]), {}


def assessment_ceiling_builder(_: Path) -> tuple[pl.LazyFrame, dict]:
    # `east` is built here out of the establishment's federal state.
    return frame(persnr=[1], year=[2000], ao_bula=[1]), {}


def marginal_builder(_: Path) -> tuple[pl.LazyFrame, dict]:
    # This one runs after the ceiling step, so `east` already exists.
    return frame(persnr=[1], year=[2000], east=[0], tentgelt=[100.0]), {}


def deflation_builder(_: Path) -> tuple[pl.LazyFrame, dict]:
    return frame(persnr=[1], year=[2000], tentgelt=[100.0],
                 limit_marginal=[10.0], limit_assess=[300.0]), {}


def imputation_builder(_: Path) -> tuple[pl.LazyFrame, dict]:
    """Two BEH spells in one imputation cell, one of them above the ceiling.

    The cell is far too thin to fit, which is the point: the step has to hand
    back a plan either way, and the unfittable branch is the one that would
    otherwise only ever run on real data.
    """
    return frame(
        persnr=[1, 2],
        spell=[1, 1],
        # begepi completes the key: the step sorts each cell on all three
        # before it draws, because persnr and spell alone do not name a row
        # once the episodes have been split.
        begepi=[dt.date(2000, 1, 1), dt.date(2000, 1, 1)],
        betnr=[10, 10],
        quelle=[1, 1],
        year=[2000, 2000],
        age=[30, 45],
        frau=[0, 1],
        teilzeit=[0, 0],
        tage_job=[100, 900],
        educ=[2, 2],
        east=[0, 0],
        marginal=[0, 0],
        cpi=[100.0, 100.0],
        wage_defl=[80.0, 400.0],
        limit_assess_defl=[300.0, 300.0],
    ), {"seed": 123}


def parallel_builder(_: Path) -> tuple[pl.LazyFrame, dict]:
    # `wage_imp` comes from the imputation; the value below is arbitrary and
    # only has to be there for the sort to have a key.
    return frame(
        persnr=[1],
        spell=[1],
        level1=[0],
        level2=[0],
        begepi=[dt.date(2000, 1, 1)],
        endepi=[dt.date(2000, 6, 30)],
        quelle=[1],
        tentgelt=[100.0],
        wage_imp=[110.0],
        tage_bet=[10],
    ), {}


def panel_builder(_: Path) -> tuple[pl.LazyFrame, dict]:
    """One episode covering the whole of 2000, so both cutoffs fall inside it."""
    return frame(
        persnr=[1],
        year=[2000],
        begepi=[dt.date(2000, 1, 1)],
        endepi=[dt.date(2000, 12, 31)],
        quelle=[1],
        erwstat=[101],
        parallel_benefits=[0],
        parallel_wage_imp=[100.0],
        tage_bet=[366],
        tage_job=[366],
        tage_erw=[366],
        tage_lst=[0],
    ), {}


# ----------------------------------------------------------------------
#  The three steps that read an external Stata file
#
#  Each gets one written with pyreadstat under tmp_path, so the check stays
#  independent of local_context/ and of the FDZ test data. The cost is that
#  these three carry a fixture the other fifteen do not need: the frames stay
#  minimal, but the builders are no longer only a table.
# ----------------------------------------------------------------------

def write_dta(path: Path, columns: dict) -> Path:
    pyreadstat.write_dta(pd.DataFrame(columns), str(path))
    return path


def episodes_with_establishment() -> pl.LazyFrame:
    """What the merges key on: the person, the establishment and the year."""
    return frame(
        persnr=[1],
        spell=[1],
        begepi=[dt.date(2000, 3, 1)],
        betnr=[10],
        year=[2000],
        quelle=[1],
    )


def basic_bhp_builder(tmp_path: Path) -> tuple[pl.LazyFrame, dict]:
    path = write_dta(tmp_path / "bhp_basis.dta",
                     {"betnr_siab": [10], "jahr": [2000],
                      "ao_bula": [1], "w93_3_gen": [101]})
    return episodes_with_establishment(), {"bhp_file": path}


def annual_bhp_builder(tmp_path: Path) -> tuple[pl.LazyFrame, dict]:
    """All five files of 11_merge_BHP.do, one establishment-year each.

    Every module gets a file, so the step runs its whole chain rather than
    warning its way past the missing ones.
    """
    prefix = "LAZY_BHP"
    folder = tmp_path / "bhp_annual"
    folder.mkdir()
    write_dta(folder / f"{prefix}_bhp_v1_2000.dta",
              {"betnr_siab": [10], "jahr": [2000], "az_f": [12], "az_reg": [3]})
    write_dta(folder / f"{prefix}_bhp_inflow_v1.dta",
              {"betnr_siab": [10], "jahr": [2000], "ein_ges": [100]})
    write_dta(folder / f"{prefix}_bhp_outflow_v1.dta",
              {"betnr_siab": [10], "jahr": [2000], "aus_ges": [50]})
    write_dta(folder / f"{prefix}_bhp_entry_v1.dta",
              {"betnr_siab": [10], "jahr": [2000], "eintritt": [1]})
    write_dta(folder / f"{prefix}_bhp_exit_v1.dta",
              {"betnr_siab": [10], "jahr": [2000], "austritt": [0]})
    return episodes_with_establishment(), {
        "bhp_folder": folder, "prefix": prefix, "years": [2000],
    }


def akm_builder(tmp_path: Path) -> tuple[pl.LazyFrame, dict]:
    """One establishment row and one person row; the effects are arbitrary."""
    windows = ["1985_1992", "1993_2000", "2001_2008", "2009_2016", "2017_2023"]
    estab = write_dta(
        tmp_path / "akm_estab.dta",
        {"betnr_siab": [10], **{f"feff_{w}": [0.1] for w in windows}},
    )
    pers = write_dta(
        tmp_path / "akm_pers.dta",
        {"persnr_siab": [1], **{f"peff_{w}": [0.2] for w in windows}},
    )
    return episodes_with_establishment(), {
        "akm_estab_file": estab, "akm_pers_file": pers,
    }


# In the order siab.steps.__all__ lists them, which is the order main.py calls
# them in.
REGISTRY: list[tuple[Callable, Builder]] = [
    (drop_empty_columns, spell_builder),
    (split_episodes, spell_builder),
    (reallocate_one_time_payments, spell_builder),
    (generate_biographic_variables, spell_builder),
    (restrict_observation_period, observation_period_builder),
    (generate_occupation_variables, occupations_builder),
    (merge_basic_bhp, basic_bhp_builder),
    (generate_industry_variables, industries_builder),
    (generate_educ_variable, education_builder),
    (generate_limit_assess, assessment_ceiling_builder),
    (generate_limit_marginal, marginal_builder),
    (deflate_wages, deflation_builder),
    (impute_wages, imputation_builder),
    (merge_annual_bhp, annual_bhp_builder),
    (merge_akm, akm_builder),
    (handle_parallel_episodes, parallel_builder),
    (build_yearly_panel, panel_builder),
    (build_monthly_panel, panel_builder),
]

STEP_IDS = [step.__name__ for step, _ in REGISTRY]


@pytest.fixture(params=REGISTRY, ids=STEP_IDS)
def step_case(request, tmp_path):
    """One step, the minimal frame it reads and the arguments it needs."""
    step, builder = request.param
    data, kwargs = builder(tmp_path)
    assert isinstance(data, pl.LazyFrame), (
        f"the registry entry for {step.__name__} does not hand in a LazyFrame"
    )
    return step, data, kwargs


def test_step_takes_and_returns_a_lazyframe(step_case):
    step, data, kwargs = step_case
    result = step(data, **kwargs)
    assert isinstance(result, pl.LazyFrame), (
        f"{step.__name__} returned {type(result).__name__}, not a LazyFrame"
    )


def test_step_result_collects(step_case):
    """A LazyFrame that cannot be collected proves nothing, so collect it once."""
    step, data, kwargs = step_case
    result = step(data, **kwargs)
    assert isinstance(result, pl.LazyFrame), (
        f"{step.__name__} returned {type(result).__name__}, not a LazyFrame"
    )
    assert result.collect().height >= 1, (
        f"{step.__name__} collected to an empty table, so the plan it handed "
        f"back says nothing about whether it runs"
    )


def test_the_registry_covers_every_step():
    """A step added to the package and not to the registry fails here.

    Without this the check would go on passing while the pipeline grew past
    it, which is the failure mode the whole file exists to rule out.
    """
    covered = {step.__name__ for step, _ in REGISTRY}
    missing = [name for name in steps.__all__
               if name not in covered and name not in NOT_PORTED]

    assert not missing, (
        "no lazy check covers " + ", ".join(missing) +
        "; add a minimal frame for each to REGISTRY"
    )

    # And the other way round, so a step deleted from the package does not
    # leave a registry entry behind that no longer means anything.
    stale = [name for name in covered if name not in steps.__all__]
    assert not stale, "the registry names steps the package does not export: " + \
        ", ".join(stale)
