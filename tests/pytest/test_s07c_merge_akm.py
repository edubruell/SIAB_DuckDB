"""
Synthetic tests for the AKM merge.

Two tiny AKM files and a handful of episodes in a tmp_path, so these run
without the FDZ test data, without the fabricated AKM files and without Stata.

The step is two many-to-one joins keeping every SIAB episode: the establishment
effects on betnr, the person effects on persnr, neither carrying a year. What
is worth checking is therefore which episode receives which row, and that an
episode outside the connected set comes out with five missing effects rather
than being dropped or filled with zero. Nothing here says anything about AKM
effects themselves; the values below are arbitrary.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd
import polars as pl
import pyreadstat
import pytest

from siab.steps import merge_akm

WINDOWS = ["1985_1992", "1993_2000", "2001_2008", "2009_2016", "2017_2023"]
FEFF = [f"feff_{w}" for w in WINDOWS]
PEFF = [f"peff_{w}" for w in WINDOWS]


def write_dta(path, columns: dict) -> None:
    pyreadstat.write_dta(pd.DataFrame(columns), str(path))


def episodes() -> pl.LazyFrame:
    """Four episodes covering the four corners of a two-sided merge.

    persnr 1  betnr 10   both sides match
    persnr 2  betnr 20   the establishment is outside the connected set
    persnr 3  betnr 10   the establishment matches, the person does not
    persnr 4  no betnr   a benefit episode: only the person side can reach it
    """
    return pl.LazyFrame(
        {
            "persnr": [1, 2, 3, 4],
            "spell": [1, 1, 1, 1],
            "begepi": [dt.date(2000, 3, 1), dt.date(2000, 4, 1),
                       dt.date(2000, 5, 1), dt.date(2000, 6, 1)],
            "betnr": [10, 20, 10, None],
            "quelle": [1, 1, 1, 2],
        },
        schema_overrides={"persnr": pl.Int32, "spell": pl.Int32,
                          "betnr": pl.Int32, "quelle": pl.Int32},
    )


@pytest.fixture
def akm_files(tmp_path):
    """The two AKM files: one row per establishment, one per person, no year.

    Establishment 10 is in the connected set in every window; establishment 30
    is in the file but in no episode. Persons 1 and 4 have an effect and 2 and
    3 do not. The 1985-1992 window is missing for establishment 10 on purpose:
    an effect is estimated per window, so coverage is per column, not per row.
    """
    estab = tmp_path / "akm_estab.dta"
    pers = tmp_path / "akm_pers.dta"
    write_dta(estab, {"betnr_siab": [10, 30],
                      "feff_1985_1992": [float("nan"), -0.5],
                      "feff_1993_2000": [0.11, -0.51],
                      "feff_2001_2008": [0.12, -0.52],
                      "feff_2009_2016": [0.13, -0.53],
                      "feff_2017_2023": [0.14, -0.54]})
    write_dta(pers, {"persnr_siab": [1, 4],
                     "peff_1985_1992": [0.21, 0.41],
                     "peff_1993_2000": [0.22, 0.42],
                     "peff_2001_2008": [0.23, 0.43],
                     "peff_2009_2016": [0.24, 0.44],
                     "peff_2017_2023": [0.25, 0.45]})
    return estab, pers


def merged(akm_files, **kwargs) -> pl.DataFrame:
    estab, pers = akm_files
    kwargs.setdefault("akm_estab_file", estab)
    kwargs.setdefault("akm_pers_file", pers)
    out = merge_akm(episodes(), **kwargs)
    assert isinstance(out, pl.LazyFrame), "the step must hand back a LazyFrame"
    return out.collect().sort("persnr")


# ====================================================================
#  What a left join has to leave alone
# ====================================================================


def test_the_merge_keeps_every_episode(akm_files):
    out = merged(akm_files)
    assert out.height == 4
    assert out["persnr"].to_list() == [1, 2, 3, 4]


def test_a_using_row_that_matches_nothing_is_discarded(akm_files):
    """Establishment 30 is in the file and in no episode, and adds no row."""
    out = merged(akm_files)
    assert out.height == 4
    assert -0.52 not in out["feff_2001_2008"].to_list()


def test_both_sides_add_exactly_their_five_columns(akm_files):
    out = merged(akm_files)
    added = [c for c in out.columns
             if c not in episodes().collect_schema().names()]
    assert sorted(added) == sorted(FEFF + PEFF)


# ====================================================================
#  Which episode carries which effect
# ====================================================================


def test_the_establishment_effects_land_on_their_establishment(akm_files):
    out = merged(akm_files)
    assert out["feff_2001_2008"].to_list() == [0.12, None, 0.12, None]


def test_the_person_effects_land_on_their_person(akm_files):
    out = merged(akm_files)
    assert out["peff_2001_2008"].to_list() == [0.23, None, None, 0.43]


def test_an_establishment_outside_the_file_keeps_every_effect_missing(akm_files):
    row = merged(akm_files).filter(pl.col("persnr") == 2)
    for column in FEFF:
        assert row[column][0] is None, f"{column} was filled for a non-match"


def test_a_missing_window_stays_missing_for_a_matched_establishment(akm_files):
    """Coverage is per window: matching the file is not carrying an effect."""
    out = merged(akm_files)
    assert out["feff_1985_1992"].to_list() == [None, None, None, None]
    assert out["feff_1993_2000"].to_list() == [0.11, None, 0.11, None]


def test_a_non_match_is_not_filled_with_zero(akm_files):
    """The reference keeps master and matched rows and fills nothing in."""
    out = merged(akm_files)
    for column in FEFF + PEFF:
        misread = out.filter(pl.col(column) == 0.0).height
        assert misread == 0, f"{column} carries a zero where an effect is absent"


def test_the_two_sides_are_independent(akm_files):
    """persnr 3 matches on betnr and not on persnr, and keeps both answers."""
    row = merged(akm_files).filter(pl.col("persnr") == 3)
    assert row["feff_2017_2023"][0] == 0.14
    assert row["peff_2017_2023"][0] is None


# ====================================================================
#  One side at a time
# ====================================================================


def test_the_establishment_side_alone_adds_only_feff(akm_files):
    estab, _ = akm_files
    out = merge_akm(episodes(), akm_estab_file=estab).collect()
    assert all(c in out.columns for c in FEFF)
    assert not any(c in out.columns for c in PEFF)


def test_the_person_side_alone_adds_only_peff(akm_files):
    _, pers = akm_files
    out = merge_akm(episodes(), akm_pers_file=pers).collect()
    assert all(c in out.columns for c in PEFF)
    assert not any(c in out.columns for c in FEFF)


def test_neither_file_hands_the_frame_back_untouched():
    out = merge_akm(episodes()).collect()
    assert out.columns == episodes().collect_schema().names()
    assert out.height == 4


# ====================================================================
#  Refusals
# ====================================================================


def test_an_absent_establishment_file_is_refused(tmp_path, akm_files):
    _, pers = akm_files
    missing = tmp_path / "not_here.dta"
    with pytest.raises(FileNotFoundError, match="AKM establishment file"):
        merge_akm(episodes(), akm_estab_file=missing, akm_pers_file=pers)


def test_an_absent_person_file_is_refused(tmp_path, akm_files):
    estab, _ = akm_files
    missing = tmp_path / "not_here_either.dta"
    with pytest.raises(FileNotFoundError, match="AKM person file"):
        merge_akm(episodes(), akm_estab_file=estab, akm_pers_file=missing)


def test_a_file_without_the_merge_key_is_refused(tmp_path):
    odd = tmp_path / "odd.dta"
    write_dta(odd, {"werksnr": [10], "feff_2001_2008": [0.12]})
    with pytest.raises(ValueError, match="no betnr column"):
        merge_akm(episodes(), akm_estab_file=odd)


def test_a_file_that_repeats_the_key_is_refused(tmp_path):
    """The reference merges m:1, which errors on a repeated key; so does this."""
    doubled = tmp_path / "doubled.dta"
    write_dta(doubled, {"betnr_siab": [10, 10],
                        "feff_2001_2008": [0.12, 0.99]})
    with pytest.raises(ValueError, match="not unique by betnr"):
        merge_akm(episodes(), akm_estab_file=doubled)
