"""
Synthetic tests for the basic Establishment History Panel merge.

The R arm has no synthetic test for this step, only the fixture comparison in
tests/testthat/test-reference-04_merge_basic_BHP.R; the cases below pin the
contract 04_merge_basic_BHP.do sets and the things the port has to get right on
its own. The establishment side is written to a Stata file under `tmp_path`, so
nothing here reads the FDZ test data.

The reference runs `merge m:1 betnr jahr using ... , keep(master match)`: a
many-to-one left join that keeps every SIAB episode, aborts when the using file
is not unique on the key, and never changes the number of rows.
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd
import polars as pl
import pyreadstat
import pytest

from siab.steps import merge_basic_bhp


def write_bhp(path, betnr_siab, jahr, ao_bula, w93_3_gen):
    """Write one establishment file the way the delivery names its columns."""
    frame = pd.DataFrame(
        {
            "betnr_siab": betnr_siab,
            "jahr": np.asarray(jahr, dtype="int32"),
            "ao_bula": np.asarray(ao_bula, dtype="int32"),
            "w93_3_gen": np.asarray(w93_3_gen, dtype="int32"),
        }
    )
    pyreadstat.write_dta(frame, str(path))
    return path


@pytest.fixture
def bhp_file(tmp_path):
    """Two establishments over two years, unique on betnr and year."""
    return write_bhp(
        tmp_path / "bhp_basis.dta",
        betnr_siab=np.array([10, 10, 11], dtype="int32"),
        jahr=[2000, 2001, 2000],
        ao_bula=[1, 1, 9],
        w93_3_gen=[101, 101, 551],
    )


def siab_frame(betnr, year, quelle=None) -> pl.LazyFrame:
    """A handful of SIAB episodes, one per row, keyed as the pipeline keys them."""
    n = len(betnr)
    return pl.LazyFrame(
        {
            "persnr": list(range(1, n + 1)),
            "spell": [1] * n,
            "begepi": [dt.date(y, 3, 1) for y in year],
            "betnr": betnr,
            "year": year,
            "quelle": quelle if quelle is not None else [1] * n,
        },
        schema_overrides={
            "persnr": pl.Int32, "spell": pl.Int32, "betnr": pl.Int32,
            "year": pl.Int32, "quelle": pl.Int32,
        },
    )


def test_the_merge_adds_the_establishment_variables(bhp_file):
    frame = siab_frame(betnr=[10, 11], year=[2000, 2000])

    out = merge_basic_bhp(frame, bhp_file).collect().sort("persnr")

    assert out["ao_bula"].to_list() == [1, 9]
    assert out["w93_3_gen"].to_list() == [101, 551]


def test_the_step_takes_and_returns_a_lazyframe(bhp_file):
    result = merge_basic_bhp(siab_frame(betnr=[10], year=[2000]), bhp_file)

    assert isinstance(result, pl.LazyFrame)


def test_the_year_is_part_of_the_key(bhp_file):
    """Establishment 11 exists in 2000 only, so its 2001 episode cannot match."""
    frame = siab_frame(betnr=[10, 10, 11, 11], year=[2000, 2001, 2000, 2001])

    out = merge_basic_bhp(frame, bhp_file).collect().sort("persnr")

    assert out["ao_bula"].to_list() == [1, 1, 9, None]
    assert out["w93_3_gen"].to_list() == [101, 101, 551, None]


def test_many_episodes_share_one_establishment_year_and_the_row_count_holds(bhp_file):
    """The merge is many-to-one: four episodes on two establishment-years."""
    frame = siab_frame(betnr=[10, 10, 11, 11], year=[2000, 2000, 2000, 2000])

    out = merge_basic_bhp(frame, bhp_file).collect().sort("persnr")

    assert out.height == 4
    assert out["ao_bula"].to_list() == [1, 1, 9, 9]
    assert out["w93_3_gen"].to_list() == [101, 101, 551, 551]


def test_an_unmatched_episode_survives_with_missing_variables(bhp_file):
    """`keep(master match)` keeps the master row, industry or no industry."""
    frame = siab_frame(betnr=[10, 99, None], year=[2000, 2000, 2000])

    out = merge_basic_bhp(frame, bhp_file).collect().sort("persnr")

    assert out.height == 3
    assert out["ao_bula"].to_list() == [1, None, None]
    assert out["w93_3_gen"].to_list() == [101, None, None]


def test_a_duplicate_establishment_year_in_the_using_file_is_refused(tmp_path):
    """Stata's `merge m:1` aborts here, and a silent m:m would duplicate episodes."""
    path = write_bhp(
        tmp_path / "duplicated.dta",
        betnr_siab=np.array([10, 10], dtype="int32"),
        jahr=[2000, 2000],
        ao_bula=[1, 2],
        w93_3_gen=[101, 151],
    )
    frame = siab_frame(betnr=[10], year=[2000])

    with pytest.raises(ValueError, match="not unique by betnr and year"):
        merge_basic_bhp(frame, path)


def test_a_float_join_key_in_the_using_file_still_joins(tmp_path):
    """pyreadstat hands back Float64 for a Stata column carrying any missing.

    A Float64 key against the pipeline's Int32 betnr aborts a polars join with a
    SchemaError rather than matching, so the step casts the using side onto the
    master's dtypes. The missing establishment number below is what turns the
    column into a float in the first place.
    """
    path = write_bhp(
        tmp_path / "float_key.dta",
        betnr_siab=np.array([10.0, 11.0, np.nan]),
        jahr=[2000, 2000, 2000],
        ao_bula=[1, 9, 5],
        w93_3_gen=[101, 551, 401],
    )
    # The file really does come back as a float, otherwise the test is vacuous.
    back, _ = pyreadstat.read_dta(str(path))
    assert back["betnr_siab"].dtype == np.dtype("float64")

    frame = siab_frame(betnr=[10, 11], year=[2000, 2000])

    out = merge_basic_bhp(frame, path).collect().sort("persnr")

    assert out["ao_bula"].to_list() == [1, 9]
    assert out["w93_3_gen"].to_list() == [101, 551]


def test_keep_variables_picks_what_is_merged(bhp_file):
    """`keepusing()` in the reference, and the wide file makes it worth having."""
    frame = siab_frame(betnr=[10], year=[2000])

    out = merge_basic_bhp(frame, bhp_file, keep_variables=("ao_bula",)).collect()

    assert out["ao_bula"].to_list() == [1]
    assert "w93_3_gen" not in out.columns


def test_the_master_columns_are_untouched(bhp_file):
    frame = siab_frame(betnr=[10, 99], year=[2000, 2001], quelle=[1, 2])

    out = merge_basic_bhp(frame, bhp_file).collect().sort("persnr")

    assert out["persnr"].to_list() == [1, 2]
    assert out["spell"].to_list() == [1, 1]
    assert out["betnr"].to_list() == [10, 99]
    assert out["year"].to_list() == [2000, 2001]
    assert out["quelle"].to_list() == [1, 2]
    assert out["begepi"].to_list() == [dt.date(2000, 3, 1), dt.date(2001, 3, 1)]


def test_no_establishment_file_is_refused():
    with pytest.raises(ValueError, match="Please set bhp_file"):
        merge_basic_bhp(siab_frame(betnr=[10], year=[2000]), None)


def test_a_missing_establishment_file_is_refused(tmp_path):
    with pytest.raises(FileNotFoundError, match="Basic Establishment File not found"):
        merge_basic_bhp(siab_frame(betnr=[10], year=[2000]),
                        tmp_path / "not_there.dta")


def test_a_master_without_the_join_key_is_refused(bhp_file):
    frame = pl.LazyFrame(
        {"persnr": [1], "spell": [1], "year": [2000], "quelle": [1]},
        schema_overrides={"persnr": pl.Int32, "spell": pl.Int32,
                          "year": pl.Int32, "quelle": pl.Int32},
    )

    with pytest.raises(ValueError, match="betnr"):
        merge_basic_bhp(frame, bhp_file)
