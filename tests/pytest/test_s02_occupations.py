"""
Synthetic tests for the occupation variables.

The counterpart of tests/testthat/test-02_occupations.R. The step joins two
crosswalks onto the 3-digit SIAB occupation code `beruf`: the 2-digit KldB-88
Berufsgruppe (occ_kldb88_2) and the Blossfeld classification (occ_blo). Both are
left joins, so a code that is in neither table has to keep its row.
occ_kldb88_2 then comes back null; occ_blo comes back 99, "not assignable",
because the reference's recode closes with `(else = 99)` and Stata's `else`
covers missing values too.

Both crosswalk csv files are written by the R arm and spell a missing value
`NA`, so they are read here the way the step reads them.
"""

import polars as pl
import pytest

from siab.common import classifications_dir
from siab.steps import generate_occupation_variables

CSV_NULLS = ["NA"]


def kldb_crosswalk() -> pl.DataFrame:
    return pl.read_csv(classifications_dir() / "kldb88_beruf.csv",
                       null_values=CSV_NULLS)


def blo_crosswalk() -> pl.DataFrame:
    return pl.read_csv(classifications_dir() / "walkover_beruf_occblo.csv",
                       null_values=CSV_NULLS)


def lookup(table: pl.DataFrame, code: int, column: str):
    matched = table.filter(pl.col("beruf") == code)[column]
    assert matched.len() == 1, f"beruf {code} is not unique in the crosswalk"
    return matched[0]


def beruf_frame(codes, **extra):
    return pl.LazyFrame(
        {"persnr": list(range(1, len(codes) + 1)), "beruf": codes, **extra},
        schema_overrides={"persnr": pl.Int32, "beruf": pl.Int32},
    )


def test_both_occupation_variables_are_joined_on_beruf():
    kldb = kldb_crosswalk()
    blo = blo_crosswalk()
    codes = [11, 12]

    out = generate_occupation_variables(beruf_frame(codes)).collect().sort("persnr")

    assert out["occ_kldb88_2"].to_list() == [lookup(kldb, c, "kldb88_2") for c in codes]
    assert out["occ_blo"].to_list() == [lookup(blo, c, "occ_blo") for c in codes]


def test_a_beruf_outside_both_crosswalks_keeps_its_row_and_gets_null():
    out = generate_occupation_variables(
        beruf_frame([11, 998])
    ).collect().sort("persnr")

    assert out.height == 2
    assert out["occ_kldb88_2"][0] is not None
    assert out["occ_kldb88_2"][1] is None


def test_a_missing_beruf_keeps_its_row_with_no_berufsgruppe_and_occ_blo_99():
    out = generate_occupation_variables(
        beruf_frame([11, None])
    ).collect().sort("persnr")

    assert out.height == 2
    assert out["occ_kldb88_2"][1] is None
    assert out["occ_blo"][1] == 99


def test_a_beruf_outside_the_blossfeld_walkover_also_gets_occ_blo_99():
    # Every benefit and job-search episode arrives here without an occupation,
    # so this branch covers a third of the test data rather than an edge case.
    out = generate_occupation_variables(
        beruf_frame([11, 998])
    ).collect().sort("persnr")

    assert out["occ_blo"][1] == 99


def test_an_administrative_code_is_not_assignable_to_a_blossfeld_class():
    # 555, 666, 888 and the 9xx codes are SIAB administrative categories rather
    # than occupations, and the step's own docstring names them as the ones
    # taking occ_blo = 99.
    codes = [555, 666, 888, 971, 981, 982, 983, 991, 995, 996, 997]

    out = generate_occupation_variables(
        beruf_frame(codes)
    ).collect().sort("persnr")

    assert out["occ_blo"].to_list() == [99] * len(codes)


def test_a_code_outside_the_kldb88_structure_gets_no_berufsgruppe():
    # A smaller set than the one above, and a different one: 971 and the 98x
    # codes do sit in the KldB-88 structure and carry a Berufsgruppe, while 470
    # and 924 do not although they are assignable to a Blossfeld class. The
    # crosswalk is the oracle for which is which.
    kldb = kldb_crosswalk()
    unmapped = kldb.filter(pl.col("kldb88_2").is_null())["beruf"].to_list()

    out = generate_occupation_variables(
        beruf_frame(unmapped)
    ).collect().sort("persnr")

    assert len(unmapped) > 0
    assert out["occ_kldb88_2"].to_list() == [None] * len(unmapped)


def test_the_berufsgruppe_is_the_first_two_digits_of_the_berufsordnung():
    # occ_kldb88_2 is the R arm's addition and 14_occ_blossfeld.do builds no
    # such column, so there is nothing in the Stata fixture to compare it with
    # and the whole of its coverage sits here. Every code the crosswalk maps is
    # run through the step and checked against the KldB-88 structure itself.
    kldb = kldb_crosswalk()
    mapped = kldb.filter(pl.col("kldb88_2").is_not_null())
    codes = mapped["beruf"].to_list()

    out = generate_occupation_variables(
        beruf_frame(codes)
    ).collect().sort("persnr")

    assert out["occ_kldb88_2"].to_list() == mapped["kldb88_2"].to_list()
    assert out["occ_kldb88_2"].to_list() == [code // 10 for code in codes]
    assert out.schema["occ_kldb88_2"] == pl.Int32


def test_neither_join_duplicates_a_row():
    kldb = kldb_crosswalk()
    codes = kldb.filter(pl.col("kldb88_2").is_not_null())["beruf"].to_list()[:50]

    out = generate_occupation_variables(beruf_frame(codes)).collect()

    assert out.height == len(codes)


def test_the_step_adds_exactly_occ_kldb88_2_and_occ_blo():
    frame = pl.LazyFrame(
        {"persnr": [1, 2, 3], "beruf": [11, 12, 71], "tentgelt": [1.0, 2.0, 3.0]},
        schema_overrides={"persnr": pl.Int32, "beruf": pl.Int32},
    )
    before = frame.collect()

    out = generate_occupation_variables(frame).collect().sort("persnr")

    assert set(out.columns) - set(before.columns) == {"occ_kldb88_2", "occ_blo"}
    assert out.select(before.columns).equals(before)


def test_the_crosswalk_covers_every_kldb88_code_with_a_berufsgruppe_between_1_and_99():
    kldb = kldb_crosswalk()
    in_kldb88 = pl.col("in_kldb88")
    if kldb.schema["in_kldb88"] == pl.String:
        in_kldb88 = in_kldb88.str.to_uppercase() == "TRUE"
    mapped = kldb.filter(in_kldb88)["kldb88_2"]

    assert mapped.len() > 0
    assert mapped.null_count() == 0
    assert mapped.min() >= 1
    assert mapped.max() <= 99


def test_the_step_hands_a_lazyframe_back():
    assert isinstance(generate_occupation_variables(beruf_frame([11])), pl.LazyFrame)
