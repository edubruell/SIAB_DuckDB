"""
Synthetic tests for the batched Stata reader.

`python/stata_to_db_batch_read.py` is the step before the pipeline: it turns a
SIAB delivery into the `orig` table main.py expects to find. It is the
counterpart of `R/stata_to_db_batch_read.R` and it answers to the same target,
the table that script builds, rather than to a Stata fixture: there is no
fixture for a read-in.

The tables below are written out as real .dta files with pyreadstat and read
back, so the three things the reader has to get right are exercised end to end:
the key rename, the Stata date epoch, and the storage types that decide whether
a column of integers with one missing value comes back as an integer or a
float.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import numpy as np
import pandas as pd
import polars as pl
import pyreadstat
import pytest

from stata_to_db_batch_read import (
    STATA_EPOCH_OFFSET,
    batch_bounds,
    column_types,
    ingest,
    person_key,
    stata_metadata,
)

# 1 January 1960 is Stata day zero, and polars counts from 1 January 1970.
DAYS_1960_TO_1970 = 3653


def write_delivery(path, frame: pd.DataFrame, formats: dict[str, str] | None = None):
    """Write a small table out as a Stata file, dates included."""
    pyreadstat.write_dta(frame, str(path), variable_format=formats or {})
    return path


@pytest.fixture
def delivery(tmp_path):
    """Four persons, ten rows, keyed the way a 7523 v2 delivery is.

    `alo_beg` carries a missing in a column that is otherwise whole days, which
    is the case that decides whether the reader keeps an integer column an
    integer.
    """
    # The integer columns are written as 32-bit, because that is what a Stata
    # `long` is and what the delivery stores a person number as. pyreadstat
    # writes a pandas int64 out as a double, which would make the fixture test
    # something the delivery never does.
    frame = pd.DataFrame({
        "persnr_siab": np.array([1, 1, 1, 2, 2, 3, 4, 4, 4, 4], dtype="int32"),
        "betnr_siab": np.array([10, 10, 11, 20, 20, 30, 40, 40, 41, 41],
                               dtype="int32"),
        "spell": np.array([1, 1, 2, 1, 2, 1, 1, 1, 2, 2], dtype="int32"),
        "begepi": np.array([11688, 12054, 12419, 11688, 12054, 12419, 11688,
                            12054, 12419, 12784], dtype="int32"),
        "tentgelt": [100.5, 110.25, 0.0, 50.0, 60.0, 70.0, 80.0, 90.0, 95.0,
                     99.0],
        "alo_beg": [11000.0, np.nan, np.nan, 11500.0, np.nan, np.nan, np.nan,
                    np.nan, np.nan, 12000.0],
    })
    return write_delivery(tmp_path / "SIAB_test.dta", frame,
                          {"begepi": "%tdD_m_CY", "alo_beg": "%tdD_m_CY"})


# ======================================================================
#  The pieces
# ======================================================================

def test_the_stata_epoch_offset_is_the_ten_years_between_1960_and_1970():
    assert STATA_EPOCH_OFFSET == -DAYS_1960_TO_1970
    assert (dt.date(1970, 1, 1) + dt.timedelta(days=STATA_EPOCH_OFFSET)
            == dt.date(1960, 1, 1))


def test_the_person_key_is_read_off_the_delivery(delivery):
    assert person_key(stata_metadata(delivery)) == "persnr_siab"


def test_a_date_column_is_a_date_whatever_its_storage_type(delivery):
    plan = column_types(stata_metadata(delivery))

    assert plan["begepi"] == pl.Date
    assert plan["alo_beg"] == pl.Date
    assert plan["persnr_siab"] == pl.Int32
    assert plan["spell"] == pl.Int32
    assert plan["tentgelt"] == pl.Float64


# ======================================================================
#  The batching
# ======================================================================

def test_the_batches_cover_every_row_exactly_once():
    persons = pl.Series("persnr", [1] * 3 + [2] * 4 + [3] * 3)

    bounds = batch_bounds(persons, batch_size=4)

    covered = []
    for _, offset, length in bounds:
        covered.extend(range(offset, offset + length))
    assert covered == list(range(10))


def test_a_batch_boundary_never_falls_inside_a_person():
    persons = pl.Series("persnr", [1] * 3 + [2] * 4 + [3] * 3)

    for _, offset, length in batch_bounds(persons, batch_size=4):
        rows = persons[offset:offset + length]
        # Every row of a person this batch touches is in this batch.
        for value in rows.unique():
            assert (persons == value).sum() == (rows == value).sum()


def test_a_person_longer_than_a_batch_keeps_its_own_batch_number():
    # The cumulative total jumps straight past two labels, and the R script
    # carries the label rather than renumbering, so this port does too.
    persons = pl.Series("persnr", [1] * 2 + [2] * 25 + [3] * 2)

    bounds = batch_bounds(persons, batch_size=10)
    labels = [batch for batch, _, _ in bounds]

    assert labels == sorted(labels)
    assert len(set(labels)) == len(labels)
    assert sum(length for _, _, length in bounds) == 29


def test_no_rows_means_no_batches():
    assert batch_bounds(pl.Series("persnr", [], dtype=pl.Int64), 1000) == []


# ======================================================================
#  The whole read-in
# ======================================================================

def test_the_delivery_becomes_an_orig_table_with_the_pipeline_s_names(
        delivery, tmp_path):
    written = ingest(delivery, tmp_path / "siab.duckdb", batch_size=4)

    assert written == 10

    con = duckdb.connect(str(tmp_path / "siab.duckdb"), read_only=True)
    columns = {row[1]: row[2] for row in con.execute(
        "PRAGMA table_info('orig')").fetchall()}

    assert "persnr" in columns and "persnr_siab" not in columns
    assert "betnr" in columns and "betnr_siab" not in columns
    assert columns["persnr"] == "INTEGER"
    assert columns["spell"] == "INTEGER"
    assert columns["tentgelt"] == "DOUBLE"
    assert columns["begepi"] == "DATE"
    assert columns["pn_batch"] == "INTEGER"
    con.close()


def test_a_stata_date_becomes_the_day_it_stands_for(delivery, tmp_path):
    ingest(delivery, tmp_path / "siab.duckdb", batch_size=4)

    con = duckdb.connect(str(tmp_path / "siab.duckdb"), read_only=True)
    first = con.execute("SELECT begepi FROM orig ORDER BY persnr, spell, begepi"
                        " LIMIT 1").fetchone()[0]
    con.close()

    # Stata day 11688 counted from 1 January 1960.
    assert first == dt.date(1960, 1, 1) + dt.timedelta(days=11688)


def test_a_missing_date_stays_missing_rather_than_becoming_a_far_off_day(
        delivery, tmp_path):
    # This is the case the R read-in gets wrong: readstata13 turns the missing
    # into a sentinel and the conversion turns that into a date in the year
    # 5877642 BC. The whole column is unused by the prep, which is why it went
    # unnoticed, and a missing here has to stay missing.
    ingest(delivery, tmp_path / "siab.duckdb", batch_size=4)

    con = duckdb.connect(str(tmp_path / "siab.duckdb"), read_only=True)
    counts = con.execute(
        "SELECT count(*) FILTER (WHERE alo_beg IS NULL) AS missing,"
        "       min(alo_beg) AS earliest, max(alo_beg) AS latest FROM orig"
    ).fetchone()
    con.close()

    assert counts[0] == 7
    assert counts[1] == dt.date(1960, 1, 1) + dt.timedelta(days=11000)
    assert counts[2] == dt.date(1960, 1, 1) + dt.timedelta(days=12000)


def test_every_row_carries_the_batch_it_came_in(delivery, tmp_path):
    ingest(delivery, tmp_path / "siab.duckdb", batch_size=4)

    con = duckdb.connect(str(tmp_path / "siab.duckdb"), read_only=True)
    per_person = con.execute(
        "SELECT persnr, count(DISTINCT pn_batch) AS batches FROM orig"
        " GROUP BY persnr").fetchall()
    batches = con.execute("SELECT count(DISTINCT pn_batch) FROM orig").fetchone()[0]
    con.close()

    assert all(count == 1 for _, count in per_person), (
        "a person was split across batches")
    assert batches > 1, "the test asks for more than one batch"


def test_reading_the_same_delivery_twice_leaves_one_copy(delivery, tmp_path):
    database = tmp_path / "siab.duckdb"

    ingest(delivery, database, batch_size=4)
    ingest(delivery, database, batch_size=4)

    con = duckdb.connect(str(database), read_only=True)
    rows = con.execute("SELECT count(*) FROM orig").fetchone()[0]
    con.close()

    assert rows == 10


def test_a_delivery_that_is_not_sorted_by_person_is_refused(tmp_path):
    # A row range is a set of whole persons only if the persons are contiguous.
    # A file somebody has re-sorted would be read into batches that each hold
    # part of a person, silently, so the reader checks instead of trusting.
    frame = pd.DataFrame({
        "persnr_siab": np.array([1, 2, 1, 2], dtype="int32"),
        "spell": np.array([1, 1, 2, 2], dtype="int32"),
        "tentgelt": [1.0, 2.0, 3.0, 4.0],
    })
    scrambled = write_delivery(tmp_path / "scrambled.dta", frame)

    with pytest.raises(ValueError, match="not sorted"):
        ingest(scrambled, tmp_path / "siab.duckdb", batch_size=2)


def test_a_missing_delivery_says_so(tmp_path):
    with pytest.raises(FileNotFoundError):
        ingest(tmp_path / "nothing.dta", tmp_path / "siab.duckdb")
