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
    read_batch,
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


@pytest.fixture
def narrow_delivery(tmp_path):
    """The same shape, written at the widths a real delivery uses.

    pyreadstat's writer widens every integer to 32 bits, so the fixture above
    cannot say what the reader does with a Stata `byte` or `int`. pandas' Stata
    writer keeps the width, which is what a delivery carries and what the cast
    plan has to follow.
    """
    frame = pd.DataFrame({
        "persnr_siab": np.array([1, 1, 2, 2], dtype="int32"),
        "spell": np.array([1, 2, 1, 2], dtype="int8"),
        "gebjahr": np.array([1955, 1955, 1972, 1972], dtype="int16"),
        "quelle": pd.array([1, None, 2, 3], dtype="Int8"),
        "begepi": np.array([11688, 12054, 11688, 12054], dtype="int16"),
        "tentgelt": np.array([100.5, 110.25, 50.0, 60.0], dtype="float64"),
    })
    path = tmp_path / "SIAB_narrow.dta"
    frame.to_stata(str(path), write_index=False, version=118)
    # The delivery's own date format, which is the only thing that marks the
    # column as a date. pandas writes a plain numeric format for an integer.
    _set_format(path, list(frame.columns), "begepi", "%tdD_m_CY")
    return path


def _set_format(path, columns, name, display):
    """Overwrite one variable's display format in a written .dta header."""
    data = path.read_bytes()
    start = data.index(b"<formats>") + len(b"<formats>")
    width = (data.index(b"</formats>") - start) // len(columns)
    slot = start + columns.index(name) * width
    path.write_bytes(data[:slot]
                     + display.encode("ascii").ljust(width, b"\x00")
                     + data[slot + width:])


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


def test_each_integer_keeps_the_width_the_delivery_declared(narrow_delivery):
    plan = column_types(stata_metadata(narrow_delivery))

    assert plan["spell"] == pl.Int8
    assert plan["quelle"] == pl.Int8
    assert plan["gebjahr"] == pl.Int16
    assert plan["persnr_siab"] == pl.Int32
    assert plan["tentgelt"] == pl.Float64
    # A date is a date whatever it is stored as, and this one is a Stata `int`.
    assert plan["begepi"] == pl.Date


def test_a_narrow_delivery_keeps_its_values_and_its_missing(narrow_delivery,
                                                            tmp_path):
    target = tmp_path / "narrow.duckdb"
    ingest(narrow_delivery, target)

    with duckdb.connect(str(target)) as connection:
        table = connection.sql("SELECT * FROM orig ORDER BY persnr, spell").pl()

    assert table["spell"].dtype == pl.Int8
    assert table["gebjahr"].dtype == pl.Int16
    assert table["persnr"].dtype == pl.Int32
    assert table["spell"].to_list() == [1, 2, 1, 2]
    assert table["gebjahr"].to_list() == [1955, 1955, 1972, 1972]
    assert table["quelle"].to_list() == [1, None, 2, 3]
    assert table["begepi"][0] == dt.date(1992, 1, 1)


def test_a_value_outside_the_declared_width_stops_the_read_in(narrow_delivery):
    """The strict half of the cast is a guard, so it has to fail loudly.

    A well-formed delivery cannot reach this: the file's own header is what
    says how wide a column is. A plan that under-declares one stands in for a
    header that disagrees with its data.
    """
    plan = column_types(stata_metadata(narrow_delivery))
    plan["gebjahr"] = pl.Int8  # 1955 does not fit eight bits

    with pytest.raises(Exception):
        read_batch(narrow_delivery, 0, 4, plan, 1)


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


# ======================================================================
#  The same read-in, into a Parquet store
# ======================================================================

def test_the_delivery_can_be_read_into_a_folder_of_parquet_files(
        delivery, tmp_path):
    # A target that is not a .duckdb file is a folder, and `orig` is one
    # Parquet file in it. Nothing about the reading changes, so this is the
    # same delivery arriving in a store that needs no database engine.
    store = tmp_path / "store"

    written = ingest(delivery, store, batch_size=4)

    assert written == 10
    assert (store / "orig.parquet").exists()

    table = pl.read_parquet(store / "orig.parquet")
    assert table.height == 10
    assert "persnr" in table.columns and "persnr_siab" not in table.columns
    assert table.schema["persnr"] == pl.Int32
    assert table.schema["tentgelt"] == pl.Float64
    assert table.schema["begepi"] == pl.Date
    assert table.schema["pn_batch"] == pl.Int32


def test_both_stores_get_the_same_table_out_of_the_same_delivery(
        delivery, tmp_path):
    # The two stores are two ways of holding one table, so the table they hold
    # has to be the same one, column for column and row for row.
    ingest(delivery, tmp_path / "siab.duckdb", batch_size=4)
    ingest(delivery, tmp_path / "store", batch_size=4)

    con = duckdb.connect(str(tmp_path / "siab.duckdb"), read_only=True)
    from_database = con.execute("SELECT * FROM orig").pl()
    con.close()
    from_folder = pl.read_parquet(tmp_path / "store" / "orig.parquet")

    assert from_folder.columns == from_database.columns
    assert from_folder.equals(from_database)


def test_a_missing_date_stays_missing_in_a_parquet_store_too(delivery, tmp_path):
    ingest(delivery, tmp_path / "store", batch_size=4)

    alo_beg = pl.read_parquet(tmp_path / "store" / "orig.parquet")["alo_beg"]

    assert alo_beg.null_count() == 7
    assert alo_beg.min() == dt.date(1960, 1, 1) + dt.timedelta(days=11000)


def test_reading_the_same_delivery_twice_into_a_folder_leaves_one_copy(
        delivery, tmp_path):
    store = tmp_path / "store"

    ingest(delivery, store, batch_size=4)
    ingest(delivery, store, batch_size=4)

    assert pl.read_parquet(store / "orig.parquet").height == 10
    assert sorted(p.name for p in store.glob("*.parquet")) == ["orig.parquet"]


def test_every_batch_lands_in_the_parquet_file(delivery, tmp_path):
    # pyarrow writes one row group per batch, and a batch that never reached
    # the file would show up as a missing person rather than as an error.
    ingest(delivery, tmp_path / "store", batch_size=4)

    table = pl.read_parquet(tmp_path / "store" / "orig.parquet")

    assert table["pn_batch"].n_unique() > 1
    assert table.group_by("persnr").agg(
        pl.col("pn_batch").n_unique().alias("batches"))["batches"].max() == 1
