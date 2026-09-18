"""
Synthetic tests for the Parquet store in siab/common.py.

The eighteen steps of the Python arm compute in polars and contain no SQL, so
the store only has to hold a table between them. A DuckDB database does that
and so does a folder of Parquet files, and `open_store()` picks by the name of
the target. These tests pin the Parquet half: that it round-trips a table
unchanged, that a step replacing the table it is reading does not truncate it,
and that none of it needs DuckDB to be importable at all.

The DuckDB half is tested in test_common.py, against the same claims.
"""

from __future__ import annotations

import builtins
import datetime as dt
import importlib
import sys

import polars as pl
import pytest

from siab.common import (
    ParquetStore,
    count_rows,
    drop_table,
    open_store,
    read_table,
    store_description,
    table_names,
    write_table,
)


def frame() -> pl.DataFrame:
    """A frame with a null in every nullable position and three column types."""
    return pl.DataFrame(
        {
            "persnr": [1, 2, 3],
            "label": ["a", "b", None],
            "begepi": [dt.date(1992, 1, 1), dt.date(1993, 6, 30), None],
            "wage": [10.5, None, 12.25],
            "flag": [True, False, None],
        }
    )


@pytest.fixture
def store(tmp_path) -> ParquetStore:
    store = open_store(tmp_path / "store")
    frame().write_parquet(store.path("data"))
    return store


# ======================================================================
#  Which store a target name opens
# ======================================================================

def test_a_duckdb_suffix_opens_a_database(tmp_path):
    connection = open_store(tmp_path / "siab.duckdb")
    try:
        assert type(connection).__name__ == "DuckDBPyConnection"
    finally:
        connection.close()


def test_any_other_name_opens_a_parquet_folder(tmp_path):
    assert isinstance(open_store(tmp_path / "siab_store"), ParquetStore)


def test_the_folder_is_created_if_it_is_not_there(tmp_path):
    target = tmp_path / "not" / "there" / "yet"
    assert open_store(target).folder.is_dir()


def test_a_table_is_one_parquet_file_named_after_it(store):
    assert store.path("data").name == "data.parquet"
    assert store.path("orig").parent == store.folder


# ======================================================================
#  What the boundary preserves
# ======================================================================

def test_a_round_trip_changes_nothing(store):
    before = read_table(store).collect()

    write_table(store, read_table(store))

    assert read_table(store).collect().equals(before)


def test_the_column_order_and_the_types_survive(store):
    before = read_table(store).collect_schema()

    write_table(store, read_table(store))

    assert read_table(store).collect_schema() == before


def test_nulls_come_back_as_nulls(store):
    write_table(store, read_table(store))

    after = read_table(store).collect()
    assert after["label"].to_list() == ["a", "b", None]
    assert after["wage"].to_list() == [10.5, None, 12.25]
    assert after["flag"].to_list() == [True, False, None]


def test_a_step_that_adds_a_column_lands_in_the_table(store):
    write_table(store, read_table(store).with_columns(
        doubled=pl.col("persnr") * 2))

    after = read_table(store).collect()
    assert after["doubled"].to_list() == [2, 4, 6]


def test_an_eager_frame_is_accepted_as_well(store):
    write_table(store, frame().with_columns(extra=pl.lit(1)))

    assert "extra" in read_table(store).collect_schema().names()


def test_sequential_steps_compose(store):
    write_table(store, read_table(store).with_columns(a=pl.col("persnr") + 1))
    write_table(store, read_table(store).with_columns(b=pl.col("a") * 10))

    after = read_table(store).collect()
    assert after["b"].to_list() == [20, 30, 40]


def test_a_table_other_than_data_can_be_written_and_read(store):
    write_table(store, frame(), "side")

    assert store.path("side").exists()
    assert read_table(store, "side").collect().equals(frame())


# ======================================================================
#  The file the step is reading is the file it replaces
# ======================================================================

def test_replacing_the_table_being_read_does_not_truncate_it(store):
    # polars scans data.parquet lazily, so sinking the plan straight back into
    # that path would overwrite the file the plan is still reading. The write
    # goes to a pending file and is moved over the table once it is complete.
    for _ in range(3):
        write_table(store, read_table(store).with_columns(
            wage=pl.col("wage") * 2))

    after = read_table(store).collect()
    assert after.height == 3
    assert after["wage"].to_list() == [84.0, None, 98.0]


def test_no_pending_file_is_left_behind(store):
    write_table(store, read_table(store))

    assert sorted(p.name for p in store.folder.glob("*.parquet")) == ["data.parquet"]


def test_an_interrupted_write_leaves_the_table_it_started_from(store):
    before = read_table(store).collect()

    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        write_table(store, read_table(store).with_columns(
            pl.col("no_such_column")))

    assert read_table(store).collect().equals(before)


# ======================================================================
#  What main.py needs of a store besides the two table functions
# ======================================================================

def test_table_names_lists_every_table(store):
    write_table(store, frame(), "orig")

    assert table_names(store) == ["data", "orig"]


def test_drop_table_removes_one(store):
    write_table(store, frame(), "scratch")

    drop_table(store, "scratch")

    assert table_names(store) == ["data"]


def test_dropping_a_table_that_is_not_there_is_quiet(store):
    drop_table(store, "never_existed")

    assert table_names(store) == ["data"]


def test_count_rows_counts_without_collecting_the_table(store):
    assert count_rows(store, "data") == 3


def test_the_description_names_the_folder(store):
    assert str(store.folder) in store_description(store)


def test_reading_a_table_that_is_not_there_says_which(store):
    with pytest.raises(FileNotFoundError, match="orig"):
        read_table(store, "orig")


# ======================================================================
#  The Parquet store needs no DuckDB
# ======================================================================

def test_the_module_imports_and_the_store_runs_without_duckdb(tmp_path, monkeypatch):
    # An environment that never installed DuckDB has to reach a finished panel
    # all the same, which is the point of the Parquet store. The import is
    # blocked here rather than uninstalled, which tests the same line.
    real_import = builtins.__import__

    def blocked(name, *args, **kwargs):
        if name == "duckdb":
            raise ModuleNotFoundError("No module named 'duckdb'")
        return real_import(name, *args, **kwargs)

    monkeypatch.delitem(sys.modules, "duckdb", raising=False)
    monkeypatch.delitem(sys.modules, "siab.common", raising=False)
    monkeypatch.setattr(builtins, "__import__", blocked)

    common = importlib.import_module("siab.common")
    store = common.open_store(tmp_path / "store")
    frame().write_parquet(store.path("data"))
    common.write_table(store, common.read_table(store).with_columns(
        doubled=pl.col("persnr") * 2))

    assert common.read_table(store).collect()["doubled"].to_list() == [2, 4, 6]
    assert "duckdb" not in sys.modules


def test_asking_for_a_duckdb_store_without_duckdb_says_so(tmp_path, monkeypatch):
    real_import = builtins.__import__

    def blocked(name, *args, **kwargs):
        if name == "duckdb":
            raise ModuleNotFoundError("No module named 'duckdb'")
        return real_import(name, *args, **kwargs)

    monkeypatch.delitem(sys.modules, "duckdb", raising=False)
    monkeypatch.delitem(sys.modules, "siab.common", raising=False)
    monkeypatch.setattr(builtins, "__import__", blocked)

    common = importlib.import_module("siab.common")
    with pytest.raises(ModuleNotFoundError, match="Parquet"):
        common.open_store(tmp_path / "siab.duckdb")


def test_a_half_written_table_is_not_listed_as_one(store):
    # A crash during a write leaves data.pending.parquet behind. It holds part
    # of a table, so a later run must not pick it up as `data.pending`.
    (store.folder / "data.pending.parquet").write_bytes(b"")

    assert table_names(store) == ["data"]
