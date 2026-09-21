"""
Synthetic tests for the step boundary in siab/common.py.

`read_table()` and `write_table()` are the only two places the pipeline moves
the dataset between DuckDB and polars, which makes them the only two places the
out-of-core claim can be won or lost. Since 2026-09-18 a DuckDB store has two
ways of doing it, and `open_store()` picks one: `memory`, the default, collects
the table and registers the result back, and `parquet` copies it to a Parquet
file with DuckDB's own writer, scans that, and sinks the step's plan into a file
DuckDB reads back.

These tests pin what that boundary has to preserve, which is everything and is
the same under either mode: the rows, the column order, the types, and the
nulls. Those six run twice, once per mode. The tests below them are about the
handover files themselves and are pinned to `parquet`, which is the only mode
that has any. The counterpart of the R arm's test-00_common_functions.R, which
tests `compute_and_overwrite()` the same way.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import polars as pl
import pytest

from siab.common import (
    BOUNDARIES,
    DEFAULT_BOUNDARY,
    DuckDBStore,
    boundary_dir,
    compact_store,
    database_path,
    open_store,
    read_table,
    write_table,
)

ROWS = """
    CREATE TABLE data AS
    SELECT * FROM (VALUES
        (1, 'a', DATE '1992-01-01', 10.5, TRUE),
        (2, 'b', DATE '1993-06-30', NULL, FALSE),
        (3, NULL, NULL, 12.25, NULL)
    ) AS t(persnr, label, begepi, wage, flag)
"""


@pytest.fixture(params=BOUNDARIES)
def connection(request, tmp_path):
    """A file-backed store, because that is what the pipeline runs on.

    Parametrised over both handovers: everything in the first section has to
    hold whichever one the store was opened with.
    """
    store = DuckDBStore(duckdb.connect(str(tmp_path / "siab.duckdb")), request.param)
    store.execute(ROWS)
    yield store
    store.close()


@pytest.fixture
def spilling(tmp_path):
    """A store pinned to the Parquet handover, for the tests about its files."""
    store = DuckDBStore(duckdb.connect(str(tmp_path / "siab.duckdb")), "parquet")
    store.execute(ROWS)
    yield store
    store.close()


# ======================================================================
#  What the boundary preserves, under either handover
# ======================================================================

def test_a_round_trip_changes_nothing(connection):
    before = connection.execute("SELECT * FROM data ORDER BY persnr").pl()

    write_table(connection, read_table(connection))

    after = connection.execute("SELECT * FROM data ORDER BY persnr").pl()
    assert after.equals(before)


def test_the_column_order_and_the_types_survive(connection):
    before = connection.execute("DESCRIBE SELECT * FROM data").fetchall()

    write_table(connection, read_table(connection))

    after = connection.execute("DESCRIBE SELECT * FROM data").fetchall()
    assert [(c[0], c[1]) for c in after] == [(c[0], c[1]) for c in before]


def test_a_null_stays_a_null_on_the_way_through(connection):
    write_table(connection, read_table(connection))

    nulls = connection.execute(
        "SELECT count(*) FILTER (WHERE label IS NULL) AS label,"
        "       count(*) FILTER (WHERE begepi IS NULL) AS begepi,"
        "       count(*) FILTER (WHERE wage IS NULL) AS wage,"
        "       count(*) FILTER (WHERE flag IS NULL) AS flag FROM data"
    ).fetchone()

    assert nulls == (1, 1, 1, 1)


def test_a_step_that_adds_a_column_writes_it_back(connection):
    write_table(connection,
                read_table(connection).with_columns(
                    (pl.col("persnr") * 2).alias("doubled")))

    rows = connection.execute(
        "SELECT persnr, doubled FROM data ORDER BY persnr").fetchall()
    assert rows == [(1, 2), (2, 4), (3, 6)]


def test_an_eager_frame_is_accepted_as_well_as_a_plan(connection):
    # Two steps hand back a frame they have already collected, so the writer
    # takes either.
    frame = pl.DataFrame({"persnr": [7, 8], "begepi": [dt.date(2001, 1, 1)] * 2})

    write_table(connection, frame)

    assert connection.execute("SELECT count(*) FROM data").fetchone()[0] == 2


def test_a_named_table_other_than_data_can_be_read_and_written(connection):
    connection.execute("CREATE TABLE side AS SELECT 1 AS x")

    write_table(connection, read_table(connection, "side").with_columns(
        y=pl.col("x") + 1), "side")

    assert connection.execute("SELECT x, y FROM side").fetchone() == (1, 2)


# ======================================================================
#  Which handover a store was opened with
# ======================================================================

def test_the_default_is_the_in_memory_handover(tmp_path):
    # The Parquet handover costs 28 percent of the wall clock on data that
    # fits, so it is asked for.
    store = open_store(tmp_path / "siab.duckdb")
    try:
        assert DEFAULT_BOUNDARY == "memory"
        assert store.boundary == "memory"
    finally:
        store.close()


def test_the_handover_can_be_chosen_when_the_store_is_opened(tmp_path):
    store = open_store(tmp_path / "siab.duckdb", "parquet")
    try:
        assert store.boundary == "parquet"
    finally:
        store.close()


def test_a_handover_that_does_not_exist_is_refused_at_the_door(tmp_path):
    # Rejected where the name is given. Left to the first step, it would
    # surface eighteen steps and one merge later.
    with pytest.raises(ValueError, match="Unknown boundary"):
        open_store(tmp_path / "siab.duckdb", "parqet")


def test_a_bare_connection_still_works_and_takes_the_default(tmp_path):
    # The dump writer opens its own connection and passes it straight in.
    con = duckdb.connect(str(tmp_path / "siab.duckdb"))
    try:
        con.execute(ROWS)
        write_table(con, read_table(con))
        assert con.execute("SELECT count(*) FROM data").fetchone()[0] == 3
        assert list(boundary_dir(con).glob("*.parquet")) == []
    finally:
        con.close()


def test_the_store_passes_anything_it_does_not_know_to_the_connection(tmp_path):
    # `store.execute()`, `store.register()` and the rest are the connection's,
    # and the read-in and the dump writer call them on whatever they are given.
    store = open_store(tmp_path / "siab.duckdb")
    try:
        store.execute("CREATE TABLE t AS SELECT 1 AS x")
        assert store.sql("SELECT x FROM t").fetchone() == (1,)
        assert "boundary='memory'" in repr(store)
    finally:
        store.close()


def test_the_in_memory_handover_writes_no_file(tmp_path):
    store = open_store(tmp_path / "siab.duckdb")
    try:
        store.execute(ROWS)
        write_table(store, read_table(store))
        assert list(boundary_dir(store).glob("*.parquet")) == []
    finally:
        store.close()


# ======================================================================
#  Where the handover files go, and that they do not stay
# ======================================================================

def test_the_handover_goes_beside_the_database(spilling, tmp_path):
    assert database_path(spilling) == tmp_path / "siab.duckdb"
    assert boundary_dir(spilling).parent == tmp_path


def test_an_in_memory_database_falls_back_to_the_temporary_directory():
    con = duckdb.connect()
    try:
        assert database_path(con) is None
        assert boundary_dir(con).exists()
    finally:
        con.close()


def test_siab_spill_moves_the_handover_somewhere_else(spilling, tmp_path,
                                                      monkeypatch):
    # What a database on a volume with no room for a second copy of itself
    # needs.
    elsewhere = tmp_path / "spill"
    monkeypatch.setenv("SIAB_SPILL", str(elsewhere))

    assert boundary_dir(spilling) == elsewhere

    write_table(spilling, read_table(spilling))
    assert spilling.execute("SELECT count(*) FROM data").fetchone()[0] == 3


def test_no_handover_file_is_left_behind(spilling):
    # The files are the size of the dataset, so a run that kept one per step
    # would leave the disk holding the data eighteen times over.
    folder = boundary_dir(spilling)

    write_table(spilling, read_table(spilling))

    assert list(folder.glob("*.parquet")) == []


def test_a_second_step_reads_what_the_first_one_wrote(spilling):
    # The read file and the write file have different names on purpose: a step
    # scans the one while the sink fills the other, and reusing one path would
    # have the plan reading the file it is writing.
    write_table(spilling, read_table(spilling).with_columns(
        one=pl.lit(1)))
    write_table(spilling, read_table(spilling).with_columns(
        two=pl.lit(2)))

    rows = spilling.execute("SELECT one, two FROM data LIMIT 1").fetchone()
    assert rows == (1, 2)


def test_two_databases_in_one_folder_do_not_share_a_handover(tmp_path):
    # Both arms of this project keep a table called `data`, and a test run
    # keeps its working database beside the test database. Without the database
    # name in the handover file, one run would overwrite the other's.
    first = DuckDBStore(duckdb.connect(str(tmp_path / "one.duckdb")), "parquet")
    second = DuckDBStore(duckdb.connect(str(tmp_path / "two.duckdb")), "parquet")
    try:
        first.execute("CREATE TABLE data AS SELECT 1 AS x")
        second.execute("CREATE TABLE data AS SELECT 2 AS x")

        a = read_table(first)
        b = read_table(second)

        assert a.collect()["x"].to_list() == [1]
        assert b.collect()["x"].to_list() == [2]
    finally:
        first.close()
        second.close()


# ======================================================================
#  Keeping the store the size of what is in it
# ======================================================================
#
# Every step rewrites the whole table, so the blocks the old copy held are free
# as soon as it is dropped. DuckDB reuses free blocks only across a checkpoint
# and never shrinks a file by itself, which is why `write_table()` checkpoints
# and why a finished prep is copied into a fresh file. The counterparts are
# tested in tests/testthat/test-00_common_functions.R.

def _rewrite(store, steps: int = 10) -> None:
    """A run's worth of rewrites of the same table."""
    for step in range(steps):
        write_table(store, read_table(store).with_columns(
            wage=pl.col("wage") + step))


def _big_table(store) -> None:
    store.execute("DROP TABLE IF EXISTS data")
    store.execute(
        "CREATE TABLE data AS "
        "SELECT i AS persnr, i * 1.5 AS wage FROM range(50000) t(i)")


def test_a_write_leaves_no_unreused_blocks_behind(connection):
    _big_table(connection)

    def blocks() -> dict:
        return connection.execute("PRAGMA database_size").pl().to_dicts()[0]

    free = []
    for _ in range(10):
        write_table(connection, read_table(connection).with_columns(
            wage=pl.col("wage") + 1))
        free.append(blocks()["free_blocks"])

    assert max(free) < 3 * blocks()["used_blocks"]


def test_compact_store_returns_the_file_to_the_size_of_what_is_in_it(tmp_path):
    database = tmp_path / "siab.duckdb"
    store = open_store(database)
    _big_table(store)
    _rewrite(store)
    before = read_table(store).collect().sort("persnr")
    store.execute("CHECKPOINT")
    grown = database.stat().st_size
    store.close()

    after_bytes = compact_store(database)

    assert after_bytes < grown
    assert after_bytes == database.stat().st_size
    assert not (tmp_path / "siab.duckdb.wal").exists()
    assert not (tmp_path / "siab.duckdb.compact").exists()

    reopened = open_store(database)
    try:
        assert read_table(reopened).collect().sort("persnr").equals(before)
    finally:
        reopened.close()


def test_compact_store_leaves_a_parquet_folder_alone(tmp_path):
    folder = tmp_path / "store"
    store = open_store(folder)
    write_table(store, pl.DataFrame({"persnr": [1, 2], "wage": [1.0, 2.0]}).lazy())
    sizes = {path.name: path.stat().st_size for path in folder.iterdir()}

    assert compact_store(folder) is None
    assert {path.name: path.stat().st_size for path in folder.iterdir()} == sizes


def test_compact_store_says_nothing_about_a_path_with_no_store_at_it(tmp_path):
    assert compact_store(tmp_path / "no_such_store.duckdb") is None
