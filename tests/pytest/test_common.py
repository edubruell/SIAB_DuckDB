"""
Synthetic tests for the step boundary in siab/common.py.

`read_table()` and `write_table()` are the only two places the pipeline moves
the dataset between DuckDB and polars, which makes them the only two places the
out-of-core claim can be won or lost. Since 2026-09-18 neither holds the table
in memory: DuckDB copies it to a Parquet file with its own writer, polars scans
that file, and `sink_parquet()` runs the step's plan back into a file that
DuckDB reads into the table.

These tests pin what that boundary has to preserve, which is everything: the
rows, the column order, the types, and the nulls. The counterpart of the R
arm's test-00_common_functions.R, which tests `compute_and_overwrite()` the
same way.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import polars as pl
import pytest

from siab.common import boundary_dir, database_path, read_table, write_table


@pytest.fixture
def connection(tmp_path):
    """A file-backed database, because that is what the pipeline runs on."""
    con = duckdb.connect(str(tmp_path / "siab.duckdb"))
    con.execute(
        """
        CREATE TABLE data AS
        SELECT * FROM (VALUES
            (1, 'a', DATE '1992-01-01', 10.5, TRUE),
            (2, 'b', DATE '1993-06-30', NULL, FALSE),
            (3, NULL, NULL, 12.25, NULL)
        ) AS t(persnr, label, begepi, wage, flag)
        """
    )
    yield con
    con.close()


# ======================================================================
#  What the boundary preserves
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
#  Where the handover files go, and that they do not stay
# ======================================================================

def test_the_handover_goes_beside_the_database(connection, tmp_path):
    assert database_path(connection) == tmp_path / "siab.duckdb"
    assert boundary_dir(connection).parent == tmp_path


def test_an_in_memory_database_falls_back_to_the_temporary_directory():
    con = duckdb.connect()
    try:
        assert database_path(con) is None
        assert boundary_dir(con).exists()
    finally:
        con.close()


def test_siab_spill_moves_the_handover_somewhere_else(connection, tmp_path,
                                                      monkeypatch):
    # What a database on a volume with no room for a second copy of itself
    # needs.
    elsewhere = tmp_path / "spill"
    monkeypatch.setenv("SIAB_SPILL", str(elsewhere))

    assert boundary_dir(connection) == elsewhere

    write_table(connection, read_table(connection))
    assert connection.execute("SELECT count(*) FROM data").fetchone()[0] == 3


def test_no_handover_file_is_left_behind(connection):
    # The files are the size of the dataset, so a run that kept one per step
    # would leave the disk holding the data eighteen times over.
    folder = boundary_dir(connection)

    write_table(connection, read_table(connection))

    assert list(folder.glob("*.parquet")) == []


def test_a_second_step_reads_what_the_first_one_wrote(connection):
    # The read file and the write file have different names on purpose: a step
    # scans the one while the sink fills the other, and reusing one path would
    # have the plan reading the file it is writing.
    write_table(connection, read_table(connection).with_columns(
        one=pl.lit(1)))
    write_table(connection, read_table(connection).with_columns(
        two=pl.lit(2)))

    rows = connection.execute("SELECT one, two FROM data LIMIT 1").fetchone()
    assert rows == (1, 2)


def test_two_databases_in_one_folder_do_not_share_a_handover(tmp_path):
    # Both arms of this project keep a table called `data`, and a test run
    # keeps its working database beside the test database. Without the database
    # name in the handover file, one run would overwrite the other's.
    first = duckdb.connect(str(tmp_path / "one.duckdb"))
    second = duckdb.connect(str(tmp_path / "two.duckdb"))
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
