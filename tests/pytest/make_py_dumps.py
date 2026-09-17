"""
Dump the Python pipeline's state after each step, for comparison against the
committed Stata fixtures.

This is main.py broken open: the same steps in the same order over the same test
database, but with a parquet dump of the touched columns written after each one.
The dumps stay untracked, because they can be regenerated from the test database
in a few minutes. The Stata side, in tests/testthat/fixtures/, is the committed
half, and it is the half both arms answer to.

    uv run python ../tests/pytest/make_py_dumps.py

Four environment variables set the folders, each with a fallback:

  SIAB_TEST_DB    the DuckDB file holding the test data, opened READ ONLY
  SIAB_PY_DB      the working database this script builds, which must not be
                  the R arm's, because both write a table called `data`
  SIAB_PY_DUMP    the folder these dumps are written to. The comparison tests
                  read the same variable, through conftest.py, so set it for
                  both or neither.
  SIAB_LOG        the folder the per-step logs are written to

The comparison tests in tests/pytest/test_reference_*.py skip when these dumps
are absent, so the fast synthetic tests still run without them.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent.parent
sys.path.insert(0, str(PROJECT / "python"))

from siab.common import folder_reference_factory, read_table, write_table  # noqa: E402
from siab.steps import (  # noqa: E402
    generate_biographic_variables,
    reallocate_one_time_payments,
    split_episodes,
)

# The same per-step column lists make_r_dumps.R uses, with the Python names
# where they differ from the Stata ones. `jahr` is `year` in both ports.
KEY = ["persnr", "spell", "begepi"]

TOUCHED = {
    "01_split_episodes": ["begepi", "endepi", "begepi_orig", "endepi_orig",
                          "year", "age"],
    "02_grund154": ["tentgelt"],
    "03_SIAB_bio": ["azubi", "ein_erw", "tage_erw", "ein_bet", "tage_bet",
                    "ein_job", "tage_job", "anz_lst", "tage_lst"],
}


def dump_step(con: duckdb.DuckDBPyConnection, step: str, dump_dir: Path) -> None:
    present = [row[1] for row in con.execute("PRAGMA table_info('data')").fetchall()]
    wanted = [c for c in dict.fromkeys(KEY + TOUCHED[step]) if c in present]
    missing = [c for c in dict.fromkeys(KEY + TOUCHED[step]) if c not in present]

    out = dump_dir / f"{step}.parquet"
    con.execute(
        f"COPY (SELECT {', '.join(wanted)} FROM data) "
        f"TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    rows = con.execute("SELECT count(*) FROM data").fetchone()[0]
    note = f"  MISSING: {', '.join(missing)}" if missing else ""
    print(f"{step + '.parquet':<32} {rows:>8d} rows  {len(wanted):>2d} cols{note}")


def main() -> None:
    db_file = Path(os.environ.get(
        "SIAB_TEST_DB", PROJECT / "local_context" / "testdb" / "siab_test.duckdb"))
    py_db = Path(os.environ.get(
        "SIAB_PY_DB", PROJECT / "local_context" / "testdb" / "siab_py.duckdb"))
    dump_dir = Path(os.environ.get(
        "SIAB_PY_DUMP", PROJECT / "local_context" / "testdb" / "py_dump"))
    log_dir = folder_reference_factory(os.environ.get("SIAB_LOG", PROJECT / "log"))

    dump_dir.mkdir(parents=True, exist_ok=True)

    if not db_file.exists():
        raise SystemExit(
            f"No test database at {db_file}. Write one with the read-in script, "
            f"or set SIAB_TEST_DB.")

    # The test database is attached read only and its `orig` table is copied
    # into a working database of this arm's own. Both arms write a table called
    # `data`, so sharing one file would mean each run destroying the other's
    # state halfway through.
    if py_db.exists():
        py_db.unlink()
    con = duckdb.connect(str(py_db))
    con.execute(f"ATTACH '{db_file}' AS src (READ_ONLY)")

    tables = [row[0] for row in con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = 'src'").fetchall()]
    if "orig" not in tables:
        raise SystemExit(f"The test database has no `orig` table: {db_file}")

    # ================================================================
    #  Step 0, from main.py
    # ================================================================
    # 00_master_SIAB.do keeps only the employment history before it generates
    # jahr and age: `keep if inlist(quelle,1,2,3)`. Sources 4 to 7 are dropped.
    con.execute(
        """
        CREATE OR REPLACE TABLE data AS
        SELECT *, year(begepi) AS year, year(begepi) - gebjahr AS age
        FROM src.orig
        WHERE quelle IN (1, 2, 3)
        """
    )
    con.execute("DETACH src")

    # ================================================================
    #  The steps, dumped one at a time
    # ================================================================
    write_table(con, split_episodes(
        read_table(con, "data"), log_file=log_dir("py_01_split_episodes.log")), "data")
    dump_step(con, "01_split_episodes", dump_dir)

    write_table(con, reallocate_one_time_payments(
        read_table(con, "data"), log_file=log_dir("py_01b_grund154.log")), "data")
    dump_step(con, "02_grund154", dump_dir)

    write_table(con, generate_biographic_variables(
        read_table(con, "data"), log_file=log_dir("py_01_SIAB_bio.log")), "data")
    dump_step(con, "03_SIAB_bio", dump_dir)

    print(f"\nPython dumps written to {dump_dir}")
    con.close()


if __name__ == "__main__":
    main()
