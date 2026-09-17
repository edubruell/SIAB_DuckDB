"""
The Python arm's pipeline, the counterpart of siab_main.R.

Three of the sixteen reference steps are ported so far, so this runs the prep up
to 03_SIAB_bio.do and stops. The remaining steps are added here in the
reference's order as they land.

Three environment variables set the folders, each with a fallback:

  SIAB_DB        the DuckDB file holding the data, with an `orig` table
  SIAB_LOG       the folder the per-step logs are written to

Run it with:

  uv run python main.py
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

from siab.common import folder_reference_factory, read_table, write_table
from siab.steps import (
    generate_biographic_variables,
    reallocate_one_time_payments,
    split_episodes,
)

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent


def main() -> None:
    db_file = os.environ.get("SIAB_DB", str(PROJECT / "local_context" / "testdb" / "siab_test.duckdb"))
    log_dir = folder_reference_factory(os.environ.get("SIAB_LOG", str(PROJECT / "log")))

    if not Path(db_file).exists():
        raise SystemExit(
            f"No database at {db_file}. Write one with the read-in script, or set SIAB_DB."
        )

    con = duckdb.connect(db_file)
    if "orig" not in [row[0] for row in con.execute("SHOW TABLES").fetchall()]:
        raise SystemExit(f"The database has no `orig` table: {db_file}")

    # 00_master_SIAB.do keeps only the employment history before it generates
    # jahr and age: `keep if inlist(quelle,1,2,3)`. Sources 4 to 7 are dropped.
    con.execute(
        """
        CREATE OR REPLACE TABLE data AS
        SELECT *, year(begepi) AS year, year(begepi) - gebjahr AS age
        FROM orig
        WHERE quelle IN (1, 2, 3)
        """
    )

    frame = read_table(con, "data")
    frame = split_episodes(frame, log_file=log_dir("01_split_episodes.log"))
    write_table(con, frame, "data")

    frame = read_table(con, "data")
    frame = reallocate_one_time_payments(frame, log_file=log_dir("01b_grund154.log"))
    write_table(con, frame, "data")

    frame = read_table(con, "data")
    frame = generate_biographic_variables(frame, log_file=log_dir("01_SIAB_bio.log"))
    write_table(con, frame, "data")

    rows = con.execute("SELECT count(*) FROM data").fetchone()[0]
    print(f"\nPipeline finished, {rows} rows in the `data` table of {db_file}")
    con.close()


if __name__ == "__main__":
    main()
