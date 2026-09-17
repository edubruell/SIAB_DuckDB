"""
The Python arm's pipeline, the counterpart of siab_main.R.

Seventeen of the eighteen step functions are ported, and the pipeline below runs
every one of them that the reference master calls. The imputation of
right-censored wages is the exception: it needs a censored normal regression and
is not ported, so the call is in place and commented out, and the two steps
downstream of it are commented out with it. Uncomment all three once
`impute_wages()` has a body.

Three environment variables set the folders, each with a fallback:

  SIAB_DB        the DuckDB file holding the data, with an `orig` table
  SIAB_RAW       the folder the raw SIAB delivery sits in
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
    deflate_wages,
    drop_empty_columns,
    generate_biographic_variables,
    generate_educ_variable,
    generate_industry_variables,
    generate_limit_assess,
    generate_limit_marginal,
    generate_occupation_variables,
    handle_parallel_episodes,
    merge_basic_bhp,
    reallocate_one_time_payments,
    restrict_observation_period,
    split_episodes,
)

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent


def main() -> None:
    db_file = os.environ.get(
        "SIAB_DB", str(Path.home() / "data" / "siab_db" / "siab.duckdb"))
    rawdata = folder_reference_factory(
        os.environ.get("SIAB_RAW", str(Path.home() / "data" / "siab_raw")))
    log_dir = folder_reference_factory(os.environ.get("SIAB_LOG", str(PROJECT / "log")))

    if not Path(db_file).exists():
        raise SystemExit(
            f"No database at {db_file}. Write one with the read-in script, or set SIAB_DB."
        )

    con = duckdb.connect(db_file)
    if "orig" not in [row[0] for row in con.execute("SHOW TABLES").fetchall()]:
        raise SystemExit(f"The database has no `orig` table: {db_file}")

    # ================================================================
    #  1. Generate the variables `year` and `age` in the database
    # ================================================================
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

    # ================================================================
    #  2. Prepare the SIAB as a yearly panel
    # ================================================================
    # Each step reads the `data` table, works over it in polars and writes the
    # table back. DuckDB owns the table in between, which is where the
    # out-of-core guarantee comes from.
    def run(step, **kwargs) -> None:
        write_table(con, step(read_table(con, "data"), **kwargs), "data")

    # 00_master_SIAB.do drops every variable that holds only missings right
    # after the source restriction above, before it generates anything else.
    # Most of what goes are the variables only the benefit spells ever filled.
    run(drop_empty_columns, log_file=log_dir("00b_drop_empty_columns.log"))
    run(split_episodes, log_file=log_dir("01_split_episodes.log"))
    run(reallocate_one_time_payments, log_file=log_dir("01b_grund154.log"))
    run(generate_biographic_variables, log_file=log_dir("01_SIAB_bio.log"))
    run(restrict_observation_period, min_year=1975, max_year=2023,
        log_file=log_dir("03d_observation_period.log"))
    run(generate_occupation_variables, log_file=log_dir("02_occupations.log"))
    run(merge_basic_bhp,
        bhp_file=rawdata("SIAB_7523_v2_bhp_basis_v1.dta"),
        log_file=log_dir("03b_bhp_basis.log"))
    run(generate_industry_variables, log_file=log_dir("03c_industries.log"))
    run(generate_educ_variable, log_file=log_dir("03_education.log"))
    run(generate_limit_assess, log_file=log_dir("04_wage_assesment_ceiling.log"))
    run(generate_limit_marginal, log_file=log_dir("05_wages_marginal.log"))
    run(deflate_wages, log_file=log_dir("06_wages_deflation.log"))

    # The imputation of right-censored wages is not ported. `wage_imp` is what
    # the parallel-episode step and both panel builders read, so those calls are
    # commented out with it. See siab/steps/wages_imputation.py.
    #
    # run(impute_wages, log_file=log_dir("07_wages_imputation.log"))

    # 11_merge_BHP.do and 12_merge_AKM.do are switched off in the reference
    # master, because every file they read has to be requested from the FDZ on
    # top of the SIAB itself. Uncomment either call once the files are in place.
    #
    # run(merge_annual_bhp, bhp_folder=rawdata(""),
    #     log_file=log_dir("07b_bhp_annual.log"))
    # run(merge_akm,
    #     akm_estab_file=rawdata("SIAB_7523_v2_akm_estab.dta"),
    #     akm_pers_file=rawdata("SIAB_7523_v2_akm_pers.dta"),
    #     log_file=log_dir("07c_akm.log"))
    #
    # run(handle_parallel_episodes, handling="wage",
    #     log_file=log_dir("08_parallel_episodes.log"))
    # run(build_yearly_panel, cutoff_month=6, cutoff_day=30,
    #     log_file=log_dir("09_yearly_panel.log"))
    #
    # 16_monthly_panel.do is an alternative to the yearly panel, not a step
    # after it: it cuts every episode into one row per calendar month and keeps
    # the month's 15th. Swap the call above for this one to build that panel.
    #
    # run(build_monthly_panel, cutoff_day=15,
    #     log_file=log_dir("09b_monthly_panel.log"))

    # ================================================================
    #  3. Clean up
    # ================================================================
    # Only `data` and `orig` should remain as tables.
    for (table,) in con.execute("SHOW TABLES").fetchall():
        if table not in ("orig", "data"):
            con.execute(f"DROP TABLE {table}")

    rows = con.execute("SELECT count(*) FROM data").fetchone()[0]
    print(f"\nPipeline finished, {rows} rows in the `data` table of {db_file}")
    con.close()


if __name__ == "__main__":
    main()
