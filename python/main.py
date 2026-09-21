"""
The Python arm's pipeline, the counterpart of siab_main.R.

All eighteen step functions are ported, and the pipeline below runs every one of
them that the reference master calls. The two merges that read files the FDZ
only delivers on a separate request are the exception, and are commented out
here exactly as the reference master switches them off.

`stata_to_db_batch_read.py` runs before this and writes the `orig` table this
reads. Nothing here creates one.

Nothing in the eighteen steps is SQL: they compute in polars, and the store
only has to hold a table between them. Two stores can do that, and the name of
the target picks which. A name ending in `.duckdb` is a DuckDB database, which
is what the R arm uses and what leaves a database to query afterwards. Any
other name is a folder, and each table is a Parquet file in it, which needs no
database engine at all.

Environment variables set the folders, each with a fallback:

  SIAB_DB        the store: a `.duckdb` file, or a folder for Parquet tables.
                 Defaults to siab.duckdb inside SIAB_DB_FOLDER.
  SIAB_DB_FOLDER the folder that default database sits in, the same variable
                 the read-in and the R arm use
  SIAB_RAW       the folder the raw SIAB delivery sits in, defaulting to
                 SIAB_RAW_FOLDER, the name the read-in and the R arm use
  SIAB_LOG       the folder the per-step logs are written to
  SIAB_BOUNDARY  how a DuckDB store hands a table between steps: `memory`, the
                 default, which collects it, or `parquet`, which writes a file
                 at each end, so peak memory stays at what one step's plan
                 needs. A Parquet store has no handover and ignores it.
  SIAB_SPILL     where the Parquet handover files between steps are written,
                 beside the database by default. It applies to SIAB_BOUNDARY
                 `parquet` only; a Parquet store has no handover files.

Run it with:

  uv run --project python python python/main.py
"""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl

from siab.common import (
    DEFAULT_BOUNDARY,
    compact_store,
    count_rows,
    drop_table,
    folder_reference_factory,
    open_store,
    read_table,
    store_description,
    table_names,
    write_table,
)
from siab.steps import (
    build_yearly_panel,
    deflate_wages,
    drop_empty_columns,
    generate_biographic_variables,
    generate_educ_variable,
    generate_industry_variables,
    generate_limit_assess,
    generate_limit_marginal,
    generate_occupation_variables,
    handle_parallel_episodes,
    impute_wages,
    merge_basic_bhp,
    reallocate_one_time_payments,
    restrict_observation_period,
    split_episodes,
)

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent


def main() -> None:
    db_folder = os.environ.get(
        "SIAB_DB_FOLDER", str(Path.home() / "data" / "siab_db"))
    target = os.environ.get("SIAB_DB", str(Path(db_folder) / "siab.duckdb"))
    rawdata = folder_reference_factory(
        os.environ.get("SIAB_RAW",
                       os.environ.get("SIAB_RAW_FOLDER",
                                      str(Path.home() / "data" / "siab_raw"))))
    log_dir = folder_reference_factory(os.environ.get("SIAB_LOG", str(PROJECT / "log")))

    if not Path(target).exists():
        raise SystemExit(
            f"No store at {target}. Write one with "
            f"python/stata_to_db_batch_read.py, or set SIAB_DB."
        )

    store = open_store(
        target,
        os.environ.get("SIAB_BOUNDARY", DEFAULT_BOUNDARY),
        memory_limit=os.environ.get("SIAB_DUCKDB_MEMORY_LIMIT"),
        temp_directory=os.environ.get("SIAB_DUCKDB_TEMP_DIR"),
    )
    if "orig" not in table_names(store):
        raise SystemExit(f"There is no `orig` table in {store_description(store)}")

    # ================================================================
    #  1. Generate the variables `year` and `age` in the database
    # ================================================================
    # 00_master_SIAB.do keeps only the employment history before it generates
    # jahr and age: `keep if inlist(quelle,1,2,3)`. Sources 4 to 7 are dropped.
    # The R arm writes this as SQL. Here it is polars, like every step below,
    # which is what lets the same pipeline run over a store that has no query
    # engine in it.
    write_table(
        store,
        read_table(store, "orig")
        .filter(pl.col("quelle").is_in([1, 2, 3]))
        .with_columns(year=pl.col("begepi").dt.year())
        .with_columns(age=pl.col("year") - pl.col("gebjahr")),
        "data",
    )

    # ================================================================
    #  2. Prepare the SIAB as a yearly panel
    # ================================================================
    # Each step reads the `data` table, works over it in polars and writes the
    # table back. The store owns the table in between. A Parquet store, and a
    # DuckDB store opened with SIAB_BOUNDARY=parquet, hand it over as a file,
    # so neither side holds the dataset and peak memory is one step's own plan;
    # a DuckDB store on its default collects it at both ends, which is faster
    # on data that fits.
    def run(step, **kwargs) -> None:
        write_table(store, step(read_table(store, "data"), **kwargs), "data")

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

    # The imputation draws a random term for every censored wage and sets no
    # seed of its own, so two runs of the pipeline give two different wage_imp
    # columns. Pass `seed=` to fix them.
    run(impute_wages, log_file=log_dir("07_wages_imputation.log"))

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

    run(handle_parallel_episodes, handling="wage",
        log_file=log_dir("08_parallel_episodes.log"))
    run(build_yearly_panel, cutoff_month=6, cutoff_day=30,
        log_file=log_dir("09_yearly_panel.log"))

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
    for table in table_names(store):
        if table not in ("orig", "data"):
            drop_table(store, table)

    rows = count_rows(store, "data")
    print(f"\nPipeline finished, {rows} rows in the `data` table of "
          f"{store_description(store)}")
    store.close()

    # The run allocated a block for every copy of `data` it wrote and every
    # helper table it dropped, and a DuckDB file never shrinks on its own.
    # Copying the two surviving tables into a fresh database returns all of it
    # and hands the store on at the size of what is in it. A Parquet folder
    # holds one file per table and is left alone.
    compact_store(target)


if __name__ == "__main__":
    main()
