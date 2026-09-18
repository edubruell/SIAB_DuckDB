"""
Dump the Python pipeline's state after each step, for comparison against the
committed Stata fixtures.

This is main.py broken open: the same steps in the same order over the same test
database, but with a parquet dump of the touched columns written after each one.
The dumps stay untracked, because they can be regenerated from the test database
in a few minutes. The Stata side, in tests/testthat/fixtures/, is the committed
half, and it is the half both arms answer to.

    uv run python ../tests/pytest/make_py_dumps.py

The chain runs the whole prep. The last three steps are dumped off a second
branch of it: 16_yearly_panel.do and 16_monthly_panel.do are alternatives rather
than one after the other, so both are built from the same parallel-episode data,
exactly as make_fixtures.do reloads the step 15 dump for the second one.

Six environment variables set the folders, each with a fallback:

  SIAB_TEST_DB    the DuckDB file holding the test data, opened READ ONLY
  SIAB_PY_DB      the working database this script builds, which must not be
                  the R arm's, because both write a table called `data`
  SIAB_PY_DUMP    the folder these dumps are written to. The comparison tests
                  read the same variable, through conftest.py, so set it for
                  both or neither.
  SIAB_TEST_DATA  the folder holding the FDZ test data the merges read
  SIAB_AKM_DIR    the folder holding the two fabricated AKM files. Both arms
                  have to read the same two files or the comparison means
                  nothing, so this points at the Stata fixture run's orig
                  folder by default.
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
    build_monthly_panel,
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
    merge_akm,
    merge_annual_bhp,
    merge_basic_bhp,
    reallocate_one_time_payments,
    restrict_observation_period,
    split_episodes,
)

# The same per-step column lists make_r_dumps.R uses, with the Python names
# where they differ from the Stata ones. `jahr` is `year` in both ports.
KEY = ["persnr", "spell", "begepi"]

# The three late steps that drop part of the key. The Stata side drops `spell`
# in 15_parallel_episodes.do and `begepi` in 16_yearly_panel.do; the port keeps
# the columns, so the narrower key here is what makes the two halves joinable.
STEP_KEY = {
    "15_parallel_episodes": ["persnr", "begepi"],
    "16_yearly_panel": ["persnr", "year"],
    "16_monthly_panel": ["persnr", "year", "begepi_monthly"],
}

TOUCHED = {
    "01_split_episodes": ["begepi", "endepi", "begepi_orig", "endepi_orig",
                          "year", "age"],
    "02_grund154": ["tentgelt"],
    "03_SIAB_bio": ["azubi", "ein_erw", "tage_erw", "ein_bet", "tage_bet",
                    "ein_job", "tage_job", "anz_lst", "tage_lst"],
    "04_merge_basic_BHP": ["ao_bula", "w93_3_gen"],
    "05_educ_broad": ["educ"],
    "06_wages_assessment_ceiling": ["east", "limit_assess"],
    "07_wages_marginal": ["tentgelt", "limit_marginal", "marginal"],
    "08_wages_deflation": ["tentgelt", "cpi", "wage_defl",
                           "limit_marginal_defl", "limit_assess_defl"],
    "09_restrictions": [],
    # The same four columns make_r_dumps.R takes. quelle rides along because the
    # imputation only ever touches the employment history, and cens is flagged 0
    # rather than missing off it, which a comparison has to be able to see.
    "10_wages_imputation": ["quelle", "cens", "wage", "wage_imp"],
    # 11_merge_BHP.do merges five files, and the columns below are everything the
    # test delivery's versions of them carry. `besch` is the one column two of the
    # files share, which is where the merges' `update` option does real work.
    # quelle rides along because only employment episodes carry an establishment
    # number, and a test has no other way to check that nothing else matched.
    "11_merge_BHP": ["quelle",
                     "az_f", "az_reg", "az_azubi", "az_atz",
                     "az_tz", "az_f_vz", "az_f_tz", "az_reg_vz",
                     "ein_ges", "ein_gf", "ein_vz",
                     "aus_ges", "aus_gf", "aus_vz",
                     "eintritt", "besch", "besch_vor",
                     "status_vor", "inflow",
                     "austritt", "besch_nach", "status_nach",
                     "outflow"],
    # The AKM effects themselves are fabricated noise, so the fixture is compared
    # on which episodes carry one, never on a value. See make_synth_akm.do.
    "12_merge_AKM": ["feff_1985_1992", "feff_1993_2000",
                     "feff_2001_2008", "feff_2009_2016",
                     "feff_2017_2023",
                     "peff_1985_1992", "peff_1993_2000",
                     "peff_2001_2008", "peff_2009_2016",
                     "peff_2017_2023"],
    # 13_industries_1digit.do maps the time-consistent three-digit industry to two
    # one-digit codes. The port builds both in generate_industry_variables(),
    # directly after the basic BHP merge that brings w93_3_gen in; the dump is
    # taken here, where the reference creates the columns.
    "13_industries_1digit": ["w93_3_gen", "industry1_destatis",
                             "industry1_estpanel"],
    # The port reaches occ_blo in generate_occupation_variables(), which runs
    # far earlier than the reference's 14_occ_blossfeld.do; the column is dumped
    # here, at the position the reference creates it, so the two are comparable.
    "14_occ_blossfeld": ["beruf", "occ_blo"],
    "15_parallel_episodes": ["quelle", "tage_bet", "wage_imp", "nspell",
                             "parallel_jobs", "parallel_wage",
                             "parallel_wage_imp", "parallel_benefits"],
    # begepi and endepi have no Stata counterpart here: 16_yearly_panel.do drops
    # both and the port keeps them. They ride along so a test can check that every
    # surviving episode really does cover the cutoff date.
    "16_yearly_panel": ["quelle", "erwstat", "parallel_benefits",
                        "year_days_emp", "year_days_benefits",
                        "year_labor_earn",
                        "tage_bet", "tage_job", "tage_erw",
                        "tage_lst", "begepi", "endepi"],
    # The monthly panel is an alternative to the yearly one, so its dump is taken
    # from a second run over the same step 15 data. `year` carries the reference's
    # `jahr` and its `year` at once: the reference generates a second year column
    # from the month, and after 01_split_episodes.do no episode crosses a year
    # boundary, so the two agree row by row.
    "16_monthly_panel": ["quelle", "erwstat", "parallel_benefits",
                         "year_days_emp", "year_days_benefits",
                         "year_labor_earn",
                         "tage_bet", "tage_job", "tage_erw",
                         "tage_lst",
                         "month", "month_num", "endepi_monthly",
                         "begepi", "endepi"],
}


def key_for(step: str) -> list[str]:
    return STEP_KEY.get(step, KEY)


def dump_step(con: duckdb.DuckDBPyConnection, step: str, dump_dir: Path) -> None:
    present = [row[1] for row in con.execute("PRAGMA table_info('data')").fetchall()]
    asked = list(dict.fromkeys(key_for(step) + TOUCHED[step]))
    wanted = [c for c in asked if c in present]
    missing = [c for c in asked if c not in present]

    out = dump_dir / f"{step}.parquet"
    con.execute(
        f"COPY (SELECT {', '.join(wanted)} FROM data) "
        f"TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    rows = con.execute("SELECT count(*) FROM data").fetchone()[0]
    note = f"  MISSING: {', '.join(missing)}" if missing else ""
    print(f"{step + '.parquet':<32} {rows:>8d} rows  {len(wanted):>2d} cols{note}")


def run(con: duckdb.DuckDBPyConnection, step, **kwargs) -> None:
    """Read the `data` table, run one step over it and write the table back."""
    write_table(con, step(read_table(con, "data"), **kwargs), "data")


def main() -> None:
    db_file = Path(os.environ.get(
        "SIAB_TEST_DB", PROJECT / "local_context" / "testdb" / "siab_test.duckdb"))
    py_db = Path(os.environ.get(
        "SIAB_PY_DB", PROJECT / "local_context" / "testdb" / "siab_py.duckdb"))
    dump_dir = Path(os.environ.get(
        "SIAB_PY_DUMP", PROJECT / "local_context" / "testdb" / "py_dump"))
    testdata = folder_reference_factory(os.environ.get(
        "SIAB_TEST_DATA", PROJECT / "local_context" / "testdata" / "siab_7523_v2"))
    akm_dir = folder_reference_factory(os.environ.get(
        "SIAB_AKM_DIR", PROJECT / "local_context" / "stata_fixtures" / "orig"))
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

    # The master drops every variable that holds only missings here, and
    # make_fixtures.do keeps that position, so this side does it too. It can only
    # drop a subset of what the reference drops, because Stata counts the empty
    # string as missing and this port counts only null, so no compared column can
    # go missing on one side alone.
    run(con, drop_empty_columns,
        log_file=log_dir("py_00b_drop_empty_columns.log"))

    run(con, split_episodes, log_file=log_dir("py_01_split_episodes.log"))
    dump_step(con, "01_split_episodes", dump_dir)

    run(con, reallocate_one_time_payments, log_file=log_dir("py_01b_grund154.log"))
    dump_step(con, "02_grund154", dump_dir)

    run(con, generate_biographic_variables, log_file=log_dir("py_01_SIAB_bio.log"))
    dump_step(con, "03_SIAB_bio", dump_dir)

    # The master restricts to the observation period here, between 03_SIAB_bio.do
    # and 04_merge_basic_BHP.do, and make_fixtures.do keeps that position. With the
    # full span of the delivery it drops nothing, so the dumps stay comparable.
    run(con, restrict_observation_period, min_year=1975, max_year=2023,
        log_file=log_dir("py_03d_observation_period.log"))

    # The occupation crosswalks have no Stata counterpart at this position: the
    # reference merges them in 14_occ_blossfeld.do, long after step 09. The step is
    # run anyway so the dumps come off the same pipeline main.py exercises.
    run(con, generate_occupation_variables, log_file=log_dir("py_02_occupations.log"))

    run(con, merge_basic_bhp,
        bhp_file=testdata("SIAB_7523_v2_bhp_basis_v1.dta"),
        log_file=log_dir("py_03b_bhp_basis.log"))
    dump_step(con, "04_merge_basic_BHP", dump_dir)

    # The industry mappings have no Stata counterpart at this position either: the
    # reference builds them in 13_industries_1digit.do, after the AKM merge. The
    # step runs here because w93_3_gen arrives with the merge above.
    run(con, generate_industry_variables, log_file=log_dir("py_03c_industries.log"))

    run(con, generate_educ_variable, log_file=log_dir("py_03_education.log"))
    dump_step(con, "05_educ_broad", dump_dir)

    run(con, generate_limit_assess, log_file=log_dir("py_04_wage_assesment_ceiling.log"))
    dump_step(con, "06_wages_assessment_ceiling", dump_dir)

    run(con, generate_limit_marginal, log_file=log_dir("py_05_wages_marginal.log"))
    dump_step(con, "07_wages_marginal", dump_dir)

    run(con, deflate_wages, log_file=log_dir("py_06_wages_deflation.log"))
    dump_step(con, "08_wages_deflation", dump_dir)

    # 09_restrictions has no counterpart in either port. The Stata fixture run
    # takes its dump and then continues from the step 08 data for the same
    # reason: the step imposes one project's sample cut.
    dump_step(con, "09_restrictions", dump_dir)

    # impute_wages() draws a random term for every censored wage. The seed here
    # is the dump's, not the pipeline's: it makes this file reproducible without
    # changing what main.py does. Unlike the R arm's, this side sorts each cell
    # on persnr and spell before it draws, so the seed determines the draws
    # rather than only the sequence they are taken from. It does not bring them
    # any closer to Stata's, which come from a different generator seeded inside
    # the reference step, so wage_imp can only ever be compared distributionally.
    run(con, impute_wages, seed=123,
        log_file=log_dir("py_07_wages_imputation.log"))
    dump_step(con, "10_wages_imputation", dump_dir)

    # 11_merge_BHP.do reads the yearly establishment panel and the four extension
    # files straight out of the delivery, so this side reads the same folder the
    # Stata fixture run staged its copies from.
    run(con, merge_annual_bhp, bhp_folder=testdata(""),
        log_file=log_dir("py_07b_bhp_annual.log"))
    dump_step(con, "11_merge_BHP", dump_dir)

    # The AKM files are fabricated, not delivered, so both sides have to read the
    # same two files or the comparison means nothing. make_synth_akm.do writes
    # them into the Stata fixture run's orig folder and this reads them back.
    run(con, merge_akm,
        akm_estab_file=akm_dir("SIAB_7523_v2_akm_estab.dta"),
        akm_pers_file=akm_dir("SIAB_7523_v2_akm_pers.dta"),
        log_file=log_dir("py_07c_akm.log"))
    dump_step(con, "12_merge_AKM", dump_dir)

    # Both one-digit industries were generated far earlier, by
    # generate_industry_variables() after the basic BHP merge, and so were the
    # Blossfeld occupations, by generate_occupation_variables(). Nothing between
    # those positions and this one touches w93_3_gen or beruf, so the dumps are
    # taken here, at the point of the chain the reference creates each column.
    dump_step(con, "13_industries_1digit", dump_dir)
    dump_step(con, "14_occ_blossfeld", dump_dir)

    # The reference's uncommented rule defines the main episode as the job with
    # the longest tenure, using the imputed wage only to break a tie.
    # handling = "wage", which main.py passes, sorts on the imputed wage first;
    # those draws differ between Python and Stata by construction, so under that
    # setting the two sides would keep different episodes and nothing downstream
    # would compare.
    run(con, handle_parallel_episodes, handling="tenure",
        log_file=log_dir("py_08_parallel_episodes.log"))
    dump_step(con, "15_parallel_episodes", dump_dir)

    # 16_yearly_panel.do and 16_monthly_panel.do are alternatives: both start
    # from the step 15 data and the reference master calls neither. The step 15
    # state is therefore kept aside here, so the monthly panel can be built from
    # the same input the yearly one was, exactly as make_fixtures.do reloads the
    # step 15 dump for it.
    con.execute("CREATE TABLE parallel_episodes AS SELECT * FROM data")

    run(con, build_yearly_panel, cutoff_month=6, cutoff_day=30,
        log_file=log_dir("py_09_yearly_panel.log"))
    dump_step(con, "16_yearly_panel", dump_dir)

    con.execute("DROP TABLE data")
    con.execute("ALTER TABLE parallel_episodes RENAME TO data")

    # The reference hardcodes the 15th of the month as the cutoff, which is the
    # port's default.
    run(con, build_monthly_panel, cutoff_day=15,
        log_file=log_dir("py_09b_monthly_panel.log"))
    dump_step(con, "16_monthly_panel", dump_dir)

    print(f"\nPython dumps written to {dump_dir}")
    con.close()


if __name__ == "__main__":
    main()
