# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Run the whole preparation against the FDZ test data.
#
# This is siab_main.R with the two folder references pointed at a test database
# and the test data folder instead of the production paths, so that a full pass
# is reproducible without editing the main script. It is a smoke test, not a
# correctness test: it shows that every step executes and writes, not that any
# column matches the Stata reference. Column-level tests live under
# tests/testthat/.
#
# Both paths can be overridden:
#   SIAB_TEST_DB    the DuckDB file, default local_context/testdb/siab_test.duckdb
#   SIAB_TEST_DATA  the folder holding SIAB_7523_v2_bhp_basis_v1.dta,
#                   default local_context/testdata/siab_7523_v2
#
# The database must already carry the `orig` table, written by
# stata_to_db_batch_read.R. The run overwrites `data` and drops the helper
# tables it creates.
#
#   Rscript run_testdata.R
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library("pacman")
p_load(dplyr, dbplyr, readr, tidyr, purrr, duckdb, stringr, glue, scales,
       data.table, readstata13, here, logger, survival)

here("functions") |>
  dir() |>
  walk(~source(here("functions", .x)))

db_file <- Sys.getenv(
  "SIAB_TEST_DB",
  here("local_context", "testdb", "siab_test.duckdb")
)
testdata <- folder_reference_factory(Sys.getenv(
  "SIAB_TEST_DATA",
  here("local_context", "testdata", "siab_7523_v2")
))

if (!file.exists(db_file)) {
  stop("No test database at ", db_file,
       ". Write one with stata_to_db_batch_read.R, or set SIAB_TEST_DB.")
}

con <- dbConnect(duckdb::duckdb(), dbdir = db_file, read_only = FALSE)

if (!"orig" %in% dbListTables(con)) {
  stop("The test database has no `orig` table: ", db_file)
}

#====================================================================
#  1. Generate variables 'year' and 'age' in the database
#====================================================================

tbl(con, "orig") |>
  mutate(year = year(begepi),
         age  = year - gebjahr) |>
  compute_and_overwrite("data")

#====================================================================
#  2. Prepare the SIAB as a yearly panel
#====================================================================

pipeline <- con |>
  split_episodes(               log_file = here("log", "01_split_episodes.log")) |>
  generate_biographic_variables(log_file = here("log", "01_SIAB_Bio.log")) |>
  generate_occupation_variables(log_file = here("log", "02_occupations.log")) |>
  generate_educ_variable(       log_file = here("log", "03_education.log")) |>
  merge_basic_bhp(              log_file = here("log", "03b_bhp_basis.log"),
                                bhp_file = testdata("SIAB_7523_v2_bhp_basis_v1.dta")) |>
  generate_limit_assess(        log_file = here("log", "04_wage_assesment_ceiling.log")) |>
  generate_limit_marginal(      log_file = here("log", "05_wages_marginal.log")) |>
  deflate_wages(                log_file = here("log", "06_wages_deflation.log")) |>
  impute_wages(                 log_file = here("log", "07_wages_imputation.log")) |>
  handle_parallel_episodes(     log_file = here("log", "08_parallel_episodes.log"),
                                handling = "wage") |>
  build_yearly_panel(           log_file = here("log", "09_yearly_panel.log"),
                                cutoff_month = 6,
                                cutoff_day   = 30)

#====================================================================
#  3. Clean up and report
#====================================================================

dbListTables(con) |>
  purrr::discard(~{.x %in% c("orig", "data")}) |>   # scales masks purrr::discard
  walk(~dbRemoveTable(con, .x))

result <- tbl(con, "data")

cat("\n",
    "rows:    ", result |> count() |> pull(n), "\n",
    "years:   ", result |> summarise(from = min(year, na.rm = TRUE),
                                     to   = max(year, na.rm = TRUE)) |>
                 collect() |> unlist() |> paste(collapse = " - "), "\n",
    "columns: ", length(colnames(result)), "\n",
    "tables:  ", paste(dbListTables(con), collapse = ", "), "\n",
    sep = "")

dbDisconnect(con, shutdown = TRUE)
