# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#  Dump the R pipeline's state after each step, for comparison against the
#  committed Stata fixtures.
#
#  This is run_testdata.R broken open: the same steps in the same order over the
#  same test database, but with a parquet dump of the touched columns written
#  after each one. The dumps go to local_context/testdb/r_dump/ and stay
#  untracked, because they can be regenerated from the test database in a few
#  minutes. The Stata side, in tests/testthat/fixtures/, is the committed half.
#
#    Rscript tests/fixtures/make_r_dumps.R
#
#  The comparison tests in tests/testthat/test-compare-*.R skip when these
#  dumps are absent, so the fast synthetic suite still runs without them.
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
dump_dir <- here("local_context", "testdb", "r_dump")
dir.create(dump_dir, showWarnings = FALSE, recursive = TRUE)

if (!file.exists(db_file)) {
  stop("No test database at ", db_file,
       ". Write one with stata_to_db_batch_read.R, or set SIAB_TEST_DB.")
}

con <- dbConnect(duckdb::duckdb(), dbdir = db_file, read_only = FALSE)
if (!"orig" %in% dbListTables(con)) {
  stop("The test database has no `orig` table: ", db_file)
}

# The same per-step column lists tests/fixtures/make_fixtures.R uses, with the
# R names where they differ from the Stata ones. `jahr` is `year` in the port.
key <- c("persnr", "spell", "begepi")
touched <- list(
  "01_split_episodes"           = c("begepi", "endepi", "begepi_orig",
                                    "endepi_orig", "year", "age"),
  "02_grund154"                 = c("tentgelt"),
  "03_SIAB_bio"                 = c("azubi", "ein_erw", "tage_erw", "ein_bet",
                                    "tage_bet", "ein_job", "tage_job",
                                    "anz_lst", "tage_lst"),
  "04_merge_basic_BHP"          = c("ao_bula", "w93_3_gen"),
  "05_educ_broad"               = c("educ"),
  "06_wages_assessment_ceiling" = c("east", "limit_assess"),
  "07_wages_marginal"           = c("tentgelt", "limit_marginal", "marginal"),
  "08_wages_deflation"          = c("tentgelt", "cpi", "wage_defl",
                                    "limit_marginal_defl", "limit_assess_defl"),
  "09_restrictions"             = character(0),
  "10_wages_imputation"         = c("quelle", "cens", "wage", "wage_imp"),
  # 11_merge_BHP.do merges five files, and the columns below are everything the
  # test delivery's versions of them carry. `besch` is the one column two of the
  # files share, which is where the merges' `update` option does real work.
  # quelle rides along because only employment episodes carry an establishment
  # number, and a test has no other way to check that nothing else matched.
  "11_merge_BHP"                = c("quelle",
                                    "az_f", "az_reg", "az_azubi", "az_atz",
                                    "az_tz", "az_f_vz", "az_f_tz", "az_reg_vz",
                                    "ein_ges", "ein_gf", "ein_vz",
                                    "aus_ges", "aus_gf", "aus_vz",
                                    "eintritt", "besch", "besch_vor",
                                    "status_vor", "inflow",
                                    "austritt", "besch_nach", "status_nach",
                                    "outflow"),
  # The AKM effects themselves are fabricated noise, so the fixture is compared
  # on which episodes carry one, never on a value. See make_synth_akm.do.
  "12_merge_AKM"                = c("feff_1985_1992", "feff_1993_2000",
                                    "feff_2001_2008", "feff_2009_2016",
                                    "feff_2017_2023",
                                    "peff_1985_1992", "peff_1993_2000",
                                    "peff_2001_2008", "peff_2009_2016",
                                    "peff_2017_2023")
)

dump_step <- function(step) {
  cols <- touched[[step]]
  present <- colnames(tbl(con, "data"))
  wanted <- intersect(unique(c(key, cols)), present)
  missing_cols <- setdiff(unique(c(key, cols)), present)

  out <- file.path(dump_dir, paste0(step, ".parquet"))
  dbExecute(con, glue(
    "COPY (SELECT {paste(wanted, collapse = ', ')} FROM data) ",
    "TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)"
  ))

  n <- tbl(con, "data") |> count() |> pull(n)
  cat(sprintf("%-32s %8d rows  %2d cols%s\n", paste0(step, ".parquet"), n,
              length(wanted),
              if (length(missing_cols)) {
                paste0("  MISSING: ", paste(missing_cols, collapse = ", "))
              } else ""))
}

#====================================================================
#  Step 0, from run_testdata.R
#====================================================================

tbl(con, "orig") |>
  # 00_master_SIAB.do keeps only the employment history before it generates
  # jahr and age: `keep if inlist(quelle,1,2,3)`. Sources 4 to 7 are dropped.
  filter(quelle %in% c(1L, 2L, 3L)) |>
  mutate(year = year(begepi),
         age  = year - gebjahr) |>
  compute_and_overwrite("data")

#====================================================================
#  The steps, dumped one at a time
#====================================================================

con |> split_episodes(log_file = here("log", "01_split_episodes.log"))
dump_step("01_split_episodes")

con |> reallocate_one_time_payments(log_file = here("log", "01b_grund154.log"))
dump_step("02_grund154")

con |> generate_biographic_variables(log_file = here("log", "01_SIAB_Bio.log"))
dump_step("03_SIAB_bio")

# The occupation crosswalks have no Stata counterpart at this position: the
# reference merges them in 14_occ_blossfeld.do, long after step 09. The step is
# run anyway so the dumps come off the same pipeline run_testdata.R exercises.
con |> generate_occupation_variables(log_file = here("log", "02_occupations.log"))

con |> merge_basic_bhp(log_file = here("log", "03b_bhp_basis.log"),
                       bhp_file = testdata("SIAB_7523_v2_bhp_basis_v1.dta"))
dump_step("04_merge_basic_BHP")

con |> generate_educ_variable(log_file = here("log", "03_education.log"))
dump_step("05_educ_broad")

con |> generate_limit_assess(log_file = here("log", "04_wage_assesment_ceiling.log"))
dump_step("06_wages_assessment_ceiling")

con |> generate_limit_marginal(log_file = here("log", "05_wages_marginal.log"))
dump_step("07_wages_marginal")

con |> deflate_wages(log_file = here("log", "06_wages_deflation.log"))
dump_step("08_wages_deflation")

# 09_restrictions has no R counterpart either. The Stata fixture run takes its
# dump and then continues from the step 08 data for the same reason: the step
# imposes one project's sample cut, so the imputation is compared on the whole
# dataset instead.
dump_step("09_restrictions")

# impute_wages() draws a random term for every censored wage and sets no seed of
# its own, so two runs of the pipeline give two different wage_imp columns. The
# seed here is the dump's, not the pipeline's: it makes this file reproducible
# without changing what siab_main.R does. It does not bring the draws any closer
# to Stata's, which come from a different generator seeded inside the reference
# step, so wage_imp can only ever be compared distributionally.
set.seed(123)
con |> impute_wages(log_file = here("log", "07_wages_imputation.log"))
dump_step("10_wages_imputation")

# 11_merge_BHP.do reads the yearly establishment panel and the four extension
# files straight out of the delivery, so the R side reads the same folder the
# Stata fixture run staged its copies from.
con |> merge_annual_bhp(log_file   = here("log", "07b_bhp_annual.log"),
                        bhp_folder = testdata(""))
dump_step("11_merge_BHP")

# The AKM files are fabricated, not delivered, so both sides have to read the
# same two files or the comparison means nothing. tests/fixtures/make_synth_akm.do
# writes them into the Stata fixture run's orig folder and this reads them back.
akm_dir <- here("local_context", "stata_fixtures", "orig")
con |> merge_akm(log_file       = here("log", "07c_akm.log"),
                 akm_estab_file = file.path(akm_dir, "SIAB_7523_v2_akm_estab.dta"),
                 akm_pers_file  = file.path(akm_dir, "SIAB_7523_v2_akm_pers.dta"))
dump_step("12_merge_AKM")

#====================================================================
#  Clean up
#====================================================================

dbListTables(con) |>
  purrr::discard(~{.x %in% c("orig", "data")}) |>
  walk(~dbRemoveTable(con, .x))

cat("\nR dumps written to ", dump_dir, "\n", sep = "")
dbDisconnect(con, shutdown = TRUE)
