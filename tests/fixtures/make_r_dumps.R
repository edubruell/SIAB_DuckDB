# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#  Dump the R pipeline's state after each step, for comparison against the
#  committed Stata fixtures.
#
#  This is run_testdata.R broken open: the same steps in the same order over the
#  same test database, but with a parquet dump of the touched columns written
#  after each one. The dumps stay untracked, because they can be regenerated
#  from the test database in a few minutes. The Stata side, in
#  tests/testthat/fixtures/, is the committed half.
#
#    Rscript tests/fixtures/make_r_dumps.R
#
#  Four environment variables set the folders, each with a fallback:
#
#    SIAB_TEST_DB    the DuckDB file holding the test data
#    SIAB_TEST_DATA  the folder holding the FDZ test data
#    SIAB_R_DUMP     the folder these dumps are written to. The comparison
#                    tests read the same variable, through
#                    tests/testthat/helper-siab.R, so set it for both or
#                    neither.
#    SIAB_AKM_DIR    the folder holding the two fabricated AKM files. The
#                    Python dump writer reads the same variable, and both
#                    arms have to read the same two files or the comparison
#                    means nothing.
#
#  The comparison tests in tests/testthat/test-reference-*.R skip when these
#  dumps are absent, so the fast synthetic suite still runs without them.
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library("pacman")
p_load(dplyr, dbplyr, readr, tidyr, purrr, duckdb, stringr, glue, scales,
       data.table, readstata13, here, logger, survival)

here("R", "functions") |>
  dir() |>
  walk(~source(here("R", "functions", .x)))

db_file <- Sys.getenv(
  "SIAB_TEST_DB",
  here("local_context", "testdb", "siab_test.duckdb")
)
testdata <- folder_reference_factory(Sys.getenv(
  "SIAB_TEST_DATA",
  here("local_context", "testdata", "siab_7523_v2")
))
dump_dir <- Sys.getenv("SIAB_R_DUMP", here("local_context", "testdb", "r_dump"))
dir.create(dump_dir, showWarnings = FALSE, recursive = TRUE)

if (!file.exists(db_file)) {
  stop("No test database at ", db_file,
       ". Write one with R/stata_to_db_batch_read.R, or set SIAB_TEST_DB.")
}

con <- siab_connect(db_file)
if (!"orig" %in% dbListTables(con)) {
  stop("The test database has no `orig` table: ", db_file)
}

# The same per-step column lists make_fixtures.R uses, with the
# R names where they differ from the Stata ones. `jahr` is `year` in the port.
key <- c("persnr", "spell", "begepi")

# The two late steps that drop part of the key. The Stata side drops `spell` in
# 15_parallel_episodes.do and `begepi` in 16_yearly_panel.do; the R port keeps
# the columns, so the narrower key here is what makes the two halves joinable.
step_key <- list(
  "15_parallel_episodes" = c("persnr", "begepi"),
  "16_yearly_panel"      = c("persnr", "year"),
  "16_monthly_panel"     = c("persnr", "year", "begepi_monthly")
)

key_for <- function(step) if (is.null(step_key[[step]])) key else step_key[[step]]

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
                                    "peff_2017_2023"),
  # 13_industries_1digit.do maps the time-consistent three-digit industry to two
  # one-digit codes. The port builds both in generate_industry_variables(),
  # directly after the basic BHP merge that brings w93_3_gen in; the dump is
  # taken here, where the reference creates the columns.
  "13_industries_1digit"        = c("w93_3_gen", "industry1_destatis",
                                    "industry1_estpanel"),
  # The R port reaches occ_blo in generate_occupation_variables(), which runs
  # far earlier than the reference's 14_occ_blossfeld.do; the column is dumped
  # here, at the position the reference creates it, so the two are comparable.
  "14_occ_blossfeld"            = c("beruf", "occ_blo"),
  "15_parallel_episodes"        = c("quelle", "tage_bet", "wage_imp", "nspell",
                                    "parallel_jobs", "parallel_wage",
                                    "parallel_wage_imp", "parallel_benefits"),
  # begepi and endepi have no Stata counterpart here: 16_yearly_panel.do drops
  # both and the port keeps them. They ride along so a test can check that every
  # surviving episode really does cover the cutoff date.
  "16_yearly_panel"             = c("quelle", "erwstat", "parallel_benefits",
                                    "year_days_emp", "year_days_benefits",
                                    "year_labor_earn",
                                    "tage_bet", "tage_job", "tage_erw",
                                    "tage_lst", "begepi", "endepi"),
  # The monthly panel is an alternative to the yearly one, so its dump is taken
  # from a second run over the same step 15 data. `year` carries the reference's
  # `jahr` and its `year` at once: the reference generates a second year column
  # from the month, and after 01_split_episodes.do no episode crosses a year
  # boundary, so the two agree row by row.
  "16_monthly_panel"            = c("quelle", "erwstat", "parallel_benefits",
                                    "year_days_emp", "year_days_benefits",
                                    "year_labor_earn",
                                    "tage_bet", "tage_job", "tage_erw",
                                    "tage_lst",
                                    "month", "month_num", "endepi_monthly",
                                    "begepi", "endepi")
)

dump_step <- function(step) {
  cols <- touched[[step]]
  present <- colnames(tbl(con, "data"))
  wanted <- intersect(unique(c(key_for(step), cols)), present)
  missing_cols <- setdiff(unique(c(key_for(step), cols)), present)

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

# The master drops every variable that holds only missings here, and
# make_fixtures.do keeps that position, so the R side does it too. It can only
# drop a subset of what the reference drops, because Stata counts the empty
# string as missing and this port counts only NULL, so no compared column can
# go missing on one side alone.
con |> drop_empty_columns(log_file = here("log", "00b_drop_empty_columns.log"))

con |> split_episodes(log_file = here("log", "01_split_episodes.log"))
dump_step("01_split_episodes")

con |> reallocate_one_time_payments(log_file = here("log", "01b_grund154.log"))
dump_step("02_grund154")

con |> generate_biographic_variables(log_file = here("log", "01_SIAB_Bio.log"))
dump_step("03_SIAB_bio")

# The master restricts to the observation period here, between 03_SIAB_bio.do
# and 04_merge_basic_BHP.do, and make_fixtures.do keeps that position. With the
# full span of the delivery it drops nothing, so the dumps stay comparable.
con |> restrict_observation_period(min_year = 1975, max_year = 2023,
                                   log_file = here("log", "03d_observation_period.log"))

# The occupation crosswalks have no Stata counterpart at this position: the
# reference merges them in 14_occ_blossfeld.do, long after step 09. The step is
# run anyway so the dumps come off the same pipeline run_testdata.R exercises.
con |> generate_occupation_variables(log_file = here("log", "02_occupations.log"))

con |> merge_basic_bhp(log_file = here("log", "03b_bhp_basis.log"),
                       bhp_file = testdata("SIAB_7523_v2_bhp_basis_v1.dta"))
dump_step("04_merge_basic_BHP")

# The industry mappings have no Stata counterpart at this position either: the
# reference builds them in 13_industries_1digit.do, after the AKM merge. The
# step runs here because w93_3_gen arrives with the merge above.
con |> generate_industry_variables(log_file = here("log", "03c_industries.log"))

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
# same two files or the comparison means nothing. make_synth_akm.do
# writes them into the Stata fixture run's orig folder and this reads them back.
akm_dir <- Sys.getenv(
  "SIAB_AKM_DIR",
  here("local_context", "stata_fixtures", "orig")
)
con |> merge_akm(log_file       = here("log", "07c_akm.log"),
                 akm_estab_file = file.path(akm_dir, "SIAB_7523_v2_akm_estab.dta"),
                 akm_pers_file  = file.path(akm_dir, "SIAB_7523_v2_akm_pers.dta"))
dump_step("12_merge_AKM")

# Both one-digit industries were generated far earlier, by
# generate_industry_variables() after the basic BHP merge, and so were the
# Blossfeld occupations, by generate_occupation_variables(). Nothing between
# those positions and this one touches w93_3_gen or beruf, so the dumps are
# taken here, at the point of the chain the reference creates each column.
dump_step("13_industries_1digit")
dump_step("14_occ_blossfeld")

# The reference's uncommented rule defines the main episode as the job with the
# longest tenure, using the imputed wage only to break a tie. handling = "wage",
# which run_testdata.R passes, sorts on the imputed wage first; those draws
# differ between R and Stata by construction, so under that setting the two
# sides would keep different episodes and nothing downstream would compare.
con |> handle_parallel_episodes(log_file = here("log", "08_parallel_episodes.log"),
                                handling = "tenure")
dump_step("15_parallel_episodes")

# 16_yearly_panel.do and 16_monthly_panel.do are alternatives: both start from
# the step 15 data and the reference master calls neither. The step 15 state is
# therefore kept aside here, so the monthly panel can be built from the same
# input the yearly one was, exactly as make_fixtures.do reloads
# the step 15 dump for it.
dbExecute(con, "CREATE TABLE parallel_episodes AS SELECT * FROM data")

con |> build_yearly_panel(log_file     = here("log", "09_yearly_panel.log"),
                          cutoff_month = 6,
                          cutoff_day   = 30)
dump_step("16_yearly_panel")

dbExecute(con, "DROP TABLE data")
dbExecute(con, "ALTER TABLE parallel_episodes RENAME TO data")

# The reference hardcodes the 15th of the month as the cutoff, which is the
# port's default.
con |> build_monthly_panel(log_file   = here("log", "09b_monthly_panel.log"),
                           cutoff_day = 15)
dump_step("16_monthly_panel")

#====================================================================
#  Clean up
#====================================================================

dbListTables(con) |>
  purrr::discard(~{.x %in% c("orig", "data")}) |>
  walk(~dbRemoveTable(con, .x))

cat("\nR dumps written to ", dump_dir, "\n", sep = "")
dbDisconnect(con, shutdown = TRUE)
