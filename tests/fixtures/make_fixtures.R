# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#  Turn the Stata reference dumps into the committed parquet fixtures.
#
#  tests/fixtures/make_fixtures.do runs the unmodified reference preparation
#  over the FDZ test data and saves the whole dataset after each step into
#  local_context/stata_fixtures/dump/. Those dumps are 40 to 100 MB each and
#  stay untracked. This script cuts each one down to the key plus the columns
#  that step actually writes, and puts the result in tests/testthat/fixtures/,
#  which is committed.
#
#    Rscript tests/fixtures/make_fixtures.R
#
#  Naming steps converts only those, which is what you want after extending the
#  do-file by one step: the fixtures already committed are left alone.
#
#    Rscript tests/fixtures/make_fixtures.R 10_wages_imputation
#
#  Parquet is written through DuckDB rather than arrow, so the test suite adds
#  no runtime dependency beyond the ones the preparation already uses.
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

suppressPackageStartupMessages({
  library("dplyr")
  library("duckdb")
  library("readstata13")
  library("here")
  library("purrr")
})

dump_dir    <- here("local_context", "stata_fixtures", "dump")
fixture_dir <- here("tests", "testthat", "fixtures")
dir.create(fixture_dir, showWarnings = FALSE, recursive = TRUE)

# A row is identified by the person, the spell counter and the episode start.
# persnr and spell alone stop being unique at step 01, which cuts a spell that
# runs over a year boundary into one row per calendar year.
key <- c("persnr", "spell", "begepi")

# Two late steps drop part of that key. 15_parallel_episodes.do keeps one
# episode per person and episode start and drops `spell` on its way out;
# 16_yearly_panel.do then keeps one episode per person and year and drops
# `begepi` as well. Each is still unique on what is left, and the check below
# proves it on every conversion.
step_key <- list(
  "15_parallel_episodes" = c("persnr", "begepi"),
  "16_yearly_panel"      = c("persnr", "jahr")
)

key_for <- function(step) if (is.null(step_key[[step]])) key else step_key[[step]]

# The columns each step writes or changes, in the order the reference creates
# them. A step is compared on these and on the key, not on the columns it
# carries through untouched.
touched <- list(
  "01_split_episodes"           = c("begepi", "endepi", "begepi_orig",
                                    "endepi_orig", "jahr", "age"),
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
  # 09 removes rows rather than adding columns, so the key is the fixture.
  "09_restrictions"             = character(0),
  # 10_wages_imputation.do drops every intermediate it builds before it ends,
  # so the three variables its own header names are all that survive into the
  # dump. quelle rides along because each of the step's guards is `quelle == 1`
  # and a test has no other way to tell a BeH spell from the rest.
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
  ,
  # 13_industries_1digit.do maps the time-consistent three-digit industry to two
  # one-digit codes. The port builds both in generate_industry_variables(),
  # right after the basic BHP merge that brings w93_3_gen in.
  "13_industries_1digit"        = c("w93_3_gen", "industry1_destatis",
                                    "industry1_estpanel"),
  # 14_occ_blossfeld.do recodes `beruf` into the Blossfeld classification. The R
  # port does this far earlier, in generate_occupation_variables(), so the two
  # sides reach the same column from different positions in the pipeline.
  "14_occ_blossfeld"            = c("beruf", "occ_blo"),
  # 15_parallel_episodes.do keeps the main episode and aggregates over the
  # parallel ones it drops. parallel_wage_imp sums the imputed wage, whose draws
  # differ between the two sides by construction, so it is the one column of the
  # step compared as a distribution rather than row by row.
  "15_parallel_episodes"        = c("quelle", "tage_bet", "wage_imp", "nspell",
                                    "parallel_jobs", "parallel_wage",
                                    "parallel_wage_imp", "parallel_benefits"),
  # 16_yearly_panel.do keeps the episode covering 30 June of each year, totals
  # the days and earnings over the whole year, and trims the four duration
  # counters at the cutoff. year_labor_earn is built from parallel_wage_imp and
  # inherits its draws.
  "16_yearly_panel"             = c("quelle", "erwstat", "parallel_benefits",
                                    "year_days_emp", "year_days_benefits",
                                    "year_labor_earn",
                                    "tage_bet", "tage_job", "tage_erw",
                                    "tage_lst")
)


# Stata's extended missings (.a to .z) do not survive the trip through
# readstata13 cleanly. In a variable the reference formatted as a date they come
# back as an integer NA that the Date conversion then reads as a day count,
# which is where the -5877641-06-23 in the first round of fixtures came from.
# Anything outside the SIAB's own range is one of those, and becomes NA here.
#
# ein_job is the other way round: 03_SIAB_bio.do only formats it as a date
# inside a block that is commented out, so it arrives as a plain count of days
# since 1960-01-01 and has to be converted by hand. Without this the column
# cannot be compared against the R port at all.
stata_date_columns <- list("03_SIAB_bio" = "ein_job")

as_stata_dates <- function(tab, step) {
  for (col in intersect(stata_date_columns[[step]], names(tab))) {
    tab[[col]] <- as.Date(tab[[col]], origin = "1960-01-01")
  }
  for (col in names(tab)) {
    if (!inherits(tab[[col]], "Date")) next
    out_of_range <- !is.na(tab[[col]]) &
      (tab[[col]] < as.Date("1900-01-01") | tab[[col]] > as.Date("2100-01-01"))
    tab[[col]][out_of_range] <- NA
  }
  tab
}

requested <- commandArgs(trailingOnly = TRUE)
if (length(requested) > 0) {
  unknown <- setdiff(requested, names(touched))
  if (length(unknown) > 0) {
    stop("No such step: ", paste(unknown, collapse = ", "))
  }
  touched <- touched[requested]
}

con <- dbConnect(duckdb::duckdb())
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

written <- imap(touched, function(cols, step) {
  dump <- file.path(dump_dir, paste0(step, ".dta"))
  if (!file.exists(dump)) {
    stop("No dump for ", step, " at ", dump,
         ". Run tests/fixtures/make_fixtures.do first.")
  }

  step_keys <- key_for(step)
  wanted <- unique(c(step_keys, cols))
  tab <- read.dta13(dump, select.cols = wanted, convert.factors = FALSE)

  missing_cols <- setdiff(wanted, names(tab))
  if (length(missing_cols) > 0) {
    stop("Step ", step, " does not carry: ", paste(missing_cols, collapse = ", "))
  }

  tab <- tab[wanted]
  tab <- as_stata_dates(tab, step)

  if (anyDuplicated(tab[step_keys]) > 0) {
    stop("The key ", paste(step_keys, collapse = "/"),
         " is not unique after ", step, ".")
  }

  out <- file.path(fixture_dir, paste0(step, ".parquet"))
  duckdb_register(con, "fixture", tab)
  dbExecute(con, paste0(
    "COPY fixture TO '", out, "' (FORMAT PARQUET, COMPRESSION ZSTD)"
  ))
  duckdb_unregister(con, "fixture")

  cat(sprintf("%-32s %8d rows  %2d cols  %6.1f MB\n",
              basename(out), nrow(tab), ncol(tab),
              file.size(out) / 1024^2))
  out
})

cat("\n", length(written), " fixtures written to tests/testthat/fixtures/\n", sep = "")
