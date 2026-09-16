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
  "09_restrictions"             = character(0)
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

con <- dbConnect(duckdb::duckdb())
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

written <- imap(touched, function(cols, step) {
  dump <- file.path(dump_dir, paste0(step, ".dta"))
  if (!file.exists(dump)) {
    stop("No dump for ", step, " at ", dump,
         ". Run tests/fixtures/make_fixtures.do first.")
  }

  wanted <- unique(c(key, cols))
  tab <- read.dta13(dump, select.cols = wanted, convert.factors = FALSE)

  missing_cols <- setdiff(wanted, names(tab))
  if (length(missing_cols) > 0) {
    stop("Step ", step, " does not carry: ", paste(missing_cols, collapse = ", "))
  }

  tab <- tab[wanted]
  tab <- as_stata_dates(tab, step)

  if (anyDuplicated(tab[key]) > 0) {
    stop("The key persnr/spell/begepi is not unique after ", step, ".")
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
