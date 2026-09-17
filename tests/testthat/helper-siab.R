# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#  Shared setup for the test suite.
#
#  Sourced by testthat before any test-*.R file. It loads the packages the
#  preparation steps need, sources functions/ the same way siab_main.R does,
#  and defines the helpers that give a step function a small in-memory DuckDB
#  to work on.
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

suppressPackageStartupMessages({
  library("dplyr")
  library("dbplyr")
  library("readr")
  library("tidyr")
  library("purrr")
  library("duckdb")
  library("stringr")
  library("glue")
  library("here")
  library("logger")
})

# The step functions are sourced into the global environment, exactly as
# siab_main.R and run_testdata.R do it.
here("functions") |>
  dir() |>
  purrr::walk(~source(here("functions", .x)))

#====================================================================
#  A disposable database for one test
#====================================================================

# compute_and_overwrite() reads the connection from a variable called `con`
# in the global environment rather than from an argument. A test therefore
# has to put its own connection there and take it away again afterwards.
# That coupling is a known wart in functions/00_common_functions.R; the helper
# works around it instead of hiding it.

siab_db <- function(data, env = parent.frame()) {
  connection <- DBI::dbConnect(duckdb::duckdb())

  DBI::dbWriteTable(connection, "data", as.data.frame(data))

  had_con <- exists("con", envir = globalenv(), inherits = FALSE)
  old_con <- if (had_con) get("con", envir = globalenv()) else NULL
  assign("con", connection, envir = globalenv())

  withr::defer(
    {
      if (had_con) {
        assign("con", old_con, envir = globalenv())
      } else {
        rm("con", envir = globalenv())
      }
      DBI::dbDisconnect(connection, shutdown = TRUE)
    },
    envir = env
  )

  connection
}

# Pull the `data` table back into R. Steps write in whatever row order DuckDB
# finds convenient, so every test sorts before it compares.
siab_collect <- function(connection, order_by = NULL) {
  out <- dplyr::collect(dplyr::tbl(connection, "data"))
  if (!is.null(order_by)) {
    out <- out[do.call(order, unname(as.list(out[order_by]))), , drop = FALSE]
  }
  out
}

# The step functions log to the console on every call. Tests care about the
# table they write, not the chatter, so run them through this.
quietly_run <- function(expr) {
  out <- NULL
  invisible(utils::capture.output(
    invisible(utils::capture.output(
      out <- suppressMessages(suppressWarnings(force(expr))),
      type = "message"
    )),
    type = "output"
  ))
  out
}

#====================================================================
#  Stata fixtures
#====================================================================

# Fixtures are parquet dumps taken from the Stata reference prep, one per
# step, committed under tests/testthat/fixtures/. The scripts that produce them
# need Stata and the reference do-files and are not part of this repo, which
# tests/README.md explains. A test that needs a fixture calls this; when the
# file is not there the test skips rather than fails.
siab_fixture <- function(name) {
  path <- here("tests", "testthat", "fixtures", paste0(name, ".parquet"))
  testthat::skip_if_not(
    file.exists(path),
    paste0("Stata fixture not present: tests/testthat/fixtures/", name, ".parquet")
  )
  connection <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)
  DBI::dbGetQuery(
    connection,
    paste0("SELECT * FROM read_parquet('", path, "')")
  ) |>
    dplyr::as_tibble()
}

#====================================================================
#  Comparing a pipeline step against the Stata reference
#====================================================================

# The Stata half of a comparison is a parquet file under
# tests/testthat/fixtures/, committed.
# The R half is a parquet file in the folder SIAB_R_DUMP names, untracked,
# produced by tests/fixtures/make_r_dumps.R over the FDZ test database. That
# script reads the same variable, so set it for both or neither. Both halves are
# keyed on persnr, spell and begepi.

siab_reference_paths <- function(step) {
  list(
    stata = here("tests", "testthat", "fixtures", paste0(step, ".parquet")),
    r     = file.path(
      Sys.getenv("SIAB_R_DUMP", here("local_context", "testdb", "r_dump")),
      paste0(step, ".parquet")
    )
  )
}

# Open a DuckDB connection over both halves of one step and hand back a
# function that runs SQL against the views `stata` and `r`. Skips the test when
# either half is missing.
# `also` names further steps to expose, each as two more views, prefixed with
# the name given. A test that has to hold one step's mismatches apart from an
# earlier step's uses it: passing also = c(wage = "02_grund154") adds the views
# wage_stata and wage_r.
siab_reference_query <- function(step, also = character(0), env = parent.frame()) {
  paths <- siab_reference_paths(step)

  testthat::skip_if_not(
    file.exists(paths$stata),
    paste0("No Stata fixture for ", step,
           ". The committed fixtures are made outside this repo, see tests/README.md.")
  )
  testthat::skip_if_not(
    file.exists(paths$r),
    paste0("No R dump for ", step,
           ". Produce one with tests/fixtures/make_r_dumps.R.")
  )

  connection <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(connection, shutdown = TRUE), envir = env)

  DBI::dbExecute(connection, paste0(
    "CREATE VIEW stata AS SELECT * FROM read_parquet('", paths$stata, "')"))
  DBI::dbExecute(connection, paste0(
    "CREATE VIEW r AS SELECT * FROM read_parquet('", paths$r, "')"))

  for (prefix in names(also)) {
    extra <- siab_reference_paths(also[[prefix]])
    testthat::skip_if_not(
      file.exists(extra$stata) && file.exists(extra$r),
      paste0("No fixture pair for ", also[[prefix]], "."))
    DBI::dbExecute(connection, paste0(
      "CREATE VIEW ", prefix, "_stata AS SELECT * FROM read_parquet('",
      extra$stata, "')"))
    DBI::dbExecute(connection, paste0(
      "CREATE VIEW ", prefix, "_r AS SELECT * FROM read_parquet('",
      extra$r, "')"))
  }

  function(sql) DBI::dbGetQuery(connection, sql)
}

# How many rows of one column disagree between the two halves, over the keys
# they share. `tolerance` is a relative tolerance; NULL means exact equality.
# The reference stores most generated variables as Stata `float`, which carries
# about seven decimal digits, so a variable that is stored rather than recoded
# needs a tolerance even when the arithmetic is identical.
# `key` is the column set the two halves join on. It defaults to the key every
# step up to 12 shares. 15_parallel_episodes.do drops `spell` and
# 16_yearly_panel.do drops `begepi`, so tests for those pass a narrower one.
# Give it as a named vector where the two sides call a key column differently:
# the name is the Stata column, the value the R one, as in c(persnr = "persnr",
# jahr = "year") for the yearly panel.
siab_key_on <- function(key) {
  stata_names <- if (is.null(names(key))) key else names(key)
  paste(
    paste0("stata.", stata_names, " = r.", unname(key)),
    collapse = " AND "
  )
}

siab_column_diff <- function(query, column, r_column = column, tolerance = NULL,
                             key = c("persnr", "spell", "begepi")) {
  predicate <- if (is.null(tolerance)) {
    paste0("stata.", column, " IS DISTINCT FROM r.", r_column)
  } else {
    paste0(
      "(stata.", column, " IS NULL) <> (r.", r_column, " IS NULL) OR ",
      "abs(stata.", column, " - r.", r_column, ") > ",
      format(tolerance, scientific = FALSE), " * greatest(abs(stata.", column, "), 1e-12)"
    )
  }

  query(paste0(
    "SELECT count(*) AS shared, ",
    "       count(*) FILTER (WHERE ", predicate, ") AS differing ",
    "FROM stata JOIN r ON ", siab_key_on(key)
  ))
}

# The same comparison for a column whose two sides cannot agree row by row,
# because it is built from the imputed wage and both sides draw their own random
# terms. Returns the mean and the two quartiles of each half over the shared
# keys, so a test can bound the gap instead of demanding equality.
siab_column_moments <- function(query, column, r_column = column,
                                key = c("persnr", "spell", "begepi")) {
  query(paste0(
    "SELECT count(*) AS shared, ",
    "       avg(stata.", column, ") AS stata_mean, ",
    "       avg(r.", r_column, ") AS r_mean, ",
    "       quantile_cont(stata.", column, ", 0.25) AS stata_q25, ",
    "       quantile_cont(r.", r_column, ", 0.25) AS r_q25, ",
    "       quantile_cont(stata.", column, ", 0.75) AS stata_q75, ",
    "       quantile_cont(r.", r_column, ", 0.75) AS r_q75 ",
    "FROM stata JOIN r ON ", siab_key_on(key)
  ))
}

#====================================================================
#  Lookup tables as the steps see them
#====================================================================

# The step functions round each statutory table to Stata float precision on
# read-in, so a test that predicts an expected value has to read it the same
# way. `value` names the column holding the figure.
siab_lookup <- function(file, value) {
  readr::read_csv(here("classifications", file), show_col_types = FALSE) |>
    dplyr::mutate(dplyr::across(dplyr::all_of(value), stata_float))
}
