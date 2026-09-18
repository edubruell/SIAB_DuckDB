# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#  Test runner for SIAB_DuckDB
#
#  This repo is a collection of preparation scripts, not an R package, so the
#  tests are run with test_dir() over tests/testthat rather than with
#  devtools::test(). Everything under tests/testthat/helper-*.R is sourced
#  first; that is where R/functions/ gets loaded and where the in-memory test
#  database is built.
#
#  Run the whole suite from the project root:
#
#    Rscript tests/testthat.R
#
#  Or, interactively, a single file:
#
#    testthat::test_file("tests/testthat/test-06_wages_deflation.R")
#
#  The suite needs no FDZ test data and no Stata installation. Tests that
#  compare against a committed Stata fixture skip themselves when the fixture
#  is absent.
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library("testthat")
library("here")

testthat::test_dir(
  here::here("tests", "testthat"),
  stop_on_failure = TRUE
)
