# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#  The cast plan of the R read-in.
#
#  R/stata_to_db_batch_read.R is the step before the pipeline: it turns a SIAB
#  delivery into the `orig` table every runner expects to find. How wide that
#  table is decides what the whole arm costs per row, so what each Stata storage
#  type becomes in DuckDB is worth a test of its own. An R integer is 32 bits
#  whatever the delivery declared, which is why the table is created from the
#  .dta header rather than from the first batch.
#
#  The file is a script rather than a set of functions in R/functions/, so the
#  definitions are read out of it up to the line that runs it. The fixture is
#  written here with readstata13 and needs neither the FDZ data nor Stata.
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library("readstata13")

# Read the script's definitions without its bottom-line call, which names a
# delivery this project does not carry.
read_in_environment <- function() {
  folder <- withr::local_tempdir(.local_envir = parent.frame())
  withr::local_envvar(c(SIAB_DB_FOLDER = folder, SIAB_RAW_FOLDER = folder),
                      .local_envir = parent.frame())

  lines <- readLines(here("R", "stata_to_db_batch_read.R"), warn = FALSE)
  driver <- grep("^#Convert a SIAB SUF to a duckdb database object", lines)
  expect_length(driver, 1)

  where <- new.env(parent = globalenv())
  eval(parse(text = paste(lines[seq_len(driver - 1)], collapse = "\n")),
       envir = where)
  where$.folder <- folder
  where
}

# A stand-in delivery: `compress = TRUE` makes readstata13 pick the smallest
# type each column fits, which is what a real delivery carries.
write_fixture <- function(folder) {
  frame <- data.frame(
    persnr_siab = c(1L, 1L, 2L),
    spell       = c(1L, 2L, 1L),
    gebjahr     = c(1955L, 1955L, 1972L),
    begepi      = as.Date(c("1992-01-01", "1993-01-01", "1992-01-01")),
    tentgelt    = c(100.5, 110.25, 50)
  )
  path <- file.path(folder, "fixture.dta")
  save.dta13(frame, path, compress = TRUE)
  path
}


test_that("each Stata storage type becomes the DuckDB type of the same width", {
  where <- read_in_environment()
  types <- where$column_types(write_fixture(where$.folder))

  # A Stata byte, an int and a double, at the widths the file declares them.
  expect_equal(unname(types[["spell"]]), "TINYINT")
  expect_equal(unname(types[["gebjahr"]]), "SMALLINT")
  expect_equal(unname(types[["tentgelt"]]), "DOUBLE")
})


test_that("a column displayed as a date is a date whatever it is stored as", {
  where <- read_in_environment()
  types <- where$column_types(write_fixture(where$.folder))

  # readstata13 stores this one as a double and marks it %td. The display
  # format is the only thing that says a column is a date.
  expect_equal(unname(types[["begepi"]]), "DATE")
})


test_that("the plan carries the pipeline's names for the two keys", {
  where <- read_in_environment()
  types <- where$column_types(write_fixture(where$.folder))

  expect_true("persnr" %in% names(types))
  expect_false("persnr_siab" %in% names(types))
})


test_that("the mapping covers every numeric type a .dta can declare", {
  where <- read_in_environment()

  # Stata's five numeric storage types, by the codes the .dta header carries.
  expect_setequal(names(where$stata_storage_types),
                  c("65526", "65527", "65528", "65529", "65530"))
})


test_that("an unknown storage type stops the read-in rather than guessing", {
  where <- read_in_environment()
  path <- write_fixture(where$.folder)

  # A string variable is the case a delivery does not have and the mapping does
  # not cover, so it stands in for a header this reader cannot read.
  frame <- read.dta13(path, convert.factors = FALSE)
  attr(frame, "var.labels") <- NULL
  frame$note <- c("a", "b", "c")
  with_string <- file.path(where$.folder, "with_string.dta")
  save.dta13(frame, with_string, compress = TRUE)

  expect_error(where$column_types(with_string), "Unknown Stata storage type")
})


test_that("the orig table is created at the widths the delivery declared", {
  where <- read_in_environment()
  path <- write_fixture(where$.folder)

  con <- siab_connect(file.path(where$.folder, "created.duckdb"))
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  where$create_orig_table(con, where$column_types(path))

  schema <- dbGetQuery(con, paste(
    "SELECT column_name, data_type FROM information_schema.columns",
    "WHERE table_name = 'orig' ORDER BY ordinal_position"))
  widths <- setNames(schema$data_type, schema$column_name)

  expect_equal(unname(widths[["spell"]]), "TINYINT")
  expect_equal(unname(widths[["gebjahr"]]), "SMALLINT")
  expect_equal(unname(widths[["begepi"]]), "DATE")
  expect_equal(unname(widths[["persnr"]]), "TINYINT")
  # The batch number the reader adds is the one column no delivery carries.
  expect_equal(unname(widths[["pn_batch"]]), "INTEGER")
})


test_that("creating the table twice leaves one table and not two columns of it", {
  where <- read_in_environment()
  types <- where$column_types(write_fixture(where$.folder))

  con <- siab_connect(file.path(where$.folder, "twice.duckdb"))
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  where$create_orig_table(con, types)
  where$create_orig_table(con, types)

  expect_equal(dbGetQuery(con, "SELECT count(*) AS n FROM orig")$n, 0)
  expect_equal(
    dbGetQuery(con, paste("SELECT count(*) AS n FROM information_schema.columns",
                          "WHERE table_name = 'orig'"))$n,
    length(types) + 1
  )
})
