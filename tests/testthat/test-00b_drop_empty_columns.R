# Tests for drop_empty_columns() in functions/00b_drop_empty_columns.R.
#
# 00_master_SIAB.do drops every variable that holds only missings, in a loop
# over `varlist _all` guarded by `capture assert missing(`var')`, run right
# after the source restriction.
#
# The step changes no value, only the column set, so there is nothing to compare
# against a Stata fixture and the tests assert the column set directly.
#
# The one place where the port and the reference part company is the empty
# string: Stata's missing() is true for it, DuckDB's NULL is a different value,
# and the port drops on NULL alone. That case has a test of its own.

test_that("a column that is missing throughout is dropped", {
  connection <- siab_db(data.frame(
    persnr = 1:3,
    year   = c(2000L, 2001L, 2002L),
    nvvz   = rep(NA_integer_, 3)
  ))

  quietly_run(drop_empty_columns(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(names(out), c("persnr", "year"))
  expect_equal(out$persnr, 1:3)
})

test_that("one non-missing value is enough to keep a column", {
  connection <- siab_db(data.frame(
    persnr  = 1:3,
    grund   = c(NA_integer_, NA_integer_, 154L),
    alonach = rep(NA_real_, 3)
  ))

  quietly_run(drop_empty_columns(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(names(out), c("persnr", "grund"))
  expect_equal(out$grund, c(NA_integer_, NA_integer_, 154L))
})

test_that("a column of empty strings is kept", {
  # Stata would drop this one, because missing("") is true for a string
  # variable. In the database an empty string is a value and NULL is not, and
  # the port drops on NULL alone. See the note in the function.
  connection <- siab_db(data.frame(
    persnr = 1:3,
    blank  = rep("", 3),
    gone   = rep(NA_character_, 3),
    stringsAsFactors = FALSE
  ))

  quietly_run(drop_empty_columns(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(names(out), c("persnr", "blank"))
  expect_equal(out$blank, rep("", 3))
})

test_that("the switch keeps every column", {
  connection <- siab_db(data.frame(
    persnr = 1:3,
    nvvz   = rep(NA_integer_, 3),
    blank  = rep(NA_character_, 3),
    stringsAsFactors = FALSE
  ))

  quietly_run(drop_empty_columns(connection, drop = FALSE))
  out <- siab_collect(connection, "persnr")

  expect_equal(names(out), c("persnr", "nvvz", "blank"))
})

test_that("a table with nothing to drop is left alone", {
  connection <- siab_db(data.frame(
    persnr   = 1:3,
    tentgelt = c(1.5, 2.5, 3.5)
  ))

  quietly_run(drop_empty_columns(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(names(out), c("persnr", "tentgelt"))
  expect_equal(out$tentgelt, c(1.5, 2.5, 3.5))
})

test_that("several empty columns go in one pass and the rest keep their order", {
  connection <- siab_db(data.frame(
    persnr = 1:2,
    a      = rep(NA_integer_, 2),
    year   = c(2000L, 2001L),
    b      = rep(NA_real_, 2),
    quelle = c(1L, 2L),
    c      = rep(NA_character_, 2),
    stringsAsFactors = FALSE
  ))

  quietly_run(drop_empty_columns(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(names(out), c("persnr", "year", "quelle"))
})

test_that("an empty table keeps its columns rather than losing its schema", {
  # With no rows every column would qualify, which would leave no schema at all
  # for the steps downstream. The step refuses and warns instead.
  connection <- siab_db(data.frame(
    persnr = integer(0),
    year   = integer(0)
  ))

  quietly_run(drop_empty_columns(connection))
  out <- siab_collect(connection)

  expect_equal(names(out), c("persnr", "year"))
  expect_equal(nrow(out), 0L)
})

test_that("a table that is missing throughout is refused", {
  connection <- siab_db(data.frame(
    a = rep(NA_integer_, 2),
    b = rep(NA_character_, 2),
    stringsAsFactors = FALSE
  ))

  expect_error(
    quietly_run(drop_empty_columns(connection)),
    "would leave no column at all"
  )
})

test_that("a switch that is not TRUE or FALSE is refused", {
  connection <- siab_db(data.frame(persnr = 1:2, year = c(2000L, 2001L)))

  expect_error(
    quietly_run(drop_empty_columns(connection, drop = NA)),
    "drop has to be TRUE or FALSE"
  )
})
