# Tests for split_episodes() in R/functions/01_split_episodes.R.
#
# A SIAB spell can run over a year boundary. The step cuts every such spell
# into one row per calendar year, keeping the original dates in begepi_orig
# and endepi_orig, and recomputes year and age on the cut pieces. This is the
# port of 01_split_episodes.do.

spell_frame <- function(...) {
  data.frame(..., stringsAsFactors = FALSE)
}

test_that("a spell inside one calendar year is left alone", {
  connection <- siab_db(spell_frame(
    persnr  = 1L,
    spell   = 1L,
    begepi  = as.Date("2000-03-01"),
    endepi  = as.Date("2000-09-30"),
    gebjahr = 1970L,
    year    = 2000L,
    age     = 30L
  ))

  quietly_run(split_episodes(connection))
  out <- siab_collect(connection)

  expect_equal(nrow(out), 1L)
  expect_equal(out$begepi, as.Date("2000-03-01"))
  expect_equal(out$endepi, as.Date("2000-09-30"))
  expect_equal(out$begepi_orig, as.Date("2000-03-01"))
  expect_equal(out$endepi_orig, as.Date("2000-09-30"))
})

test_that("a spell over three calendar years becomes three rows cut at the year boundaries", {
  connection <- siab_db(spell_frame(
    persnr  = 1L,
    spell   = 1L,
    begepi  = as.Date("1999-06-01"),
    endepi  = as.Date("2001-03-15"),
    gebjahr = 1970L,
    year    = 1999L,
    age     = 29L
  ))

  quietly_run(split_episodes(connection))
  out <- siab_collect(connection, "begepi")

  expect_equal(nrow(out), 3L)
  expect_equal(out$begepi, as.Date(c("1999-06-01", "2000-01-01", "2001-01-01")))
  expect_equal(out$endepi, as.Date(c("1999-12-31", "2000-12-31", "2001-03-15")))
})

test_that("the original dates are carried on every piece of a split spell", {
  connection <- siab_db(spell_frame(
    persnr  = 1L,
    spell   = 1L,
    begepi  = as.Date("1999-06-01"),
    endepi  = as.Date("2001-03-15"),
    gebjahr = 1970L,
    year    = 1999L,
    age     = 29L
  ))

  quietly_run(split_episodes(connection))
  out <- siab_collect(connection, "begepi")

  expect_equal(out$begepi_orig, rep(as.Date("1999-06-01"), 3))
  expect_equal(out$endepi_orig, rep(as.Date("2001-03-15"), 3))
})

test_that("year and age are recomputed on each piece", {
  connection <- siab_db(spell_frame(
    persnr  = 1L,
    spell   = 1L,
    begepi  = as.Date("1999-06-01"),
    endepi  = as.Date("2001-03-15"),
    gebjahr = 1970L,
    year    = 1999L,
    age     = 29L
  ))

  quietly_run(split_episodes(connection))
  out <- siab_collect(connection, "begepi")

  expect_equal(out$year, c(1999L, 2000L, 2001L))
  expect_equal(out$age, c(29, 30, 31))
})

test_that("a spell running exactly to 31 December is not split further", {
  connection <- siab_db(spell_frame(
    persnr  = 1L,
    spell   = 1L,
    begepi  = as.Date("2000-01-01"),
    endepi  = as.Date("2000-12-31"),
    gebjahr = 1970L,
    year    = 2000L,
    age     = 30L
  ))

  quietly_run(split_episodes(connection))
  out <- siab_collect(connection)

  expect_equal(nrow(out), 1L)
  expect_equal(out$begepi, as.Date("2000-01-01"))
  expect_equal(out$endepi, as.Date("2000-12-31"))
})

test_that("splitting happens per spell and leaves the helper columns out of the result", {
  connection <- siab_db(spell_frame(
    persnr  = c(1L, 1L, 2L),
    spell   = c(1L, 2L, 1L),
    begepi  = as.Date(c("2000-01-01", "2000-11-01", "1998-05-01")),
    endepi  = as.Date(c("2000-10-31", "2001-04-30", "1998-06-30")),
    gebjahr = c(1970L, 1970L, 1960L),
    year    = c(2000L, 2000L, 1998L),
    age     = c(30L, 30L, 38L)
  ))

  quietly_run(split_episodes(connection))
  out <- siab_collect(connection, c("persnr", "spell", "begepi"))

  # One row for spell 1, two for spell 2, one for person 2.
  expect_equal(nrow(out), 4L)
  expect_equal(out$persnr, c(1L, 1L, 1L, 2L))
  expect_equal(out$spell, c(1L, 2L, 2L, 1L))
  expect_false(any(c("span_year", "year_instance") %in% names(out)))
})

test_that("the expansion plan is dropped from the database again", {
  connection <- siab_db(spell_frame(
    persnr  = 1L, spell = 1L,
    begepi  = as.Date("2000-01-01"), endepi = as.Date("2000-12-31"),
    gebjahr = 1970L, year = 2000L, age = 30L
  ))

  quietly_run(split_episodes(connection))

  expect_false("expansion_plan" %in% DBI::dbListTables(connection))
})
