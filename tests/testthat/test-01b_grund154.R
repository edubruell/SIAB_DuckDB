# Tests for reallocate_one_time_payments() in functions/01b_grund154.R.
#
# The step ports 02_grund154.do. A spell with deregistration reason 154 holds a
# one-time payment reported apart from the employment spell it belongs to. The
# step sums those payments per person, establishment and year, drops the 154
# spells, and spreads the money over the remaining spells of the same
# combination in proportion to their duration.

spell_frame <- function(...) {
  out <- data.frame(...)
  out$spell <- seq_len(nrow(out))
  out
}

test_that("a one-time payment is spread over the year in proportion to duration", {
  # One person at one establishment: a 100-day spell at 50 euro a day, a 50-day
  # spell at 50 euro a day, and a 154 spell carrying 10 days at 30 euro, so 300
  # euro to reallocate. Total remaining duration is 150 days, so every day of
  # either spell gains 300 / 150 = 2 euro.
  connection <- siab_db(spell_frame(
    persnr   = c(1L, 1L, 1L),
    betnr    = c(7L, 7L, 7L),
    year     = c(2000L, 2000L, 2000L),
    grund    = c(30L, 30L, 154L),
    begepi   = as.Date(c("2000-01-01", "2000-06-01", "2000-12-01")),
    endepi   = as.Date(c("2000-04-09", "2000-07-20", "2000-12-10")),
    tentgelt = c(50, 50, 30)
  ))

  quietly_run(reallocate_one_time_payments(connection))
  out <- siab_collect(connection, "spell")

  expect_equal(nrow(out), 2L)
  expect_equal(out$tentgelt, c(52, 52))
  expect_false("tentgelt154" %in% names(out))
  expect_false("episode_length" %in% names(out))
})

test_that("the reallocation stays inside one person, establishment and year", {
  # The same person at two establishments in the same year, and one payment at
  # the first. Only the first establishment's spell may gain.
  connection <- siab_db(spell_frame(
    persnr   = c(1L, 1L, 1L),
    betnr    = c(7L, 8L, 7L),
    year     = c(2000L, 2000L, 2000L),
    grund    = c(30L, 30L, 154L),
    begepi   = as.Date(c("2000-01-01", "2000-01-01", "2000-12-01")),
    endepi   = as.Date(c("2000-01-10", "2000-01-10", "2000-12-10")),
    tentgelt = c(50, 50, 10)
  ))

  quietly_run(reallocate_one_time_payments(connection))
  out <- siab_collect(connection, "spell")

  expect_equal(out$tentgelt, c(60, 50))
})

test_that("spells with no payment in their group are left alone", {
  connection <- siab_db(spell_frame(
    persnr   = c(1L, 2L),
    betnr    = c(7L, 7L),
    year     = c(2000L, 2000L),
    grund    = c(30L, 30L),
    begepi   = as.Date(c("2000-01-01", "2000-01-01")),
    endepi   = as.Date(c("2000-01-10", "2000-01-10")),
    tentgelt = c(50, 50)
  ))

  quietly_run(reallocate_one_time_payments(connection))
  out <- siab_collect(connection, "spell")

  expect_equal(nrow(out), 2L)
  expect_equal(out$tentgelt, c(50, 50))
})

test_that("a missing grund is kept and a missing wage stays missing", {
  # `drop if grund == 154` does not drop a missing grund in Stata, and
  # `replace tentgelt = ... if !missing(tentgelt)` leaves a missing wage alone
  # even when the group has a payment to hand out.
  connection <- siab_db(spell_frame(
    persnr   = c(1L, 1L, 1L),
    betnr    = c(7L, 7L, 7L),
    year     = c(2000L, 2000L, 2000L),
    grund    = c(NA_integer_, 30L, 154L),
    begepi   = as.Date(c("2000-01-01", "2000-01-01", "2000-12-01")),
    endepi   = as.Date(c("2000-01-10", "2000-01-10", "2000-12-10")),
    tentgelt = c(NA_real_, 50, 10)
  ))

  quietly_run(reallocate_one_time_payments(connection))
  out <- siab_collect(connection, "spell")

  expect_equal(nrow(out), 2L)
  expect_true(is.na(out$tentgelt[1]))
  # 100 euro over the 20 remaining days of the group, so 5 euro a day.
  expect_equal(out$tentgelt[2], 55)
})

test_that("a missing wage on the 154 spell contributes nothing", {
  # Stata's egen sum() counts a missing summand as zero.
  connection <- siab_db(spell_frame(
    persnr   = c(1L, 1L),
    betnr    = c(7L, 7L),
    year     = c(2000L, 2000L),
    grund    = c(30L, 154L),
    begepi   = as.Date(c("2000-01-01", "2000-12-01")),
    endepi   = as.Date(c("2000-01-10", "2000-12-10")),
    tentgelt = c(50, NA_real_)
  ))

  quietly_run(reallocate_one_time_payments(connection))
  out <- siab_collect(connection, "spell")

  expect_equal(nrow(out), 1L)
  expect_equal(out$tentgelt, 50)
})

test_that("the recomputed wage is rounded to two decimals", {
  # 100 euro over 3 days is 33.333... a day on top of 50, so 83.33.
  connection <- siab_db(spell_frame(
    persnr   = c(1L, 1L),
    betnr    = c(7L, 7L),
    year     = c(2000L, 2000L),
    grund    = c(30L, 154L),
    begepi   = as.Date(c("2000-01-01", "2000-12-01")),
    endepi   = as.Date(c("2000-01-03", "2000-12-10")),
    tentgelt = c(50, 10)
  ))

  quietly_run(reallocate_one_time_payments(connection))
  out <- siab_collect(connection, "spell")

  expect_equal(out$tentgelt, 83.33)
})

test_that("an episode longer than 366 days stops the step", {
  connection <- siab_db(spell_frame(
    persnr   = 1L,
    betnr    = 7L,
    year     = 2000L,
    grund    = 30L,
    begepi   = as.Date("2000-01-01"),
    endepi   = as.Date("2002-01-01"),
    tentgelt = 50
  ))

  expect_error(quietly_run(reallocate_one_time_payments(connection)),
               "episode_length outside 1 to 366")
})
