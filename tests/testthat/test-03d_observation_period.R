# Tests for restrict_observation_period() in R/functions/03d_observation_period.R.
#
# 00_master_SIAB.do keeps the episodes whose year lies in the observation
# period, `keep if inrange(jahr,${minYear},${maxYear})`, and runs it after the
# biography step so that the cumulative biographic variables are built over the
# whole history first. The port's `year` is the reference's `jahr`.
#
# The bounds are inclusive on both sides, which is what inrange() does.

test_that("both bounds are inclusive", {
  connection <- siab_db(data.frame(
    persnr = 1:5,
    year   = c(1994L, 1995L, 2010L, 2023L, 2024L)
  ))

  quietly_run(restrict_observation_period(connection,
                                          min_year = 1995,
                                          max_year = 2023))
  out <- siab_collect(connection, "persnr")

  expect_equal(out$persnr, 2:4)
})

test_that("the full span of the delivery keeps every episode", {
  # This is the default, and the position in the pipeline is only observable
  # when a user narrows the period, so the default has to be a no-op.
  connection <- siab_db(data.frame(
    persnr = 1:4,
    year   = c(1975L, 1999L, 2023L, 2023L)
  ))

  quietly_run(restrict_observation_period(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(nrow(out), 4L)
})

test_that("an episode with no year is dropped", {
  # Stata's inrange() is false for a missing value, so the reference drops such
  # an episode as well. Every episode reaching this step has a year, because
  # the master builds it from begepi before any step runs, so this is a guard
  # against a hole upstream rather than a case the data produces.
  connection <- siab_db(data.frame(
    persnr = 1:3,
    year   = c(2000L, NA_integer_, 2001L)
  ))

  quietly_run(restrict_observation_period(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(out$persnr, c(1L, 3L))
})

test_that("a single-year period keeps that year alone", {
  connection <- siab_db(data.frame(
    persnr = 1:3,
    year   = c(1999L, 2000L, 2001L)
  ))

  quietly_run(restrict_observation_period(connection,
                                          min_year = 2000,
                                          max_year = 2000))
  out <- siab_collect(connection, "persnr")

  expect_equal(out$persnr, 2L)
})

test_that("a reversed period is refused rather than silently emptying the data", {
  connection <- siab_db(data.frame(persnr = 1:2, year = c(2000L, 2001L)))

  expect_error(
    quietly_run(restrict_observation_period(connection,
                                            min_year = 2010,
                                            max_year = 2000)),
    "min_year is after max_year"
  )
})

test_that("the other columns survive the restriction", {
  connection <- siab_db(data.frame(
    persnr   = 1:3,
    year     = c(1994L, 2000L, 2001L),
    tage_erw = c(10L, 20L, 30L),
    tentgelt = c(1.5, 2.5, 3.5)
  ))

  quietly_run(restrict_observation_period(connection, min_year = 1995))
  out <- siab_collect(connection, "persnr")

  expect_equal(names(out), c("persnr", "year", "tage_erw", "tentgelt"))
  expect_equal(out$tage_erw, c(20L, 30L))
  expect_equal(out$tentgelt, c(2.5, 3.5))
})
