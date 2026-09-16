# Tests for generate_limit_marginal() in functions/05_wages_marginal.R.
#
# The step joins the marginal part-time income threshold
# (Geringfuegigkeitsgrenze) on year and Rechtskreis, then flags a spell as
# marginal when the daily wage is at or below it. It runs after step 04, so
# `east` already exists.

test_that("marginal is 1 at or below the threshold and 0 above it", {
  threshold <- siab_lookup("limit_marginal.csv", "limit_marginal")
  limit_2000_west <- threshold$limit_marginal[threshold$year == 2000 &
                                              threshold$east == 0]

  connection <- siab_db(data.frame(
    persnr   = 1:3,
    year     = rep(2000L, 3),
    east     = rep(0, 3),
    tentgelt = c(limit_2000_west - 1, limit_2000_west, limit_2000_west + 1)
  ))

  quietly_run(generate_limit_marginal(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(out$marginal, c(1, 1, 0))
  expect_equal(out$limit_marginal, rep(limit_2000_west, 3))
})

test_that("the flag is never missing, and follows Stata's missing-value ordering", {
  # 07_wages_marginal.do starts the flag at 0 and raises it to 1 where
  # tentgelt <= limit_marginal. A missing wage is an extended missing in the
  # SIAB and sorts above everything, so it never compares as at or below the
  # threshold and stays 0, even when the threshold is missing too. A missing
  # threshold with a real wage goes the other way and gives 1. Reproduced on
  # purpose; see functions/05_wages_marginal.R.
  connection <- siab_db(data.frame(
    persnr   = 1:4,
    year     = c(2000L, 2025L, 2025L, 2000L),
    east     = c(0, 0, 0, 0),
    tentgelt = c(NA_real_, 10, NA_real_, 50)
  ))

  quietly_run(generate_limit_marginal(connection))
  out <- siab_collect(connection, "persnr")

  expect_false(any(is.na(out$marginal)))
  expect_equal(
    out$marginal,
    c(0,  # wage missing, threshold known: never at or below it
      1,  # threshold missing, wage real: Stata compares as at or below
      0,  # both missing: the wage's extended missing is the larger one
      0)  # 50 euro a day is above the 2000 threshold
  )
})

test_that("a missing east leaves the threshold missing from 1992 on", {
  connection <- siab_db(data.frame(
    persnr   = 1:2,
    year     = c(1991L, 2000L),
    east     = c(NA_real_, NA_real_),
    tentgelt = c(10, 10)
  ))

  quietly_run(generate_limit_marginal(connection))
  out <- siab_collect(connection, "persnr")

  # Before 1992 there was one nationwide threshold, and the reference assigns
  # it on the year alone, so an unknown Rechtskreis still gets a value.
  expect_false(is.na(out$limit_marginal[1]))
  expect_true(is.na(out$limit_marginal[2]))
})

test_that("East and West get different thresholds in the years the law split them", {
  threshold <- siab_lookup("limit_marginal.csv", "limit_marginal")

  connection <- siab_db(data.frame(
    persnr   = 1:4,
    year     = c(1995L, 1995L, 2000L, 2000L),
    east     = c(0, 1, 0, 1),
    tentgelt = rep(10, 4)
  ))

  quietly_run(generate_limit_marginal(connection))
  out <- siab_collect(connection, "persnr")

  expect_false(out$limit_marginal[1] == out$limit_marginal[2])  # split in 1995
  expect_true(out$limit_marginal[3] == out$limit_marginal[4])   # levelled by 2000
  expect_equal(
    out$limit_marginal,
    purrr::map2_dbl(c(1995, 1995, 2000, 2000), c(0, 1, 0, 1),
                    ~threshold$limit_marginal[threshold$year == .x &
                                              threshold$east == .y])
  )
})

test_that("the join adds no rows", {
  connection <- siab_db(data.frame(
    persnr   = 1:5,
    year     = rep(2010L, 5),
    east     = c(0, 0, 1, 1, 0),
    tentgelt = c(1, 2, 3, 4, 5)
  ))

  quietly_run(generate_limit_marginal(connection))
  expect_equal(nrow(siab_collect(connection)), 5L)
})
