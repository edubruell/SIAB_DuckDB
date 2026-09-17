# Tests for build_monthly_panel() in R/functions/09b_monthly_panel.R.
#
# The step follows 16_monthly_panel.do: total the days and earnings of each
# calendar year, cut every episode into one row per calendar month, keep the row
# whose month holds the cutoff day inside the episode, and trim the four
# duration counters so they end on that day.
#
# The comparison against the reference itself, over the FDZ test data, is in
# test-reference-16_monthly_panel.R. These tests need no fixture and no test
# data: they pin the arithmetic on tables small enough to check by hand.

# One employment episode and one benefit episode, both inside 2000.
episodes <- function() {
  data.frame(
    persnr            = c(1L, 2L),
    begepi            = as.Date(c("2000-01-20", "2000-03-01")),
    endepi            = as.Date(c("2000-04-10", "2000-05-31")),
    year              = c(2000L, 2000L),
    quelle            = c(1L, 2L),
    erwstat           = c(101L, NA),
    parallel_benefits = c(0, 1),
    parallel_wage_imp = c(100, 0),
    tage_bet          = c(82L, 0L),
    tage_job          = c(82L, 0L),
    tage_erw          = c(82L, 0L),
    tage_lst          = c(0L, 92L)
  )
}

test_that("an episode becomes one row per month whose cutoff day it covers", {
  connection <- siab_db(episodes())

  quietly_run(build_monthly_panel(connection))
  out <- siab_collect(connection, c("persnr", "month"))

  # Person 1 runs 20 January to 10 April. January and April both fall out: the
  # 15th lies before the episode starts in January and after it ends in April.
  employment <- out[out$persnr == 1L, ]
  expect_equal(employment$month, as.Date(c("2000-02-01", "2000-03-01")))
  expect_equal(employment$month_num, c(2, 3))
  expect_equal(employment$begepi_monthly,
               as.Date(c("2000-02-01", "2000-03-01")))
  expect_equal(employment$endepi_monthly,
               as.Date(c("2000-02-29", "2000-03-31")))
})

test_that("the three yearly totals are the ones the yearly panel builds", {
  connection <- siab_db(episodes())

  quietly_run(build_monthly_panel(connection))
  out <- siab_collect(connection, c("persnr", "month"))

  # Person 1 works 82 days at an imputed 100, person 2 receives benefits for 92.
  expect_equal(unique(out$year_days_emp[out$persnr == 1L]), 82)
  expect_equal(unique(out$year_labor_earn[out$persnr == 1L]), 8200)
  expect_equal(unique(out$year_days_benefits[out$persnr == 2L]), 92)
})

test_that("monthly_vars = FALSE leaves the three yearly totals out", {
  connection <- siab_db(episodes())

  quietly_run(build_monthly_panel(connection, monthly_vars = FALSE))
  out <- siab_collect(connection)

  expect_false(any(c("year_days_emp", "year_days_benefits", "year_labor_earn")
                   %in% names(out)))
})

test_that("the employment counters end on the cutoff day", {
  connection <- siab_db(episodes())

  quietly_run(build_monthly_panel(connection))
  out <- siab_collect(connection, c("persnr", "month"))
  employment <- out[out$persnr == 1L, ]

  # 82 days less the part of the month that runs past the 15th: 14 days in
  # February and 16 in March.
  expect_equal(employment$tage_bet, 82L - c(14L, 16L))
  expect_equal(employment$tage_job, employment$tage_bet)
  expect_equal(employment$tage_erw, employment$tage_bet)
})

# 16_monthly_panel.do trims tage_lst in two separate `replace` statements, one on
# quelle and one on parallel_benefits, and 15_parallel_episodes.do sets
# parallel_benefits on the benefit episode itself, so a surviving LeH row meets
# both conditions and loses the overhang twice. The port reproduces the
# reference as published; the exact match on tage_lst over 3,374,420 rows of the
# test data, in test-reference-16_monthly_panel.R, is what shows the reference
# really does subtract twice.
test_that("a benefit episode loses the overhang from tage_lst twice", {
  connection <- siab_db(episodes())

  quietly_run(build_monthly_panel(connection))
  out <- siab_collect(connection, c("persnr", "month"))
  benefits <- out[out$persnr == 2L, ]

  # March, April and May, with 16, 15 and 16 days past the 15th.
  expect_equal(benefits$month_num, c(3, 4, 5))
  expect_equal(benefits$tage_lst, 92L - 2L * c(16L, 15L, 16L))
})

test_that("a cutoff day longer than the month is capped at the month's end", {
  connection <- siab_db(episodes())

  quietly_run(build_monthly_panel(connection, cutoff_day = 31))
  out <- siab_collect(connection, c("persnr", "month"))

  # February 2000 has 29 days. Without the cap the month would fall out of the
  # panel; with it the row survives and ends on 29 February.
  february <- out[out$persnr == 1L & out$month == as.Date("2000-02-01"), ]
  expect_equal(nrow(february), 1L)
  expect_equal(february$endepi_monthly, as.Date("2000-02-29"))
})

test_that("the step refuses data that still holds parallel episodes", {
  parallel <- episodes()[c(1L, 1L), ]
  parallel$persnr <- c(1L, 1L)
  connection <- siab_db(parallel)

  expect_error(quietly_run(build_monthly_panel(connection)),
               "do not identify a row")
})
