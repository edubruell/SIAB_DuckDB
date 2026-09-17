# Tests for deflate_wages() in R/functions/06_wages_deflation.R.
#
# The step joins the consumer price index on year and divides three money
# variables by it, following 08_wages_deflation.do:
#   wage_defl           = 100 * tentgelt       / cpi
#   limit_marginal_defl = 100 * limit_marginal / cpi
#   limit_assess_defl   = 100 * limit_assess   / cpi
# The index has 2015 as its base, so a 2015 wage has to come back unchanged.

cpi_table <- function() {
  siab_lookup("cpi.csv", "cpi")
}

test_that("the three deflated variables are the nominal ones over the index", {
  cpi <- cpi_table()
  years <- c(1975L, 1990L, 2015L, 2024L)
  input <- data.frame(
    persnr         = seq_along(years),
    year           = years,
    tentgelt       = c(30, 60, 90, 120),
    limit_marginal = c(5, 10, 15, 20),
    limit_assess   = c(50, 100, 150, 200)
  )
  connection <- siab_db(input)

  quietly_run(deflate_wages(connection))
  out <- siab_collect(connection, "persnr")

  index <- purrr::map_dbl(years, ~cpi$cpi[cpi$year == .x])
  expect_equal(out$cpi, index)
  expect_equal(out$wage_defl,           100 * input$tentgelt       / index)
  expect_equal(out$limit_marginal_defl, 100 * input$limit_marginal / index)
  expect_equal(out$limit_assess_defl,   100 * input$limit_assess   / index)
})

test_that("2015 is the base year, so nothing moves", {
  connection <- siab_db(data.frame(
    persnr         = 1L,
    year           = 2015L,
    tentgelt       = 123.45,
    limit_marginal = 11.11,
    limit_assess   = 222.22
  ))

  quietly_run(deflate_wages(connection))
  out <- siab_collect(connection)

  expect_equal(out$wage_defl, 123.45)
  expect_equal(out$limit_marginal_defl, 11.11)
  expect_equal(out$limit_assess_defl, 222.22)
})

test_that("deflating raises pre-2015 wages and lowers post-2015 wages", {
  connection <- siab_db(data.frame(
    persnr         = 1:3,
    year           = c(1980L, 2015L, 2024L),
    tentgelt       = rep(100, 3),
    limit_marginal = rep(10, 3),
    limit_assess   = rep(200, 3)
  ))

  quietly_run(deflate_wages(connection))
  out <- siab_collect(connection, "persnr")

  expect_gt(out$wage_defl[1], 100)
  expect_equal(out$wage_defl[2], 100)
  expect_lt(out$wage_defl[3], 100)
})

test_that("a missing wage stays missing and the row survives", {
  connection <- siab_db(data.frame(
    persnr         = 1:2,
    year           = c(2000L, 2000L),
    tentgelt       = c(NA_real_, 50),
    limit_marginal = c(10, 10),
    limit_assess   = c(200, 200)
  ))

  quietly_run(deflate_wages(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(nrow(out), 2L)
  expect_true(is.na(out$wage_defl[1]))
  expect_false(is.na(out$wage_defl[2]))
})

test_that("a year the index does not cover gives NA rather than dropping the row", {
  connection <- siab_db(data.frame(
    persnr         = 1:2,
    year           = c(1974L, 2025L),
    tentgelt       = c(50, 50),
    limit_marginal = c(10, 10),
    limit_assess   = c(200, 200)
  ))

  quietly_run(deflate_wages(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(nrow(out), 2L)
  expect_true(all(is.na(out$cpi)))
  expect_true(all(is.na(out$wage_defl)))
})

test_that("the join adds no rows and leaves the input columns alone", {
  input <- data.frame(
    persnr         = 1:6,
    year           = c(1980L, 1980L, 1990L, 2000L, 2010L, 2020L),
    tentgelt       = c(10, 20, 30, 40, 50, 60),
    limit_marginal = rep(10, 6),
    limit_assess   = rep(200, 6)
  )
  connection <- siab_db(input)

  quietly_run(deflate_wages(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(nrow(out), 6L)
  expect_equal(out[names(input)], tibble::as_tibble(input))
  expect_setequal(
    setdiff(names(out), names(input)),
    c("cpi", "wage_defl", "limit_marginal_defl", "limit_assess_defl")
  )
})

# The comparison against the Stata reference lives in
# tests/testthat/test-reference-08_wages_deflation.R, named after the reference
# step 08_wages_deflation.do rather than after the R file.
