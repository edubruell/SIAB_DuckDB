# Tests for the small helpers in R/functions/00_common_functions.R.

test_that("%nin% is the negation of %in%", {
  expect_equal(c(1, 2, 3) %nin% c(2, 4), c(TRUE, FALSE, TRUE))
  expect_equal(character(0) %nin% "a", logical(0))
  expect_true(NA %nin% c(1, 2))
})

test_that("folder_reference_factory pastes a relative path onto its folder", {
  ref <- folder_reference_factory("/data/siab")
  expect_equal(ref("x.dta"), "/data/siab/x.dta")
  expect_equal(ref("sub", "x.dta"), "/data/siab/sub/x.dta")
  expect_equal(ref(), "/data/siab")
})

test_that("folder_reference_factory passes an absolute path through untouched", {
  ref <- folder_reference_factory("/data/siab")
  expect_equal(ref("/elsewhere/x.dta"), "/elsewhere/x.dta")
  expect_equal(ref("~/x.dta"), "~/x.dta")
  expect_equal(ref("C:/x.dta"), "C:/x.dta")
})

test_that("folder_reference_factory refuses a mix of absolute and relative", {
  ref <- folder_reference_factory("/data/siab")
  expect_error(
    ref(c("/absolute", "relative")),
    "Combination of absolute and relative paths not supported"
  )
})

test_that("folder_reference_factory refuses a NULL folder", {
  expect_error(folder_reference_factory(NULL), "Please set the path to a target_folder")
})

test_that("quick_year matches base R over fifty years of dates", {
  dates <- seq(as.Date("1975-01-01"), as.Date("2024-12-31"), by = "day")
  expect_equal(quick_year(dates), as.integer(format(dates, "%Y")))
})

test_that("quick_year gets the leap-day and year boundaries right", {
  edges <- as.Date(c("1970-01-01", "1971-12-31", "1972-02-29", "1972-12-31",
                     "1999-12-31", "2000-01-01", "2000-02-29", "2024-12-31"))
  expect_equal(quick_year(edges), as.integer(format(edges, "%Y")))
})

test_that("validate_inputs stops on the first failing predicate and is silent otherwise", {
  expect_error(
    validate_inputs(list("handling must be one of wage, drop" = FALSE)),
    "handling must be one of wage, drop"
  )
  expect_silent(validate_inputs(list("always true" = TRUE, "also true" = TRUE)))
})

test_that("compute_and_overwrite replaces the target table and leaves no temp behind", {
  connection <- siab_db(data.frame(persnr = 1:3, wage = c(10, 20, 30)))

  tbl(connection, "data") |>
    mutate(wage = wage * 2) |>
    compute_and_overwrite()

  expect_equal(siab_collect(connection, "persnr")$wage, c(20, 40, 60))
  expect_false(DBI::dbExistsTable(connection, "temp"))
})

test_that("compute_and_overwrite can write to a table other than data", {
  connection <- siab_db(data.frame(persnr = 1:3, wage = c(10, 20, 30)))

  tbl(connection, "data") |>
    filter(wage > 10) |>
    compute_and_overwrite("subset")

  expect_true(DBI::dbExistsTable(connection, "subset"))
  expect_equal(nrow(dbGetQuery(connection, "SELECT * FROM subset")), 2L)
  # The source table is untouched.
  expect_equal(nrow(siab_collect(connection)), 3L)
})

test_that("compute_and_overwrite refuses to run when a temp table is already there", {
  connection <- siab_db(data.frame(persnr = 1:3, wage = c(10, 20, 30)))
  DBI::dbWriteTable(connection, "temp", data.frame(x = 1))

  expect_error(
    tbl(connection, "data") |> compute_and_overwrite(),
    "Temporary table 'temp' allready exsists"
  )
})

test_that("stata_float rounds to single precision and leaves exact values alone", {
  expect_equal(stata_float(13.15), 13.149999618530273)
  expect_equal(stata_float(c(0, 1, 0.5, -2)), c(0, 1, 0.5, -2))
  expect_equal(stata_float(NA_real_), NA_real_)
  expect_equal(stata_float(integer(0)), numeric(0))
})

test_that("stata_float makes a threshold comparison agree with the reference", {
  # A daily wage of exactly 13.15 euro sits on the 2005 marginal threshold. The
  # reference stores that threshold as a Stata float, which is a hair below
  # 13.15, so the spell is not marginal there.
  expect_false(13.15 <= stata_float(13.15))
  expect_true(13.15 <= 13.15)
})
