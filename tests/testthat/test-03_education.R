# Tests for generate_educ_variable() in functions/03_education.R.
#
# The step recodes the FDZ's imputed training variable `ausbildung_imp` into
# three broad groups, following 05_educ_broad.do:
#   1  no vocational training and no degree      (ausbildung_imp 1, 3)
#   2  vocational training                       (ausbildung_imp 2, 4)
#   3  degree from a university or a university
#      of applied science                        (ausbildung_imp 5, 6)
# Every other value, including a missing one, has to come out as NA. The input
# is the imputed variable, not the raw `ausbildung`: the raw one carries the
# full administrative code list and would put most spells in the wrong group.

test_that("ausbildung_imp is recoded into the three broad education groups", {
  connection <- siab_db(data.frame(
    persnr         = 1:6,
    ausbildung_imp = 1:6
  ))

  quietly_run(generate_educ_variable(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(out$educ, c(1L, 2L, 1L, 2L, 3L, 3L))
})

test_that("a code outside 1 to 6 gives a missing educ", {
  # ausbildung_imp only takes 1 to 6, so anything else is a data error rather
  # than a category. The reference leaves educ at Stata's missing there.
  connection <- siab_db(data.frame(
    persnr         = 1:4,
    ausbildung_imp = c(0L, 7L, 11L, 12L)
  ))

  quietly_run(generate_educ_variable(connection))
  out <- siab_collect(connection, "persnr")

  expect_true(all(is.na(out$educ)))
})

test_that("a missing ausbildung_imp gives a missing educ", {
  connection <- siab_db(data.frame(persnr = 1:2,
                                   ausbildung_imp = c(NA_integer_, 2L)))

  quietly_run(generate_educ_variable(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(out$educ, c(NA_integer_, 2L))
})

test_that("the raw ausbildung variable is ignored", {
  # The two variables disagree on purpose here. Reading the raw one was the
  # port's original bug, found by the comparison against the Stata reference.
  connection <- siab_db(data.frame(
    persnr         = 1:2,
    ausbildung     = c(12L, 1L),
    ausbildung_imp = c(1L, 5L)
  ))

  quietly_run(generate_educ_variable(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(out$educ, c(1L, 3L))
})

test_that("the step adds educ and changes nothing else", {
  input <- data.frame(
    persnr         = 1:3,
    ausbildung_imp = c(1L, 2L, 5L),
    tentgelt       = c(50.5, 60.5, 70.5),
    year           = c(1999L, 2000L, 2001L)
  )
  connection <- siab_db(input)

  quietly_run(generate_educ_variable(connection))
  out <- siab_collect(connection, "persnr")

  expect_setequal(names(out), c(names(input), "educ"))
  expect_equal(out[names(input)], tibble::as_tibble(input))
})

test_that("the step returns its connection so it can be piped", {
  connection <- siab_db(data.frame(persnr = 1L, ausbildung_imp = 2L))
  expect_identical(quietly_run(generate_educ_variable(connection)), connection)
})
