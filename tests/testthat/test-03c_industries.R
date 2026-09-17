# Tests for generate_industry_variables() in R/functions/03c_industries.R.
#
# The step maps the time-consistent three-digit WZ93 industry `w93_3_gen` onto
# two one-digit classifications, following 13_industries_1digit.do: the
# Statistisches Bundesamt's 16 categories and the IAB establishment panel's 9.
# Both mappings are ranges, so the tests below walk the endpoints of every
# range, then the gaps between them. A code in a gap has to come out missing,
# which is what the reference's chain of `replace ... if` leaves behind.

#Both endpoints of every Statistisches Bundesamt range, with the code it gets
destatis_endpoints <- tibble::tribble(
  ~w93_3_gen, ~expected,
   11L,  1L,  20L,  1L,
   50L,  2L,
  101L,  3L, 145L,  3L,
  151L,  4L, 372L,  4L,
  401L,  5L, 410L,  5L,
  451L,  6L, 455L,  6L,
  501L,  7L, 527L,  7L,
  551L,  8L, 555L,  8L,
  601L,  9L, 642L,  9L,
  651L, 10L, 672L, 10L,
  701L, 11L, 748L, 11L,
  751L, 12L, 753L, 12L,
  801L, 13L, 804L, 13L,
  851L, 14L, 853L, 14L,
  900L, 15L, 930L, 15L,
  950L, 16L, 990L, 16L
)

#Both endpoints of every IAB establishment panel range, with the code it gets
estpanel_endpoints <- tibble::tribble(
  ~w93_3_gen, ~expected,
   11L, 1L,  50L, 1L,
  101L, 1L, 145L, 1L,
  371L, 1L, 410L, 1L,
  900L, 1L,
  151L, 2L, 160L, 2L,
  171L, 3L, 193L, 3L,
  221L, 3L, 223L, 3L,
  361L, 3L, 366L, 3L,
  201L, 4L, 212L, 4L,
  231L, 4L, 287L, 4L,
  291L, 5L, 355L, 5L,
  451L, 6L, 455L, 6L,
  551L, 7L, 555L, 7L,
  921L, 7L, 930L, 7L,
  501L, 7L, 527L, 7L,
  601L, 8L, 634L, 8L,
  641L, 8L, 642L, 8L,
  651L, 8L, 672L, 8L,
  701L, 8L, 703L, 8L,
  711L, 8L, 714L, 8L,
  721L, 8L, 726L, 8L,
  731L, 8L, 744L, 8L,
  745L, 8L, 748L, 8L,
  801L, 9L, 804L, 9L,
  851L, 9L, 853L, 9L,
  911L, 9L, 913L, 9L,
  751L, 9L, 753L, 9L
)

run_on_codes <- function(codes, ...) {
  connection <- siab_db(data.frame(
    persnr    = seq_along(codes),
    w93_3_gen = as.integer(codes)
  ))

  quietly_run(generate_industry_variables(connection, ...))
  siab_collect(connection, "persnr")
}

test_that("every Statistisches Bundesamt range maps its endpoints", {
  out <- run_on_codes(destatis_endpoints$w93_3_gen)

  expect_equal(out$industry1_destatis, destatis_endpoints$expected)
})

test_that("every IAB establishment panel range maps its endpoints", {
  out <- run_on_codes(estpanel_endpoints$w93_3_gen)

  expect_equal(out$industry1_estpanel, estpanel_endpoints$expected)
})

test_that("a code between two Statistisches Bundesamt ranges is missing", {
  # One code out of each gap in the 16 ranges, plus one below the first range
  # and one above the last.
  gaps <- c(10L, 21L, 49L, 51L, 100L, 146L, 150L, 373L, 400L, 411L, 450L,
            456L, 500L, 528L, 550L, 556L, 600L, 643L, 650L, 673L, 700L,
            749L, 750L, 754L, 800L, 805L, 850L, 854L, 899L, 931L, 949L, 991L)

  out <- run_on_codes(gaps)

  expect_true(all(is.na(out$industry1_destatis)))
})

test_that("a code between two IAB establishment panel ranges is missing", {
  # 194 to 200 and 288 to 290 sit inside manufacturing and still get nothing:
  # the establishment panel mapping covers less of the classification than the
  # Statistisches Bundesamt one does.
  gaps <- c(10L, 51L, 100L, 146L, 150L, 161L, 170L, 194L, 200L, 213L, 220L,
            224L, 230L, 288L, 290L, 356L, 360L, 367L, 370L, 411L, 450L, 456L,
            500L, 528L, 550L, 556L, 600L, 635L, 640L, 643L, 650L, 673L, 700L,
            704L, 710L, 715L, 720L, 727L, 730L, 749L, 750L, 754L, 800L, 805L,
            850L, 854L, 899L, 901L, 910L, 914L, 920L, 931L, 990L)

  out <- run_on_codes(gaps)

  expect_true(all(is.na(out$industry1_estpanel)))
})

test_that("a missing industry stays missing in both classifications", {
  # Stata treats missing as larger than any number, so `w93_3_gen >= 11 &
  # w93_3_gen <= 20` is false for it and the reference leaves the category
  # missing. A negative special code falls in no range for the same reason.
  out <- run_on_codes(c(NA, -9L, -7L))

  expect_true(all(is.na(out$industry1_destatis)))
  expect_true(all(is.na(out$industry1_estpanel)))
})

test_that("mappings picks which classification is built", {
  # 00_master_SIAB.do gates each mapping behind its own macro, `destatis` and
  # `estpanel`, and runs the step when either is switched on.
  destatis_only <- run_on_codes(c(101L, 551L), mappings = "destatis")
  estpanel_only <- run_on_codes(c(101L, 551L), mappings = "estpanel")

  expect_true("industry1_destatis" %in% names(destatis_only))
  expect_false("industry1_estpanel" %in% names(destatis_only))

  expect_true("industry1_estpanel" %in% names(estpanel_only))
  expect_false("industry1_destatis" %in% names(estpanel_only))
})

test_that("the step stops when w93_3_gen is not there", {
  # w93_3_gen arrives with merge_basic_bhp(). Without it the mappings would
  # silently produce nothing, so the step refuses instead.
  connection <- siab_db(data.frame(persnr = 1:2, beruf = c(11L, 12L)))

  expect_error(quietly_run(generate_industry_variables(connection)),
               "w93_3_gen")
})
