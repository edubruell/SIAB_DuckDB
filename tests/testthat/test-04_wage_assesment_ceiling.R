# Tests for generate_limit_assess() in functions/04_wage_assesment_ceiling.R.
#
# The step builds the Rechtskreis dummy `east` from the establishment's federal
# state `ao_bula` and then joins the contribution assessment ceiling
# (Beitragsbemessungsgrenze) for that year and Rechtskreis. Berlin is the case
# that decides the whole variable: it counts as West up to 1991 and as East
# from 1992, which is what 06_wages_assessment_ceiling.do does.

test_that("east follows the Rechtskreis split, with Berlin changing side in 1992", {
  connection <- siab_db(data.frame(
    persnr  = 1:8,
    year    = c(1991L, 1992L, 1991L, 1991L, 2000L, 2000L, 2000L, 2000L),
    ao_bula = c(11L,   11L,   1L,    12L,   9L,    16L,   10L,   13L)
  ))

  quietly_run(generate_limit_assess(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(
    out$east,
    c(0,  # Berlin 1991, still West
      1,  # Berlin 1992, now East
      0,  # Schleswig-Holstein
      1,  # Brandenburg
      0,  # Bavaria
      1,  # Thuringia
      0,  # Saarland
      1)  # Mecklenburg-Western Pomerania
  )
})

test_that("an unknown or missing federal state gives a missing east", {
  connection <- siab_db(data.frame(
    persnr  = 1:3,
    year    = c(2000L, 2000L, 2000L),
    ao_bula = c(NA_integer_, 17L, 99L)
  ))

  quietly_run(generate_limit_assess(connection))
  out <- siab_collect(connection, "persnr")

  expect_true(all(is.na(out$east)))
  expect_true(all(is.na(out$limit_assess)))
})

test_that("Berlin with a missing year falls to East, as it does in Stata", {
  # The first branch, ao_bula == 11 & year < 1992, cannot be true when the year
  # is missing, so the row drops through to the East branch. Stata's `if
  # ao_bula==11 & jahr<1992` behaves the same way, because a missing jahr is
  # not less than 1992. The test records the behaviour rather than endorsing it.
  connection <- siab_db(data.frame(persnr = 1L, year = NA_integer_, ao_bula = 11L))

  quietly_run(generate_limit_assess(connection))
  out <- siab_collect(connection)

  expect_equal(out$east, 1)
})

test_that("limit_assess is the value the statutory table holds for that year and Rechtskreis", {
  ceiling <- siab_lookup("wa_ceiling.csv", "limit_assess")

  connection <- siab_db(data.frame(
    persnr  = 1:4,
    year    = c(1980L, 2000L, 2000L, 2024L),
    ao_bula = c(9L,    9L,    14L,   14L)
  ))

  quietly_run(generate_limit_assess(connection))
  out <- siab_collect(connection, "persnr")

  expected <- purrr::map2_dbl(
    c(1980, 2000, 2000, 2024), c(0, 0, 1, 1),
    ~ceiling$limit_assess[ceiling$year == .x & ceiling$east == .y]
  )
  expect_equal(out$limit_assess, expected)
})

test_that("a year outside the statutory table gives a missing ceiling rather than dropping the row", {
  connection <- siab_db(data.frame(
    persnr  = 1:2,
    year    = c(1974L, 2025L),
    ao_bula = c(9L, 9L)
  ))

  quietly_run(generate_limit_assess(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(nrow(out), 2L)
  expect_true(all(is.na(out$limit_assess)))
})

test_that("a missing east still gets the nationwide ceiling before 1992", {
  # 06_wages_assessment_ceiling.do assigns the ceiling on the year alone up to
  # 1991 and only conditions on east from 1992 on, so a spell whose federal
  # state is unknown keeps a ceiling in the earlier years.
  ceiling <- siab_lookup("wa_ceiling.csv", "limit_assess")

  connection <- siab_db(data.frame(
    persnr  = 1:2,
    year    = c(1991L, 1992L),
    ao_bula = c(NA_integer_, NA_integer_)
  ))

  quietly_run(generate_limit_assess(connection))
  out <- siab_collect(connection, "persnr")

  expect_true(all(is.na(out$east)))
  expect_equal(out$limit_assess[1],
               ceiling$limit_assess[ceiling$year == 1991 & ceiling$east == 0])
  expect_true(is.na(out$limit_assess[2]))
})
