# Tests for generate_occupation_variables() in functions/02_occupations.R.
#
# The step joins two crosswalks onto the 3-digit SIAB occupation code `beruf`:
# the 2-digit KldB-88 Berufsgruppe (occ_kldb88_2) and the Blossfeld
# classification (occ_blo). Both are left joins, so a code that is in neither
# table has to come back as NA without losing the row.

kldb_crosswalk <- function() {
  readr::read_csv(here::here("classifications", "kldb88_beruf.csv"),
                  show_col_types = FALSE)
}

blo_crosswalk <- function() {
  readr::read_csv(here::here("classifications", "walkover_beruf_occblo.csv"),
                  show_col_types = FALSE)
}

test_that("both occupation variables are joined on beruf", {
  kldb <- kldb_crosswalk()
  blo <- blo_crosswalk()
  codes <- c(11L, 12L)

  connection <- siab_db(data.frame(persnr = seq_along(codes), beruf = codes))
  quietly_run(generate_occupation_variables(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(out$occ_kldb88_2,
               purrr::map_dbl(codes, ~kldb$kldb88_2[kldb$beruf == .x]))
  expect_equal(out$occ_blo,
               purrr::map_dbl(codes, ~blo$occ_blo[blo$beruf == .x]))
})

test_that("a beruf outside both crosswalks keeps its row and gets NA", {
  connection <- siab_db(data.frame(persnr = 1:2, beruf = c(11L, 998L)))

  quietly_run(generate_occupation_variables(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(nrow(out), 2L)
  expect_false(is.na(out$occ_kldb88_2[1]))
  expect_true(is.na(out$occ_kldb88_2[2]))
})

test_that("a missing beruf keeps its row and gets NA on both variables", {
  connection <- siab_db(data.frame(persnr = 1:2, beruf = c(11L, NA_integer_)))

  quietly_run(generate_occupation_variables(connection))
  out <- siab_collect(connection, "persnr")

  expect_equal(nrow(out), 2L)
  expect_true(is.na(out$occ_kldb88_2[2]))
  expect_true(is.na(out$occ_blo[2]))
})

test_that("neither join duplicates a row", {
  kldb <- kldb_crosswalk()
  codes <- head(kldb$beruf[!is.na(kldb$kldb88_2)], 50)

  connection <- siab_db(data.frame(persnr = seq_along(codes), beruf = codes))
  quietly_run(generate_occupation_variables(connection))

  expect_equal(nrow(siab_collect(connection)), length(codes))
})

test_that("the step adds exactly occ_kldb88_2 and occ_blo", {
  input <- data.frame(persnr = 1:3, beruf = c(11L, 12L, 71L), tentgelt = c(1, 2, 3))
  connection <- siab_db(input)

  quietly_run(generate_occupation_variables(connection))
  out <- siab_collect(connection, "persnr")

  expect_setequal(setdiff(names(out), names(input)),
                  c("occ_kldb88_2", "occ_blo"))
  expect_equal(out[names(input)], tibble::as_tibble(input))
})

test_that("the crosswalk covers every KldB-88 code with a Berufsgruppe between 1 and 99", {
  kldb <- kldb_crosswalk()
  mapped <- kldb$kldb88_2[kldb$in_kldb88]
  expect_false(any(is.na(mapped)))
  expect_true(all(mapped >= 1 & mapped <= 99))
})
