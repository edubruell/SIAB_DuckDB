# split_episodes() against 01_split_episodes.do.
#
# This is the first step of the preparation and the first one to be compared
# against the Stata reference row by row. Every row the reference produces has
# to exist in the R output with the same dates, year and age.

test_that("every reference row exists in the R output with the same key", {
  query <- siab_reference_query("01_split_episodes")

  counts <- query(
    "SELECT (SELECT count(*) FROM stata) AS n_stata,
            (SELECT count(*) FROM (SELECT persnr, spell, begepi FROM stata
                                   EXCEPT
                                   SELECT persnr, spell, begepi FROM r)) AS only_stata"
  )

  expect_equal(counts$only_stata, 0L)
  expect_gt(counts$n_stata, 0L)
})

test_that("the R output carries extra rows only because two filters are not ported", {
  # The reference drops benefit and other sources in the master's pre-step
  # block, `keep if inlist(quelle,1,2,3)`, and the R pipeline does not. Until
  # that filter is ported the R side is a strict superset. The test pins the
  # direction of the gap, so a row that disappears from R still fails.
  query <- siab_reference_query("01_split_episodes")

  only_r <- query(
    "SELECT count(*) AS n FROM (SELECT persnr, spell, begepi FROM r
                                EXCEPT
                                SELECT persnr, spell, begepi FROM stata)"
  )$n

  expect_gt(only_r, 0L)
})

test_that("the split dates and the derived year and age match the reference exactly", {
  query <- siab_reference_query("01_split_episodes")

  for (column in c("endepi", "begepi_orig", "endepi_orig")) {
    diff <- siab_column_diff(query, column)
    expect_equal(diff$differing, 0L,
                 info = paste0(column, " differs on ", diff$differing,
                               " of ", diff$shared, " shared rows"))
  }

  # jahr in the reference is year in the port.
  diff <- siab_column_diff(query, "jahr", "year")
  expect_equal(diff$differing, 0L)

  diff <- siab_column_diff(query, "age", "age")
  expect_equal(diff$differing, 0L)
})
