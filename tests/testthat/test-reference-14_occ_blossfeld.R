# generate_occupation_variables() against 14_occ_blossfeld.do.
#
# The reference recodes the three-digit occupation code `beruf` into the twelve
# Blossfeld classes, and closes the recode with `(else = 99)`. Stata's `else`
# covers missing values as well as unmatched ones, so an episode with no
# occupation at all leaves the step carrying 99, "not assignable".
#
# The two sides reach the column from different positions in the pipeline. The
# reference builds it at step 14, after the AKM merge; the port builds it in
# generate_occupation_variables(), immediately after the biography step. Nothing
# between the two positions touches `beruf`, which is what makes the comparison
# meaningful, and the first test below is what makes that claim testable.

test_that("the Blossfeld classification matches the reference exactly", {
  query <- siab_reference_query("14_occ_blossfeld")

  diff <- siab_column_diff(query, "occ_blo")
  expect_equal(diff$shared, 505050L)
  expect_equal(diff$differing, 0L)
})

test_that("beruf itself is untouched between the two positions", {
  query <- siab_reference_query("14_occ_blossfeld")

  diff <- siab_column_diff(query, "beruf")
  expect_equal(diff$differing, 0L)
})

# The reference's `(else = 99)` reaches every episode without an occupation,
# which on the test data is a fifth of all rows: benefit and job-search spells
# carry no beruf. A port using a plain left join alone leaves them missing.
test_that("an episode with no occupation carries 99 on both sides", {
  query <- siab_reference_query("14_occ_blossfeld")

  no_beruf <- query(
    "SELECT count(*) AS n,
            count(*) FILTER (WHERE stata.occ_blo = 99) AS stata_99,
            count(*) FILTER (WHERE r.occ_blo = 99) AS r_99
     FROM stata JOIN r USING (persnr, spell, begepi)
     WHERE stata.beruf IS NULL"
  )

  expect_gt(no_beruf$n, 0L)
  expect_equal(no_beruf$stata_99, no_beruf$n)
  expect_equal(no_beruf$r_99, no_beruf$n)
})

test_that("no row is lost or duplicated by the two crosswalk joins", {
  query <- siab_reference_query("14_occ_blossfeld")

  counts <- query(
    "SELECT (SELECT count(*) FROM stata) AS n_stata,
            (SELECT count(*) FROM r) AS n_r,
            (SELECT count(*) FROM (SELECT persnr, spell, begepi FROM stata
                                   EXCEPT
                                   SELECT persnr, spell, begepi FROM r)) AS only_stata"
  )

  expect_equal(counts$n_stata, counts$n_r)
  expect_equal(counts$only_stata, 0L)
})
