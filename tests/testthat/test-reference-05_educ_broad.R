# generate_educ_variable() against 05_educ_broad.do.
#
# educ is an integer recode of the FDZ's imputed training variable, so it is
# compared exactly with no tolerance.

test_that("educ matches the reference exactly", {
  query <- siab_reference_query("05_educ_broad")

  diff <- siab_column_diff(query, "educ")
  expect_equal(diff$differing, 0L,
               info = paste0("educ differs on ", diff$differing,
                             " of ", diff$shared, " shared rows"))
})
