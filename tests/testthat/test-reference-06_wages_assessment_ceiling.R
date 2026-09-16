# generate_limit_assess() against 06_wages_assessment_ceiling.do.
#
# Both columns are compared exactly. east is an integer recode of the federal
# state. limit_assess is a statutory euro figure that the reference stores as a
# Stata `float`; the R port rounds classifications/wa_ceiling.csv to the same
# precision on read-in with stata_float(), so the two agree bit for bit.

test_that("east matches the reference exactly", {
  query <- siab_reference_query("06_wages_assessment_ceiling")

  diff <- siab_column_diff(query, "east")
  expect_equal(diff$differing, 0L,
               info = paste0("east differs on ", diff$differing,
                             " of ", diff$shared, " shared rows"))
})

test_that("limit_assess matches the reference exactly", {
  query <- siab_reference_query("06_wages_assessment_ceiling")

  diff <- siab_column_diff(query, "limit_assess")
  expect_equal(diff$differing, 0L,
               info = paste0("limit_assess differs on ", diff$differing,
                             " of ", diff$shared, " shared rows"))
})
