# reallocate_one_time_payments() against 02_grund154.do.
#
# The step drops the spells with deregistration reason 154 and raises the daily
# wage of the remaining spells of the same person, establishment and year by
# their share of the payment. tentgelt is compared exactly, with no tolerance:
# the two intermediates the reference generates are Stata floats and the port
# reproduces that loss, and the rounding is written in Stata's own form, so the
# two sides agree to the last bit.

test_that("tentgelt matches the reference exactly", {
  query <- siab_reference_query("02_grund154")

  diff <- siab_column_diff(query, "tentgelt")
  expect_equal(diff$differing, 0L,
               info = paste0("tentgelt differs on ", diff$differing,
                             " of ", diff$shared, " shared rows"))
})

test_that("every reference row survives the drop on the R side too", {
  query <- siab_reference_query("02_grund154")

  missing <- query(
    "SELECT count(*) AS n FROM stata ANTI JOIN r USING (persnr, spell, begepi)")
  expect_equal(missing$n, 0L)
})
