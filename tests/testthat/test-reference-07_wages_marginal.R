# generate_limit_marginal() against 07_wages_marginal.do.
#
# limit_marginal is a statutory figure the reference stores as a Stata `float`,
# and the R port rounds the lookup to the same precision, so it is compared
# exactly. The marginal flag compares the daily wage against that threshold,
# and the daily wage is not yet right: 02_grund154.do, which reallocates
# one-time payments across a year's spells, has no R counterpart. That test is
# restricted to rows whose wage already agrees.

test_that("limit_marginal matches the reference exactly", {
  query <- siab_reference_query("07_wages_marginal")

  diff <- siab_column_diff(query, "limit_marginal")
  expect_equal(diff$differing, 0L,
               info = paste0("limit_marginal differs on ", diff$differing,
                             " of ", diff$shared, " shared rows"))
})

test_that("the marginal flag matches the reference wherever the daily wage does", {
  # Restricting to rows whose tentgelt already agrees isolates this step from
  # the missing step 02. Drop the restriction once 02_grund154.do is ported.
  query <- siab_reference_query("07_wages_marginal")

  differing <- query(
    "SELECT count(*) AS n
     FROM stata JOIN r USING (persnr, spell, begepi)
     WHERE stata.tentgelt IS NOT DISTINCT FROM r.tentgelt
       AND stata.marginal IS DISTINCT FROM r.marginal"
  )$n

  expect_equal(differing, 0L,
               info = paste0("marginal differs on ", differing,
                             " rows whose tentgelt already matches"))
})
