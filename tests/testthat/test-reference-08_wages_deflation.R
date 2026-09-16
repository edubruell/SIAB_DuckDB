# deflate_wages() against 08_wages_deflation.do.
#
# The consumer price index and the three deflated variables are stored by the
# reference as Stata `float`, which holds about seven decimal digits, so they
# get a relative tolerance of 1e-6. The arithmetic itself is one division and
# is expected to agree far more closely than that; the tolerance covers the
# storage, not the calculation.
#
# wage_defl additionally depends on the daily wage, which is not yet right:
# 02_grund154.do has no R counterpart, so tentgelt differs on about 13 percent
# of rows. That test is restricted to rows where the wage already agrees.

test_that("cpi matches the reference to Stata float precision", {
  # The R port rounds classifications/cpi.csv to float on read-in, which makes
  # 44 of the 50 years agree bit for bit. Six years, 1977, 1983 and 1987 to
  # 1990, land one unit in the last place apart, because the reference computed
  # the index from an expression and classifications/build_statutory_tables.R
  # arrives at the neighbouring float. The gap is 9e-08 relative, well inside
  # the tolerance, and below anything the float can represent.
  query <- siab_reference_query("08_wages_deflation")

  diff <- siab_column_diff(query, "cpi", tolerance = 1e-6)
  expect_equal(diff$differing, 0L,
               info = paste0("cpi differs on ", diff$differing,
                             " of ", diff$shared, " shared rows"))
})

test_that("the deflated statutory limits match the reference to float precision", {
  query <- siab_reference_query("08_wages_deflation")

  for (column in c("limit_marginal_defl", "limit_assess_defl")) {
    diff <- siab_column_diff(query, column, tolerance = 1e-6)
    expect_equal(diff$differing, 0L,
                 info = paste0(column, " differs on ", diff$differing,
                               " of ", diff$shared, " shared rows"))
  }
})

test_that("wage_defl matches the reference wherever the daily wage does", {
  # Drop the tentgelt restriction once 02_grund154.do is ported.
  query <- siab_reference_query("08_wages_deflation")

  differing <- query(
    "SELECT count(*) AS n
     FROM stata JOIN r USING (persnr, spell, begepi)
     WHERE stata.tentgelt IS NOT DISTINCT FROM r.tentgelt
       AND ((stata.wage_defl IS NULL) <> (r.wage_defl IS NULL)
            OR abs(stata.wage_defl - r.wage_defl)
               > 1e-6 * greatest(abs(stata.wage_defl), 1e-12))"
  )$n

  expect_equal(differing, 0L,
               info = paste0("wage_defl differs on ", differing,
                             " rows whose tentgelt already matches"))
})
