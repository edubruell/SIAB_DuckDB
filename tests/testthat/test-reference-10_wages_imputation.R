# impute_wages() against 10_wages_imputation.do.
#
# This is the first comparison past 09_restrictions.do. That step cuts the data
# down to one project's population, and the R pipeline has no counterpart for
# it, so make_fixtures.do takes its dump and then continues from
# the step 08 data. Both halves of this comparison therefore carry the whole
# dataset.
#
# 10_wages_imputation.do drops every intermediate it builds, so the columns the
# two sides have in common are the three the step's own header names: cens, wage
# and wage_imp. The first two are arithmetic and are compared row for row. The
# third cannot be: the reference draws a random term for every censored wage
# from Stata's generator, seeded inside the step, and the port draws from R's.
# The two draws are different numbers by construction, so wage_imp is compared
# as a distribution.
#
# Both sides run both imputation steps: the one on observables from Gartner
# (2005) and the extended one whose regressors include leave-one-out mean wages
# per worker and per plant.

test_that("the two halves carry the same rows", {
  query <- siab_reference_query("10_wages_imputation")

  counts <- query(
    "SELECT (SELECT count(*) FROM stata) AS stata_rows,
            (SELECT count(*) FROM r)     AS r_rows,
            (SELECT count(*) FROM stata JOIN r USING (persnr, spell, begepi))
              AS shared"
  )

  expect_equal(counts$stata_rows, counts$shared)
  expect_equal(counts$r_rows, counts$shared)
})

test_that("cens matches the reference on every row", {
  # Not restricted to the employment history. The reference generates the flag
  # as 0 for every row, so the port does too, and the two agree off the BeH as
  # well as on it. The 15,738 spells from 1992 on whose east flag is missing,
  # and whose assessment ceiling is therefore unknown, are the case that made
  # this worth testing: Stata orders missing above every number, so its
  # `wage_defl > limit_assess4` is false there and the flag stays 0.
  query <- siab_reference_query("10_wages_imputation")

  diff <- siab_column_diff(query, "cens")
  expect_equal(diff$differing, 0L,
               info = paste0("cens differs on ", diff$differing,
                             " of ", diff$shared, " rows"))
})

test_that("wage matches the reference to Stata float precision", {
  # wage is generated from wage_defl and limit_assess4, both floats, so it is
  # stored as a float and gets the same relative tolerance the deflated
  # variables get in test-reference-08_wages_deflation.R.
  query <- siab_reference_query("10_wages_imputation")

  diff <- siab_column_diff(query, "wage", tolerance = 1e-6)
  expect_equal(diff$differing, 0L,
               info = paste0("wage differs on ", diff$differing,
                             " of ", diff$shared, " shared rows"))
})

test_that("wage_imp is imputed on exactly the same rows as in the reference", {
  # The row set is deterministic even though the values are not: which spells
  # get an imputed wage follows from the estimation sample, the carry-through
  # of uncensored wages, and the fallback from the second step to the first.
  query <- siab_reference_query("10_wages_imputation")

  coverage <- query(
    "SELECT count(*) FILTER (WHERE stata.wage_imp IS NOT NULL) AS stata_has,
            count(*) FILTER (WHERE r.wage_imp IS NOT NULL) AS r_has,
            count(*) FILTER (WHERE stata.wage_imp IS NOT NULL
                               AND r.wage_imp IS NULL) AS stata_only,
            count(*) FILTER (WHERE stata.wage_imp IS NULL
                               AND r.wage_imp IS NOT NULL) AS r_only
     FROM stata JOIN r USING (persnr, spell, begepi)"
  )

  expect_equal(coverage$stata_only, 0L,
               info = paste0(coverage$stata_only,
                             " rows are imputed by the reference and not by the port"))
  expect_equal(coverage$r_only, 0L,
               info = paste0(coverage$r_only,
                             " rows are imputed by the port and not by the reference"))
  expect_equal(coverage$r_has, coverage$stata_has)
})

test_that("an uncensored wage is carried through unchanged on both sides", {
  # Nothing is drawn for these rows: both sides set the imputed wage to the
  # observed one, so they agree with the raw wage and with each other.
  query <- siab_reference_query("10_wages_imputation")

  diff <- query(
    "SELECT count(*) AS shared,
            count(*) FILTER (WHERE abs(stata.wage_imp - stata.wage)
                               > 1e-5 * stata.wage) AS stata_differs,
            count(*) FILTER (WHERE abs(r.wage_imp - r.wage)
                               > 1e-5 * r.wage) AS r_differs,
            count(*) FILTER (WHERE abs(stata.wage_imp - r.wage_imp)
                               > 1e-5 * stata.wage_imp) AS between
     FROM stata JOIN r USING (persnr, spell, begepi)
     WHERE stata.cens = 0
       AND stata.wage_imp IS NOT NULL AND r.wage_imp IS NOT NULL"
  )

  expect_equal(diff$stata_differs, 0L)
  expect_equal(diff$r_differs, 0L)
  expect_equal(diff$between, 0L,
               info = paste0("the two sides differ on ", diff$between,
                             " of ", diff$shared, " uncensored rows"))
})

test_that("the imputed wage distribution matches the reference within one percent", {
  # This is the whole of what a censored wage can be compared on. The reference
  # draws its random term from Stata's generator, seeded inside the step, and
  # the port draws from R's, so the two columns are different numbers by
  # construction and only their distributions are comparable.
  #
  # The bound is one percent. Measured on the test data the three statistics
  # land within 0.2066, 0.1271 and 0.2262 percent, so the bound has room for the
  # sampling noise of a different draw without being loose enough to pass a
  # port that models something else: before the second imputation step was
  # written the same three sat 1.1, 1.9 and 3.8 percent apart.
  #
  # Those three figures are the step's and not one run's. Until 2026-09-18 they
  # were one run's: the port drew in whatever row order DuckDB returned and
  # summed each leave-one-out group in whatever order its threads finished, so a
  # seeded regeneration moved the gaps by a factor of six and once failed this
  # bound. impute_wages() now sorts every cell before it draws and orders those
  # sums, and five seeded regenerations gave wage_imp identical on all 505,050
  # rows.
  query <- siab_reference_query("10_wages_imputation")

  stats <- query(
    "SELECT count(*) AS n,
            median(stata.wage_imp) AS stata_median,
            median(r.wage_imp) AS r_median,
            quantile_cont(stata.wage_imp, 0.9) AS stata_p90,
            quantile_cont(r.wage_imp, 0.9) AS r_p90,
            avg(stata.wage_imp) AS stata_mean,
            avg(r.wage_imp) AS r_mean
     FROM stata JOIN r USING (persnr, spell, begepi)
     WHERE stata.cens = 1
       AND stata.wage_imp IS NOT NULL AND r.wage_imp IS NOT NULL"
  )

  expect_gt(stats$n, 10000)

  for (statistic in c("median", "p90", "mean")) {
    reference <- stats[[paste0("stata_", statistic)]]
    port      <- stats[[paste0("r_", statistic)]]
    expect_lt(abs(port - reference) / reference, 0.01,
              label = paste0("relative gap in the ", statistic))
  }
})
