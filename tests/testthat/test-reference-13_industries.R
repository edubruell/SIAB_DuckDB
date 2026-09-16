# generate_industry_variables() against 13_industries_1digit.do.
#
# The reference maps the time-consistent three-digit industry `w93_3_gen` onto
# two one-digit classifications, the Statistisches Bundesamt's and the IAB
# establishment panel's, and leaves a code that falls in no range missing.
#
# The two sides reach the columns from different positions in the pipeline. The
# reference builds them at step 13, after the AKM merge; the port builds them in
# generate_industry_variables(), directly after merge_basic_bhp(), which is the
# step that brings w93_3_gen in. Nothing between the two positions touches
# w93_3_gen, which is what makes the comparison meaningful, and the last test
# below is what makes that claim testable.

test_that("the Statistisches Bundesamt industry matches the reference exactly", {
  query <- siab_reference_query("13_industries_1digit")

  diff <- siab_column_diff(query, "industry1_destatis")
  expect_equal(diff$shared, 505050L)
  expect_equal(diff$differing, 0L)
})

test_that("the IAB establishment panel industry matches the reference exactly", {
  query <- siab_reference_query("13_industries_1digit")

  diff <- siab_column_diff(query, "industry1_estpanel")
  expect_equal(diff$shared, 505050L)
  expect_equal(diff$differing, 0L)
})

# An episode with no establishment carries no industry, and on the test data
# that is most of the non-employment spells. Both sides have to leave the two
# categories missing there rather than assign a code.
test_that("an episode with no industry carries no category on either side", {
  query <- siab_reference_query("13_industries_1digit")

  no_industry <- query(
    "SELECT count(*) AS n,
            count(*) FILTER (WHERE stata.industry1_destatis IS NULL) AS stata_destatis,
            count(*) FILTER (WHERE r.industry1_destatis IS NULL) AS r_destatis,
            count(*) FILTER (WHERE stata.industry1_estpanel IS NULL) AS stata_estpanel,
            count(*) FILTER (WHERE r.industry1_estpanel IS NULL) AS r_estpanel
     FROM stata JOIN r USING (persnr, spell, begepi)
     WHERE stata.w93_3_gen IS NULL"
  )

  expect_gt(no_industry$n, 0L)
  expect_equal(no_industry$stata_destatis, no_industry$n)
  expect_equal(no_industry$r_destatis, no_industry$n)
  expect_equal(no_industry$stata_estpanel, no_industry$n)
  expect_equal(no_industry$r_estpanel, no_industry$n)
})

test_that("w93_3_gen itself is untouched between the two positions", {
  query <- siab_reference_query("13_industries_1digit")

  diff <- siab_column_diff(query, "w93_3_gen")
  expect_equal(diff$differing, 0L)
})

test_that("no row is lost or duplicated by the two mappings", {
  query <- siab_reference_query("13_industries_1digit")

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
