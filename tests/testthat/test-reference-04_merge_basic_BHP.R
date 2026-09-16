# merge_basic_bhp() against 04_merge_basic_BHP.do.
#
# The reference merges the Basic Establishment File onto the SIAB on betnr and
# jahr, keeping master and matched rows. Two of its variables are used
# downstream: the establishment's federal state ao_bula, which decides the
# Rechtskreis, and the generated 3-digit industry w93_3_gen.

test_that("ao_bula and w93_3_gen match the reference exactly", {
  query <- siab_reference_query("04_merge_basic_BHP")

  for (column in c("ao_bula", "w93_3_gen")) {
    diff <- siab_column_diff(query, column)
    expect_equal(diff$differing, 0L,
                 info = paste0(column, " differs on ", diff$differing,
                               " of ", diff$shared, " shared rows"))
  }
})

test_that("the merge loses no reference row", {
  query <- siab_reference_query("04_merge_basic_BHP")

  only_stata <- query(
    "SELECT count(*) AS n FROM (SELECT persnr, spell, begepi FROM stata
                                EXCEPT
                                SELECT persnr, spell, begepi FROM r)"
  )$n

  expect_equal(only_stata, 0L)
})
