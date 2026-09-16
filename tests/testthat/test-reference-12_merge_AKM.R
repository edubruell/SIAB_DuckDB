# merge_akm() against 12_merge_AKM.do.
#
# The reference merges two files onto the SIAB, the establishment wage effects
# many-to-one on betnr and the person wage effects many-to-one on persnr, both
# keeping master and matched rows. Neither file carries a year: an effect is
# estimated once per window, so the five columns of each side arrive together.
#
# NEITHER SIDE OF THIS COMPARISON USES REAL AKM EFFECTS. No FDZ test product
# carries them, so tests/fixtures/make_synth_akm.do fabricates both files from
# the shape the FDZ methodology report describes, and the Stata reference run
# and the R pipeline then read the same two fabricated files. The values are
# normal draws with the documented dispersion and nothing else.
#
# That is enough to compare the step, because the step is a merge: what it
# decides is which episode receives which row, and that is settled by the keys.
# It is not enough to say anything about AKM effects, wages, or merge rates on
# the real delivery, and no test here should be read as doing so.

estab_columns <- c("feff_1985_1992", "feff_1993_2000", "feff_2001_2008",
                   "feff_2009_2016", "feff_2017_2023")

person_columns <- c("peff_1985_1992", "peff_1993_2000", "peff_2001_2008",
                    "peff_2009_2016", "peff_2017_2023")

test_that("the establishment effects match the reference exactly", {
  query <- siab_reference_query("12_merge_AKM")

  for (column in estab_columns) {
    diff <- siab_column_diff(query, column)
    expect_equal(diff$differing, 0L,
                 info = paste0(column, " differs on ", diff$differing,
                               " of ", diff$shared, " shared rows"))
  }
})

test_that("the person effects match the reference exactly", {
  query <- siab_reference_query("12_merge_AKM")

  for (column in person_columns) {
    diff <- siab_column_diff(query, column)
    expect_equal(diff$differing, 0L,
                 info = paste0(column, " differs on ", diff$differing,
                               " of ", diff$shared, " shared rows"))
  }
})

# The merge keeps master and matched rows, so an episode whose establishment is
# outside the connected set keeps every effect missing. Coverage is below 100
# per cent by construction in the fabricated files, and a port that dropped the
# non-matching rows, or filled them with zero, would pass a column comparison on
# the matched rows alone and fail here.
test_that("episodes that match neither file keep missing effects", {
  query <- siab_reference_query("12_merge_AKM")

  counts <- query(
    "SELECT count(*) AS n,
            count(*) FILTER (WHERE feff_2001_2008 IS NULL) AS no_estab,
            count(*) FILTER (WHERE peff_2001_2008 IS NULL) AS no_person
     FROM r"
  )

  expect_gt(counts$no_estab, 0L)
  expect_gt(counts$no_person, 0L)

  same <- query(
    "SELECT count(*) AS n
     FROM stata JOIN r USING (persnr, spell, begepi)
     WHERE (stata.feff_2001_2008 IS NULL) <> (r.feff_2001_2008 IS NULL)
        OR (stata.peff_2001_2008 IS NULL) <> (r.peff_2001_2008 IS NULL)"
  )$n

  expect_equal(same, 0L)
})

test_that("the merge loses no reference row", {
  query <- siab_reference_query("12_merge_AKM")

  only_stata <- query(
    "SELECT count(*) AS n FROM (SELECT persnr, spell, begepi FROM stata
                                EXCEPT
                                SELECT persnr, spell, begepi FROM r)"
  )$n

  expect_equal(only_stata, 0L)
})
