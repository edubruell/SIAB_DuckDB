# generate_biographic_variables() against 03_SIAB_bio.do.
#
# All nine columns are compared exactly, with no tolerance: every one of them is
# a date or a count of days, so there is no arithmetic that could round
# differently on the two sides.
#
# Two of them, anz_lst and tage_lst, only became comparable once the reference
# step was given an explicit tie-break. It takes running totals in `spell`
# order, but after the episode split a spell that crosses a year boundary is one
# row per year and all of them keep the same spell number, so Stata was free to
# shuffle. See the header of make_fixtures.do.

bio_columns <- c(
  "azubi",     # apprenticeship flag
  "ein_erw",   # first day in employment
  "tage_erw",  # days in employment so far
  "ein_bet",   # first day in the establishment
  "tage_bet",  # days in the establishment so far
  "ein_job",   # first day in the job
  "tage_job",  # days in the job so far
  "anz_lst",   # number of benefit receipts so far
  "tage_lst"   # days of benefit receipt so far
)

for (column in bio_columns) {
  local({
    this <- column
    test_that(paste(this, "matches the reference exactly"), {
      query <- siab_reference_query("03_SIAB_bio")

      diff <- siab_column_diff(query, this)
      expect_equal(diff$differing, 0L,
                   info = paste0(this, " differs on ", diff$differing,
                                 " of ", diff$shared, " shared rows"))
    })
  })
}

test_that("the establishment and job columns are empty outside the employment history", {
  # 03_SIAB_bio.do blanks ein_bet, tage_bet, ein_job and tage_job wherever
  # quelle is not 1, so a benefit spell carries no establishment history. The
  # port did not do this at first, which was most of the ein_bet mismatch.
  query <- siab_reference_query("03_SIAB_bio")

  kept <- query(
    "SELECT count(*) AS n FROM r JOIN stata USING (persnr, spell, begepi)
     WHERE stata.ein_bet IS NULL AND (r.ein_bet IS NOT NULL OR
                                      r.tage_bet IS NOT NULL OR
                                      r.ein_job IS NOT NULL OR
                                      r.tage_job IS NOT NULL)")
  expect_equal(kept$n, 0L)
})
