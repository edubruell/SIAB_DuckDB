# The reference steps that have no R counterpart yet.
#
# Each of these has a committed Stata fixture under tests/testthat/fixtures/,
# so the comparison is ready to run the moment the step is ported. Until then
# the test skips with the reason, which makes the gap show up on every run
# instead of living only in a note.
#
# 13_industries_1digit.do was ported on 2026-09-16, into
# generate_industry_variables(); its comparison lives in
# test-reference-13_industries.R.
#
# The master's source restriction was ported on 2026-09-16, so the R output is
# no longer a superset: both sides carry 505,050 rows on the test data. The
# row-set equality is checked in test-reference-01_split_episodes.R.

test_that("09_restrictions.do is ported", {
  skip(paste(
    "Not ported: 09_restrictions.do cuts the sample to certain groups and takes",
    "it from 505,050 rows to 83,817. The reference README calls this step",
    "project-specific rather than part of the reusable prep, so porting it is a",
    "design decision, not only a translation."
  ))
})

# 17_clean_up.do needs no counterpart and no fixture. Its three working lines
# are a sort, an xtset and a compress: none of them changes a value, and the R
# pipeline's output is a database table rather than a Stata dataset, so there is
# nothing for the port to reproduce. tests/fixtures/make_fixtures.do does not
# run it.
