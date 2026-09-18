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
# 16_monthly_panel.do was ported on 2026-09-16, into build_monthly_panel(); its
# comparison lives in test-reference-16_monthly_panel.R. It is an alternative to
# 16_yearly_panel.do rather than a step after it, so both fixtures are taken
# from separate runs over the step 15 data.
#
# The master's source restriction was ported on 2026-09-16, so the R output is
# no longer a superset: both sides carry 505,050 rows on the test data. The
# row-set equality is checked in test-reference-01_split_episodes.R.
#
# The master's loop over all-missing variables was ported on 2026-09-17, into
# drop_empty_columns(). It changes no value, only the column set, so it has no
# fixture comparison; its tests live in test-00b_drop_empty_columns.R.

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
# nothing for the port to reproduce. make_fixtures.do does not
# run it.
