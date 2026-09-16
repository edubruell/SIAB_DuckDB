# The reference steps that have no R counterpart yet.
#
# Each of these has a committed Stata fixture under tests/testthat/fixtures/,
# so the comparison is ready to run the moment the step is ported. Until then
# the test skips with the reason, which makes the gap show up on every run
# instead of living only in a note.
#
# The master's source restriction also explains why the R output is a strict
# superset of the reference: it carries 615,670 rows against 505,050.

test_that("the source restriction from the master do-file is ported", {
  skip(paste(
    "Not ported: 00_master_SIAB.do keeps only quelle 1, 2 and 3, the employment",
    "history. The R pipeline keeps all seven sources, which is why its output is",
    "a strict superset of the reference on the test data."
  ))
})

test_that("09_restrictions.do is ported", {
  skip(paste(
    "Not ported: 09_restrictions.do cuts the sample to certain groups and takes",
    "it from 505,050 rows to 83,817. The reference README calls this step",
    "project-specific rather than part of the reusable prep, so porting it is a",
    "design decision, not only a translation."
  ))
})
