# merge_annual_bhp() against 11_merge_BHP.do.
#
# The reference merges five files onto the SIAB, all many-to-one on betnr and
# jahr, all keeping master and matched rows: the yearly establishment variable
# blocks, one file per calendar year, and the four extension files for worker
# inflow, worker outflow, establishment entry and establishment exit.
#
# The step is off by default in 00_master_SIAB.do because every one of the five
# has to be requested from the FDZ separately. The FDZ test data carries all of
# them, so the fixture run turns all five switches on.
#
# The R port does not reproduce the year-by-year loop. Each yearly file holds
# exactly one calendar year and is unique on betnr, so a SIAB episode can match
# at most one of the forty-nine, and joining once against all of them stacked
# gives the same answer. The first test below is what makes that claim testable
# rather than asserted.

stata_columns <- c("az_f", "az_reg", "az_azubi", "az_atz", "az_tz",
                   "az_f_vz", "az_f_tz", "az_reg_vz")

flow_columns <- c("ein_ges", "ein_gf", "ein_vz",
                  "aus_ges", "aus_gf", "aus_vz")

entry_exit_columns <- c("eintritt", "besch", "besch_vor", "status_vor",
                        "inflow", "austritt", "besch_nach", "status_nach",
                        "outflow")

test_that("the yearly establishment blocks match the reference exactly", {
  query <- siab_reference_query("11_merge_BHP")

  for (column in stata_columns) {
    diff <- siab_column_diff(query, column)
    expect_equal(diff$differing, 0L,
                 info = paste0(column, " differs on ", diff$differing,
                               " of ", diff$shared, " shared rows"))
  }
})

test_that("the worker-flow columns match the reference exactly", {
  query <- siab_reference_query("11_merge_BHP")

  for (column in flow_columns) {
    diff <- siab_column_diff(query, column)
    expect_equal(diff$differing, 0L,
                 info = paste0(column, " differs on ", diff$differing,
                               " of ", diff$shared, " shared rows"))
  }
})

test_that("the entry and exit columns match the reference exactly", {
  query <- siab_reference_query("11_merge_BHP")

  for (column in entry_exit_columns) {
    diff <- siab_column_diff(query, column)
    expect_equal(diff$differing, 0L,
                 info = paste0(column, " differs on ", diff$differing,
                               " of ", diff$shared, " shared rows"))
  }
})

# `besch` is carried by both the entry file and the exit file, and both merges
# run with `update`, which fills a missing value in the master from the using
# file and leaves a non-missing one alone. Entry runs first, so a row that both
# files describe keeps the entry value. This is the one place in the step where
# the option changes the answer, and a port that used a plain left join for the
# exit merge would overwrite instead of fill.
test_that("besch is filled by the exit merge and not overwritten by it", {
  query <- siab_reference_query("11_merge_BHP")

  filled <- query(
    "SELECT count(*) FILTER (WHERE besch IS NOT NULL) AS with_besch,
            count(*) FILTER (WHERE eintritt IS NOT NULL) AS with_entry
     FROM stata"
  )

  expect_gt(filled$with_besch, filled$with_entry)

  diff <- siab_column_diff(query, "besch")
  expect_equal(diff$differing, 0L)
})

test_that("the merge loses no reference row", {
  query <- siab_reference_query("11_merge_BHP")

  only_stata <- query(
    "SELECT count(*) AS n FROM (SELECT persnr, spell, begepi FROM stata
                                EXCEPT
                                SELECT persnr, spell, begepi FROM r)"
  )$n

  expect_equal(only_stata, 0L)
})

test_that("only employment episodes match an establishment file", {
  query <- siab_reference_query("11_merge_BHP")

  # quelle 2 and 3 are benefit and job-search episodes: they carry no
  # establishment number, so nothing in this step can reach them. A port that
  # matched any of them would have joined on the wrong key.
  stray <- query(
    "SELECT count(*) AS n FROM r WHERE az_f IS NOT NULL AND quelle <> 1"
  )$n

  expect_equal(stray, 0L)
})
