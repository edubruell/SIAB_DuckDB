# build_yearly_panel() against 16_yearly_panel.do.
#
# The reference totals days and earnings over each calendar year, keeps the one
# episode of that year covering 30 June, and trims the four duration counters so
# they end at the cutoff rather than at the episode's own end.
#
# It also drops seven columns on its way out: nspell, begorig, endorig, begepi,
# endepi, begepi_orig and endepi_orig. The port keeps all seven, so the two
# halves join on the person and the year alone, and the year is `jahr` on the
# Stata side and `year` on the R side.

step_key <- c(persnr = "persnr", jahr = "year")

test_that("both halves keep one episode per person and year, the same ones", {
  query <- siab_reference_query("16_yearly_panel")

  counts <- query(
    "SELECT (SELECT count(*) FROM stata) AS n_stata,
            (SELECT count(*) FROM r) AS n_r,
            (SELECT count(*) FROM (SELECT DISTINCT persnr, jahr FROM stata)) AS distinct_stata,
            (SELECT count(*) FROM (SELECT persnr, jahr FROM stata
                                   EXCEPT
                                   SELECT persnr, year FROM r)) AS only_stata,
            (SELECT count(*) FROM (SELECT persnr, year FROM r
                                   EXCEPT
                                   SELECT persnr, jahr FROM stata)) AS only_r"
  )

  expect_equal(counts$n_stata, counts$n_r)
  expect_equal(counts$distinct_stata, counts$n_stata)
  expect_equal(counts$only_stata, 0L)
  expect_equal(counts$only_r, 0L)
})

test_that("the yearly totals match the reference exactly", {
  query <- siab_reference_query("16_yearly_panel")

  for (column in c("year_days_emp", "year_days_benefits")) {
    diff <- siab_column_diff(query, column, key = step_key)
    expect_equal(diff$differing, 0L,
                 info = paste0(column, " differs on ", diff$differing,
                               " of ", diff$shared, " shared person-years"))
  }
})

test_that("the four trimmed duration counters match the reference exactly", {
  query <- siab_reference_query("16_yearly_panel")

  for (column in c("tage_bet", "tage_job", "tage_erw", "tage_lst")) {
    diff <- siab_column_diff(query, column, key = step_key)
    expect_equal(diff$differing, 0L,
                 info = paste0(column, " differs on ", diff$differing,
                               " of ", diff$shared, " shared person-years"))
  }
})

# The employment counter is trimmed for every employment episode except the
# trainee statuses, and Stata's inlist() reads a missing erwstat as "not one of
# these", so an episode with no status recorded is trimmed too. SQL's NOT IN
# returns NULL there, which a plain if_else() turns into a missing counter: 966
# rows of the test data before the port coalesced the comparison.
test_that("an episode with no employment status still has its counter trimmed", {
  query <- siab_reference_query("16_yearly_panel")

  no_status <- query(
    "SELECT count(*) AS n,
            count(*) FILTER (WHERE stata.tage_erw IS NULL) AS stata_null,
            count(*) FILTER (WHERE r.tage_erw IS NULL) AS r_null
     FROM stata JOIN r ON stata.persnr = r.persnr AND stata.jahr = r.year
     WHERE stata.quelle = 1 AND stata.erwstat IS NULL"
  )

  expect_gt(no_status$n, 0L)
  expect_equal(no_status$r_null, no_status$stata_null)
})

# year_labor_earn multiplies the summed imputed wage by the days employed, so it
# inherits the random draws behind that wage and is bounded as a distribution,
# the same way parallel_wage_imp is at step 15.
test_that("yearly labour earnings agree in distribution within one percent", {
  query <- siab_reference_query("16_yearly_panel")

  m <- siab_column_moments(query, "year_labor_earn", key = step_key)

  expect_equal(m$r_mean, m$stata_mean, tolerance = 0.01)
  expect_equal(m$r_q25, m$stata_q25, tolerance = 0.01)
  expect_equal(m$r_q75, m$stata_q75, tolerance = 0.01)
})

# The episodes the step keeps are the ones running across 30 June, so every
# surviving episode must have started on or before it and ended on or after it.
# The R half still carries the episode dates the reference drops, which is what
# makes this checkable at all.
test_that("every surviving R episode covers the cutoff date", {
  query <- siab_reference_query("16_yearly_panel")

  outside <- query(
    "SELECT count(*) AS n FROM r
     WHERE NOT (100 * month(begepi) + day(begepi) <= 630
                AND 630 <= 100 * month(endepi) + day(endepi))"
  )$n

  expect_equal(outside, 0L)
})
