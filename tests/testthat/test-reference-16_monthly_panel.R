# build_monthly_panel() against 16_monthly_panel.do.
#
# The reference totals days and earnings over each calendar year, cuts every
# episode into one row per calendar month, keeps the row whose month contains
# the 15th inside the episode, and trims the four duration counters so they end
# on the 15th rather than at the episode's own end.
#
# It is an alternative to 16_yearly_panel.do, not a step after it: both start
# from the parallel-episode data and the reference master calls neither. Both
# halves of this comparison therefore come from a second run over that data.
#
# The reference drops seven columns on its way out: nspell, begorig, endorig,
# begepi, endepi, begepi_orig and endepi_orig. The port keeps all seven, so the
# two halves join on the person, the year and the monthly episode start, and the
# year is `jahr` on the Stata side and `year` on the R side.

step_key <- c(persnr = "persnr", jahr = "year", begepi_monthly = "begepi_monthly")

test_that("both halves hold the same person-months", {
  query <- siab_reference_query("16_monthly_panel")

  counts <- query(
    "SELECT (SELECT count(*) FROM stata) AS n_stata,
            (SELECT count(*) FROM r) AS n_r,
            (SELECT count(*) FROM (SELECT DISTINCT persnr, jahr, begepi_monthly FROM stata)) AS distinct_stata,
            (SELECT count(*) FROM (SELECT persnr, jahr, begepi_monthly FROM stata
                                   EXCEPT
                                   SELECT persnr, year, begepi_monthly FROM r)) AS only_stata,
            (SELECT count(*) FROM (SELECT persnr, year, begepi_monthly FROM r
                                   EXCEPT
                                   SELECT persnr, jahr, begepi_monthly FROM stata)) AS only_r"
  )

  expect_equal(counts$n_stata, counts$n_r)
  expect_equal(counts$distinct_stata, counts$n_stata)
  expect_equal(counts$only_stata, 0L)
  expect_equal(counts$only_r, 0L)
})

test_that("the monthly episode bounds and the month itself match the reference", {
  query <- siab_reference_query("16_monthly_panel")

  for (column in c("endepi_monthly", "month_num")) {
    diff <- siab_column_diff(query, column, key = step_key)
    expect_equal(diff$differing, 0L,
                 info = paste0(column, " differs on ", diff$differing,
                               " of ", diff$shared, " shared person-months"))
  }
})

# The reference generates a second year column from the month it builds, beside
# the `jahr` it inherits from the episode start. 01_split_episodes.do cuts every
# episode at the year boundary, so the two can never disagree, which is why the
# port carries one column for both.
test_that("the year of the month is the year of the episode", {
  query <- siab_reference_query("16_monthly_panel")

  diff <- siab_column_diff(query, "year", r_column = "year", key = step_key)
  expect_equal(diff$differing, 0L)

  own <- query("SELECT count(*) FILTER (WHERE year IS DISTINCT FROM jahr) AS n FROM stata")$n
  expect_equal(own, 0L)
})

test_that("the yearly totals match the reference exactly", {
  query <- siab_reference_query("16_monthly_panel")

  for (column in c("year_days_emp", "year_days_benefits")) {
    diff <- siab_column_diff(query, column, key = step_key)
    expect_equal(diff$differing, 0L,
                 info = paste0(column, " differs on ", diff$differing,
                               " of ", diff$shared, " shared person-months"))
  }
})

test_that("the four trimmed duration counters match the reference exactly", {
  query <- siab_reference_query("16_monthly_panel")

  for (column in c("tage_bet", "tage_job", "tage_erw", "tage_lst")) {
    diff <- siab_column_diff(query, column, key = step_key)
    expect_equal(diff$differing, 0L,
                 info = paste0(column, " differs on ", diff$differing,
                               " of ", diff$shared, " shared person-months"))
  }
})

# The employment counter is trimmed for every employment episode except the
# trainee statuses, and Stata's inlist() reads a missing erwstat as "not one of
# these", so an episode with no status recorded is trimmed too. The port has to
# coalesce the comparison to keep that reading, as build_yearly_panel() does.
test_that("an episode with no employment status still has its counter trimmed", {
  query <- siab_reference_query("16_monthly_panel")

  no_status <- query(
    "SELECT count(*) AS n,
            count(*) FILTER (WHERE stata.tage_erw IS NULL) AS stata_null,
            count(*) FILTER (WHERE r.tage_erw IS NULL) AS r_null
     FROM stata JOIN r ON stata.persnr = r.persnr AND stata.jahr = r.year
                       AND stata.begepi_monthly = r.begepi_monthly
     WHERE stata.quelle = 1 AND stata.erwstat IS NULL"
  )

  expect_gt(no_status$n, 0L)
  expect_equal(no_status$r_null, no_status$stata_null)
})

# 16_monthly_panel.do trims tage_lst in two separate `replace` statements, one
# on quelle and one on parallel_benefits, where 16_yearly_panel.do joins both
# conditions in a single one. 15_parallel_episodes.do sets parallel_benefits for
# every episode of a person and episode start that includes a benefit episode,
# the benefit episode itself included, so a surviving LeH row meets both
# conditions and the reference subtracts the overhang from it twice. The port
# reproduces that, and the exact match on tage_lst above is what proves the
# reference behaves this way: trimming once would put every such row out by the
# overhang. test-09b_monthly_panel.R pins the same arithmetic on a small
# synthetic table, without a fixture.

# year_labor_earn multiplies the summed imputed wage by the days employed, so it
# inherits the random draws behind that wage and is bounded as a distribution,
# the same way it is at the yearly panel.
test_that("yearly labour earnings agree in distribution within one percent", {
  query <- siab_reference_query("16_monthly_panel")

  m <- siab_column_moments(query, "year_labor_earn", key = step_key)

  expect_equal(m$r_mean, m$stata_mean, tolerance = 0.01)
  expect_equal(m$r_q25, m$stata_q25, tolerance = 0.01)
  expect_equal(m$r_q75, m$stata_q75, tolerance = 0.01)
})

# Every surviving row describes one month of one episode: its bounds lie inside
# that month, inside the episode, and they straddle the 15th. The R half still
# carries the episode dates the reference drops, which is what makes the second
# half of this checkable at all.
test_that("every surviving R row is one month of its episode and covers the 15th", {
  query <- siab_reference_query("16_monthly_panel")

  outside <- query(
    "SELECT count(*) FILTER (WHERE begepi_monthly < month
                                OR endepi_monthly > last_day(month)) AS beyond_month,
            count(*) FILTER (WHERE begepi_monthly < begepi
                                OR endepi_monthly > endepi) AS beyond_episode,
            count(*) FILTER (WHERE day(begepi_monthly) > 15
                                OR day(endepi_monthly) < 15) AS missing_the_15th
     FROM r"
  )

  expect_equal(outside$beyond_month, 0L)
  expect_equal(outside$beyond_episode, 0L)
  expect_equal(outside$missing_the_15th, 0L)
})
