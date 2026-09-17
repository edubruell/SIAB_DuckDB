# handle_parallel_episodes() against 15_parallel_episodes.do.
#
# The reference aggregates over the episodes a person has running at the same
# start date, keeps one of them, and numbers what is left. It defines the kept
# one by a sort: source first, then longest tenure, then highest imputed wage.
# The R port takes that rule as handling = "tenure"; its other setting sorts on
# the imputed wage first, and because both sides draw their own random terms for
# a censored wage, that setting would make the two halves keep different rows.
# tests/fixtures/make_r_dumps.R therefore calls the port with "tenure".
#
# Both sides carry `spell` as the last sort key, which the reference's own
# comment beside the line asks for and which
# 15_parallel_episodes_tiebreak.patch supplies. Without it 1,945
# of the 479,806 person-episode groups are tied on everything the sort looks at
# and the kept row is arbitrary on each side separately.
#
# The step drops `spell` on its way out, so the two halves join on the person
# and the episode start alone.

step_key <- c("persnr", "begepi")

test_that("the two halves keep the same set of episodes", {
  query <- siab_reference_query("15_parallel_episodes")

  counts <- query(
    "SELECT (SELECT count(*) FROM stata) AS n_stata,
            (SELECT count(*) FROM r) AS n_r,
            (SELECT count(*) FROM (SELECT persnr, begepi FROM stata
                                   EXCEPT
                                   SELECT persnr, begepi FROM r)) AS only_stata,
            (SELECT count(*) FROM (SELECT persnr, begepi FROM r
                                   EXCEPT
                                   SELECT persnr, begepi FROM stata)) AS only_r"
  )

  expect_equal(counts$n_stata, counts$n_r)
  expect_equal(counts$only_stata, 0L)
  expect_equal(counts$only_r, 0L)
})

# The selection is settled by quelle and tage_bet, so a mismatch on either is a
# mismatch on which row of a parallel group survived, not on a value.
test_that("the same row of each parallel group survives on both sides", {
  query <- siab_reference_query("15_parallel_episodes")

  for (column in c("quelle", "tage_bet")) {
    diff <- siab_column_diff(query, column, key = step_key)
    expect_equal(diff$differing, 0L,
                 info = paste0(column, " differs on ", diff$differing,
                               " of ", diff$shared, " shared episodes"))
  }
})

test_that("the counts and the benefit indicator match the reference exactly", {
  query <- siab_reference_query("15_parallel_episodes")

  for (column in c("parallel_jobs", "parallel_benefits", "nspell")) {
    diff <- siab_column_diff(query, column, key = step_key)
    expect_equal(diff$differing, 0L,
                 info = paste0(column, " differs on ", diff$differing,
                               " of ", diff$shared, " shared episodes"))
  }
})

# parallel_wage sums the reported wage over a group. Stata's egen total() stores
# its result as a float, which carries about seven decimal digits, so the two
# sides agree to that and no further.
test_that("the summed reported wage matches to Stata float precision", {
  query <- siab_reference_query("15_parallel_episodes")

  diff <- siab_column_diff(query, "parallel_wage", tolerance = 1e-6,
                           key = step_key)
  expect_equal(diff$differing, 0L)
})

# parallel_wage_imp sums the imputed wage instead. Every censored wage on either
# side carries a random term drawn from that side's own generator, so the column
# cannot agree row by row and is bounded as a distribution, the same way
# wage_imp itself is at step 10.
test_that("the summed imputed wage agrees in distribution within one percent", {
  query <- siab_reference_query("15_parallel_episodes")

  m <- siab_column_moments(query, "parallel_wage_imp", key = step_key)

  expect_equal(m$r_mean, m$stata_mean, tolerance = 0.01)
  expect_equal(m$r_q25, m$stata_q25, tolerance = 0.01)
  expect_equal(m$r_q75, m$stata_q75, tolerance = 0.01)
})

# The aggregate counts only employment episodes, so a person with none in a
# group has to come out at zero rather than missing: Stata's egen total()
# treats a missing contribution as nothing at all.
test_that("a group with no employment episode gets zero, not missing", {
  query <- siab_reference_query("15_parallel_episodes")

  nulls <- query(
    "SELECT count(*) FILTER (WHERE stata.parallel_jobs IS NULL) AS stata_null,
            count(*) FILTER (WHERE r.parallel_jobs IS NULL) AS r_null,
            count(*) FILTER (WHERE r.parallel_jobs = 0) AS r_zero
     FROM stata JOIN r ON stata.persnr = r.persnr AND stata.begepi = r.begepi"
  )

  expect_equal(nulls$stata_null, 0L)
  expect_equal(nulls$r_null, 0L)
  expect_gt(nulls$r_zero, 0L)
})
