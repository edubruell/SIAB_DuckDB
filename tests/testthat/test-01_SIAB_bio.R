# Tests for generate_biographic_variables() in R/functions/01_SIAB_bio_modified.R.
#
# The reference comparison in test-reference-03_SIAB_bio.R is the strong test:
# it checks all nine columns against a Stata run over the FDZ test data. These
# cases are the small ones, each pinning a rule that the port had wrong before
# the comparison found it, so a future edit fails here with a readable message
# rather than as a row count in a fixture diff.

bio_frame <- function(...) {
  out <- data.frame(...)
  defaults <- list(
    persnr  = 1L,
    betnr   = 7L,
    quelle  = 1L,
    grund   = 30L,
    erwstat = 101L
  )
  for (nm in names(defaults)) {
    if (is.null(out[[nm]])) out[[nm]] <- defaults[[nm]]
  }
  if (is.null(out$begorig)) out$begorig <- out$begepi
  out$spell <- seq_len(nrow(out))
  out
}

test_that("azubi is 0, not missing, when erwstat is missing", {
  # 03_SIAB_bio.do writes inlist(erwstat, ...), and Stata's inlist() on a
  # missing value is 0. A plain `%in%` in SQL gives NULL instead, which left
  # azubi missing on 1,809 rows of the test data and poisoned emp with it.
  connection <- siab_db(bio_frame(
    begepi  = as.Date(c("2000-01-01", "2000-02-01", "2000-03-01")),
    endepi  = as.Date(c("2000-01-31", "2000-02-28", "2000-03-31")),
    erwstat = c(102L, NA_integer_, 101L)
  ))

  quietly_run(generate_biographic_variables(connection))
  out <- siab_collect(connection, "spell")

  expect_equal(out$azubi, c(1L, 0L, 0L))
})

test_that("ein_erw is the person's first employment and reaches every row", {
  # The reference assigns it with egen max() over the whole person, so a
  # benefit spell carries the same date as an employment spell. The port used
  # to group by employment status as well, which gave the benefit rows the
  # earliest date of the benefit spells instead.
  connection <- siab_db(bio_frame(
    begepi  = as.Date(c("1998-01-01", "2000-01-01", "2002-01-01")),
    endepi  = as.Date(c("1998-12-31", "2000-12-31", "2002-12-31")),
    quelle  = c(1L, 1L, 2L),
    erwstat = c(102L, 101L, NA_integer_)
  ))

  quietly_run(generate_biographic_variables(connection))
  out <- siab_collect(connection, "spell")

  # The 1998 spell is vocational training, so employment starts in 2000.
  expect_equal(out$ein_erw, rep(as.Date("2000-01-01"), 3))
})

test_that("ein_bet counts only employment-history spells", {
  # 03_SIAB_bio.do takes the minimum over the rows with quelle == 1 and a
  # known establishment. Taking it over every row of the establishment would
  # date the entry from a spell that is not employment at all.
  connection <- siab_db(bio_frame(
    begepi = as.Date(c("1998-01-01", "2000-01-01")),
    endepi = as.Date(c("1998-12-31", "2000-12-31")),
    quelle = c(2L, 1L)
  ))

  quietly_run(generate_biographic_variables(connection))
  out <- siab_collect(connection, "spell")

  expect_true(is.na(out$ein_bet[1]))
  expect_equal(out$ein_bet[2], as.Date("2000-01-01"))
})

test_that("ein_bet dates the entry from begorig, not from the split episode", {
  # A spell that runs over a year boundary is two rows after step 01, and the
  # second one starts on 1 January. begorig still holds the real start.
  connection <- siab_db(bio_frame(
    begepi  = as.Date(c("1999-06-01", "2000-01-01")),
    endepi  = as.Date(c("1999-12-31", "2000-05-31")),
    begorig = as.Date(c("1999-06-01", "1999-06-01"))
  ))

  quietly_run(generate_biographic_variables(connection))
  out <- siab_collect(connection, "spell")

  expect_equal(out$ein_bet, rep(as.Date("1999-06-01"), 2))
})

test_that("the establishment and job columns are empty outside quelle 1", {
  connection <- siab_db(bio_frame(
    begepi = as.Date(c("2000-01-01", "2001-01-01")),
    endepi = as.Date(c("2000-12-31", "2001-12-31")),
    quelle = c(1L, 2L)
  ))

  quietly_run(generate_biographic_variables(connection))
  out <- siab_collect(connection, "spell")

  expect_false(any(is.na(c(out$ein_bet[1], out$ein_job[1]))))
  expect_true(all(is.na(c(out$ein_bet[2], out$tage_bet[2],
                          out$ein_job[2], out$tage_job[2]))))
})

test_that("a gap over 366 days starts a new job, a shorter one does not", {
  # grund 30 is not one of the reasons that report the end of employment, so
  # the 92-day rule does not apply and only the 366-day rule can break the job.
  connection <- siab_db(bio_frame(
    begepi = as.Date(c("2000-01-01", "2001-12-01")),
    endepi = as.Date(c("2000-12-31", "2002-11-30"))
  ))
  quietly_run(generate_biographic_variables(connection))
  same <- siab_collect(connection, "spell")
  expect_equal(same$ein_job, rep(as.Date("2000-01-01"), 2))

  connection <- siab_db(bio_frame(
    begepi = as.Date(c("2000-01-01", "2002-01-05")),
    endepi = as.Date(c("2000-12-31", "2002-12-31"))
  ))
  quietly_run(generate_biographic_variables(connection))
  broken <- siab_collect(connection, "spell")
  expect_equal(broken$ein_job,
               as.Date(c("2000-01-01", "2002-01-05")))
})

test_that("after an end-of-employment notice a gap over 92 days starts a new job", {
  # grund 130 is a deregistration for end of employment, so 93 days apart is
  # already a new job where 366 would otherwise be needed.
  connection <- siab_db(bio_frame(
    begepi = as.Date(c("2000-01-01", "2001-05-01")),
    endepi = as.Date(c("2000-12-31", "2001-12-31")),
    grund  = c(130L, 30L)
  ))
  quietly_run(generate_biographic_variables(connection))
  broken <- siab_collect(connection, "spell")
  expect_equal(broken$ein_job, as.Date(c("2000-01-01", "2001-05-01")))

  connection <- siab_db(bio_frame(
    begepi = as.Date(c("2000-01-01", "2001-03-01")),
    endepi = as.Date(c("2000-12-31", "2001-12-31")),
    grund  = c(130L, 30L)
  ))
  quietly_run(generate_biographic_variables(connection))
  same <- siab_collect(connection, "spell")
  expect_equal(same$ein_job, rep(as.Date("2000-01-01"), 2))
})

test_that("tage_job counts a parallel episode once", {
  # Two spells with the same start and end at the same establishment are one
  # period of work reported twice, so only the first contributes its days.
  connection <- siab_db(bio_frame(
    begepi = as.Date(c("2000-01-01", "2000-01-01", "2000-02-01")),
    endepi = as.Date(c("2000-01-10", "2000-01-10", "2000-02-10"))
  ))

  quietly_run(generate_biographic_variables(connection))
  out <- siab_collect(connection, "spell")

  expect_equal(out$tage_job, c(10L, 10L, 20L))
  expect_equal(out$ein_job, rep(as.Date("2000-01-01"), 3))
})

test_that("a benefit receipt resuming within 10 days is not a new receipt", {
  connection <- siab_db(bio_frame(
    begepi = as.Date(c("2000-01-01", "2000-02-05", "2000-04-01")),
    endepi = as.Date(c("2000-01-31", "2000-02-28", "2000-04-30")),
    quelle = rep(2L, 3),
    betnr  = rep(NA_integer_, 3)
  ))

  quietly_run(generate_biographic_variables(connection))
  out <- siab_collect(connection, "spell")

  # Five days after the first receipt ends is the same receipt; 33 days after
  # the second ends is a new one.
  expect_equal(out$anz_lst, c(1L, 1L, 2L))
  expect_equal(out$tage_lst, c(31L, 55L, 85L))
})
