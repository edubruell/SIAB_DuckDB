# Tests for the small helpers in R/functions/00_common_functions.R.

test_that("%nin% is the negation of %in%", {
  expect_equal(c(1, 2, 3) %nin% c(2, 4), c(TRUE, FALSE, TRUE))
  expect_equal(character(0) %nin% "a", logical(0))
  expect_true(NA %nin% c(1, 2))
})

test_that("folder_reference_factory pastes a relative path onto its folder", {
  ref <- folder_reference_factory("/data/siab")
  expect_equal(ref("x.dta"), "/data/siab/x.dta")
  expect_equal(ref("sub", "x.dta"), "/data/siab/sub/x.dta")
  expect_equal(ref(), "/data/siab")
})

test_that("folder_reference_factory passes an absolute path through untouched", {
  ref <- folder_reference_factory("/data/siab")
  expect_equal(ref("/elsewhere/x.dta"), "/elsewhere/x.dta")
  expect_equal(ref("~/x.dta"), "~/x.dta")
  expect_equal(ref("C:/x.dta"), "C:/x.dta")
})

test_that("folder_reference_factory refuses a mix of absolute and relative", {
  ref <- folder_reference_factory("/data/siab")
  expect_error(
    ref(c("/absolute", "relative")),
    "Combination of absolute and relative paths not supported"
  )
})

test_that("folder_reference_factory refuses a NULL folder", {
  expect_error(folder_reference_factory(NULL), "Please set the path to a target_folder")
})

test_that("quick_year matches base R over fifty years of dates", {
  dates <- seq(as.Date("1975-01-01"), as.Date("2024-12-31"), by = "day")
  expect_equal(quick_year(dates), as.integer(format(dates, "%Y")))
})

test_that("quick_year gets the leap-day and year boundaries right", {
  edges <- as.Date(c("1970-01-01", "1971-12-31", "1972-02-29", "1972-12-31",
                     "1999-12-31", "2000-01-01", "2000-02-29", "2024-12-31"))
  expect_equal(quick_year(edges), as.integer(format(edges, "%Y")))
})

test_that("validate_inputs stops on the first failing predicate and is silent otherwise", {
  expect_error(
    validate_inputs(list("handling must be one of wage, drop" = FALSE)),
    "handling must be one of wage, drop"
  )
  expect_silent(validate_inputs(list("always true" = TRUE, "also true" = TRUE)))
})

test_that("compute_and_overwrite replaces the target table and leaves no temp behind", {
  connection <- siab_db(data.frame(persnr = 1:3, wage = c(10, 20, 30)))

  tbl(connection, "data") |>
    mutate(wage = wage * 2) |>
    compute_and_overwrite()

  expect_equal(siab_collect(connection, "persnr")$wage, c(20, 40, 60))
  expect_false(DBI::dbExistsTable(connection, "temp"))
})

test_that("compute_and_overwrite can write to a table other than data", {
  connection <- siab_db(data.frame(persnr = 1:3, wage = c(10, 20, 30)))

  tbl(connection, "data") |>
    filter(wage > 10) |>
    compute_and_overwrite("subset")

  expect_true(DBI::dbExistsTable(connection, "subset"))
  expect_equal(nrow(dbGetQuery(connection, "SELECT * FROM subset")), 2L)
  # The source table is untouched.
  expect_equal(nrow(siab_collect(connection)), 3L)
})

test_that("compute_and_overwrite refuses to run when a temp table is already there", {
  connection <- siab_db(data.frame(persnr = 1:3, wage = c(10, 20, 30)))
  DBI::dbWriteTable(connection, "temp", data.frame(x = 1))

  expect_error(
    tbl(connection, "data") |> compute_and_overwrite(),
    "Temporary table 'temp' allready exsists"
  )
})

test_that("stata_float rounds to single precision and leaves exact values alone", {
  expect_equal(stata_float(13.15), 13.149999618530273)
  expect_equal(stata_float(c(0, 1, 0.5, -2)), c(0, 1, 0.5, -2))
  expect_equal(stata_float(NA_real_), NA_real_)
  expect_equal(stata_float(integer(0)), numeric(0))
})

test_that("stata_float makes a threshold comparison agree with the reference", {
  # A daily wage of exactly 13.15 euro sits on the 2005 marginal threshold. The
  # reference stores that threshold as a Stata float, which is a hair below
  # 13.15, so the spell is not marginal there.
  expect_false(13.15 <= stata_float(13.15))
  expect_true(13.15 <= 13.15)
})

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#  What the two arms do to keep a store the size of what is in it
#
#  Every step rewrites the whole table, so the blocks the old copy held are
#  free as soon as it is dropped. DuckDB reuses free blocks only across a
#  checkpoint and never shrinks a file by itself, which is why
#  compute_and_overwrite() checkpoints and why a finished prep is copied into a
#  fresh file. The counterparts are tested in tests/pytest/test_common.py.
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# A file-backed database, which is the only kind that has blocks to reuse.
on_disk_db <- function(rows, env = parent.frame()) {
  path       <- withr::local_tempfile(fileext = ".duckdb", .local_envir = env)
  connection <- siab_connect(path)

  DBI::dbWriteTable(connection, "data", as.data.frame(rows))

  had_con <- exists("con", envir = globalenv(), inherits = FALSE)
  old_con <- if (had_con) get("con", envir = globalenv()) else NULL
  assign("con", connection, envir = globalenv())

  withr::defer(
    {
      if (had_con) assign("con", old_con, envir = globalenv())
      else rm("con", envir = globalenv())
      suppressWarnings(try(DBI::dbDisconnect(connection, shutdown = TRUE),
                           silent = TRUE))
    },
    envir = env
  )

  list(connection = connection, path = path)
}

test_that("compute_and_overwrite leaves no unreused blocks behind", {
  store <- on_disk_db(data.frame(persnr = 1:20000, wage = runif(20000)))

  after_rewrite <- function(step) {
    tbl(store$connection, "data") |>
      mutate(wage = wage + step) |>
      compute_and_overwrite()
    DBI::dbGetQuery(store$connection, "PRAGMA database_size")$free_blocks[1]
  }

  # Ten rewrites of the same table. Without the checkpoint every one of them
  # allocates a fresh copy and frees the one before it, so the free blocks
  # climb with the number of steps; with it, they do not.
  free <- vapply(1:10, after_rewrite, numeric(1))

  expect_lt(max(free), 3 * DBI::dbGetQuery(
    store$connection, "PRAGMA database_size")$used_blocks[1])
})

test_that("compact_store returns the file to the size of what is in it", {
  store <- on_disk_db(data.frame(persnr = 1:50000, wage = runif(50000)))

  # A run's worth of rewrites, each leaving its predecessor's blocks free.
  for (step in 1:10) {
    tbl(store$connection, "data") |>
      mutate(wage = wage + step) |>
      compute_and_overwrite()
  }
  before  <- DBI::dbGetQuery(store$connection, "SELECT * FROM data") |>
    dplyr::arrange(persnr)
  # What the file is once everything is in it rather than in the write-ahead
  # log, so that the comparison below is about the compaction and not about
  # whether the rewrites were checkpointed.
  DBI::dbExecute(store$connection, "CHECKPOINT")
  grown   <- file.size(store$path)
  DBI::dbDisconnect(store$connection, shutdown = TRUE)

  expect_output(compact_store(store$path), "Store compacted")

  expect_lt(file.size(store$path), grown)
  expect_false(file.exists(paste0(store$path, ".wal")))
  expect_false(file.exists(paste0(store$path, ".compact")))

  connection <- siab_connect(store$path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)
  after <- DBI::dbGetQuery(connection, "SELECT * FROM data") |>
    dplyr::arrange(persnr)
  expect_equal(after, before)
})

test_that("compact_store refuses a path with no database at it", {
  expect_error(compact_store(file.path(tempdir(), "no_such_store.duckdb")),
               "No such database file")
})
