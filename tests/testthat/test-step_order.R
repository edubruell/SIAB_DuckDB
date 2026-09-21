# The guard on the hand-written step order, R side.
#
# The pipeline's step order is spelled out by hand in five scripts, three of
# them R: R/siab_main.R, R/run_testdata.R and tests/fixtures/make_r_dumps.R.
# Nothing generates them from one definition, and the user ruled on 2026-09-21c
# that the duplication stays (contention C7). What replaces a generator is
# tests/step_order.json and these tests: the file states the order once, each
# script declares the steps it leaves out and the arguments it passes
# differently, and a change made in some scripts and not the others fails here.
#
# The failure this is for is the silent one. A step added to two files of
# three, or an argument changed in one, leaves every comparison running and no
# longer testing what it claims to test. The comparisons cannot see it: they
# check values, and a pipeline that skipped a step still produces values.
#
# tests/pytest/test_step_order.py is the same guard over the two Python
# scripts, reading the same file.

step_order <- jsonlite::fromJSON(here::here("tests", "step_order.json"),
                                 simplifyVector = FALSE)

canonical <- unlist(step_order$steps)

r_scripts <- Filter(function(spec) spec$arm == "R", step_order$scripts)

# The sequence of step calls a script makes, in the order it makes them.
# Comment lines are dropped first, which is what keeps the commented-out calls
# a script declares as skipped from counting as calls, and what keeps the
# project's long explanatory comments from matching on a step name they discuss.
step_calls <- function(file) {
  lines <- readLines(here::here(file), warn = FALSE)
  lines <- lines[!grepl("^\\s*#", lines)]
  calls <- character(0)
  for (line in lines) {
    hits <- regmatches(line, gregexpr(
      paste0("\\b(", paste(canonical, collapse = "|"), ")\\s*\\("), line))[[1]]
    calls <- c(calls, sub("\\s*\\($", "", hits))
  }
  calls
}

# Every value a script passes as `handling =`, which is the one argument the
# runners and the dump writers deliberately disagree on.
handling_arguments <- function(file) {
  lines <- readLines(here::here(file), warn = FALSE)
  lines <- lines[!grepl("^\\s*#", lines)]
  hits <- regmatches(lines, gregexpr("handling\\s*=\\s*\"[a-z]+\"", lines))
  unique(gsub(".*\"([a-z]+)\".*", "\\1", unlist(hits)))
}

test_that("tests/step_order.json lists each step once", {
  expect_equal(anyDuplicated(canonical), 0L)
  expect_true(length(canonical) > 0)
})

for (file in names(r_scripts)) {
  spec <- r_scripts[[file]]
  skipped <- names(spec$skips)

  test_that(paste(file, "runs the steps in the canonical order"), {
    expect_true(all(skipped %in% canonical),
                info = "a skip names a step the definition does not list")
    expect_equal(step_calls(file), setdiff(canonical, skipped))
  })

  test_that(paste(file, "passes the arguments it declares"), {
    expect_equal(handling_arguments(file), spec$arguments$handling)
  })
}
