# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# 01b.) Reallocation of one-time payments (deregistration reason 154)
#
# Port of 02_grund154.do. Spells with grund == 154 carry a one-time payment that
# the employer reported separately from the employment spell it belongs to, so
# the daily wage on those spells is meaningless and the wage on the employment
# spell is too low. Only BEH spells are affected. For the description of the
# problem see Frodermann et al. (2021), Sections 5.5.1 and 5.5.12.
#
# Procedure, following the reference step by step:
#   a) spell duration (episode_length) and spell earnings (episode_entgelt)
#   b) per person, establishment and year, the total earnings of the 154 spells
#   c) that total is carried to every spell of the same combination (tentgelt154)
#   d) the 154 spells are dropped
#   e) per combination, the total duration of the remaining spells (total_length)
#   f) the total from b) is spread over those spells in proportion to duration
#   g) the daily wage tentgelt is recomputed and rounded to two decimals
#
# Modifies the variable:
#   - tentgelt: daily wage, raised by the reallocated one-time payments
#
# Drops the spells with grund == 154.
#
# Author(s): Eduard Brüll
# R/duckdb reimplementation of the original procedure by Heiko Stüber,
# Wolfgang Dauth and Johann Eppelsheimer
#
# Version: 1.0
# Created: 2026-09-16
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

reallocate_one_time_payments <- function(connection, log_file = NULL){

  # Remove old log file if it exists
  if (!is.null(log_file) && file.exists(log_file)) {
    file.remove(log_file)
  }

  # Clear existing log appenders
  log_appender(NULL, namespace = "grund154")

  # Initialize console logger
  log_appender(appender_console, namespace = "grund154")

  # Initialize file logger if log_file is specified
  if (!is.null(log_file)) {
    log_appender(appender_tee(file = log_file), namespace = "grund154")
  }

  log_info("Reallocation of one-time payments started", namespace = "grund154")

  #==================================================================
  #  a) to d): spell earnings, the group total, and the drop
  #==================================================================

  # `bysort persnr betnr jahr: egen earnings154 = sum(episode_entgelt) if grund == 154`
  # followed by `egen tentgelt154 = max(earnings154)` is one grouped sum over the
  # 154 spells, carried to every row of the group. Stata's egen sum() counts a
  # missing summand as zero, which coalesce() reproduces.
  #
  # Both `gen` and `egen` make a float, so episode_entgelt and the group total
  # carry about seven digits and no more. That loss reaches the result: it is
  # what the rounding in g) sees. Carrying the full double instead puts 13 of
  # the test data's spells a cent above the reference.
  #
  # `drop if grund == 154` keeps rows with a missing grund, because a missing
  # value is not equal to 154 in Stata either. The non-BEH sources have no
  # deregistration reason at all, so this is not a corner case.
  tbl(connection, "data") |>
    mutate(episode_length  = endepi - begepi + 1L) |>
    mutate(episode_entgelt = !!sql_stata_float("tentgelt * episode_length")) |>
    group_by(persnr, betnr, year) |>
    mutate(earnings154 = sum(if_else(!is.na(grund) & grund == 154L,
                                     coalesce(episode_entgelt, 0),
                                     0),
                             na.rm = TRUE)) |>
    ungroup() |>
    mutate(tentgelt154 = !!sql_stata_float("earnings154")) |>
    select(-earnings154) |>
    filter(is.na(grund) | grund != 154L) |>
    compute_and_overwrite()

  # `assert episode_length >= 1 & episode_length <= 366`. The reference asserts
  # before the drop; here it runs after, which tests the same rows apart from
  # the 154 spells that no longer exist.
  bad_length <- tbl(connection, "data") |>
    filter(is.na(episode_length) | episode_length < 1L | episode_length > 366L) |>
    count() |>
    pull(n)

  if (bad_length > 0) {
    stop(glue("episode_length outside 1 to 366 on {bad_length} spells"))
  }
  log_success(" ->  episode_length is between 1 and 366 on every spell",
              namespace = "grund154")

  n_reallocating <- tbl(connection, "data") |>
    filter(tentgelt154 > 0) |>
    count() |>
    pull(n)
  log_info(glue("{n_reallocating} spells receive a share of a one-time payment"),
           namespace = "grund154")

  #==================================================================
  #  e) to g): spread the total and recompute the daily wage
  #==================================================================

  # total_length is summed over the spells that survive the drop, so it has to
  # be computed in a second pass. It needs no float cast: it is a sum of day
  # counts, and every value it can take is exact in four bytes.
  #
  # The wage expression is kept in the reference's own form rather than
  # simplified to `tentgelt + tentgelt154 / total_length`: the two are equal in
  # exact arithmetic and need not be equal in the last bit of a double, and this
  # step is compared against the Stata output. The result itself is not cut back
  # to float: tentgelt is a double in the SIAB, so `replace` stores the rounded
  # value at full precision. Only the two intermediates above lose digits.
  #
  # The rounding is written as `0.01 * round(x / 0.01)`, which is what Stata's
  # round(x,.01) does, rather than as round(x, 2). DuckDB's two-argument round
  # scales the other way round and lands a few times 1e-13 away, which is
  # invisible in a wage and still enough to keep 64,449 of the test data's
  # spells from comparing equal. In this form every one of them matches.
  tbl(connection, "data") |>
    mutate(tentgelt_orig = tentgelt) |>
    group_by(persnr, betnr, year) |>
    mutate(total_length = sum(episode_length, na.rm = TRUE)) |>
    ungroup() |>
    mutate(tentgelt = if_else(
      !is.na(tentgelt),
      0.01 * round((episode_entgelt + tentgelt154 * episode_length / total_length) /
                     episode_length / 0.01),
      tentgelt
    )) |>
    compute_and_overwrite()

  # `assert tentgelt >= tentgelt_orig if !missing(tentgelt_orig) & !missing(tentgelt)`
  shrunk <- tbl(connection, "data") |>
    filter(!is.na(tentgelt_orig), !is.na(tentgelt), tentgelt < tentgelt_orig) |>
    count() |>
    pull(n)

  if (shrunk > 0) {
    stop(glue("Reallocation lowered tentgelt on {shrunk} spells"))
  }
  log_success(" ->  no spell lost wage in the reallocation", namespace = "grund154")

  #==================================================================
  #  Clean up the working variables
  #==================================================================

  tbl(connection, "data") |>
    select(-episode_length, -episode_entgelt, -tentgelt154, -total_length,
           -tentgelt_orig) |>
    compute_and_overwrite()

  log_success("Reallocation of one-time payments finished", namespace = "grund154")

  #Return the connection so we can pipe prepare functions
  return(connection)
}
