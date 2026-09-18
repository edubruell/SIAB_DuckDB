# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   
#   SIAB Preparation
# 
# Restrict the data to the observation period
# 
# Generates no variable. It keeps the episodes whose year lies between
# min_year and max_year and drops the rest.
# 
# Notes:
#   00_master_SIAB.do does this inline rather than in a numbered step:
# `keep if inrange(jahr,${minYear},${maxYear})`, run after 03_SIAB_bio.do and
# before 04_merge_basic_BHP.do. The position is what makes the biographic
# variables right: tage_erw, tage_bet and the rest count over the whole history,
# so they have to be built before any year is cut away.
# 
#   The default span is the whole SIAB 7523 v2 delivery, 1975 to 2023, which
# keeps every episode. A narrower period is a user's sample choice, the same way
# the reference's minYear and maxYear macros are.
# 
#   `year` is the port's name for the reference's `jahr`, and both are built
# from begepi. An episode split at a year boundary by 01_split_episodes.do
# carries the year of its own start, so the cut works on the split episodes.
# 
# Author(s): Eduard Bruell based on code by Wolfgang Dauth, Johann Eppelsheimer
#            and Heiko Stueber
# 
# Version: 1.0
# Created: 2026-09-16
# 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

restrict_observation_period <- function(connection,
                                        min_year = 1975,
                                        max_year = 2023,
                                        log_file = NULL){
  
  if (!is.numeric(min_year) || !is.numeric(max_year) ||
      length(min_year) != 1 || length(max_year) != 1) {
    stop("min_year and max_year are single years, as in min_year = 1975")
  }
  if (min_year > max_year) {
    stop("min_year is after max_year: ", min_year, " > ", max_year)
  }
  
  # Remove old log file if it exists
  if (!is.null(log_file) && file.exists(log_file)) {
    file.remove(log_file)
  }
  
  # Clear existing log appenders
  log_appender(NULL, namespace = "obs_period")
  
  # Initialize console logger
  log_appender(appender_console, namespace = "obs_period")
  
  # Initialize file logger if log_file is specified
  if (!is.null(log_file)) {
    log_appender(appender_tee(file = log_file), namespace = "obs_period")
  }
  
  log_info("Restricting the data to {min_year} to {max_year}",
           namespace = "obs_period")
  
  before <- tbl(connection, "data") |> count() |> pull(n)
  
  tbl(connection, "data") |>
    filter(year >= min_year, year <= max_year) |>
    compute_and_overwrite()
  
  after <- tbl(connection, "data") |> count() |> pull(n)
  
  log_info("{before} episodes before the restriction, {after} after, {before - after} dropped",
           namespace = "obs_period")
  
  if (after == 0L) {
    log_warn("The observation period keeps no episode at all",
             namespace = "obs_period")
  }
  
  log_success("Observation period applied", namespace = "obs_period")
  #Return the connection so we can pipe prepare functions
  return(connection)
}
