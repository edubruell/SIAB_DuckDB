# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#   SIAB Preparation
#
# Merge the yearly Establishment History Panel (BHP) and the worker-flow and
# entry-and-exit extension files to the SIAB
#
# Merges up to five sources, each many-to-one on betnr and year, each keeping
# every SIAB episode whether it matched or not:
#   - annual:  one file per calendar year, the yearly establishment variable
#              blocks (az_f, az_reg, az_azubi, ...)
#   - inflow:  hirings from the Worker Flows extension (ein_ges, ein_gf, ein_vz)
#   - outflow: separations from the same extension (aus_ges, aus_gf, aus_vz)
#   - entry:   establishment entries from the Entry and Exit extension
#   - exit:    establishment exits from the same extension
#
# Notes:
#   The reference merges the yearly files in a loop, one `merge m:1 betnr jahr`
# per year from minYear to maxYear, and each merge carries `update`. `update`
# fills a missing value in the master from the using file and leaves a
# non-missing one alone. Every yearly file holds exactly one calendar year and
# is unique on betnr, so a SIAB episode can match at most one of them: the loop
# therefore has the same result as one join against all the yearly files stacked,
# and that is what this function does. The forty-nine-way loop is not reproduced.
#
#   Between the four extension files `update` does real work, because entry and
# exit both carry `besch`. The entry merge creates the column, the exit merge
# fills it in wherever entry left it missing, and where both have a value the
# entry one stays. The joins here run in the reference's order and coalesce any
# column the incoming file shares with the data, which is what `update` without
# `replace` does.
#
#   The whole step is switched off in the reference master, because all five
# files have to be requested from the FDZ separately. Passing modules = NULL
# skips it here the same way; a module whose files are absent is skipped with a
# warning rather than stopping the pipeline.
#
# Port of 11_merge_BHP.do.
#
# Author(s): Eduard Bruell based on code by Wolfgang Dauth and Johann Eppelsheimer
#
# Version: 1.0
# Created: 2026-09-16
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

merge_annual_bhp <- function(connection,
                             bhp_folder,
                             prefix   = "SIAB_7523_v2",
                             years    = 1975:2023,
                             modules  = c("annual", "inflow", "outflow",
                                          "entry", "exit"),
                             log_file = NULL){

  # Remove old log file if it exists
  if (!is.null(log_file) && file.exists(log_file)) {
    file.remove(log_file)
  }

  # Clear existing log appenders
  log_appender(NULL, namespace = "bhp_annual")

  # Initialize console logger
  log_appender(appender_console, namespace = "bhp_annual")

  # Initialize file logger if log_file is specified
  if (!is.null(log_file)) {
    log_appender(appender_tee(file = log_file), namespace = "bhp_annual")
  }

  log_info("Annual BHP merge started", namespace = "bhp_annual")

  if (length(modules) == 0) {
    log_info("No modules requested, step skipped", namespace = "bhp_annual")
    return(connection)
  }

  known <- c("annual", "inflow", "outflow", "entry", "exit")
  unknown <- setdiff(modules, known)
  if (length(unknown) > 0) {
    stop(glue("Unknown BHP module: {str_c(unknown, collapse = ', ')}"),
         call. = FALSE)
  }

  stopifnot("Please set bhp_folder to the folder holding the BHP files" =
              !is.null(bhp_folder))
  folder <- if (is.function(bhp_folder)) bhp_folder("") else bhp_folder

  #The delivery keys on betnr_siab and jahr; the prepared SIAB uses betnr and year
  read_bhp <- function(path){
    read.dta13(path, convert.factors = FALSE, convert.dates = TRUE) |>
      rename(betnr = betnr_siab, year = jahr)
  }

  #`update` without `replace`: fill a missing value in the data from the
  #incoming file, leave a value that is already there alone.
  join_and_update <- function(incoming_table){
    shared <- intersect(colnames(tbl(connection, "data")),
                        colnames(tbl(connection, incoming_table)))
    shared <- setdiff(shared, c("betnr", "year"))

    joined <- tbl(connection, "data") |>
      left_join(tbl(connection, incoming_table), by = c("betnr", "year"),
                suffix = c("", "_using"))

    if (length(shared) > 0) {
      glue("`update` applies to {str_c(shared, collapse = ', ')}") |>
        log_info(namespace = "bhp_annual")
      #Written as SQL so the step needs no tidyeval helper beyond the splice
      filled <- set_names(
        map(shared, ~ sql(glue("COALESCE({.x}, {.x}_using)"))),
        shared
      )
      joined <- joined |>
        mutate(!!!filled) |>
        select(-all_of(paste0(shared, "_using")))
    }

    joined |> compute_and_overwrite()
  }

  stage <- function(table_name, tab){
    if (anyDuplicated(tab[c("betnr", "year")]) != 0) {
      stop(glue("{table_name} is not unique by betnr and year"), call. = FALSE)
    }
    if (dbExistsTable(connection, table_name)) {
      dbRemoveTable(connection, table_name)
    }
    dbWriteTable(connection, table_name, tab)
  }

  #==================================================================
  #  The yearly establishment variable blocks
  #==================================================================

  if ("annual" %in% modules) {
    files <- file.path(folder, glue("{prefix}_bhp_v1_{years}.dta"))
    files <- files[file.exists(files)]

    if (length(files) == 0) {
      warning(glue("No yearly BHP files under {folder}, module skipped"),
              call. = FALSE)
    } else {
      glue("Reading {length(files)} yearly BHP files") |>
        log_info(namespace = "bhp_annual")

      #Each file holds one calendar year, so stacking them and joining once is
      #the reference's year-by-year loop with the same result. See the header.
      annual <- map(files, read_bhp) |> bind_rows()

      glue("Stacked to {nrow(annual)} establishment-year rows") |>
        log_info(namespace = "bhp_annual")

      stage("bhp_annual", annual)
      join_and_update("bhp_annual")
      dbRemoveTable(connection, "bhp_annual")
      log_success(" -> Yearly establishment variables added",
                  namespace = "bhp_annual")
    }
  }

  #==================================================================
  #  The four extension files, in the reference's order
  #==================================================================

  for (module in intersect(c("inflow", "outflow", "entry", "exit"), modules)) {
    path <- file.path(folder, glue("{prefix}_bhp_{module}_v1.dta"))
    if (!file.exists(path)) {
      warning(glue("{basename(path)} not found, {module} module skipped"),
              call. = FALSE)
      next
    }

    tab <- read_bhp(path)
    glue("Read {nrow(tab)} establishment-year rows from {basename(path)}") |>
      log_info(namespace = "bhp_annual")

    stage(glue("bhp_{module}"), tab)
    join_and_update(glue("bhp_{module}"))
    dbRemoveTable(connection, glue("bhp_{module}"))
    log_success(glue(" -> {module} variables added"), namespace = "bhp_annual")
  }

  #==================================================================
  #  Inspect the merge the way 11_merge_BHP.do does, by source
  #==================================================================

  log_info("Match rate by source (quelle):", namespace = "bhp_annual")
  added <- intersect(c("az_f", "ein_ges", "aus_ges", "eintritt", "austritt"),
                     colnames(tbl(connection, "data")))
  if (length(added) > 0) {
    probe <- added[1]
    tbl(connection, "data") |>
      group_by(quelle) |>
      summarise(n_episodes = n(),
                n_matched  = sql(glue(
                  "SUM(CASE WHEN {probe} IS NULL THEN 0 ELSE 1 END)"))) |>
      ungroup() |>
      arrange(quelle) |>
      collect() |>
      mutate(share = round(100 * n_matched / n_episodes, 1)) |>
      glue_data("quelle = {quelle}: {n_matched} of {n_episodes} episodes matched on {probe} ({share}%)") |>
      walk(log_info, namespace = "bhp_annual")
  }

  log_success("Annual BHP merge finished", namespace = "bhp_annual")

  #Return the connection so we can pipe prepare functions
  return(connection)
}
