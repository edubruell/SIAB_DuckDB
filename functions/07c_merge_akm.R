# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#   SIAB Preparation
#
# Merge the AKM person and establishment wage effects to the SIAB
#
# Generates, from the establishment file:
#   - feff_1985_1992 ... feff_2017_2023: the establishment wage effect in each
#     of the five estimation windows
# and from the person file:
#   - peff_1985_1992 ... peff_2017_2023: the person wage effect in the same five
#
# Notes:
#   Two many-to-one joins, the establishment effects on betnr and the person
# effects on persnr, each keeping every SIAB episode whether it matched or not.
# Neither file carries a year: an effect is estimated once per window, so the
# five columns arrive side by side and it is the analysis that picks the window.
#
#   The effects are estimated on the connected set of movers among regular
# full-time workers aged 20 to 60, so an episode outside that group can match
# and still be meaningless. The reference merges on the identifier regardless
# and leaves the decision to the user, and so does this. The FDZ methodology
# report puts the usable subset at erwstat 101 and teilzeit 0.
#
#   Both effects are window-specific and centred on zero within their window,
# so anything using more than one window has to normalise them first.
#
#   The step is switched off in the reference master, because the two files
# have to be requested from the FDZ separately and are not part of any delivery.
# Leaving either path NULL skips that side here, which is what the master's two
# switches do.
#
# Reference:
#   Lochner, Benjamin; Wolter, Stefanie (2025): AKM effects for German labour
#   market data 1985-2023. FDZ-Methodenreport 03/2025 (en).
#
# Port of 12_merge_AKM.do.
#
# Author(s): Eduard Bruell based on code by Wolfgang Dauth and Johann Eppelsheimer
#
# Version: 1.0
# Created: 2026-09-16
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

merge_akm <- function(connection,
                      akm_estab_file = NULL,
                      akm_pers_file  = NULL,
                      log_file       = NULL){

  # Remove old log file if it exists
  if (!is.null(log_file) && file.exists(log_file)) {
    file.remove(log_file)
  }

  # Clear existing log appenders
  log_appender(NULL, namespace = "akm")

  # Initialize console logger
  log_appender(appender_console, namespace = "akm")

  # Initialize file logger if log_file is specified
  if (!is.null(log_file)) {
    log_appender(appender_tee(file = log_file), namespace = "akm")
  }

  log_info("AKM merge started", namespace = "akm")

  if (is.null(akm_estab_file) && is.null(akm_pers_file)) {
    log_info("Neither AKM file given, step skipped", namespace = "akm")
    return(connection)
  }

  merge_side <- function(path, key, table_name, side){
    if (!file.exists(path)) {
      stop(glue("AKM {side} file not found: {path}"), call. = FALSE)
    }

    #The delivery keys on persnr_siab and betnr_siab; the prepared SIAB has
    #already renamed both to persnr and betnr.
    tab <- read.dta13(path, convert.factors = FALSE)
    names(tab)[names(tab) == paste0(key, "_siab")] <- key

    if (!key %in% names(tab)) {
      stop(glue("The AKM {side} file has no {key} column"), call. = FALSE)
    }
    if (anyDuplicated(tab[key]) != 0) {
      stop(glue("The AKM {side} file is not unique by {key}"), call. = FALSE)
    }

    effects <- setdiff(names(tab), key)
    glue("Read {nrow(tab)} rows and {length(effects)} effects from {basename(path)}") |>
      log_info(namespace = "akm")

    if (dbExistsTable(connection, table_name)) {
      dbRemoveTable(connection, table_name)
    }
    dbWriteTable(connection, table_name, tab)

    tbl(connection, "data") |>
      left_join(tbl(connection, table_name), by = key) |>
      compute_and_overwrite()

    dbRemoveTable(connection, table_name)

    #Report the share of episodes that received an effect, per window. On the
    #real files this is the merge rate the methodology report tabulates; it is
    #the only thing about this step worth reading off the data.
    tbl(connection, "data") |>
      summarise(across(all_of(effects),
                       ~ sum(as.integer(!is.na(.x)), na.rm = TRUE)),
                n_episodes = n()) |>
      collect() |>
      pivot_longer(all_of(effects), names_to = "window", values_to = "n_matched") |>
      mutate(share = round(100 * n_matched / n_episodes, 1)) |>
      glue_data("{window}: {n_matched} of {n_episodes} episodes ({share}%)") |>
      walk(log_info, namespace = "akm")

    log_success(glue(" -> AKM {side} effects added"), namespace = "akm")
  }

  if (!is.null(akm_estab_file)) {
    merge_side(akm_estab_file, "betnr", "akm_estab", "establishment")
  }

  if (!is.null(akm_pers_file)) {
    merge_side(akm_pers_file, "persnr", "akm_pers", "person")
  }

  log_success("AKM merge finished", namespace = "akm")

  #Return the connection so we can pipe prepare functions
  return(connection)
}
