# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   
#   SIAB Preparation
# 
# Merge the basic Establishment History Panel (BHP) to the SIAB
# 
# Generates the variables:
#   - ao_bula: the federal state of the establishment
#   - w93_3_gen: the 3-digit WZ93 industry of the establishment, extrapolated
#   and whatever else is named in keep_variables
# 
# Notes:
#   The merge is many-to-one on betnr and year and keeps every SIAB episode,
# matched or not. Episodes without an establishment number cannot match, and
# non-employment episodes are the bulk of those, which is why the step logs the
# match rate by source (quelle) rather than asserting on it.
# 
# Port of 04_merge_basic_BHP.do.
# 
# Author(s): Eduard Bruell based on code by Wolfgang Dauth and Johann Eppelsheimer
# 
# Version: 1.0
# Created: 2026-09-16
# 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

merge_basic_bhp <- function(connection,
                            bhp_file,
                            keep_variables = c("ao_bula", "w93_3_gen"),
                            log_file = NULL){
  
  # Remove old log file if it exists
  if (!is.null(log_file) && file.exists(log_file)) {
    file.remove(log_file)
  }
  
  # Clear existing log appenders
  log_appender(NULL, namespace = "bhp_basis")
  
  # Initialize console logger
  log_appender(appender_console, namespace = "bhp_basis")
  
  # Initialize file logger if log_file is specified
  if (!is.null(log_file)) {
    log_appender(appender_tee(file = log_file), namespace = "bhp_basis")
  }
  
  log_info("Basic BHP merge started", namespace = "bhp_basis")
  
  stopifnot("Please set bhp_file to the Basic Establishment File" = !is.null(bhp_file))
  if (!file.exists(bhp_file)) {
    stop(glue("Basic Establishment File not found: {bhp_file}"), call. = FALSE)
  }
  
  #The delivery keys on betnr_siab and jahr; the prepared SIAB uses betnr and year
  bhp <- read.dta13(bhp_file,
                    convert.factors = FALSE,
                    convert.dates   = TRUE,
                    select.cols     = c("betnr_siab", "jahr", keep_variables)) |>
    rename(betnr = betnr_siab, year = jahr)
  
  glue("Read {nrow(bhp)} establishment-year rows from {basename(bhp_file)}") |>
    log_info(namespace = "bhp_basis")
  
  #A duplicate establishment-year would silently turn the merge into m:m
  if (anyDuplicated(bhp[c("betnr", "year")]) != 0) {
    stop("The Basic Establishment File is not unique by betnr and year", call. = FALSE)
  }
  
  glue("Merging {str_c(keep_variables, collapse = ', ')} on betnr and year") |>
    log_info(namespace = "bhp_basis")
  
  #Write the establishment file to its own table so the join runs in the database
  if (dbExistsTable(connection, "bhp_basis")) {
    dbRemoveTable(connection, "bhp_basis")
  }
  dbWriteTable(connection, "bhp_basis", bhp)
  
  tbl(connection, "data") |>
    left_join(tbl(connection, "bhp_basis"), by = c("betnr", "year")) |>
    compute_and_overwrite()
  
  log_success(" -> Basic establishment variables added", namespace = "bhp_basis")
  
  #Inspect the merge the way 04_merge_basic_BHP.do does, by source
  log_info("Match rate by source (quelle):", namespace = "bhp_basis")
  tbl(connection, "data") |>
    group_by(quelle) |>
    summarise(n_episodes = n(),
              n_matched  = sum(as.integer(!is.na(ao_bula)), na.rm = TRUE)) |>
    ungroup() |>
    arrange(quelle) |>
    collect() |>
    mutate(share = round(100 * n_matched / n_episodes, 1)) |>
    glue_data("quelle = {quelle}: {n_matched} of {n_episodes} episodes matched ({share}%)") |>
    walk(log_info, namespace = "bhp_basis")
  
  dbRemoveTable(connection, "bhp_basis")
  log_success("CLEANUP: Establishment file deleted from database", namespace = "bhp_basis")
  log_success("Basic BHP merge finished", namespace = "bhp_basis")
  
  #Return the connection so we can pipe prepare functions
  return(connection)
}
