# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   
# 03.) Add occupation classifications for the beruf variable
# 
# Generates the variables:
#   - occ_kldb88_2: the 2-digit KldB-88 Berufsgruppe of the 3-digit beruf code
#   - occ_blo: Blossfeld occupations
# 
# beruf in SIAB 7523 v2 is the 3-digit KldB-88 Berufsordnung, so both merges are
# exact and no occupation is dropped for want of a unique match. The SUF variable
# beruf_gr this step used to read was a lossy 120-category grouping that needed
# the walkover files removed in 2026-09.
# 
# Codes 555, 666, 888, 971, 981, 982, 983, 991, 995, 996 and 997 are SIAB
# administrative categories rather than occupations and take occ_blo = 99.
# 
# Author(s): Eduard Bruell
# Original R/duckdb code for the SIAB
# 
# Version: 2.0
# Created: 2024-06-01
# Rewritten for SIAB 7523 v2: 2026-09-16
# 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

generate_occupation_variables <- function(connection, log_file = NULL){
  # Remove old log file if it exists
  if (!is.null(log_file) && file.exists(log_file)) {
    file.remove(log_file)
  }
  
  # Clear existing log appenders
  log_appender(NULL, namespace = "occ_vars")
  
  # Initialize console logger
  log_appender(appender_console, namespace = "occ_vars")
  
  # Initialize file logger if log_file is specified
  if (!is.null(log_file)) {
    log_appender(appender_tee(file = log_file), namespace = "occ_vars")
  }
  
  log_info("Occupation variable script started", namespace = "occ_vars")
  
  log_info("Reading the KldB-88 occupation table", namespace = "occ_vars")
  kldb88_2d <- read_csv(here("classifications", "kldb88_beruf.csv"),
                        show_col_types = FALSE) |>
    select(beruf, occ_kldb88_2 = kldb88_2)
  
  #Report the codes that sit outside the KldB-88 structure and get no Berufsgruppe
  kldb88_2d |>
    filter(is.na(occ_kldb88_2)) |>
    glue_data("beruf = {beruf} is a SIAB administrative code outside KldB-88 and gets no Berufsgruppe") |>
    walk(log_info, namespace = "occ_vars")
  
  log_info("Merging the 2-digit KldB-88 Berufsgruppe to beruf", namespace = "occ_vars")
  tbl(connection, "data") |>
    left_join(kldb88_2d, by = "beruf", copy = TRUE) |>
    compute_and_overwrite()
  
  log_success(" -> 2-digit occupation variable (occ_kldb88_2) added", namespace = "occ_vars")
  
  log_info("Reading the Blossfeld walkover", namespace = "occ_vars")
  occblo <- read_csv(here("classifications", "walkover_beruf_occblo.csv"),
                     show_col_types = FALSE) |>
    select(beruf, occ_blo)
  
  log_info("Merging the Blossfeld classification to beruf", namespace = "occ_vars")
  # 14_occ_blossfeld.do closes its recode with `(else = 99)`, and Stata's `else`
  # covers missing values as well as unmatched ones. An episode with no beruf at
  # all, which is every benefit and job-search spell, therefore leaves the
  # reference carrying 99, "not assignable", rather than missing. The coalesce
  # reproduces that; without it the two sides differ on 121,073 of the test
  # data's 505,050 rows.
  tbl(connection, "data") |>
    left_join(occblo, by = "beruf", copy = TRUE) |>
    mutate(occ_blo = coalesce(occ_blo, 99L)) |>
    compute_and_overwrite()
  
  log_success("-> occ_blo variable added", namespace = "occ_vars")    
  log_success("Occupation variables script finished", namespace = "occ_vars")
  
  #Return the connection so we can pipe prepare functions
  return(connection)
}
