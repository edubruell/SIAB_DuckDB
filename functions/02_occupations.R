# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   
# 03.) Add occupation classifications for the beruf_gr variable in the SUF
# 
# Generates the variables:
#   - kldb92_2: For all beruf_gr that have a unique walkover to a 2-digit occupation
#                this adds the kldb92_2 digit code
#   - occ_blo: For all variables that have a unique walkover to a blossfeld occupation
#              this adds it. 
# 
# Keep in mind that neither of the walkovers is perfectly clean and you loose 
# information from some occupations if you only use the onse with an unique walkover as here.
# You can of course directly merge occupation-level data to 2- or 3-digit walkovers to 
# beruf_gr available in the classifications-folder and then modify this file to merge
# occupation level information
# 
# Author(s): Eduard Bruell
# Original R/duckdb code for the SIAB-SUF
# 
# Version: 1.0
# Created: 2024-06-01
# 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

generate_occupation_variables <- function(.connection, .log_file = NULL){
  # Remove old log file if it exists
  if (!is.null(.log_file) && file.exists(.log_file)) {
    file.remove(.log_file)
  }
  
  # Clear existing log appenders
  log_appender(NULL, namespace = "occ_vars")
  
  # Initialize console logger
  log_appender(appender_console, namespace = "occ_vars")
  
  # Initialize file logger if log_file is specified
  if (!is.null(.log_file)) {
    log_appender(appender_tee(file = .log_file), namespace = "occ_vars")
  }
  
  log_info("Occupation variable script started", namespace = "occ_vars")
  
  log_info("Reading KldB92 2-digit walkover to beruf_gr", namespace = "occ_vars")
  #Uniquely identifiable 2-digit occupations
  
  walkover_kldb92_2d_beruf_gr <-  read_csv(here("classifications", "walkover_kldb92_2d_beruf_gr.csv"))
  
  #Output info an what 2-digit occupations are not uniquely identifiable via beruf_gr
  log_info("Following KlDB 2-digit occupations have no unique walkover to beruf_gr and will not be added to the data:", namespace = "occ_vars")
  walkover_kldb92_2d_beruf_gr %>%
    filter(n_kldb92_2 != 1) %>%
    select(occ_kldb92_2 = kldb92_2,beruf_gr,n_kldb92_2) %>%
    group_by(beruf_gr) %>%
    summarise(occ2_matched = str_c(occ_kldb92_2,collapse=",") %>%
                str_replace(",([^,]*)$", " and \\1")) %>%
    glue_data("beruf_gr = {beruf_gr} matches KldB-92-2digit occupations {occ2_matched}") %>%
    walk(log_info, namespace = "occ_vars")
  
  #Get the unique walkover
  unique_kldb_2d_walkover <- walkover_kldb92_2d_beruf_gr %>%
    filter(n_kldb92_2 == 1) %>%
    select(occ_kldb92_2 = kldb92_2,beruf_gr)
  
  #Merge it
  log_info("Merging KldB92 2-digit walkover to beruf_gr", namespace = "occ_vars")
  tbl(.connection, "data") %>%
    left_join(unique_kldb_2d_walkover, by="beruf_gr", copy=TRUE) %>%    
    compute_and_overwrite()
  
  log_success(" -> Uniquely identifiable 2-digit occupation varibale (occ_kldb92_2) added", namespace = "occ_vars")
  
  log_info("Reading KldB92 2-digit walkover to Blossfeld occupation classification", namespace = "occ_vars")
  #Uniquely identifiable 2-digit occupations
  walkover_occblo_beruf_gr <- read_csv(here("classifications",
                                           "walkover_occblo_beruf_gr.csv")) 
  
  #Output info an what Blossfeld occupations are not uniquely identifiable via beruf_gr
  log_info("Following Blossfeld-classifications have no unique walkover to beruf_gr and will not be added to the data:", namespace = "occ_vars")
  walkover_occblo_beruf_gr %>%
    filter(n_occ_blo != 1) %>%
    select(occ_blo,beruf_gr,n_occ_blo) %>%
    group_by(beruf_gr) %>%
    summarise(occblo_matched = str_c(occ_blo,collapse=",") %>%
                str_replace(",([^,]*)$", " and \\1")) %>%
    glue_data("beruf_gr = {beruf_gr} matches Blossfeld-classifications {occblo_matched}") %>%
    walk(log_info, namespace = "occ_vars")
  
  #Get the unique ones
  unique_occblo_walkover <- walkover_occblo_beruf_gr %>%
    filter(n_occ_blo == 1) %>%
    select(occ_blo,beruf_gr)
  
  tbl(.connection, "data") %>%
    left_join(unique_occblo_walkover, by="beruf_gr", copy=TRUE) %>%    
    compute_and_overwrite()
  
  log_success("-> occ_blo variable added", 
              namespace = "occ_vars")    
  log_success("Occupation variables script finished", namespace = "occ_vars")
  
  #Return the .connection so we can pipe prepare functions
  return(.connection)
}

