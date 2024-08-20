# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   
#   SIAB Preparation
# 
# Add contribution assessment ceiling (1975 - 2017)
# 
# Generates the variable:
#   - ao_bula: The German state of the employer
#   - east: 1 if workplace in East Germany (incl. Berlin); 0 if West
#   - limit_assess: contribution assessment ceiling
# 
# Notes:
#   In Germany there is a contribution assessment ceiling ("Beitragsbemessungsgrenze"). Hence, wages are right-cencored.
# The generation of the variable limit_assess is based on a FDZ-Arbeitshilfe (http://doku.iab.de/fdz/Bemessungsgrenzen_de_en.xls)
# Limits for the years 1975 - 2001 are converted from DM to EUR
# 
# 
# Author(s): Eduard Bruell based on code by Wolfgang Dauth and Johann Eppelsheimer
# 
# Version: 1.0
# Created: 2024-08-19
# 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

generate_limit_assess <- function(.connection, .log_file = NULL){
  
  # Remove old log file if it exists
  if (!is.null(.log_file) && file.exists(.log_file)) {
    file.remove(.log_file)
  }
  
  # Clear existing log appenders
  log_appender(NULL, namespace = "wa_ceiling")
  
  # Initialize console logger
  log_appender(appender_console, namespace = "wa_ceiling")
  
  # Initialize file logger if log_file is specified
  if (!is.null(.log_file)) {
    log_appender(appender_tee(file = .log_file), namespace = "wa_ceiling")
  }
  
  log_info("Reading limit_assess values from csv", namespace = "wa_ceiling")
  wa_ceiling <-  read_csv(here("classifications","wa_ceiling.csv")) 
  
  
  log_info("Generating ao_bula, east and the limit_assess", namespace ="wa_ceiling")
 
  tbl(.connection, "data") %>%
    mutate(ao_bula = floor(ao_region/1000),
           east = case_when(
             #East: Berlin, Brandenburg, Mecklenburg-Western Pomerania, Saxony, Saxony-Anhalt, Thuringia
             ao_bula %in% c(11, 12, 13, 14, 15, 16) ~ 1,
             #West: Schleswig-Holstein, Hamburg, Lower Saxony, Bremen, North Rhine-Westphalia, Hesse, Rhineland-Palatinate, Baden-Wuerttemberg, Bavaria, Saarland
             ao_bula < 11 ~ 0, 
             TRUE ~ NA_real_
           ))   %>%
    left_join(wa_ceiling, by=c("east","year"), copy=TRUE) %>%    
    compute_and_overwrite()
  
  log_success(" -> ao_bula, east and limit_assess added", namespace = "wa_ceiling")
  log_success("Wage assesment ceiling file finished", namespace = "wa_ceiling")
  
  #Return the .connection so we can pipe prepare functions
  return(.connection)
  
}