# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   
#   SIAB Preparation
# 
# Add contribution assessment ceiling (1975 - 2017)
# 
# Generates the variable:
#   - east: 1 if workplace in East Germany (Berlin from 1992); 0 if West
#   - limit_assess: contribution assessment ceiling
#
# Requires ao_bula, which merge_basic_bhp() brings in from the Basic
# Establishment File. The SUF this code was written for carried ao_region
# instead, from which ao_bula was derived as floor(ao_region/1000).
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

generate_limit_assess <- function(connection, log_file = NULL){
  
  # Remove old log file if it exists
  if (!is.null(log_file) && file.exists(log_file)) {
    file.remove(log_file)
  }
  
  # Clear existing log appenders
  log_appender(NULL, namespace = "wa_ceiling")
  
  # Initialize console logger
  log_appender(appender_console, namespace = "wa_ceiling")
  
  # Initialize file logger if log_file is specified
  if (!is.null(log_file)) {
    log_appender(appender_tee(file = log_file), namespace = "wa_ceiling")
  }
  
  log_info("Reading limit_assess values from csv", namespace = "wa_ceiling")
  wa_ceiling <-  read_csv(here("classifications","wa_ceiling.csv")) |>
    mutate(limit_assess = stata_float(limit_assess))
  
  
  log_info("Generating east and the limit_assess", namespace ="wa_ceiling")
 
  tbl(connection, "data") |>
    mutate(east = case_when(
             #West: Berlin until 1991, following 06_wages_assessment_ceiling.do
             ao_bula == 11 & year < 1992 ~ 0,
             #East: Berlin (from 1992), Brandenburg, Mecklenburg-Western Pomerania, Saxony, Saxony-Anhalt, Thuringia
             ao_bula %in% c(11, 12, 13, 14, 15, 16) ~ 1,
             #West: Schleswig-Holstein, Hamburg, Lower Saxony, Bremen, North Rhine-Westphalia, Hesse, Rhineland-Palatinate, Baden-Wuerttemberg, Bavaria, Saarland
             ao_bula < 11 ~ 0, 
             TRUE ~ NA_real_
           ))   |>
    #Before 1992 there was one nationwide ceiling, and 06_wages_assessment_ceiling.do
    #assigns it on the year alone. A spell whose federal state is unknown therefore
    #still gets a ceiling in those years, and only loses one from 1992, when the
    #reference starts conditioning on east. Joining on east throughout would drop
    #those pre-1992 rows to missing.
    mutate(east_lookup = if_else(year < 1992, 0, east)) |>
    left_join(wa_ceiling |> rename(east_lookup = east),
              by = c("east_lookup", "year"), copy = TRUE) |>
    select(-east_lookup) |>
    compute_and_overwrite()
  
  log_success(" -> east and limit_assess added", namespace = "wa_ceiling")
  log_success("Wage assesment ceiling file finished", namespace = "wa_ceiling")
  
  #Return the connection so we can pipe prepare functions
  return(connection)
  
}