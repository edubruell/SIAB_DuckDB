#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  
#  SIAB Preparation
#
#Deflate wages, marginal part-time income threshold and contribution assessment ceiling
#
#Consumer Price Index:
#  Statistisches Bundesamt (2019)
#Preise - Verbraucherpreisindizes fuer Deutschland (Lange Reihe ab 1948)
#https://www.destatis.de/DE/Themen/Wirtschaft/Preise/Verbraucherpreisindex/_inhalt.html
#
#
#Generates the variables:
#  - wage_defl: daily wage, deflated
#  - limit_marginal_defl: marginal part-time income threshold, deflated
#  - limit_assess_defl: contribution assessment ceiling, deflated
#
#
#Author(s): Eduard Brüll based on code by Wolfgang Dauth and Johann Eppelsheimer
#
#Version: 1.0
#Created: 2020-08-19
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  

deflate_wages <- function(.connection, .log_file = NULL){
  # Remove old log file if it exists
  if (!is.null(.log_file) && file.exists(.log_file)) {
    file.remove(.log_file)
  }
  
  # Clear existing log appenders
  log_appender(NULL, namespace = "deflate_wages")
  
  # Initialize console logger
  log_appender(appender_console, namespace = "deflate_wages")
  
  # Initialize file logger if log_file is specified
  if (!is.null(.log_file)) {
    log_appender(appender_tee(file = .log_file), namespace = "deflate_wages")
  }
  
  log_info("Reading cpi data from csv", namespace = "deflate_wages")
  tbl_cpi <- read_csv(here("classifications","cpi.csv")) 
  
  log_info("Add cpi to data and generate wage_defl, limit_marginal_defl and limit_assess_defl", namespace ="deflate_wages")
  
  tbl(.connection, "data") %>%
    left_join(tbl_cpi, by=c("year"), copy=TRUE) %>%
    mutate(
      wage_defl           = 100 * tentgelt_gr / cpi,
      limit_marginal_defl = 100 * limit_marginal / cpi,
      limit_assess_defl   = 100 * limit_assess / cpi) %>%
    compute_and_overwrite()
  
  
  log_success(" ->  cpi, wage_defl, limit_marginal_defl and limit_assess_defl added", namespace = "deflate_wages")
  log_success("Wage defaltion file finished", namespace = "deflate_wages")
  
  #Return the .connection so we can pipe prepare functions
  return(.connection)

}