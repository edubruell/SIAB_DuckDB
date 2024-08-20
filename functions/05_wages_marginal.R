#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  
#  SIAB Preparation
#
#Add Marginal Part-Time Income Threshold and flag affected records (1975 - 2014)
#
#Generates the variables:
#  - limit_marginal: Marginal part-time income threshold
#  - marginal: 1 if marginal wage, 0 otherwise
#
#Note: Limits for the years 1975 - 2001 are converted from DM to EUR
#Based on FDZ Arbeitshilfe (http://doku.iab.de/fdz/Bemessungsgrenzen_de_en.xls)
#
#
#Author(s): Eduard Brüll based on Code by Wolfgang Dauth and Johann Eppelsheimer
#
#Version: 1.0
#Created: 2020-08-19
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

generate_limit_marginal <- function(.connection, .log_file = NULL){
  
  # Remove old log file if it exists
  if (!is.null(.log_file) && file.exists(.log_file)) {
    file.remove(.log_file)
  }
  
  # Clear existing log appenders
  log_appender(NULL, namespace = "limit_marginal")
  
  # Initialize console logger
  log_appender(appender_console, namespace = "limit_marginal")
  
  # Initialize file logger if log_file is specified
  if (!is.null(.log_file)) {
    log_appender(appender_tee(file = .log_file), namespace = "limit_marginal")
  }
  
  log_info("Reading limit_marginal values from csv", namespace = "limit_marginal")
  tbl_limit_marginal <- read_csv(here("classifications","limit_marginal.csv")) 
  
  log_info("Generating limit_marginal and marginal dummy in data", namespace ="limit_marginal")
  tbl(.connection, "data") %>%
    left_join(tbl_limit_marginal, by=c("east","year"), copy=TRUE) %>%
    mutate(marginal = case_when(
      is.na(tentgelt_gr) | is.na(limit_marginal) ~ NA_real_,
      tentgelt_gr <= limit_marginal ~ 1,
      tentgelt_gr > limit_marginal ~ 0
    )) %>%
    compute_and_overwrite()
  
  log_success(" ->  limit_amarginal and marginal added", namespace = "limit_marginal")
  log_success("Marginal Part-Time Income Threshold and Flag affected records ", namespace = "limit_marginal")
  
  #Return the .connection so we can pipe prepare functions
  return(.connection)
  
}   
  

