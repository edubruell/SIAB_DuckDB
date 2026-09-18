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

generate_limit_marginal <- function(connection, log_file = NULL){
  
  # Remove old log file if it exists
  if (!is.null(log_file) && file.exists(log_file)) {
    file.remove(log_file)
  }
  
  # Clear existing log appenders
  log_appender(NULL, namespace = "limit_marginal")
  
  # Initialize console logger
  log_appender(appender_console, namespace = "limit_marginal")
  
  # Initialize file logger if log_file is specified
  if (!is.null(log_file)) {
    log_appender(appender_tee(file = log_file), namespace = "limit_marginal")
  }
  
  log_info("Reading limit_marginal values from csv", namespace = "limit_marginal")
  tbl_limit_marginal <- read_csv(here("classifications","limit_marginal.csv")) |>
    mutate(limit_marginal = stata_float(limit_marginal))
  
  log_info("Generating limit_marginal and marginal dummy in data", namespace ="limit_marginal")
  tbl(connection, "data") |>
    #Same pre-1992 rule as the assessment ceiling: 07_wages_marginal.do assigns
    #the threshold on the year alone until 1991 and only conditions on east from
    #1992 on. See R/functions/04_wage_assesment_ceiling.R.
    mutate(east_lookup = if_else(year < 1992, 0, east)) |>
    left_join(tbl_limit_marginal |> rename(east_lookup = east),
              by = c("east_lookup", "year"), copy = TRUE) |>
    select(-east_lookup) |>
    #07_wages_marginal.do writes `gen byte marginal = 0` and then
    #`replace marginal = 1 if tentgelt <= limit_marginal`, so the flag is never
    #missing. The two missing cases below follow from how Stata orders its
    #missing values, and both are reproduced on purpose because the Stata prep
    #is the reference:
    #
    # - A missing tentgelt always gives 0. The SIAB codes an absent wage as an
    #   extended missing (.a to .z, never the system missing), and an extended
    #   missing is larger than everything, including the system missing the
    #   do-file starts limit_marginal at. So the comparison is false whatever
    #   the threshold is. Checked on the test data: all 32,848 missing wages at
    #   this step are extended, none is a system missing.
    # - A missing limit_marginal with a real wage gives 1, because any number
    #   is at or below Stata's missing.
    mutate(marginal = case_when(
      is.na(tentgelt)            ~ 0,
      is.na(limit_marginal)      ~ 1,
      tentgelt <= limit_marginal ~ 1,
      TRUE                       ~ 0
    )) |>
    compute_and_overwrite()
  
  log_success(" ->  limit_amarginal and marginal added", namespace = "limit_marginal")
  log_success("Marginal Part-Time Income Threshold and Flag affected records ", namespace = "limit_marginal")
  
  #Return the connection so we can pipe prepare functions
  return(connection)
  
}   
  

