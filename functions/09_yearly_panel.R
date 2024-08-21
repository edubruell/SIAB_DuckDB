# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  
#  SIAB Preparation
#
#Transfer data set into yearly panel (using one specific cutoff date)
#- retain information on total employment / unemployment durations and earnings per year
#
#Generates the variables:
#   - year_days_emp: total days employed per calendar year
#   - year_days_benefits: total days benefit recipience per calendar year
#   - year_labor_earn: total labor earnings per calendar year
#
#Drops the variables:
#   - nspell
#   - begorig
#   - endorig
#   - begepi
#   - endepi
#   - begepi_orig
#   - endepi_orig
#
#
#Author(s): Eduard Brüll based on code by Wolfgang Dauth, Johann Eppelsheimer
#
#Version: 1.0
#Created: 2020-08-0
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  

build_yearly_panel <- function(.connection, 
                               .log_file = NULL,
                               .cutoffMonth = 6,
                               .cutoffDay   = 30){
  
  # Remove old log file if it exists
  if (!is.null(.log_file) && file.exists(.log_file)) {
    file.remove(.log_file)
  }
  
  # Clear existing log appenders
  log_appender(NULL, namespace = "yearly_panel")
  
  # Initialize console logger
  log_appender(appender_console, namespace = "yearly_panel")
  
  # Initialize file logger if log_file is specified
  if (!is.null(.log_file)) {
    log_appender(appender_tee(file = .log_file), namespace = "yearly_panel")
  }
  
  
  #-----------------------------------------------------------------------------
  #  Aggregate employment outcomes: days employed, labor earnings 
  #-----------------------------------------------------------------------------
  
  log_info("Generating aggregate employment outcomes: days employed, labor earnings ", namespace = "yearly_panel")
  
  #Durations of episodes
  tbl(.connection, "data") %>%
    window_order(persnr,year,begepi,endepi,quelle_gr) %>%
    group_by(persnr,year,begepi,endepi) %>%
    mutate(dur_emp      = if_else(quelle_gr == 1,endepi-begepi + 1,0),
           dur_benefits = if_else(quelle_gr == 2 | parallel_benefits == 1,endepi-begepi + 1,0)
           ) %>%
    group_by(persnr,year) %>%
    #Total time working, total time receiving UI benefits
    mutate(
      year_days_emp      = sum(dur_emp,na.rm=TRUE),
      year_days_benefits = sum(dur_benefits,na.rm=TRUE),
      #Earnings=wage*duration
      year_labor_earn    = sum(parallel_wage_imp*dur_emp,na.rm=TRUE)
    ) %>%
    ungroup() %>%
    select(-dur_emp,-dur_benefits) %>%
    compute_and_overwrite()
  
  log_success("->  year_days_emp, year_days_benefits and year_labor_earn generated", namespace = "yearly_panel")
  
  #Transfer dates into strictly ascending numbers
  cutoff_num = 100 * .cutoffMonth + .cutoffDay
  #This is an artefact of the original code being from STATA, duckdb should be able to handle dates directly but alas
  
  #-----------------------------------------------------------------------------
  #  Keep only episodes that include the cutoff date
  #-----------------------------------------------------------------------------
  log_info("Keep only episodes that include the cutoff date", namespace = "yearly_panel")
  
  tbl(.connection, "data") %>%
    mutate(#begin of episodes
           begepi_num = 100 * month(begepi) + day(begepi),
           #end of episodes
           endepi_num = 100 * month(endepi) + day(endepi)) %>%
    #Keep only episodes that include the cutoff date
    filter(begepi_num <= cutoff_num & cutoff_num <= endepi_num) %>%
    select(-begepi_num,-endepi_num) %>%
    compute_and_overwrite()

  log_success("->  Only episodes including the cutoff data included", namespace = "yearly_panel")
  
  #-----------------------------------------------------------------------------
  #  Adjust durations to end at cutoff date
  #-----------------------------------------------------------------------------
  log_info("Adjust durations to end at cutoff date", namespace = "yearly_panel")

  #Get day and month into a string for easy conversion to duckdb date function
  md_string <- paste0(
    "-",
    if_else(.cutoffMonth<10,paste0("0",floor(.cutoffMonth)),paste0(floor(.cutoffMonth))),
    "-",
    floor(.cutoffDay)
  )
  
  tbl(.connection, "data") %>%
      #Generate the cutoff-date as helper variable
      mutate(cu_date = as.Date(paste0(as.integer(year),md_string)),
             tage_bet = if_else(quelle_gr==1,tage_bet - (endepi - cu_date),NA_integer_),
             tage_job = if_else(quelle_gr==1,tage_job - (endepi - cu_date),NA_integer_),
             tage_erw = if_else(quelle_gr==1 & erwstat_gr != 2,tage_erw - (endepi - cu_date),NA_integer_),
             #tage_lst = if_else(quelle_gr==2 | parallel_benefits == 1, tage_lst - (endepi - cu_date),NA_integer_)                  
             ) %>%
    select(-cu_date) %>%
    compute_and_overwrite()

  log_success("-> Durations adjusted", namespace = "yearly_panel")
  log_success("Yearly panel file finished", namespace = "yearly_panel")
  
  #Return the .connection so we can pipe prepare functions
  return(.connection)

}