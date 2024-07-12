# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   
# 01.) Epsiode splitting functions 
# 
# Split episodes that span over one year (only relevant for the sources LeH and LHG)
# 
# Modifies the variables:
#   - begepi: splitted version of begepi
#   - endepi: splitted version of endepi
#   - jahr: year
#   - age: age, measured in years
# 
# Generates the variables:
#   - begepi_orig: original version of begepi
#   - endepi_orig: original version of endepi
# 
# 
# Author(s): Eduard Bruell
# R/duckdb  Reimplementation of original  procedures by Wolfgang Dauth and Johann Eppelsheimer
# 
# Version: 1.0
# Created: 2024-06-01
# 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


split_episodes <- function(.connection, .log_file = NULL){
  # Remove old log file if it exists
  if (!is.null(.log_file) && file.exists(.log_file)) {
    file.remove(.log_file)
  }
  
  # Clear existing log appenders
  log_appender(NULL, namespace = "split_episodes")
  
  # Initialize console logger
  log_appender(appender_console, namespace = "split_episodes")
  
  # Initialize file logger if log_file is specified
  if (!is.null(.log_file)) {
    log_appender(appender_tee(file = .log_file), namespace = "split_episodes")
  }
  
  log_info("Episode splitting script started", namespace = "split_episodes")
    #Save original end and start date of episode, Create an indicator for years
    #spanned
    tbl(.connection,"data")   %>%
      mutate(begepi_orig = begepi,
             endepi_orig = endepi,
             span_year = year(endepi) - year(begepi) + 1) %>%
      compute_and_overwrite()
    
    log_success("Variables for original episode start and end dates created", namespace = "split_episodes")
    
    #Display span-year
    log_info("Checking how many observations span multiple years:", namespace = "split_episodes")
    tbl(.connection,"data")  %>%
      count(span_year) %>%
      arrange(desc(n)) %>%
      collect() %>%
      glue_data("{n} observations span {span_year} years") %>%
      walk(log_info, namespace = "split_episodes")
    
    log_info("Expanding observation that span n years n times", namespace = "split_episodes")
    #Generate a sequence for each span_year in an expansion plan in SQL
    dbExecute(.connection,"
          CREATE TEMPORARY TABLE expansion_plan AS
          WITH sequence_generator_cte AS (
              SELECT
                  persnr,
                  spell,
                  span_year,
                  UNNEST(generate_series(1, span_year::BIGINT)) AS year_instance
              FROM
                  data
          )
          SELECT * FROM sequence_generator_cte;
          ")
    log_success("-> Expansion plan written", namespace = "split_episodes")
    
    #Expand the data
    tbl(.connection,"expansion_plan") %>%
      select(-span_year) %>%
      left_join(tbl(.connection,"data"), by=c("persnr","spell")) %>%
      compute_and_overwrite(.table = "data")
    
    log_success("-> Data expanded to copy split episodes", namespace = "split_episodes")
    
    
    #Edit the copied entries in the base table
    tbl(.connection,"data") %>%
      mutate(year_instance = year(begepi) - 1 + year_instance) %>%
      mutate(
        #Set the begin date of all clones to January 1st and use the copy instance year
        endepi = if_else(year_instance<year(endepi), as.Date(paste0(as.integer(year_instance),"-12", "-31")), endepi),
        #For all clone spell except the last of the clone group: set the end date to 31.12.
        begepi = if_else(year_instance>year(begepi), as.Date(paste0(as.integer(year_instance),"-01", "-01")), begepi),
        #Update the variables 'jahr' and age
        year   = year(begepi),
        age    = year - gebjahr
      )  %>%
      select(-year_instance,-span_year) %>%
      compute_and_overwrite(.table = "data")
    log_success("-> Copied entries updated with right endepi and begepi in the base table", namespace = "split_episodes")
    
    #Delete the expansion plan from the database
    dbRemoveTable(.connection,"expansion_plan")
    log_success("CLEANUP: Expansion plan deleted from database", namespace = "split_episodes")
    log_success("Episode splitting script finished", namespace = "split_episodes")
    #Return the .connection so we can pipe prepare functions
    return(.connection)
}
  
  



