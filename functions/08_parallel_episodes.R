#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  
#SIAB Preparation
#
#Treat parallel episodes: 
#  - generate some information on parallel episodes
#  - keep only 'main' episode
#
#Generates the variables:
#  - nspell: non-parallel spell counter
#  - parallel_jobs: number of parallel jobs
#  - parallel_wage: total wage of all parallel employment spells
#  - parallel_wage_imp: total imputed wage of all parallel employment spells
#  - parallel_benefits: indicator for recipience of UI benefits
#
#	Drops the variables:
# - spell
# - level1
# - level2
#
#
#
#Author(s): Eduard Brüll based on Code by Wolfgang Dauth, Johann Eppelsheimer
#
#Version: 1.0
#Created: 2024-08-20
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  

#Comment: 
#  
#  There is no common rule of identifying the main episode. Usually the job
#  with the longest duration or the highest wage is defined as the main episode.
#  However, it is left to the user to decide which characteristic defines the
#  main episode.
#
# This is handled by an optional argument in the prepare function

handle_parallel_episodes <- function(.connection, 
                                     .log_file = NULL, 
                                     .handling="wage"){
  
  
  #Validate the inputs
  c(".handling must be either set to 'wage' or 'tenure'" = .handling %in% c("wage","tenure")) %>%
    validate_inputs()
    
  # Remove old log file if it exists
  if (!is.null(.log_file) && file.exists(.log_file)) {
    file.remove(.log_file)
  }
  
  # Clear existing log appenders
  log_appender(NULL, namespace = "parallel")
  
  # Initialize console logger
  log_appender(appender_console, namespace = "parallel")
  
  # Initialize file logger if log_file is specified
  if (!is.null(.log_file)) {
    log_appender(appender_tee(file = .log_file), namespace = "parallel")
  }
  
  
  #------------------------------------------------------
  # Identify main episode (by sorting data accordingly)
  #------------------------------------------------------
  
 # define job with longest tenure as main episode
  if(.handling!="wage"){
    log_info("Job with longest tenure is defined as main episode", namespace ="parallel")
      set_sort_order <- function(.tbl){
        window_order(.tbl ,persnr,begepi,quelle_gr,desc(tage_bet),desc(wage_imp))
      }
  }
  #define job with highest wage as main episode
  if(.handling=="wage"){
    log_info("Job with highest wage  is defined as main episode", namespace ="parallel")
    set_sort_order <- function(.tbl){
      window_order(.tbl ,persnr,begepi,quelle_gr,desc(wage_imp),desc(tage_bet))
    }
  }
  
  #------------------------------------------------------
  # Generate information on parallel jobs (parallel_jobs, parallel_wage, parallel_wage_imp)
  #------------------------------------------------------
  
  log_info("Generating info on parallel jobs and keeping only the main spell", namespace = "parallel")
  
  tbl(.connection, "data") %>%
    mutate(tmp_jobs     =  as.integer(quelle_gr == 1),
           tmp_benefits = as.integer(quelle_gr == 2),
           tmp_wage =  if_else(quelle_gr==1 & !is.na(tentgelt_gr), tentgelt_gr, 0),
           tmp_wage_imp =  if_else(quelle_gr==1 & !is.na(wage_imp), wage_imp, 0)) %>%
    group_by(persnr,begepi) %>%
    set_sort_order() %>%
    mutate(
      #count parallel employment episodes
      parallel_jobs = sum(tmp_jobs, na.rm=TRUE),
      #total wage of all parallel employment episodes
      parallel_wage = sum(tmp_wage, na.rm=TRUE),
      #total (imputed) wage of all parallel employment episodes
      parallel_wage_imp = sum(tmp_wage_imp, na.rm=TRUE),
      #Attach indicator for recipience of UI benefits to all parallel episodes
      parallel_benefits = max(tmp_benefits, na.rm=TRUE)
    ) %>%
    #Only keep the main episode
    filter(row_number()==1) %>%
    window_order(persnr,begepi) %>%
    group_by(persnr) %>%
    mutate(nspell = row_number()) %>%
    ungroup()  %>%
    compute_and_overwrite()
    
  log_success("-> parallel episodes handled", namespace = "parallel")
  
  #------------------------------------------------------
  # Cleanup
  #------------------------------------------------------
  tbl(.connection, "data") %>%
    select(-spell,-level1,-level2,-starts_with("tmp_")) %>%
    arrange(persnr,nspell) %>%
    compute_and_overwrite()
  
  log_success(" ->  Cleanup finished", namespace = "parallel")
  log_success("Parallel episodes file finished", namespace = "parallel")
  
  #Return the .connection so we can pipe prepare functions
  return(.connection)
  
  
  
}
  




  
  

  
  
  


