#-------------------------------------------------------------------------------
# SIAB Preparation
# 
# Create broader education groups based on imputed education ("ausbildung_imp")
# 
# Generates the variables:
#   - educ: Education (university and university of applied science combined), 
#            imputed based on Fitzenberger, Osikominu & Voelter (2008)
# 
# Author(s): Eduard Bruell based on  Wolfgang Dauth, Johann Eppelsheimer
# 
# Version: 1.0
# Created: 2024-08-10
#-------------------------------------------------------------------------------
generate_educ_variable <- function(.connection, .log_file = NULL){
    
    # Remove old log file if it exists
    if (!is.null(.log_file) && file.exists(.log_file)) {
      file.remove(.log_file)
    }
    
    # Clear existing log appenders
    log_appender(NULL, namespace = "educ")
    
    # Initialize console logger
    log_appender(appender_console, namespace = "educ")
    
    # Initialize file logger if log_file is specified
    if (!is.null(.log_file)) {
      log_appender(appender_tee(file = .log_file), namespace = "educ")
    }
    
  log_info("Adding broad education categories", namespace = "educ")
  
  #generate broader education classes
  tbl(.connection, "data") %>%
    mutate(educ = case_when(
      ausbildung %in% c(5, 6,11,12) ~ 3L,   # degree from a university or university of applied science (Uni or FH)
      ausbildung %in% c(2, 4) ~ 2L,   # vocational training (Ausbildung)
      ausbildung %in% c(1, 3) ~ 1L,   # neither vocational training nor degree from university (of applied science)
      TRUE ~ NA_integer_                   # If none of the conditions are met, assign NA (or some other default value)
    )) %>%
    compute_and_overwrite()
  
  tbl(.connection, "data") %>% 
    group_by(educ) %>%
    count(ausbildung) %>%
    arrange(ausbildung) %>%
    ungroup() %>%
    collect() %>%
    glue_data("ausbildung = {ausbildung} encoded as educ = {educ} for {n} cases") %>%
    walk(log_info, namespace = "educ")
  
  log_success("Educ variable generated", namespace = "educ")
  #Return the .connection so we can pipe prepare functions
  return(.connection)
}
