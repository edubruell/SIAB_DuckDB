# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   
# 02.) generate_biographic_variables function
# 
#GENERATE ADDITIONAL BIOGRAPHIC VARIABLES FROM LONGITUDINAL DATA
#
#First day in employment (ein_erw):
#  - Date of entry into first employment
#- Periods of vocational training are ignored (erwstat = 102, 121, 122, 141, 144)
#--> Missing for persons continuously in vocational training in SIAB
#- Entry into first employment can be later than entry in first establishment or job, since the latter variables include periods of vocational training
#
#Number of days in employment (tage_erw):
#  - Total number of days a person was employed until the end of the observation or until cutoff date 
#- Periods of vocational training are ignored (erwstat = 102,121,122,141,144)
#--> Value 0 for persons who are in vocational training throughout
#
#First day in establishment (ein_bet):
#  - Date of entry into establishment
#- Includes periods of vocational training
#- Not affected by interruptions of employment
#--> unique for every combination of person and establishment
#
#Number of days in establishment (tage_bet):
#  - Number of days in establishment until the end of the observation or until cutoff date
#- Includes periods of vocational training 
#- Gaps are subtracted
#
#First day in job (ein_job):
#  - Start date of current job
#- Periods of vocational training are considered as separate jobs (even if there is no time lag between training and job)
#- Re-employment after interruptions in the same establishment are considered as new jobs if:
#  a) the reason of notification to social security agency implies end of employment: (grund = 30, 34, 40, 49) 
#+ Time gap > 92 days
#b) an other reason of notification exists
#+ Time gap > 366 days
#
#Number of days in job (tage_job):
#  - Number of days in current job until the end of the observation or until cutoff date
#- see ein_job
#
#Number of benefit receipts (anz_lst):
#  - Number of benefit receipts accoring to SGB II or SGB III
#- Sources LeH LHG
#- Time gaps are ignored if interruption < 10 days
#- Change of benefit type doesn't count as new benefit receipt
#
#	Number of days with benefit receipt (tage_lst):
#		- Duration of benefit receipts until the end of the observation or until cutoff date
#		- see anz_lst
#		- Gaps are subtracted
#
#
# Author(s): Eduard Bruell
# R/duckdb  Reimplementation of original  procedures by Johanna Eberle, Alexandra Schmucker   
# 
# Version: 1.0
# Created: 2024-07-14
# 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


generate_biographic_variables <- function(.connection, .log_file = NULL){
  #---------------------------------------#
  # Setup logging for SIAB_bio            #
  #---------------------------------------# 
  
  # Remove old log file if it exists
  if (!is.null(.log_file) && file.exists(.log_file)) {
    file.remove(.log_file)
  }
  
  # Clear existing log appenders
  log_appender(NULL, namespace = "siab_bio")
  
  # Initialize console logger
  log_appender(appender_console, namespace = "siab_bio")
  
  # Initialize file logger if log_file is specified
  if (!is.null(.log_file)) {
    log_appender(appender_tee(file = .log_file), namespace = "siab_bio")
  }
  
  log_info("Script to generate additional biographic variables from longitudinal data started", namespace = "siab_bio")
  
  #---------------------------------------#
  # Add OBSERVATION COUNTER               #
  #---------------------------------------# 
  log_info("Adding level1 (PER EPISODE AND SOURCE) and level2 (PER EPISODE) observation counters ", namespace = "siab_bio")
  
  tbl(.connection, "data") %>%
    group_by(persnr, begepi, quelle_gr) %>%
    window_order(persnr, begepi, quelle_gr) %>%
    #OBSERVATION COUNTER PER EPISODE AND SOURCE
    mutate(level1 = row_number() - 1) %>%
    group_by(persnr, begepi) %>%
    window_order(persnr, begepi) %>%
    #OBSERVATION COUNTER PER EPISODE
    mutate(level2 = row_number() - 1) %>%
    ungroup() %>%
    arrange(persnr, begepi, quelle_gr) %>%
    compute_and_overwrite()
  
  log_success("-> Observation counters added", namespace = "siab_bio")
  
  
  assertion_obs_counters <- tbl(.connection, "data") %>%
    filter(is.na(level1)|is.na(level2)) %>%
    count() %>%
    pull() == 0
  
  if(assertion_obs_counters){ log_success("-> ASSERT: No missings in observation counters", namespace = "siab_bio")}
  if(!assertion_obs_counters){log_error("-> ASSERT: Missings found in observation counters", namespace = "siab_bio")
    break}
  
  #---------------------------------------#
  # FIRST DAY IN EMPLOYMENT  (ein_erw)    #
  #---------------------------------------#
  
  log_info("Creating emp, azubi and ein_erw variables", namespace = "siab_bio")
  
  tbl(.connection, "data") %>%
    mutate(
      # TAG VOCATIONAL TRAINING
      azubi = if_else(erwstat_gr == 2, 1L, 0L),
      emp = if_else(quelle_gr == 1 & azubi == 0, 1L, 0L)
    ) %>%
    group_by(persnr, emp) %>%
    window_order(persnr, emp, begorig) %>%
    mutate(ein_erw=first(begorig)) %>%
    arrange(persnr, begepi, quelle_gr) %>%
    compute_and_overwrite()
  
  log_success("-> First day in employment (ein_erw variable) created", 
              namespace = "siab_bio")
  
  #-----------------------------------------#
  # NUMBER OF DAYS IN EMPLOYMENT (tage_erw) #
  #-----------------------------------------#
  
  log_info("Computing number of days in employment", 
           namespace = "siab_bio")
  
  tbl(.connection, "data") %>%
    group_by(persnr, emp,begepi) %>%
    window_order(persnr, emp, begepi, spell) %>%
           #COUNTER OF EMPLOYMENT OBSERVATIONS PER EPISODE 
           #(WITHOUT VOCATIONAL TRAINING)
    mutate(nrE = if_else(emp == 1, row_number(), NA_integer_),
           #AUXILIARY VARIABLE FOR EMPLOYMENT DURATIONS 
           #(EXCLUDING PARALLEL EPISODES)
           d   = if_else(nrE == 1, endepi-begepi+1,0)) %>%
    group_by(persnr) %>%
    window_order(persnr, begepi, nrE) %>%
    mutate(tage_erw = cumsum(d)) %>%
    ungroup() %>%
    arrange(persnr, begepi, quelle_gr) %>%
    select(-d,-emp,-nrE)%>%
    compute_and_overwrite()
    
  log_success("-> Number of days in employment (tage_erw) variable created)", 
              namespace = "siab_bio")
  
  #---------------------------------------------------------------------#
  # FIRST DAY (ein_bet) and NUMBER OF DAYS IN ESTABLISHMENT (tage_bet)  #
  #---------------------------------------------------------------------#
  
  log_info("Computing first day and number of days in establishment", 
           namespace = "siab_bio")
  
  #For the SUF we only have bnn and not a betnr
  #bnn is a person-specific counter for firms!
  #Numbers the establishments in a person’s working life in ascending order.
  # Example: The first establishment in which a person was employed receives the
  #     value 1. If the person moves to a different establishment, this establishment
  #     receives the value 2, etc. If the person returns to an establishment in which they
  #     were previously employed, then this establishment is given the value that applied
  #     for the first period of employment there (e.g., 2). If a person returns to the first
  #     establishment after just one change of establishment, this would result in the
  #     sequence 1-2-1 for the variable “bnn” over time. Establishment numbers that are
  #     missing in the original data are set to missing (.z) in the SIAB-R 7521.
  
  tbl(.connection, "data") %>%
    # First day in establishment (ein_bet)
    group_by(persnr, bnn) %>%
    mutate(ein_bet = min(begepi)) %>%
    group_by(persnr, bnn, begepi, endepi) %>%
    window_order(persnr,bnn,begepi,endepi,spell) %>%
    mutate(nrB   = row_number(),
           dauer = if_else(nrB == 1, as.integer(endepi -begepi + 1), 0L)) %>%
    group_by(persnr, bnn) %>%
    window_order(persnr,bnn,spell) %>%
    # Number of days in establishment (tage_bet)
    mutate(tage_bet = cumsum(dauer),
           tage_bet = if_else(is.na(bnn),NA_integer_,tage_bet)) %>%
    ungroup() %>%
    select(-dauer) %>%
    arrange(persnr, begepi) %>%
    compute_and_overwrite()
  
  
  log_success("-> First day and number of days in establishment variables created)", 
              namespace = "siab_bio")  
    
  #----------------------------#
  # FIRST DAY IN JOB (ein_job) #
  #----------------------------#
  
  log_info("Computing first day in job", 
           namespace = "siab_bio")
  
  tbl(.connection, "data") %>%
    mutate(ein_job = if_else(!is.na(bnn),begepi,NA)) %>%
    window_order(persnr,azubi,bnn,spell) %>%
    mutate(job = if_else(persnr == lag(persnr) & 
                         bnn    == lag(bnn) & 
                         azubi  == lag(azubi) & !is.na(bnn),1L,NA_integer_)
    ) %>%
    ungroup() %>%
    window_order(persnr,azubi,bnn,begepi,spell) %>%
    group_by(persnr,azubi,bnn,begepi) %>%
    #Tag job endings of main spell by grund
    #Grund_gr levels
    #0: Deregistration due to end of employment
    #3: Deregistration due to interruption of employment for more than one month (also industrial conflict/dispute) (since '99)
    #5: Simultaneous registration and deregistration due to end of employment (since '99)
    mutate(end = if_else(first(grund_gr) %in% c(0, 3, 5), 1L, NA)) %>%
    ungroup() %>%
    mutate(gap     = if_else(job==1,as.integer(begepi - lag(endepi) - 1), NA_integer_),
          #COUNT AS NEW JOB IF EMPLOYER REPORTED END OF EMPLOYMENT AND GAP > 92 DAYS
          #COUNT AS NEW JOB IF GAP > 366 DAYS
          job = if_else((lag(end) == 1 & gap > 92) | gap > 366, NA, job),
          job_ep_switch = if_else(job==1&is.na(lag(job)),1L,0L),
          job_ep_switch = if_else(is.na(job_ep_switch),0L,job_ep_switch)) %>%
    group_by(persnr) %>%
    mutate(job_ep = cumsum(job_ep_switch),
           job_ep = if_else(is.na(job),NA,job_ep)
           ) %>%
    window_order(persnr,azubi,bnn,begepi,spell) %>%
    mutate(job_ep = if_else(!is.na(lead(job_ep)) & is.na(job_ep),lead(job_ep), job_ep)) %>%
    group_by(persnr,job_ep) %>%
    mutate(ein_job = min(ein_job),
           ein_job = if_else(is.na(job_ep),NA,ein_job)) %>%
    ungroup() %>%
    select(-end,-gap) %>%
    compute_and_overwrite()
  
  log_success("-> First day in job and job epsiode variables created", 
              namespace = "siab_bio")  
  
  #----------------------------------#
  # NUMBER OF DAYS IN JOB (tage_job) #
  #----------------------------------#
  
  log_info("Computing number of days in job", 
           namespace = "siab_bio")
  
  tbl(.connection, "data") %>%
    window_order(persnr, azubi, bnn, begepi,spell) %>%
    group_by(persnr, azubi, bnn, begepi) %>%
    mutate(nrA = row_number(),
      jobdauer = as.integer(endepi - begepi + 1),
      #Do not count duration in secondary jobs
      jobdauer = if_else(nrA != 1, 0L, jobdauer),
      jobdauer  = if_else(is.na(jobdauer), 0L,jobdauer)
    ) %>%
    ungroup() %>%
    window_order(persnr, azubi, bnn, begepi,spell) %>%
    group_by(persnr, job_ep) %>%
    mutate(
      jobdauer = cumsum(jobdauer),
      tage_job = if_else(is.na(job_ep),NA,jobdauer)
    ) %>%
    ungroup() %>%
    arrange(persnr,begepi,spell) %>%
    select(-jobdauer,-nrA,-job,-job_ep) %>%
    compute_and_overwrite()
  
  log_success("-> tage_job variable created", 
              namespace = "siab_bio")    
    
  #--------------------------------------*
  # NUMBER OF BENEFIT RECEIPTS (anz_lst) *
  #--------------------------------------*
    
  log_info("Computing number of benefit receipts", 
           namespace = "siab_bio")
  
  tbl(.connection, "data") %>%
    mutate(quelleL = if_else(quelle_gr %in% c(2, 16), 1L, 0L)) %>%
    window_order(persnr, begepi, quelleL,spell) %>%
    group_by(persnr, begepi, quelleL) %>%
    mutate(nrL = row_number()) %>%
    ungroup() %>%
    window_order(persnr,spell) %>%
    group_by(persnr) %>%
    # COPY END DATE OF LAST BENEFIT RECEIPT TO SUBSEQUENT OBSERVATIONS
    mutate(ende_vor = if_else(lag(quelleL)==1L, lag(endepi), NA)) %>%
    fill(ende_vor, .direction = "down") %>%
    ungroup() %>%
    # MARK OBSERVATIONS THAT COUNT AS SEPARATE BENEFIT RECEIPTS
    # Only 1 per episode, separate benefit receipt if gap > 10 days 
    mutate(lst = as.integer(quelleL==1 & nrL == 1 & (begepi-ende_vor> 10)),
           lst = if_else(is.na(lst),1L,lst)) %>%
    #RUNNING TOTAL OF BENEFIT RECEIPTS
    group_by(persnr) %>%
    mutate(lst = cumsum(lst))  %>%
    select(-ende_vor,-lst) %>%
    ungroup() %>%
    compute_and_overwrite()
  
  log_success("-> anz_lst variable created", 
              namespace = "siab_bio")    
  
  #--------------------------------------------------*
  # NUMBER OF DAYS WITH BENEFIT RECEIPT (tage_lst)   *
  #--------------------------------------------------*
  
  log_info("Computing number of days with benefit receipts", 
           namespace = "siab_bio")
  
  tbl(.connection, "data") %>%
    #DURATION OF BENEFIT RECEIPT (WITHOUT DURATION OF PARALLEL OBSERVATIONS)
    mutate(lstdauer = if_else(quelleL ==1 & nrL ==1, endepi - begepi +1, 0)) %>%
    window_order(persnr,spell) %>%
    group_by(persnr) %>%
    mutate(tage_lst = cumsum(lstdauer)) %>%
    ungroup() %>%
    select(-lstdauer,-quelleL,-nrL) %>%
    arrange(persnr,spell)
    
  log_success("-> tage_lst variable created", 
              namespace = "siab_bio")    
  log_success("Biographic variables script finished", namespace = "split_episodes")
  #Return the .connection so we can pipe prepare functions
  return(.connection)
    
}