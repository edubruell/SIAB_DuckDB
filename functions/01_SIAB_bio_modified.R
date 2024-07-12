
.connection = con
.log_file = here("log","01_SIAB_Bio.log")
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
  mutate(level1 = row_number() - 1) %>%
  group_by(persnr, begepi) %>%
  window_order(persnr, begepi) %>%
  mutate(level2 = row_number() - 1) %>%
  ungroup() %>%
  arrange(persnr, begepi, quelle_gr) %>%
  compute_and_overwrite()




tbl(.connection, "data") %>%
  group_by(persnr, begepi, quelle_gr) %>%
  arrange(persnr, begepi, quelle_gr) %>%
  #OBSERVATION COUNTER PER EPISODE AND SOURCE
  mutate(level1 = row_number() - 1) %>%
  compute() %>%
  group_by(persnr, begepi) %>%
  arrange(persnr, begepi) %>%
  #OBSERVATION COUNTER PER EPISODE
  mutate(level2 = row_number() - 1) %>%
  ungroup() %>%
  arrange(persnr, begepi, quelle_gr) %>%
  compute_and_overwrite()

log_success("Observation counters added", namespace = "siab_bio")

assertion_obs_counters <- tbl(.connection, "data") %>%
  filter(is.na(level1)|is.na(level2)) %>%
  count() %>%
  pull() == 0

if(assertion_obs_counters){ log_success("-> ASSERT: No missings in observation counters", namespace = "siab_bio")}
if(!assertion_obs_counters){log_error("-> ASSERT: Missings found in observation counters", namespace = "siab_bio")
  break}

#-----------------------------------------*
# NUMBER OF DAYS IN EMPLOYMENT (tage_erw) *
#-----------------------------------------*

tbl(.connection, "data") %>%
  mutate(
    # TAG VOCATIONAL TRAINING
    azubi = if_else(erwstat_gr == 2, 1L, 0L),
    emp = if_else(quelle_gr == 1 & azubi == 0, 1L, 0L)
  )


#-----------------------------------------------------------------------------------


# Assuming 'con' is your database connection
data <- tbl(con, "data")

# Helper function to convert Stata dates to R dates
stata_to_date <- function(stata_date) {
  as.Date(stata_date, origin = "1960-01-01")
}

# Main data processing
result <- data %>%
  # Sort data
  arrange(persnr, spell) %>%
  

  
  # First day in employment (ein_erw)
  mutate(
    azubi = if_else(erwstat_gr == 2, 1L, 0L),
    emp = if_else(quelle_gr == 1 & azubi == 0, 1L, 0L)
  ) %>%
  group_by(persnr, emp) %>%
  mutate(h = if_else(emp == 1, min(stata_to_date(begorig)), as.Date(NA))) %>%
  group_by(persnr) %>%
  mutate(ein_erw = max(h, na.rm = TRUE)) %>%
  
  # Number of days in employment (tage_erw)
  group_by(persnr, emp, begepi) %>%
  mutate(nrE = if_else(emp == 1, row_number(), NA_integer_)) %>%
  mutate(d = if_else(nrE == 1, as.integer(stata_to_date(endepi) - stata_to_date(begepi) + 1), NA_integer_)) %>%
  group_by(persnr) %>%
  mutate(tage_erw = cumsum(coalesce(d, 0L))) %>%
  
  # First day in establishment (ein_bet)
  group_by(persnr, betnr) %>%
  mutate(ein_bet = min(stata_to_date(begepi))) %>%
  
  # Number of days in establishment (tage_bet)
  group_by(persnr, betnr, begepi, endepi) %>%
  mutate(nrB = row_number()) %>%
  mutate(dauer = if_else(nrB == 1, as.integer(stata_to_date(endepi) - stata_to_date(begepi) + 1), NA_integer_)) %>%
  group_by(persnr, betnr) %>%
  mutate(tage_bet = cumsum(coalesce(dauer, 0L))) %>%
  
  # First day in job (ein_job)
  arrange(persnr, azubi, betnr, spell) %>%
  group_by(persnr, azubi, betnr) %>%
  mutate(
    job = if_else(lag(betnr) == betnr & lag(azubi) == azubi & !is.na(betnr), 1L, 0L),
    end = if_else(grund[1] %in% c(0, 3, 5), 1L, 0L),
    gap = as.integer(stata_to_date(begepi) - lag(stata_to_date(endepi)) - 1)
  ) %>%
  mutate(
    job = if_else((lag(end) == 1 & gap > 92) | gap > 366, 0L, job),
    ein_job = if_else(!is.na(betnr), stata_to_date(begepi), as.Date(NA))
  ) %>%
  mutate(ein_job = if_else(job == 1, lag(ein_job), ein_job)) %>%
  
  # Number of days in job (tage_job)
  group_by(persnr, azubi, betnr, begepi) %>%
  mutate(nrA = row_number()) %>%
  mutate(
    jobdauer = as.integer(stata_to_date(endepi) - stata_to_date(begepi) + 1),
    jobdauer_dup = if_else(nrA != 1, jobdauer, 0L)
  ) %>%
  group_by(persnr, azubi, betnr) %>%
  mutate(
    jobdauer_kum = jobdauer - jobdauer_dup,
    jobdauer_kum = if_else(job == 1, lag(jobdauer_kum) + jobdauer_kum, jobdauer_kum),
    tage_job = jobdauer_kum
  ) %>%
  
  # Number of benefit receipts (anz_lst)
  mutate(quelleL = if_else(quelle %in% c(2, 16), 1L, 0L)) %>%
  group_by(persnr, begepi, quelleL) %>%
  mutate(nrL = row_number()) %>%
  arrange(persnr, spell) %>%
  mutate(
    ende_vor = lag(stata_to_date(endepi)),
    lst = if_else(quelleL == 1 & nrL == 1 & (stata_to_date(begepi) - ende_vor > 10), 1L, 0L)
  ) %>%
  group_by(persnr) %>%
  mutate(anz_lst = cumsum(lst)) %>%
  
  # Number of days with benefit receipt (tage_lst)
  mutate(
    lstdauer = if_else(quelleL == 1 & nrL == 1, as.integer(stata_to_date(endepi) - stata_to_date(begepi) + 1), NA_integer_)
  ) %>%
  mutate(tage_lst = cumsum(coalesce(lstdauer, 0L))) %>%
  
  # Adjust missing values
  mutate(across(c(ein_bet, tage_bet, ein_job, tage_job), 
                ~if_else(quelle_gr != 1, NA_real_, .))) %>%
  mutate(across(c(ein_bet, tage_bet, ein_job, tage_job), 
                ~if_else(quelle_gr == 1 & is.na(.), NA_real_, .))) %>%
  mutate(ein_erw = coalesce(ein_erw, NA_real_))

# Collect results (this will actually execute the query)
result_collected <- collect(result)