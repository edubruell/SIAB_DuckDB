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


generate_biographic_variables <- function(connection, log_file = NULL){
  #---------------------------------------#
  # Setup logging for SIAB_bio            #
  #---------------------------------------#

  # Remove old log file if it exists
  if (!is.null(log_file) && file.exists(log_file)) {
    file.remove(log_file)
  }

  # Clear existing log appenders
  log_appender(NULL, namespace = "siab_bio")

  # Initialize console logger
  log_appender(appender_console, namespace = "siab_bio")

  # Initialize file logger if log_file is specified
  if (!is.null(log_file)) {
    log_appender(appender_tee(file = log_file), namespace = "siab_bio")
  }

  log_info("Script to generate additional biographic variables from longitudinal data started", namespace = "siab_bio")

  # Every window below is ordered on (begepi, spell) or (spell, begepi) rather
  # than on spell alone. 01_split_episodes.do cuts a spell that runs over a year
  # boundary into one row per year and gives the pieces the same spell number,
  # so spell stopped identifying a row at the previous step. Ordering on it
  # alone leaves ties, and a running total over tied rows hands each of them a
  # different value from run to run. That is what made anz_lst and tage_lst move
  # between two runs over the same data before this was written.

  #---------------------------------------#
  # Add OBSERVATION COUNTER               #
  #---------------------------------------#
  log_info("Adding level1 (PER EPISODE AND SOURCE) and level2 (PER EPISODE) observation counters ", namespace = "siab_bio")

  tbl(connection, "data") |>
    #OBSERVATION COUNTER PER EPISODE AND SOURCE
    group_by(persnr, begepi, quelle) |>
    window_order(spell) |>
    mutate(level1 = row_number() - 1L) |>
    #OBSERVATION COUNTER PER EPISODE
    group_by(persnr, begepi) |>
    window_order(spell) |>
    mutate(level2 = row_number() - 1L) |>
    ungroup() |>
    compute_and_overwrite()

  log_success("-> Observation counters added", namespace = "siab_bio")

  missing_obs_counters <- tbl(connection, "data") |>
    filter(is.na(level1)|is.na(level2)) |>
    count() |>
    pull()

  if (missing_obs_counters > 0) {
    log_error("-> ASSERT: Missings found in observation counters", namespace = "siab_bio")
    stop(glue("{missing_obs_counters} rows have a missing observation counter"),
         call. = FALSE)
  }
  log_success("-> ASSERT: No missings in observation counters", namespace = "siab_bio")

  #---------------------------------------#
  # FIRST DAY IN EMPLOYMENT  (ein_erw)    #
  #---------------------------------------#

  log_info("Creating emp, azubi and ein_erw variables", namespace = "siab_bio")

  # 03_SIAB_bio.do:
  #   gen byte azubi = inlist(erwstat, 102, 121, 122, 141) & grund != 154
  #   gen byte emp   = 1 if azubi != 1 & quelle == 1 & grund != 154
  #
  # Both conditions have to survive a missing value the way Stata reads one.
  # inlist() on a missing erwstat is 0, not missing, and `grund != 154` is true
  # when grund is missing, so coalesce() stands in for both. Dropping the second
  # test is not an option even though 02_grund154.do has already removed every
  # grund == 154 spell: the reference keeps it, and so does this.
  #
  # emp is 1 or missing, never 0. That is what makes the two lines below work:
  # a person's first employment is the smallest begorig over the emp == 1 rows,
  # and Stata carries it to every row of the person with egen max().
  tbl(connection, "data") |>
    mutate(
      azubi = if_else(coalesce(erwstat, -1L) %in% c(102L, 121L, 122L, 141L) &
                        coalesce(grund, -1L) != 154L, 1L, 0L),
      emp   = if_else(quelle == 1L & coalesce(grund, -1L) != 154L, 1L, NA_integer_),
      emp   = if_else(azubi == 1L, NA_integer_, emp)
    ) |>
    group_by(persnr) |>
    mutate(ein_erw = min(if_else(emp == 1L, begorig, NA), na.rm = TRUE)) |>
    ungroup() |>
    compute_and_overwrite()

  log_success("-> First day in employment (ein_erw variable) created",
              namespace = "siab_bio")

  #-----------------------------------------#
  # NUMBER OF DAYS IN EMPLOYMENT (tage_erw) #
  #-----------------------------------------#

  log_info("Computing number of days in employment",
           namespace = "siab_bio")

  # Only the first spell of an episode contributes its length, so parallel
  # episodes are counted once. The running total is taken in (begepi, nrE)
  # order, and nrE is missing outside employment, which sorts last in Stata and
  # in DuckDB alike.
  tbl(connection, "data") |>
    #COUNTER OF EMPLOYMENT OBSERVATIONS PER EPISODE
    #(WITHOUT VOCATIONAL TRAINING)
    group_by(persnr, emp, begepi) |>
    window_order(spell) |>
    mutate(nrE = if_else(emp == 1L, row_number(), NA_integer_)) |>
    ungroup() |>
    #AUXILIARY VARIABLE FOR EMPLOYMENT DURATIONS
    #(EXCLUDING PARALLEL EPISODES)
    mutate(d = if_else(!is.na(nrE) & nrE == 1L,
                       as.integer(endepi - begepi + 1L), 0L)) |>
    #RUNNING TOTAL OF JOB DURATIONS
    group_by(persnr) |>
    window_order(begepi, nrE, spell) |>
    mutate(tage_erw = cumsum(d)) |>
    ungroup() |>
    select(-d, -emp, -nrE) |>
    compute_and_overwrite()

  log_success("-> Number of days in employment (tage_erw) variable created)",
              namespace = "siab_bio")

  #---------------------------------------------------------------------#
  # FIRST DAY (ein_bet) and NUMBER OF DAYS IN ESTABLISHMENT (tage_bet)  #
  #---------------------------------------------------------------------#

  log_info("Computing first day and number of days in establishment",
           namespace = "siab_bio")

  #SIAB 7523 v2 carries a real establishment identifier (betnr, from betnr_siab),
  #so the grouping is by establishment as in 03_SIAB_bio.do. The SUF this code
  #was written for had only betnr, a person-specific counter that numbered the
  #establishments in one working life in order of first appearance.
  #
  #ein_bet is the smallest begorig, the start of the unsplit spell, over the
  #employment rows of the person and establishment. begepi would be the split
  #episode's own start, which is later whenever the spell crossed a new year.
  tbl(connection, "data") |>
    mutate(emp2 = if_else(quelle == 1L & !is.na(betnr) &
                            coalesce(grund, -1L) != 154L, 1L, NA_integer_)) |>
    # First day in establishment (ein_bet)
    group_by(persnr, betnr) |>
    mutate(ein_bet = min(if_else(emp2 == 1L, begorig, NA), na.rm = TRUE)) |>
    # AUXILIARY VARIABLE MARKS DUPLICATE OBSERVATIONS PER ESTABLISHMENT AND EPISODE
    group_by(persnr, betnr, begepi, endepi) |>
    window_order(spell) |>
    mutate(nrB = if_else(!is.na(betnr) & coalesce(grund, -1L) != 154L,
                         row_number(), NA_integer_)) |>
    ungroup() |>
    # CALCULATION OF THE DURATION
    mutate(dauer = if_else(!is.na(nrB) & nrB == 1L,
                           as.integer(endepi - begepi + 1L), 0L)) |>
    # RUNNING TOTAL OF DAYS IN ESTABLISHMENT
    group_by(persnr, betnr) |>
    window_order(spell, begepi) |>
    mutate(tage_bet = if_else(is.na(betnr), NA_integer_, cumsum(dauer))) |>
    ungroup() |>
    select(-emp2, -nrB, -dauer) |>
    compute_and_overwrite()

  log_success("-> First day and number of days in establishment variables created)",
              namespace = "siab_bio")

  #-----------------------------------------------------------------#
  # FIRST DAY IN JOB (ein_job) AND NUMBER OF DAYS IN JOB (tage_job)  #
  #-----------------------------------------------------------------#

  log_info("Computing first day and number of days in job",
           namespace = "siab_bio")

  # 03_SIAB_bio.do works this block out row by row over the data sorted by
  # person, apprenticeship, establishment and spell:
  #
  #   gen byte job = 1 if persnr == persnr[_n-1] & betnr == betnr[_n-1] &
  #                       azubi == azubi[_n-1] & !missing(betnr)
  #
  # In that sort order the test is just "this is not the first row of its
  # person-apprenticeship-establishment group", which is what row_number() says
  # here without a lag. A job then ends where job is missing, so the runs of
  # job == 1 are the jobs, and a cumulative count of the breaks names them.
  #
  # ein_job is the begepi of the row that opens the run, and tage_job the
  # running duration inside it. The reference reaches both by copying from the
  # previous row, which is a running total in disguise.
  tbl(connection, "data") |>
    # CONSIDER ENDING NOTIFICATION OF PREVIOUS MAIN EMPLOYMENT IN CASE OF GAPS
    # grund levels, following 03_SIAB_bio.do: inlist(grund[1], 130, 134, 140, 149)
    # 130: Deregistration due to end of employment
    # 134: Deregistration due to interruption of employment for more than one month
    # 140: Simultaneous registration and deregistration due to end of employment
    # 149: Deregistration due to death
    # AUXILIARY VARIABLE FOR NUMBER OF PARALLEL EPISODES PER JOB
    group_by(persnr, azubi, betnr, begepi) |>
    window_order(spell) |>
    mutate(end = if_else(first(grund) %in% c(130L, 134L, 140L, 149L), 1L, 0L),
           nrA = if_else(!is.na(betnr), row_number(), NA_integer_)) |>
    # MARK SUBSEQUENT EPISODES OF JOB
    group_by(persnr, azubi, betnr) |>
    window_order(begepi, spell) |>
    mutate(job = if_else(!is.na(betnr) & row_number() > 1L, 1L, NA_integer_),
           gap = if_else(job == 1L,
                         as.integer(begepi - lag(endepi) - 1L), NA_integer_)) |>
    # COUNT AS NEW JOB IF EMPLOYER REPORTED END OF EMPLOYMENT AND GAP > 92 DAYS
    # COUNT AS NEW JOB IF GAP > 366 DAYS
    mutate(job = if_else((coalesce(lag(end), 0L) == 1L & gap > 92L) | gap > 366L,
                         NA_integer_, job)) |>
    # RUNS OF job == 1 ARE THE JOBS
    mutate(job_run = cumsum(if_else(is.na(job), 1L, 0L)),
           jobdauer = if_else(is.na(betnr), NA_integer_,
                              as.integer(endepi - begepi + 1L)),
           # The row that opens a job contributes its own length; a later row
           # contributes its length only when it is the episode's first spell,
           # so parallel episodes in the same job are not counted twice.
           jobdauer_add = case_when(
             is.na(betnr) ~ NA_integer_,
             is.na(job)   ~ jobdauer,
             nrA == 1L    ~ jobdauer,
             TRUE         ~ 0L
           )) |>
    # GENERATE START DATE OF JOB AND THE RUNNING TOTAL OF ITS DURATION
    group_by(persnr, azubi, betnr, job_run) |>
    window_order(begepi, spell) |>
    mutate(ein_job  = if_else(is.na(betnr), NA, first(begepi)),
           tage_job = if_else(is.na(betnr), NA_integer_, cumsum(jobdauer_add))) |>
    ungroup() |>
    select(-end, -nrA, -job, -gap, -job_run, -jobdauer, -jobdauer_add) |>
    compute_and_overwrite()

  log_success("-> ein_job and tage_job variables created",
              namespace = "siab_bio")

  #--------------------------------------*
  # NUMBER OF BENEFIT RECEIPTS (anz_lst) *
  #--------------------------------------*

  log_info("Computing number of benefit receipts",
           namespace = "siab_bio")

  # nrL is missing outside a benefit episode, exactly as in the reference. The
  # tage_lst block below relies on that: it sorts on nrL and needs the
  # non-benefit rows to land last.
  #
  # A receipt counts as new when more than 10 days have passed since the end of
  # the last one. Where there is no last one the difference is missing, and a
  # Stata missing is larger than 10, so the first receipt of a person always
  # counts. That is the is.na(ende_vor) branch.
  tbl(connection, "data") |>
    # MARK EPISODE OF BENEFIT RECEIPT (DATA SOURCES LeH, LHG)
    mutate(quelleL = if_else(coalesce(quelle, -1L) %in% c(2L, 3L), 1L, 0L)) |>
    # COUNTER OF BENEFIT RECEIPTS WITHIN EPISODE
    group_by(persnr, begepi, quelleL) |>
    window_order(quelle, spell) |>
    mutate(nrL = if_else(quelleL == 1L, row_number(), NA_integer_)) |>
    # COPY END DATE OF LAST BENEFIT RECEIPT TO SUBSEQUENT OBSERVATIONS
    group_by(persnr) |>
    window_order(spell, begepi) |>
    mutate(ende_vor = if_else(lag(quelleL) == 1L, lag(endepi), NA)) |>
    fill(ende_vor, .direction = "down") |>
    ungroup() |>
    # MARK OBSERVATIONS THAT COUNT AS SEPARATE BENEFIT RECEIPTS
    # Only 1 per episode, separate benefit receipt if gap > 10 days
    mutate(lst = if_else(quelleL == 1L & !is.na(nrL) & nrL == 1L &
                           (is.na(ende_vor) | begepi - ende_vor > 10L), 1L, 0L)) |>
    # RUNNING TOTAL OF BENEFIT RECEIPTS
    # 03_SIAB_bio.do: gsort persnr begepi -lst, then anz_lst accumulates lst
    group_by(persnr) |>
    window_order(begepi, desc(lst), spell) |>
    mutate(anz_lst = cumsum(lst)) |>
    ungroup() |>
    select(-ende_vor, -lst) |>
    compute_and_overwrite()

  log_success("-> anz_lst variable created",
              namespace = "siab_bio")

  #--------------------------------------------------*
  # NUMBER OF DAYS WITH BENEFIT RECEIPT (tage_lst)   *
  #--------------------------------------------------*

  log_info("Computing number of days with benefit receipts",
           namespace = "siab_bio")

  tbl(connection, "data") |>
    #DURATION OF BENEFIT RECEIPT (WITHOUT DURATION OF PARALLEL OBSERVATIONS)
    mutate(lstdauer = if_else(quelleL == 1L & !is.na(nrL) & nrL == 1L,
                              as.integer(endepi - begepi + 1L), 0L)) |>
    #03_SIAB_bio.do carries the running sum only on the main spell of each
    #episode and on non-benefit spells, then copies it to the parallel spells
    group_by(persnr) |>
    window_order(spell, begepi) |>
    mutate(tage_lst = cumsum(lstdauer),
           tage_lst = if_else(quelleL == 0L | (!is.na(nrL) & nrL == 1L),
                              tage_lst, NA_integer_)) |>
    # FILL VARIABLE FOR PARALLEL OBSERVATIONS
    #nrL is missing for non-benefit spells in Stata and therefore sorts last,
    #so benefit spells come first here
    group_by(persnr, begepi) |>
    window_order(nrL, spell) |>
    mutate(tage_lst = first(tage_lst)) |>
    ungroup() |>
    select(-lstdauer, -quelleL, -nrL) |>
    compute_and_overwrite()

  log_success("-> tage_lst variable created",
              namespace = "siab_bio")

  #-----------------------*
  # ADJUST MISSING VALUES *
  #-----------------------*

  # The four establishment and job variables say nothing outside the employment
  # history, so the reference blanks them wherever the source is not BEH. A
  # missing quelle counts as not BEH, because `quelle != 1` is true for a Stata
  # missing. Without this the columns carry a number on benefit spells that the
  # reference leaves empty.
  log_info("Blanking the establishment and job variables outside quelle 1",
           namespace = "siab_bio")

  tbl(connection, "data") |>
    mutate(not_beh = coalesce(quelle, -1L) != 1L) |>
    mutate(ein_bet  = if_else(not_beh, NA, ein_bet),
           tage_bet = if_else(not_beh, NA_integer_, tage_bet),
           ein_job  = if_else(not_beh, NA, ein_job),
           tage_job = if_else(not_beh, NA_integer_, tage_job)) |>
    select(-not_beh) |>
    compute_and_overwrite()

  log_success("-> Establishment and job variables blanked outside quelle 1",
              namespace = "siab_bio")
  log_success("Biographic variables script finished", namespace = "siab_bio")
  #Return the connection so we can pipe prepare functions
  return(connection)

}
