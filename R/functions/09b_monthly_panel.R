# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#  SIAB Preparation
#
# Transfer data set into a monthly panel (using one cutoff day per month)
# - one row per person and calendar month, the episode covering the cutoff day
# - retain information on total employment / unemployment durations and earnings
#   per year, as the yearly panel does
#
# Generates the variables:
#   - year_days_emp: total days employed per calendar year
#   - year_days_benefits: total days benefit recipience per calendar year
#   - year_labor_earn: total labor earnings per calendar year
#   - month: first day of the calendar month the row describes
#   - month_num: the month of the year, 1 to 12
#   - begepi_monthly: episode start, cut to the month
#   - endepi_monthly: episode end, cut to the month
#
# This is an alternative to build_yearly_panel(), not a step after it. Both
# start from the output of handle_parallel_episodes(), and a pipeline calls one
# or the other.
#
# Author(s): Eduard Brüll based on code by Wolfgang Dauth, Johann Eppelsheimer
#            and Heiko Stüber
#
# Version: 1.0
# Created: 2026-09-16
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


build_monthly_panel <- function(connection,
                                log_file     = NULL,
                                cutoff_day   = 15,
                                monthly_vars = TRUE){

  validate_inputs(c(
    "cutoff_day has to be a single whole number between 1 and 31" =
      length(cutoff_day) == 1 && !is.na(cutoff_day) &&
      cutoff_day == floor(cutoff_day) && cutoff_day >= 1 && cutoff_day <= 31,
    "monthly_vars has to be TRUE or FALSE" =
      length(monthly_vars) == 1 && is.logical(monthly_vars) && !is.na(monthly_vars)
  ))

  # A whole number, so that make_date() in the cutoff below gets an integer
  # rather than the decimal a plain 15 would be written into the query as.
  cutoff_day <- as.integer(cutoff_day)

  # Remove old log file if it exists
  if (!is.null(log_file) && file.exists(log_file)) {
    file.remove(log_file)
  }

  # Clear existing log appenders
  log_appender(NULL, namespace = "monthly_panel")

  # Initialize console logger
  log_appender(appender_console, namespace = "monthly_panel")

  # Initialize file logger if log_file is specified
  if (!is.null(log_file)) {
    log_appender(appender_tee(file = log_file), namespace = "monthly_panel")
  }

  #-----------------------------------------------------------------------------
  #  Aggregate employment outcomes: days employed, labor earnings
  #-----------------------------------------------------------------------------

  # 16_monthly_panel.do puts this block behind `if $monthly_vars == 1`, so the
  # argument carries the switch. The block is the one 16_yearly_panel.do runs
  # unconditionally, and it works on the unexpanded data: the three totals are
  # per person and calendar year, and every row of a year carries the same ones.
  if (monthly_vars) {

    log_info("Generating aggregate employment outcomes: days employed, labor earnings ", namespace = "monthly_panel")

    #Durations of episodes
    tbl(connection, "data") |>
      window_order(persnr, year, begepi, endepi, quelle) |>
      group_by(persnr, year, begepi, endepi) |>
      mutate(dur_emp      = if_else(quelle == 1, endepi - begepi + 1, 0),
             #16_yearly_panel.do also allows the legacy code quelle == 16 here,
             #which 16_monthly_panel.do does not. Neither occurs in SIAB 7523 v2,
             #so the two conditions select the same rows.
             dur_benefits = if_else(quelle == 2 | parallel_benefits == 1, endepi - begepi + 1, 0)
             ) |>
      group_by(persnr, year) |>
      #Total time working, total time receiving UI benefits
      mutate(
        year_days_emp      = sum(dur_emp, na.rm = TRUE),
        year_days_benefits = sum(dur_benefits, na.rm = TRUE),
        #Earnings=wage*duration
        year_labor_earn    = sum(parallel_wage_imp * dur_emp, na.rm = TRUE)
      ) |>
      ungroup() |>
      select(-dur_emp, -dur_benefits) |>
      compute_and_overwrite()

    log_success("->  year_days_emp, year_days_benefits and year_labor_earn generated", namespace = "monthly_panel")
  }

  #-----------------------------------------------------------------------------
  #  One row per episode and calendar month
  #-----------------------------------------------------------------------------
  log_info("Expanding each episode into one row per calendar month", namespace = "monthly_panel")

  # The reference counts the months of an episode, n-plicates the row with
  # Stata's expand and numbers the copies within persnr, begepi and endepi. That
  # numbering is only unambiguous while those three columns identify a row, which
  # they do after 15_parallel_episodes.do. Stop rather than silently multiply
  # rows if a caller runs this on data that still holds parallel episodes.
  episodes <- tbl(connection, "data") |>
    summarise(rows = n(), episodes = n_distinct(paste(persnr, begepi, endepi))) |>
    collect()

  if (episodes$rows != episodes$episodes) {
    stop("persnr, begepi and endepi do not identify a row: ", episodes$rows,
         " rows on ", episodes$episodes, " episodes. Run handle_parallel_episodes() first.")
  }

  # The expansion plan generates the months themselves rather than a counter:
  # one row per episode and calendar month, from the month begepi falls in to
  # the month endepi falls in. `month` is the first day of that month.
  dbExecute(connection, "
        CREATE TEMPORARY TABLE monthly_plan AS
        SELECT
            persnr,
            begepi,
            endepi,
            UNNEST(generate_series(date_trunc('month', begepi),
                                   date_trunc('month', endepi),
                                   INTERVAL 1 MONTH))::DATE AS month
        FROM
            data;
        ")
  log_success("-> Expansion plan written", namespace = "monthly_panel")

  tbl(connection, "monthly_plan") |>
    left_join(tbl(connection, "data"), by = c("persnr", "begepi", "endepi")) |>
    compute_and_overwrite()

  log_success("-> Data expanded to one row per episode and month", namespace = "monthly_panel")

  #-----------------------------------------------------------------------------
  #  Cut each row to its month and keep the one covering the cutoff day
  #-----------------------------------------------------------------------------
  log_info("Cutting episodes to their month and keeping the one covering the cutoff day", namespace = "monthly_panel")

  # The reference hardcodes the 15th. A cutoff day beyond the length of a short
  # month would drop that month entirely, so the day is capped at the month's
  # own last day; at the default of 15 the cap never binds.
  tbl(connection, "data") |>
    mutate(month_num      = month(month),
           #Update begepi and endepi to cover only this month
           begepi_monthly = greatest(begepi, month),
           endepi_monthly = least(endepi, last_day(month)),
           #The cutoff date of this month
           ref_date       = make_date(year(month), month(month),
                                      least(cutoff_day, day(last_day(month))))) |>
    #Restrict to one episode per month
    filter(ref_date >= begepi_monthly & ref_date <= endepi_monthly) |>
    compute_and_overwrite()

  log_success("-> One row per month kept, begepi_monthly and endepi_monthly generated", namespace = "monthly_panel")

  #-----------------------------------------------------------------------------
  #  Adjust durations to end at the monthly cutoff
  #-----------------------------------------------------------------------------
  log_info("Adjust durations to end at the monthly cutoff", namespace = "monthly_panel")

  #16_monthly_panel.do uses `replace ... if`, which leaves the rows that do not
  #meet the condition unchanged. The alternative branch therefore has to be the
  #existing value, not NA.
  tbl(connection, "data") |>
    mutate(overhang = as.integer(endepi_monthly - ref_date),
           tage_bet = if_else(quelle == 1, tage_bet - overhang, tage_bet),
           tage_job = if_else(quelle == 1, tage_job - overhang, tage_job),
           #Stata's inlist() returns 0 for a missing argument, so `!inlist(erwstat, ...)`
           #is true where erwstat is missing and the reference trims the counter.
           #SQL's NOT IN returns NULL there instead, which if_else() turns into a
           #missing tage_erw. The coalesce puts the Stata reading back, as in
           #build_yearly_panel().
           tage_erw = if_else(quelle == 1 & !coalesce(erwstat %in% c(102L, 121L, 122L, 141L, 144L), FALSE),
                              tage_erw - overhang, tage_erw),
           #16_monthly_panel.do also allows the legacy code quelle == 16, which
           #does not occur in SIAB 7523 v2.
           tage_lst = if_else(quelle == 2, tage_lst - overhang, tage_lst)) |>
    #The reference trims tage_lst in two separate `replace` statements, one on
    #quelle and one on parallel_benefits, where 16_yearly_panel.do uses a single
    #`replace` with both conditions joined by `or`. 15_parallel_episodes.do sets
    #parallel_benefits to 1 for every episode of a person and episode start that
    #includes a benefit episode, the benefit episode itself included, so a
    #surviving quelle 2 row meets both conditions and the reference subtracts the
    #overhang from it twice. The port reproduces that, because the fixture
    #comparison is against the reference as published, not against a corrected
    #version of it.
    mutate(tage_lst = if_else(coalesce(parallel_benefits == 1, FALSE),
                              tage_lst - overhang, tage_lst)) |>
    select(-overhang, -ref_date) |>
    compute_and_overwrite()

  log_success("-> Durations adjusted", namespace = "monthly_panel")

  #Delete the expansion plan from the database
  dbRemoveTable(connection, "monthly_plan")
  log_success("CLEANUP: Expansion plan deleted from database", namespace = "monthly_panel")
  log_success("Monthly panel file finished", namespace = "monthly_panel")

  #Return the connection so we can pipe prepare functions
  return(connection)

}
