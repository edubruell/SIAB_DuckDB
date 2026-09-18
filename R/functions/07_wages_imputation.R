#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#SIAB Preparation
#
#Imputation of right-censored wages
#
#In the original Dauth and Eppelsheimer code a more modern 2-Step imputation procedure, 
#similar to Dustmann et al. (2009) and Card et al. (2013) is used, that  does and imputation on observables in the first step 
#based on Gartner (2005) and computes imputation models including leave-one-out means (for firms) in a second step.
#
# Since this proof-of-concept is done entirely with the SUF, I can not do leave-one-out means because I only have a person-specific 
# firm identifier, so this implements only the Gartner step 

#The step draws a random term for every censored wage and sets no seed of its
#own, so a caller that wants the same wages twice calls set.seed() before it.
#Since 2026-09-18 that is enough: each cell is sorted before the draw and the
#leave-one-out sums are ordered, so a seeded run gives the same column
#whatever order DuckDB stores the data in. Before those two changes it did not,
#and two seeded runs differed on almost every imputed wage. The Python arm has
#the same property, by the same two means.
#
#Generates the variables:
#  - cens: 1 if right-censored/imputed wage, 0 otherwise; (4 EUR below assessment ceiling)
#   - wage: daily wage, not imputed, top-coded wages replaced by assessment ceiling (-4 EUR), deflated (2015)
#   - wage_imp: imputed daily wage, deflated (2015)
#
#
#Author(s): Eduard Brüll based on code by Wolfgang Dauth, Johann Eppelsheimer
#  
#  Version: 1.0
#Created: 2020-08-19
#
#References:
#  Card, D., J. Heining, and P. Kline (2013). Workplace heterogeneity and the rise of West German wage inequality. The Quarterly Journal of Economics 128 (3), 967-1015
#  Dustmann, C., J. Ludsteck, and U. Schönberg (2009). Revisiting the German wage structure. The Quarterly Journal of Economics 124 (2), 843-881.
#  Gartner, H. (2005).  The imputation of wages above the contribution limit with the German IAB employment sample. FDZ-Methodenreport 02/2005: http://doku.iab.de/fdz/reporte/2005/MR_2.pdf
#  Drechseler J., J. Ludsteck and Andreas Moczal (2023) Imputation der rechtszensierten Tagesentgelte für die BeH. FDZ-Methodenreport 05/2023
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  

impute_wages <- function(connection, log_file = NULL){
  # Remove old log file if it exists
  if (!is.null(log_file) && file.exists(log_file)) {
    file.remove(log_file)
  }
  
  # Clear existing log appenders
  log_appender(NULL, namespace = "impute_wages")
  
  # Initialize console logger
  log_appender(appender_console, namespace = "impute_wages")
  
  # Initialize file logger if log_file is specified
  if (!is.null(log_file)) {
    log_appender(appender_tee(file = log_file), namespace = "impute_wages")
  }
  
  
  #Check whether there is no temp table
  if(dbExistsTable(connection, "tmp_imputation")){
    log_warn("Temporary table for wage imputation 'tmp_imputation' already existed at start!", namespace = "impute_wages")
    dbRemoveTable(connection,  "tmp_imputation")
    log_info("-> 'tmp_imputation' table deleted to create new imputation run", namespace = "impute_wages")
    
  }
  
  #------------------------------------------------------
  #   Modify assessment limit and flag censored wages
  #------------------------------------------------------
  
  #Substract 4 EUR from exact assessment limit 
  #(to make sure all cencored wages are covered by the imputation)
  log_info("Modify assessment limit and flag censored wages", 
           namespace ="impute_wages")
  
  tbl(connection, "data") |>
    mutate(
      limit_assess4 = limit_assess_defl - (100 * 4/cpi),	
      ln_limit_assess4 = log(limit_assess4),
      #Flag censored wages. The reference generates the flag as 0 for every row
      #and only ever raises it for BeH spells:
      #  gen cens = 0
      #  replace cens = 1 if wage_defl > limit_assess4 & quelle == 1
      #so a spell off the employment history is flagged uncensored rather than
      #missing, and so is a spell whose assessment ceiling is unknown. The
      #second case is every BeH spell from 1992 on with a missing east flag,
      #15,738 rows of the test data: the ceiling differs between East and West,
      #so limit_assess_defl is missing there, and Stata's ordering of missing
      #makes the comparison false rather than missing. sql_stata_gt() carries
      #that ordering through.
      cens = if_else(quelle == 1 & !!sql_stata_gt("wage_defl", "limit_assess4"),
                     1L, 0L)
  ) |>
    compute_and_overwrite()
  
  log_success(" ->  limit_assess4, ln_limit_assess4 and cens added", namespace = "impute_wages")
  
  #------------------------------------------------------
  # Overview of censored wages
  #------------------------------------------------------
  
  #overall censoring
  tbl(connection, "data") |>
    filter(quelle == 1, !is.na(cens)) |>
    count(cens) |>
    collect() |>
    mutate(cens = factor(cens, levels=c(0,1),labels = c("below the wage assessment limit","above the wage assessment limit")),
           pct = scales::label_percent()(n/sum(n))) |>
    glue_data("{n} BeH wages ({pct}) are {cens}") |>
    walk(log_info, namespace = "impute_wages")
  log_info("--------------------", namespace ="impute_wages")

  #Censoring by education
  tbl(connection, "data") |>
    filter(quelle == 1, !is.na(cens)) |>
    count(cens,educ)  |>
    collect() |>
    pivot_wider(id_cols="educ",values_from="n",names_from="cens",names_prefix = "cens") |>
    mutate(educ = if_else(is.na(educ),0L,educ),
           educ = factor(educ, levels=c(0,1,2,3), labels=c("Missing","Low","Medium","High")),
           pct = scales::label_percent()(cens1/(cens0+cens1))
           ) |>
    glue_data("{pct} of wages are censored for {educ} Education") |>
    walk(log_info, namespace = "impute_wages")
  log_info("--------------------", namespace ="impute_wages")
  
  
  #Censorig of high-skilled workers by age groups
  
  # Define the breakpoints
  age_lower_bounds <- c(18, seq(25, 55, 5))
  age_upper_bounds <- seq(25, 60, 5)
  
  # Combine into a single vector, adjusting the last value to include the final upper bound
  age_breaks <- c(age_lower_bounds[1], age_upper_bounds)
  
  # Create labels for the categories
  age_labels <- paste(age_lower_bounds, age_upper_bounds, sep = " to ")
  
  # Use cut function to create the age categories
  tbl(connection, "data") |>
    filter(quelle == 1, !is.na(cens),educ==3L,age<=60,age>=18) |>
    mutate(age_category = cut(age, 
                              breaks = age_breaks, 
                              labels = age_labels, 
                              right = FALSE, 
                              include.lowest = TRUE)
    ) |>
    group_by(age_category) |>
    summarise(share_censored = mean(cens, na.rm=TRUE)) |>
    arrange(age_category) |>
    collect() |>
    mutate(share_censored = scales::label_percent()(share_censored)) |>
    glue_data("For the highly educated in age range {age_category} {share_censored} of wages are censored") |>
    walk(log_info, namespace = "impute_wages")
  
  log_info("--------------------", namespace ="impute_wages")
  
  #censoring by fulltime/part-time
  tbl(connection, "data") |>
    filter(quelle == 1, !is.na(cens))  |>
    group_by(teilzeit) |>
    summarise(share_censored = mean(cens, na.rm=TRUE)) |>
    collect() |>
    mutate(teilzeit = if_else(is.na(teilzeit),9L,teilzeit),
           teilzeit = factor(teilzeit, levels=c(0,1,9), labels=c("Fulltime","Parttime","Missing FT-Info")),
           share_censored = scales::label_percent()(share_censored)
    ) |>
    glue_data("{share_censored} of wages are censored for {teilzeit} employees") |>
    walk(log_info, namespace = "impute_wages")
  
  log_info("--------------------", namespace ="impute_wages")
  
    
  #censoring by gender
  tbl(connection, "data") |>
    filter(quelle == 1, !is.na(cens))  |>
    group_by(frau) |>
    summarise(share_censored = mean(cens, na.rm=TRUE)) |>
    collect() |>
    mutate(frau = factor(frau, levels=c(0,1), labels=c("Men","Women")),
           share_censored = scales::label_percent()(share_censored)
    ) |>
    glue_data("{share_censored} of wages of {frau} are censored ") |>
    walk(log_info, namespace = "impute_wages")
  
  log_info("--------------------", namespace ="impute_wages")

  #------------------------------------------------------
  #Prepare dependent variable: wage
  #------------------------------------------------------
  
  log_info("Preapering dependent variable for imputation", namespace ="impute_wages")
  
  tbl(connection, "data") |>
    mutate(
      #Daily wage, not imputed, top-coded wages replaced by assessment ceiling (-4 EUR)
      #  gen     wage = wage_defl     if quelle == 1
      #  replace wage = limit_assess4 if quelle == 1 & wage_defl > limit_assess4
      #Same ordering of missing as cens above, so a spell with an unknown
      #ceiling keeps its raw deflated wage instead of going missing.
      wage = case_when(quelle != 1 ~ NA_real_,
                       !!sql_stata_gt("wage_defl", "limit_assess4") ~ limit_assess4,
                       .default = wage_defl),
      ln_wage = if_else(wage!=0,log(wage),NA_real_),
      ln_wage_cens = if_else(cens == 0,ln_wage,NA_real_)
    ) |>
    compute_and_overwrite()
  
  log_success(" ->  wage, ln_wage and ln_wage_cens variables added", namespace = "impute_wages")
  
  
  #----------------------------------------------------------------------
  # Prepare control variables for imputation
  #----------------------------------------------------------------------
  
  log_info("Preapering control variables for imputation", namespace ="impute_wages")
  
  tbl(connection, "data") |>
    mutate(
      #express age squared in 100 years (to have readable coefficients in the regressions)
      age_sq = (age/10)^2, 			
      #dummy for "older" people
      old  = as.integer(age > 40),		
      #different age profiles for young and old workers
      age_old = age * old, 			
      #age_old squared
      age_sq_old = age_sq * old,		
      #tenure squared
      tenure_sq = (tage_job/10)^2,
      #education groups (for the imputation regard missings as low-skilled)
      educ_tmp = if_else(!is.na(educ),educ,1L)
  ) |>
    compute_and_overwrite()
  
  #10_wages_imputation.do:
  #  global controls frau teilzeit old age age_sq age_old age_sq_old tage_job tenure_sq
  controls_imputation <- c("frau", "teilzeit", "old", "age", "age_sq",
                           "age_old", "age_sq_old", "tage_job", "tenure_sq")
  
  
  log_success(" ->  Controls variables for imputation added", namespace = "impute_wages")

  #----------------------------------------------------------------------
  # Prepare the imputation plan; by default: year, skill group and East/West
  #----------------------------------------------------------------------
  #(NOTE: by default we assign Berlin to East Germany)
  #
  #The reference loops year, then education group, then East/West, and runs a
  #cell even when it holds nothing. The plan is built from the data instead, so
  #an empty cell is never visited, but the order is the reference's.

  imputation_plan <- tbl(connection, "data") |>
    distinct(year, educ_tmp, east) |>
    filter(!is.na(east)) |>
    arrange(year, educ_tmp, east) |>
    collect()

  glue("Imputation plan: {nrow(imputation_plan)} year/education/east cells, ",
       "{min(imputation_plan$year)} to {max(imputation_plan$year)}") |>
    log_info(namespace = "impute_wages")

  #----------------------------------------------------------------------
  # One cell of one imputation step
  #----------------------------------------------------------------------
  #
  #This is the body of both of the reference's loops. They differ only in the
  #regressors and in the column they write, so `extra` carries the four
  #leave-one-out terms the second step adds and `target` names the output.
  #
  #The structure follows 10_wages_imputation.do closely enough to be read
  #against it:
  #
  #  intreg ln_wage ln_wage_cens $controls if marginal == 0
  #  predict xbn if e(sample), xb
  #  gen eta = (ln_limit_assess4 - xbn) / $sdi if e(sample)
  #  gen     ln_wage_tmp = ln_wage if cens == 0
  #  replace ln_wage_tmp = xbn + $sdi * invnorm(normal(eta) + uniform()*(1-normal(eta))) ///
  #          if e(sample) & cens == 1
  #
  #Two things in that block are easy to get wrong and both were wrong here
  #before. The estimation sample is the cell minus marginal employment minus
  #any row with a missing regressor, but the line that carries an uncensored
  #wage through is NOT restricted to it: a marginal spell, or one with a
  #missing control, still keeps its own log wage. And the draw is taken only
  #for censored rows inside the estimation sample.

  impute_cell <- function(plan_year, plan_educ_tmp, plan_east,
                          extra = character(0), target) {
    regressors <- c(controls_imputation, extra)

    #The sort is what makes this step reproducible. runif() hands out its draws
    #in row order, so without it the draw a row gets is decided by the order
    #DuckDB happens to return the cell in, and a seed does not fix that: two
    #runs of this file over the same database, both under set.seed(123), gave
    #wage_imp differing on 29,651 of the 30,315 censored spells of the test data
    #on 2026-09-18, by up to 1831 EUR a day. persnr and spell are the
    #reference's own sort, the order it puts the whole dataset into before it
    #seeds; begepi is this port's addition, because episode splitting means the
    #first two do not name a row. The three together are unique, and the Python
    #arm sorts on the same three.
    cell <- tbl(connection, "data") |>
      filter(year     == plan_year,
             educ_tmp == plan_educ_tmp,
             east     == plan_east) |>
      select(persnr, spell, begepi, endepi, marginal, cens, ln_wage,
             ln_limit_assess4, all_of(regressors)) |>
      arrange(persnr, spell, begepi) |>
      collect()

    #e(sample): `if marginal == 0` excludes a missing marginal flag, and Stata
    #drops a row with a missing dependent or regressor from the estimation on
    #top of that.
    complete <- !is.na(cell$marginal) & cell$marginal == 0 & !is.na(cell$ln_wage)
    for (column in regressors) {
      complete <- complete & !is.na(cell[[column]])
    }

    #gen ln_wage_tmp = ln_wage if cens == 0, over the whole cell
    imputed <- if_else(cell$cens == 0, cell$ln_wage, NA_real_)

    #The reference wraps every estimation command in `capture noisily`, so a
    #cell too thin to fit leaves the previous cell's e() in place and imputes
    #from a model of different data. That is a bug in the reference rather than
    #a behaviour to reproduce: here a cell that cannot be fitted contributes
    #its uncensored wages and no draws, and says so in the log.
    fit <- try(
      survreg(
        as.formula(paste("Surv(ln_wage, !cens, type = 'right') ~",
                         paste(regressors, collapse = " + "))),
        data = cell[complete, , drop = FALSE],
        dist = "gaussian"
      ),
      silent = TRUE
    )

    if (inherits(fit, "try-error")) {
      glue("No fit for year={plan_year}, educ={plan_educ_tmp}, east={plan_east}: ",
           "{nrow(cell[complete, , drop = FALSE])} rows in the estimation sample. ",
           "Censored wages in this cell stay unimputed.") |>
        log_warn(namespace = "impute_wages")
    } else {
      estimated <- cell[complete, , drop = FALSE]
      se    <- fit$scale
      xb    <- predict(fit, newdata = estimated)
      alpha <- (estimated$ln_limit_assess4 - xb) / se
      draw  <- xb + se * qnorm(runif(nrow(estimated)) * (1 - pnorm(alpha)) +
                               pnorm(alpha))

      #replace ... if e(sample) & cens == 1
      take <- estimated$cens == 1
      imputed[which(complete)[take]] <- draw[take]

      below <- sum(!is.na(draw[take]) & draw[take] < estimated$ln_wage[take])
      if (below > 0) {
        glue("{below} imputed wage(s) below censoring limit in year ",
             "{plan_year} and education group {plan_educ_tmp} and east = {plan_east}") |>
          log_warn(namespace = "impute_wages")
      }
    }

    out <- cell |>
      transmute(persnr, spell, begepi, endepi, !!target := imputed)

    dbWriteTable(connection, "tmp_imputation", out, append = TRUE)
    invisible(NULL)
  }

  #A step is the plan run over every cell, plus the rows the loops never see,
  #merged back onto `data` under `target`.
  #
  #  keep if missing(east)
  #  replace ln_wage_imp = ln_wage if quelle==1
  #
  #is how the reference treats a spell with no East/West information: it is set
  #aside before the loop and carries its own log wage. Only the first step does
  #that; the second leaves ln_wage_imp2 missing there and picks the value back
  #up from the fallback at the end.
  run_imputation_step <- function(extra = character(0), target,
                                  carry_missing_east) {
    if (dbExistsTable(connection, "tmp_imputation")) {
      dbRemoveTable(connection, "tmp_imputation")
    }

    imputation_plan |>
      #pwalk matches plan columns to impute_cell() arguments by name, and the
      #names must differ from the data columns they filter on
      rename_with(~paste0("plan_", .x)) |>
      pwalk(impute_cell, extra = extra, target = target)

    #Built outside the transmute, because an `if` inside one would be handed to
    #the SQL translator rather than evaluated here.
    carried_value <- if (carry_missing_east) {
      quote(if_else(quelle == 1, ln_wage, NA_real_))
    } else {
      quote(NA_real_)
    }

    carried <- tbl(connection, "data") |>
      filter(is.na(east)) |>
      transmute(persnr, spell, begepi, endepi, !!target := !!carried_value) |>
      collect()

    dbWriteTable(connection, "tmp_imputation", carried, append = TRUE)

    tbl(connection, "data") |>
      left_join(tbl(connection, "tmp_imputation"),
                by = c("persnr", "spell", "begepi", "endepi")) |>
      compute_and_overwrite(target_table = "data")

    dbRemoveTable(connection, "tmp_imputation")
    gc()
    invisible(NULL)
  }

  #-------------------------------------------------------------------------------
  # Step 1: imputation with observable characteristics (Gartner 2005)
  #-------------------------------------------------------------------------------

  log_info("Step 1: imputation on observables", namespace = "impute_wages")
  run_imputation_step(target = "ln_wage_imp", carry_missing_east = TRUE)
  log_success(" ->  ln_wage_imp added", namespace = "impute_wages")

  #-------------------------------------------------------------------------------
  # Intermediate step: leave-one-out means of the imputed wages
  #-------------------------------------------------------------------------------
  #
  #Something like a worker and a plant fixed effect. Three details of the
  #reference decide the values and none of them is visible in the formula:
  #
  #  - `egen total()` sums a group of nothing but missings to 0, while SQL's
  #    SUM() of nothing but NULLs is NULL, hence the coalesce.
  #  - a worker seen once, or a plant with one sampled worker, has an empty
  #    leave-one-out set. The dummy records that BEFORE the gap is filled.
  #  - the fill is the mean over every row, taken before non-BeH spells are
  #    wiped, and for the plant it is the mean within the year.
  #
  #betnr is a real establishment number in SIAB 7523 v2, so the plant means are
  #computable; in the 2 percent sample most plants hold one sampled worker and
  #the fallback carries them.

  log_info("Leave-one-out means of the imputed wages", namespace = "impute_wages")

  #Every sum below is ordered, and that is the second half of what makes this
  #step reproducible. DuckDB adds a group's terms in whatever order its threads
  #hand them over, and floating-point addition is not associative, so the same
  #query over the same stored table returns sums that differ in their last bits:
  #measured on 2026-09-18, repeating one grouped sum over the test data five
  #times gave between 5,483 and 8,955 of the worker groups a different value
  #each time. These sums are step 2's regressors, so that wobble moves the fit,
  #the prediction and the draw. `window_order()` plus a frame covering the whole
  #partition turns the plain SUM(x) OVER (PARTITION BY ...) into one with an
  #ORDER BY, which fixes the summation order at the cost of a sort per
  #partition. The order is the same key the cells are sorted on above.
  ordered_window <- function(query) {
    query |>
      window_order(persnr, spell, begepi) |>
      window_frame(from = -Inf, to = Inf)
  }

  tbl(connection, "data") |>
    group_by(persnr, quelle) |>
    ordered_window() |>
    mutate(loo_obs = n(),
           loo_sum = coalesce(sum(ln_wage_imp, na.rm = TRUE), 0)) |>
    ungroup() |>
    mutate(ln_wage_mean_worker =
             if_else(loo_obs > 1 & !is.na(ln_wage_imp),
                     (loo_sum - ln_wage_imp) / (loo_obs - 1),
                     NA_real_)) |>
    select(-loo_obs, -loo_sum) |>
    mutate(only_one_obs = as.integer(is.na(ln_wage_mean_worker))) |>
    compute_and_overwrite()

  #The mean over every row, for the workers whose leave-one-out set is empty.
  #Taken as a window over the whole table rather than with summarise(), for the
  #same reason: an unordered AVG() is an unordered sum.
  overall_mean <- tbl(connection, "data") |>
    ordered_window() |>
    mutate(m = mean(ln_wage_imp, na.rm = TRUE)) |>
    select(m) |>
    head(1) |>
    collect() |>
    pull(m)

  tbl(connection, "data") |>
    mutate(ln_wage_mean_worker =
             case_when(quelle != 1 ~ NA_real_,
                       is.na(ln_wage_mean_worker) ~ overall_mean,
                       .default = ln_wage_mean_worker)) |>
    compute_and_overwrite()

  tbl(connection, "data") |>
    group_by(year, betnr) |>
    ordered_window() |>
    mutate(loo_obs = n(),
           loo_sum = coalesce(sum(ln_wage_imp, na.rm = TRUE), 0)) |>
    ungroup() |>
    group_by(year) |>
    ordered_window() |>
    mutate(year_mean = mean(ln_wage_imp, na.rm = TRUE)) |>
    ungroup() |>
    mutate(ln_wage_mean_firm =
             if_else(loo_obs > 1 & !is.na(ln_wage_imp),
                     (loo_sum - ln_wage_imp) / (loo_obs - 1),
                     NA_real_),
           only_one_worker = as.integer(is.na(ln_wage_mean_firm))) |>
    mutate(ln_wage_mean_firm =
             case_when(quelle != 1 ~ NA_real_,
                       is.na(ln_wage_mean_firm) ~ year_mean,
                       .default = ln_wage_mean_firm)) |>
    select(-loo_obs, -loo_sum, -year_mean) |>
    compute_and_overwrite()

  log_success(" ->  ln_wage_mean_worker, only_one_obs, ln_wage_mean_firm and only_one_worker added",
              namespace = "impute_wages")

  #-------------------------------------------------------------------------------
  # Step 2: extended imputation models including the leave-one-out means
  #-------------------------------------------------------------------------------

  log_info("Step 2: imputation including the leave-one-out means",
           namespace = "impute_wages")
  run_imputation_step(
    extra = c("ln_wage_mean_worker", "only_one_obs",
              "ln_wage_mean_firm", "only_one_worker"),
    target = "ln_wage_imp2",
    carry_missing_east = FALSE
  )
  log_success(" ->  ln_wage_imp2 added", namespace = "impute_wages")

  #-----------------------------------------------------------------------------
  #   Imputed wages in levels
  #-----------------------------------------------------------------------------

  log_info("Add imputed wages in levels", namespace = "impute_wages")

  tbl(connection, "data") |>
    mutate(wage_imp_int = exp(ln_wage_imp),
           wage_imp     = exp(ln_wage_imp2)) |>
    compute_and_overwrite()

  log_success(" ->  wage_imp_int and wage_imp added", namespace = "impute_wages")

  #-----------------------------------------------------------------------------
  # Minor adjustments:
  #   - Limit imputed wages at 10 * 99th percentile (in extremely rare cases an
  #     imputed wage could by chance be implausibly high)
  #   - Replace a missing second-stage wage by the first-stage one, which is
  #     what a cell that could not be fitted, or a spell with no East/West
  #     information, leaves behind
  #-----------------------------------------------------------------------------
  #
  #10_wages_imputation.do takes the 99th percentile of wage_imp, the level, not
  #of its logarithm, and only after the second step has run:
  #  sum wage_imp, d
  #  global maxWage = 10 * r(p99)
  #10 seems awfully high for me as a cutoff. 2 would seem more reasonable to
  #exclude weirdly high observations. But that's what's also in the original
  #code.
  #
  #`summarize, detail` and DuckDB's quantile_cont() do not define the 99th
  #percentile the same way, so the bound differs in its last digits. It is ten
  #times a percentile and binds on almost nothing, but it is a reason this
  #column gets a tolerance rather than an exact comparison.

  max_wage_bound <- tbl(connection, "data") |>
    summarise(max_wage = quantile(wage_imp, 0.99) * 10) |>
    collect() |>
    pull(max_wage)

  glue("Imputed wages are bounded at {round(max_wage_bound, 2)} EUR per day") |>
    log_info(namespace = "impute_wages")

  tbl(connection, "data") |>
    mutate(wage_imp_int = if_else(!is.na(wage_imp_int) & wage_imp_int > max_wage_bound,
                                  max_wage_bound, wage_imp_int),
           wage_imp     = if_else(!is.na(wage_imp) & wage_imp > max_wage_bound,
                                  max_wage_bound, wage_imp)) |>
    mutate(wage_imp = if_else(is.na(wage_imp), wage_imp_int, wage_imp)) |>
    compute_and_overwrite()

  log_success(" ->  Implausibly high wages bounded and the second stage filled from the first",
              namespace = "impute_wages")

  #-----------------------------------------------------------------------------
  # Clean up
  #-----------------------------------------------------------------------------
  #
  #The same list 10_wages_imputation.do drops, so the step leaves behind the
  #three variables its header names: cens, wage and wage_imp.

  cleanup_to_deselect <- c("educ_tmp", "old", "age_sq", "age_old", "age_sq_old",
                           "tenure_sq", "ln_wage_mean_worker", "only_one_obs",
                           "ln_wage_mean_firm", "only_one_worker",
                           "limit_assess4", "ln_limit_assess4", "ln_wage",
                           "ln_wage_cens", "wage_imp_int", "ln_wage_imp",
                           "ln_wage_imp2")

  paste0("The following variables are deselected from the data for cleanup: ",
         cleanup_to_deselect |>
          str_c(collapse=", ") |>
          str_replace(",([^,]*)$", " and \\1")) |>
    log_info(namespace = "impute_wages")

  tbl(connection, "data") |>
    select(-all_of(cleanup_to_deselect)) |>
    compute_and_overwrite()

  log_success(" ->  Cleanup finished", namespace = "impute_wages")
  log_success("Wage imputation file finished", namespace = "impute_wages")

  #Return the connection so we can pipe prepare functions
  return(connection)
}
