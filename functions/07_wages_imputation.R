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
  

impute_wages <- function(.connection, .log_file = NULL){
  # Remove old log file if it exists
  if (!is.null(.log_file) && file.exists(.log_file)) {
    file.remove(.log_file)
  }
  
  # Clear existing log appenders
  log_appender(NULL, namespace = "impute_wages")
  
  # Initialize console logger
  log_appender(appender_console, namespace = "impute_wages")
  
  # Initialize file logger if log_file is specified
  if (!is.null(.log_file)) {
    log_appender(appender_tee(file = .log_file), namespace = "impute_wages")
  }
  
  
  #Check whether there is no temp table
  if(dbExistsTable(.connection, "tmp_imputation")){
    log_warnings("Temporary table for wage imputation 'tmp_imputation' allready exsisted at start!")
    dbRemoveTable(.connection,  "tmp_imputation")
    log_info("-> 'tmp_imputation' table deleted to create new imputation run", namespace = "impute_wages")
    
  }
  
  #------------------------------------------------------
  #   Modify assessment limit and flag censored wages
  #------------------------------------------------------
  
  #Substract 4 EUR from exact assessment limit 
  #(to make sure all cencored wages are covered by the imputation)
  log_info("Modify assessment limit and flag censored wages", 
           namespace ="impute_wages")
  
  tbl(.connection, "data") %>%
    mutate(
      limit_assess4 = limit_assess_defl - (100 * 4/cpi),	
      ln_limit_assess4 = log(limit_assess4),
      #Flag censored wages:
      cens = case_when(
        #only flag BeH spells (quelle == 1)
        wage_defl <= limit_assess4 & quelle_gr == 1 ~ 0L,
        wage_defl  > limit_assess4 & quelle_gr == 1 ~ 1L, 
        .default=NA_integer_
      )
  ) %>%
    compute_and_overwrite()
  
  log_success(" ->  limit_assess4, ln_limit_assess4 and cens added", namespace = "impute_wages")
  
  #------------------------------------------------------
  # Overview of censored wages
  #------------------------------------------------------
  
  #overall censoring
  tbl(.connection, "data") %>%
    filter(quelle_gr == 1, !is.na(cens)) %>%
    count(cens) %>%
    collect() %>%
    mutate(cens = factor(cens, levels=c(0,1),labels = c("below the wage assessment limit","above the wage assessment limit")),
           pct = scales::label_percent()(n/sum(n))) %>%
    glue_data("{n} BeH wages ({pct}) are {cens}") %>%
    walk(log_info, namespace = "impute_wages")
  log_info("--------------------", namespace ="impute_wages")

  #Censoring by education
  tbl(.connection, "data") %>%
    filter(quelle_gr == 1, !is.na(cens)) %>%
    count(cens,educ)  %>%
    collect() %>%
    pivot_wider(id_cols="educ",values_from="n",names_from="cens",names_prefix = "cens") %>%
    mutate(educ = if_else(is.na(educ),0L,educ),
           educ = factor(educ, levels=c(0,1,2,3), labels=c("Missing","Low","Medium","High")),
           pct = scales::label_percent()(cens1/(cens0+cens1))
           ) %>%
    glue_data("{pct} of wages are censored for {educ} Education") %>%
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
  tbl(.connection, "data") %>%
    filter(quelle_gr == 1, !is.na(cens),educ==3L,age<=60,age>=18) %>%
    mutate(age_category = cut(age, 
                              breaks = age_breaks, 
                              labels = age_labels, 
                              right = FALSE, 
                              include.lowest = TRUE)
    ) %>%
    group_by(age_category) %>%
    summarise(share_censored = mean(cens, na.rm=TRUE)) %>%
    arrange(age_category) %>%
    collect() %>%
    mutate(share_censored = scales::label_percent()(share_censored)) %>%
    glue_data("For the highly educated in age range {age_category} {share_censored} of wages are censored") %>%
    walk(log_info, namespace = "impute_wages")
  
  log_info("--------------------", namespace ="impute_wages")
  
  #censoring by fulltime/part-time
  tbl(.connection, "data") %>%
    filter(quelle_gr == 1, !is.na(cens))  %>%
    group_by(teilzeit) %>%
    summarise(share_censored = mean(cens, na.rm=TRUE)) %>%
    collect() %>%
    mutate(teilzeit = if_else(is.na(teilzeit),9L,teilzeit),
           teilzeit = factor(teilzeit, levels=c(0,1,9), labels=c("Fulltime","Parttime","Missing FT-Info")),
           share_censored = scales::label_percent()(share_censored)
    ) %>%
    glue_data("{share_censored} of wages are censored for {teilzeit} employees") %>%
    walk(log_info, namespace = "impute_wages")
  
  log_info("--------------------", namespace ="impute_wages")
  
    
  #censoring by gender
  tbl(.connection, "data") %>%
    filter(quelle_gr == 1, !is.na(cens))  %>%
    group_by(frau) %>%
    summarise(share_censored = mean(cens, na.rm=TRUE)) %>%
    collect() %>%
    mutate(frau = factor(frau, levels=c(0,1), labels=c("Men","Women")),
           share_censored = scales::label_percent()(share_censored)
    ) %>%
    glue_data("{share_censored} of wages of {frau} are censored ") %>%
    walk(log_info, namespace = "impute_wages")
  
  log_info("--------------------", namespace ="impute_wages")

  #------------------------------------------------------
  #Prepare dependent variable: wage
  #------------------------------------------------------
  
  log_info("Preapering dependent variable for imputation", namespace ="impute_wages")
  
  tbl(.connection, "data") %>%
    mutate(
      #Daily wage, not imputed, top-coded wages replaced by assessment ceiling (-4 EUR)
      wage = case_when(quelle_gr == 1   & wage_defl <= limit_assess4 ~ wage_defl,
                       quelle_gr == 1   & wage_defl >  limit_assess4 ~ limit_assess4,
                       .default=NA_real_),
      ln_wage = if_else(wage!=0,log(wage),NA_real_),
      ln_wage_cens = if_else(cens == 0,ln_wage,NA_real_)
    ) %>%
    compute_and_overwrite()
  
  log_success(" ->  wage, ln_wage and ln_wage_cens variables added", namespace = "impute_wages")
  
  
  #----------------------------------------------------------------------
  # Prepare control variables for imputation
  #----------------------------------------------------------------------
  
  log_info("Preapering control variables for imputation", namespace ="impute_wages")
  
  tbl(.connection, "data") %>%
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
  ) %>%
    compute_and_overwrite()
  
  controls_imputation <-  c("frau","old","age","age_sq","age_old","age_sq_old","tage_job","tenure_sq")
  
  
  log_success(" ->  Controls variables for imputation added", namespace = "impute_wages")
  
  #----------------------------------------------------------------------
  # Prepare imputation plan; by default: year, skill group and East/West 
  #----------------------------------------------------------------------
  #(NOTE: by default we assign Berlin to East Germany)
  
  imputation_plan <- tbl(.connection, "data") %>%
    distinct(year,educ_tmp,east) %>%
    filter(!is.na(east)) %>%
    arrange(year,east,educ_tmp) %>%
    collect() %>%
    rename_with(~paste0(".",.x))
  
  #-------------------------------------------------------------------------------
  # Imputation with observable characteristics (for details refer to Gartner (2005)
  #-------------------------------------------------------------------------------
  
  #An internal function to run  imputation on observables with a 
  #censored normal regression from survreg
  imputw <- function(.year,.educ_tmp,.east){
    glue("Starting imputation run for year={.year}, educ= {.educ_tmp} and east = {.east}") %>%
      log_info(namespace = "impute_wages")
    temp_tbl <- tbl(.connection, "data") %>%
      filter(year==.year,
             educ_tmp==.educ_tmp,
             east==.east,
             quelle_gr==1,
             #excluding marginal wages
             marginal ==0) %>%
      select(persnr,spell,begepi,endepi,all_of(controls_imputation),ln_wage,cens,ln_limit_assess4) %>%
      collect()
    #---------------------------------------------------------------------------#
    #Comment: Here is some potential point of improvement when moving this to the 
    #         IEB. This allready tries to only get small datasets in single 
    #         collections from duckdb (a typical temp_tbl is just 25 MB large with 
    #         the 2% SUF). Yet at 50 times the size we would still need around  
    #         1.25 GB of memory per call. Should be no problem on a stronger 
    #         computer, but could still be reasonable to improve memory usage by 
    #         taking a smaller fold of the data (i.e. move dummies like frau into 
    #         the sample splits above )
    #---------------------------------------------------------------------------#
    
    
    #Censored regression on the local temporary collection of the data frame 
    #Use the censored normal regression in survreg
    #Decent descriptions of how survreg work can be found on Claus C. Pörtners Site 
    #and by Achim Zeileis
    #Note: I use the notation from there instead the one used by Dauth and Eppelsheimer
    tfit <- survreg( Surv(ln_wage, !cens, type='right') ~  
               as.factor(frau) + age + age_sq + age_old + age_sq_old + tage_job +
               tenure_sq,
             data=temp_tbl, dist='gaussian')
    #Standard error of regression
    se<-tfit[[8]]
    #Regression prediction
    temp_tbl <- temp_tbl %>%
      na.omit() %>%
      mutate(xb00       = predict(tfit),
             alpha00    = (ln_limit_assess4-xb00)/se,
             imp_values =  xb00 + se * qnorm( runif(length(xb00)) * (1-pnorm(alpha00) ) + pnorm(alpha00)))
    
    #Make sure the imputation produces no values below social security contribution
    assertion_below_social_security <- temp_tbl %>%
      filter(imp_values < ln_wage) %>%
      nrow() == 0
    
    if(!assertion_below_social_security){ 
     glue("Imputed wage(s) below censoring limit in year {.year} and education group {.educ} and east = {.east}") %>%
      log_warnings()
    }
    
    #keep only what's needed 
    temp_tbl <- temp_tbl %>% 
      transmute(
        persnr,spell,begepi,endepi,
        ln_wage_imp = if_else(cens==1,imp_values,ln_wage)) 
    
    # Append batch data to DuckDB table
    dbWriteTable(.connection, "tmp_imputation", temp_tbl, append = TRUE)
    log_success(" ->  Run finished and writen to temporary datatbase table", namespace = "impute_wages")
    #Collect garbage just in case
    gc()
    invisible(NULL)
  }
  
  #Run the imputation
  imputation_plan %>%
    pwalk(imputw)
  
  
  #-----------------------------------------------------------------------------
  # Merge imputaed wages back to the data
  #-----------------------------------------------------------------------------
  
  log_info("Merging imputed wage to main data", namespace = "impute_wages")
  
  tbl(.connection,"data") %>%
    left_join(tbl(.connection,"tmp_imputation"), by=c("persnr","spell","begepi","endepi")) %>%
    compute_and_overwrite(.table = "data")

  log_success(" ->  Imputed data merged back", namespace = "impute_wages")
  

  log_info("Deleting temporary imputation helper table from database", namespace = "impute_wages")
  dbRemoveTable(.connection,  "tmp_imputation")
  
  log_success(" ->  Table deleted", namespace = "impute_wages")
  
  #-----------------------------------------------------------------------------
  #   Imputed wages in levels
  #-----------------------------------------------------------------------------
  log_info("Add imputed wages in levels", namespace = "impute_wages")
  
  tbl(.connection,"data") %>%
    mutate(wage_imp = exp(ln_wage_imp)) %>%
    compute_and_overwrite()
  
  log_success(" ->  Imputed wages in levels added", namespace = "impute_wages")
  
  #-----------------------------------------------------------------------------    
  # Minor adjustments:
  # 	- Limit imputed wages at 10 * 99th percentile
  # (in extremely rare cases imputed wages could by chance be inplausible high)
  #-----------------------------------------------------------------------------
  
  max_wage_bound <- tbl(.connection,"data") %>%
    #10 seems awfully high for me as a cutoff. 
    #2 would seem more reasonable to exclude weirdly high observations
    #But that's what's also in the original code
    summarise(max_wage = quantile(ln_wage_imp,0.99)*10) %>%
    collect() %>%
    pull(max_wage)

  tbl(.connection,"data") %>%
    mutate(ln_wage_imp = if_else(ln_wage_imp > max_wage_bound,max_wage_bound,ln_wage_imp),
           wage_imp = if_else(ln_wage_imp > max_wage_bound,exp(max_wage_bound),wage_imp)) %>%
    compute_and_overwrite()
  
  log_success(" ->  Implausibly high wages bounded", namespace = "impute_wages")
  
  
  #-----------------------------------------------------------------------------    
  # Clean up
  #-----------------------------------------------------------------------------    
  
  cleanup_to_deselect <- c("old","age_sq","age_old","age_sq_old","tenure_sq",
    "limit_assess4","ln_limit_assess4","ln_wage_cens")
  
  paste0("The following variables are deselected from the data for cleanup: ",
         cleanup_to_deselect %>% 
          str_c(collapse=", ") %>%
          str_replace(",([^,]*)$", " and \\1")) %>%
    log_info(namespace = "impute_wages")
  

  tbl(.connection,"data") %>%
    select(-all_of(cleanup_to_deselect)) %>%
    compute_and_overwrite()
  
  log_success(" ->  Cleanup finished", namespace = "impute_wages")
  log_success("Wage imputation file finished", namespace = "impute_wages")
  
  #Return the .connection so we can pipe prepare functions
  return(.connection)
}