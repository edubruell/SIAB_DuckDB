#Load package manager and packages
library("pacman")
p_load(dplyr,  #For tidyverse compliant code
       dbplyr, #For connection to the database, translation of dplyr to sql
       readr,  #Reading in csv-files with classifications, assesment ceilings, etc.
       tidyr,  #carrying forward (tidyr::fill)and interpolationg 
       purrr,  #map-class of functions used for tidy interpolation
       duckdb, #Using duckdb as database engine for this code
       stringr,#String-interpolation and regex  
       glue,   #For concatenating strings and glue_data used for logs
       scales, #label_percent() for the censoring overviews in the imputation
       data.table, #Needed only for the old readin-part (remove dependency)
       readstata13, #Reading the Basic Establishment File for the BHP merge
       here,   #Project folder navigation
       logger, #Creating logs
       survival #Survival contains the censored normal regression needed for the imputation
       )

#Load common functions
here("functions") |>
  dir() |>
  walk(~source(here("functions",.x)))


#Set folders
dbfolder  <- folder_reference_factory("/Users/ebr/data/siab_db")
rawdata   <- folder_reference_factory("/Users/ebr/data/siab_raw")

# Open DuckDB connection
con <- dbConnect(duckdb::duckdb(), dbdir = dbfolder("siab.duckdb"), read_only = FALSE)


#Load the database and look for tables 
#(Have any prior pre-steps already been used?)
dbListTables(con)

#Setup the database connection to orig
siab_orig <- tbl(con,"orig") 

#====================================================================
#  1. Generate variables 'jahr' and 'age' in the database
#====================================================================

#Generate year and age
siab_orig |>
  # 00_master_SIAB.do keeps only the employment history before it generates
  # jahr and age: `keep if inlist(quelle,1,2,3)`. Sources 4 to 7 are dropped.
  filter(quelle %in% c(1L, 2L, 3L)) |>
  mutate(year = year(begepi),
         age  = year - gebjahr) |>
  compute_and_overwrite("data")


#====================================================================
#  2. Prepare the SIAB as a yearly panel
#====================================================================

#Prepare the SIAB-SUF with the built-in functions and write to the data table of he database
con |>
  split_episodes(               log_file = here("log","01_split_episodes.log")) |>
  reallocate_one_time_payments(  log_file = here("log","01b_grund154.log")) |>
  generate_biographic_variables(log_file = here("log","01_SIAB_Bio.log")) |>
  generate_occupation_variables(log_file = here("log","02_occupations.log")) |>
  generate_educ_variable(       log_file = here("log","03_education.log")) |>
  merge_basic_bhp(              log_file = here("log","03b_bhp_basis.log"),
                                bhp_file = rawdata("SIAB_7523_v2_bhp_basis_v1.dta")) |>
  generate_limit_assess(        log_file = here("log","04_wage_assesment_ceiling.log")) |>
  generate_limit_marginal(      log_file = here("log","05_wages_marginal.log"))|>
  deflate_wages(                log_file = here("log","06_wages_deflation.log")) |>
  impute_wages(                 log_file = here("log","07_wages_imputation.log")) |>
  # 11_merge_BHP.do and 12_merge_AKM.do are switched off in the reference
  # master, because every file they read has to be requested from the FDZ on
  # top of the SIAB itself. Uncomment either call once the files are in place.
  #
  # merge_annual_bhp(           log_file   = here("log","07b_bhp_annual.log"),
  #                             bhp_folder = rawdata("")) |>
  # merge_akm(                  log_file       = here("log","07c_akm.log"),
  #                             akm_estab_file = rawdata("SIAB_7523_v2_akm_estab.dta"),
  #                             akm_pers_file  = rawdata("SIAB_7523_v2_akm_pers.dta")) |>
  handle_parallel_episodes(     log_file = here("log","08_parallel_episodes.log"),
                                handling = "wage") |>
  build_yearly_panel(           log_file = here("log","09_yearly_panel.log"),
                                cutoff_month = 6,
                                cutoff_day   = 30) |>
  #Print the head of the table in the last step
  tbl("data")



#Cleanup intermediate database tables 
dbListTables(con) |>
  purrr::discard(~{.x %in% c("orig","data")}) |>
  walk(~dbRemoveTable(con, .x))

#Only data and orig should remain as tables
dbListTables(con)

# Close DuckDB connection
dbDisconnect(con, shutdown = TRUE)
