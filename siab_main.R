#Load package manager and packages
library("pacman")
p_load(glue,
       dplyr,
       dbplyr,
       tidyr,
       purrr,
       duckdb,
       glue,
       data.table,
       here, 
       logger)


#Load common functions
source(here("functions","00_common_functions.R"))

#Set folder
dbfolder <- folder_reference_factory("/Users/ebr/data/siab_db")

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
siab_orig %>%
  mutate(year = year(begepi),
         age  = year - gebjahr) %>%
  compute_and_overwrite("data")


con %>%
  split_episodes(.log_file = here("log","01_split_episodes.log"))
  

tbl(con,"data")  %>%
  mutate(test=1) %>%
  compute(name = "temp", temporary = FALSE)

#Setup the database connection to orig
siab_data <- tbl(con,"data")  


# Close DuckDB connection
dbDisconnect(con, shutdown = TRUE)








tbl(con,"dbplyr_msGD30vgod") 






#What is not implementable with duckdb internals needs to run on
#managable person batches locally and sent back to the DB
#Therefore we pull a vector of person-batches created at database conversion from the DB
pn_batches <- tbl(con,"siab_data") %>%
  distinct(pn_batch) %>%
  pull(pn_batch) %>%
  sort()

batch1 <- tbl(con,"data") %>%
  mutate(begepi_orig = begepi,
         endepi_orig = endepi,
         span_year = year(endepi) - year(begepi) + 1) %>%
  count(span_year) %>%
  arrange(n)

  
  
  filter(pn_batch==10) %>%
  collect()


batch1 %>%
  mutate(span_year = year(endepi) - year(begepi) + 1) %>%
  distinct(span_year)


batch1 %>%
  filter(persnr==98066)

dbRemoveTable(con, "dbplyr_msGD30vgod")