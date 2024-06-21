#Load package manager and packages
library("pacman")
p_load(glue,
       dplyr,
       purrr,
       duckdb,
       data.table,
       here)


#Load common functions
source(here("functions","00_common_functions.R"))

#Set folder
dbfolder <- folder_reference_factory("/Users/ebr/data/siab_db")

# Open DuckDB connection
con <- dbConnect(duckdb::duckdb(), dbdir = dbfolder("siab.duckdb"), read_only = FALSE)

#====================================================================
#  1. Generate variables 'jahr' and 'age' in the database
#====================================================================

#Load the database and look for tables 
#(Have any prior pre-steps already been used?)
dbListTables(con)

#Setup the database connection to orig
siab_orig <- tbl(con,"orig") 

#Generate year and age
siab_orig %>%
  mutate(year = year(begepi),
         age  = year - gebjahr) %>%
  compute(name = "data", temporary = FALSE, overwrite = TRUE)
  
#Setup the database connection to orig
siab_data <- tbl(con,"data")  

# Close DuckDB connection
dbDisconnect(con, shutdown = TRUE)







#What is not implementable with duckdb internals needs to run on
#managable person batches locally and sent back to the DB
#Therefore we pull a vector of person-batches created at database conversion from the DB
pn_batches <- tbl(con,"siab_data") %>%
  distinct(pn_batch) %>%
  pull(pn_batch) %>%
  sort()

batch1 <- tbl(con,"siab_data") %>%
  filter(pn_batch==10) %>%
  collect()


batch1 %>%
  filter(persnr==98066)