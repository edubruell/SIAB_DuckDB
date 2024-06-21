library("pacman")
p_load(readstata13,
       glue,
       dplyr,
       purrr,
       duckdb,
       here)

#Load common functions
source(here("functions","00_common_functions.R"))

#Set folders
rawdata <- folder_reference_factory("/Users/ebr/data/siab_raw")
dbfolder <- folder_reference_factory("/Users/ebr/data/siab_db")

#Setup an empty database
con <- dbConnect(duckdb(), dbdir = dbfolder("siab.duckdb"), read_only = FALSE)

# Close DuckDB connection
dbDisconnect(con, shutdown = TRUE)


# Convert the base data to a DuckDB database 
convert_to_duckdb <- function(.siab_file, .batch_size) {
  cat("Generate batches from persnr column of siab \n")
  # Read the persnr column of the STATA file and get row splits along the batch-size that are clean splits between persnr  
  read_rows <- .siab_file %>%
    read.dta13(convert.factors = FALSE, select.cols = "persnr") %>%
    group_by(persnr) %>%
    count() %>%
    ungroup() %>%
    mutate(a = cumsum(n),
           r = floor(a / .batch_size)+1) %>%
    group_by(r) %>%
    summarise(max_r = max(a)) %>%
    mutate(min_r = lag(max_r) + 1L,
           min_r = if_else(is.na(min_r), 1L, min_r))
  
  invisible(gc())
  
  num_batches <- read_rows %>% ungroup() %>% pull(r) %>% max()
  glue(" -> Read data in {num_batches} batches \n") %>% cat("\n")
  
  # Convert a batch of the file to DuckDB
  convert_batch_to_duckdb <- function(r, min_r, max_r) {
    glue("Uploading batch {r}/{num_batches} to DuckDB\n") %>% cat("\n")
    
    batch_data <- read.dta13(file = .siab_file,
                             convert.factors = FALSE,
                             convert.dates = TRUE, select.rows = c(min_r, max_r)) %>%
      mutate(pn_batch=r)
    
    # Open DuckDB connection
    con <- dbConnect(duckdb::duckdb(), dbdir = dbfolder("siab.duckdb"), read_only = FALSE)
    
    # Append batch data to DuckDB table
    dbWriteTable(con, "orig", batch_data, append = TRUE)
    
    # Close DuckDB connection
    dbDisconnect(con, shutdown = TRUE)
    
    # Apparently this is needed
    invisible(gc())
  }
  
  # Convert all batches
  read_rows %>%
    pwalk(convert_batch_to_duckdb)
}

#Convert a SIAB SUF to a duckdb database object
rawdata("siab_r_7514_v1.dta") %>%
  convert_to_duckdb(3000000)