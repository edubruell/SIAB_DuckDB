library("pacman")
p_load(readstata13,
       glue,
       dplyr,
       purrr,
       duckdb,
       here)

#Load common functions
source(here("R", "functions", "00_common_functions.R"))

#Set folders
#Two environment variables point at the data, each with a fallback:
#  SIAB_RAW_FOLDER  where the raw SIAB delivery sits, default ~/data/siab_raw
#  SIAB_DB_FOLDER   where the DuckDB database is written, default ~/data/siab_db
rawdata   <- folder_reference_factory(
  Sys.getenv("SIAB_RAW_FOLDER", path.expand("~/data/siab_raw"))
)
dbfolder  <- folder_reference_factory(
  Sys.getenv("SIAB_DB_FOLDER", path.expand("~/data/siab_db"))
)

#Setup an empty database
con <- siab_connect(dbfolder("siab.duckdb"))

# Close DuckDB connection
dbDisconnect(con, shutdown = TRUE)


#The 7523 v2 delivery keys on persnr_siab and betnr_siab; the reference, the
#pipeline and every fixture call them persnr and betnr, so the delivery is
#renamed here. A file that already carries the short names passes through
#untouched, which is what an older delivery such as the 7514 v1 needs.
rename_keys <- function(data){
  names(data)[names(data) == "persnr_siab"] <- "persnr"
  names(data)[names(data) == "betnr_siab"]  <- "betnr"
  data
}

#Which spelling of the person key the delivery uses, read off a single row
person_key <- function(siab_file){
  columns <- siab_file |>
    read.dta13(convert.factors = FALSE, select.rows = c(1, 1)) |>
    names()
  if ("persnr_siab" %in% columns) "persnr_siab" else "persnr"
}

# Convert the base data to a DuckDB database 
convert_to_duckdb <- function(siab_file, batch_size) {
  cat("Generate batches from persnr column of siab \n")
  # Read the persnr column of the STATA file and get row splits along the batch-size that are clean splits between persnr  
  read_rows <- siab_file |>
    read.dta13(convert.factors = FALSE, select.cols = person_key(siab_file)) |>
    rename_keys() |>
    group_by(persnr) |>
    count() |>
    ungroup() |>
    mutate(a = cumsum(n),
           r = floor(a / batch_size)+1) |>
    group_by(r) |>
    summarise(max_r = max(a)) |>
    mutate(min_r = lag(max_r) + 1L,
           min_r = if_else(is.na(min_r), 1L, min_r))
  
  invisible(gc())
  
  num_batches <- read_rows |> ungroup() |> pull(r) |> max()
  glue(" -> Read data in {num_batches} batches \n") |> cat("\n")
  
  # Convert a batch of the file to DuckDB
  convert_batch_to_duckdb <- function(r, min_r, max_r) {
    glue("Uploading batch {r}/{num_batches} to DuckDB\n") |> cat("\n")
    
    batch_data <- read.dta13(file = siab_file,
                             convert.factors = FALSE,
                             convert.dates = TRUE, select.rows = c(min_r, max_r)) |>
      rename_keys() |>
      mutate(pn_batch=r)
    
    # Open DuckDB connection
    con <- siab_connect(dbfolder("siab.duckdb"))
    
    # Append batch data to DuckDB table
    dbWriteTable(con, "orig", batch_data, append = TRUE)
    
    # Close DuckDB connection
    dbDisconnect(con, shutdown = TRUE)
    
    # Apparently this is needed
    invisible(gc())
  }
  
  # Convert all batches
  read_rows |>
    pwalk(convert_batch_to_duckdb)
}

#Convert a SIAB SUF to a duckdb database object
rawdata("siab_r_7514_v1.dta") |>
  convert_to_duckdb(3000000)