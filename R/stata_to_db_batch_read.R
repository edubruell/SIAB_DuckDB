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

#What each Stata storage type becomes in DuckDB, at the width Stata itself
#uses. The codes are the ones the .dta header carries: 65530 byte, 65529 int,
#65528 long, 65527 float, 65526 double. A Stata byte runs -127 to 100 with its
#missing codes at 101 to 127, an int runs to 32,740 and a long to
#2,147,483,620, so each fits the DuckDB type of the same width with room left.
#
#Reading every Stata integer as a 32-bit one, which is what the table carried
#until 2026-09-21, costs 200 bytes an `orig` row where the delivery packs the
#same 46 variables into 95. The narrowed table holds the same values and is
#about 45 percent smaller a row.
stata_storage_types <- c("65530" = "TINYINT",
                         "65529" = "SMALLINT",
                         "65528" = "INTEGER",
                         "65527" = "DOUBLE",
                         "65526" = "DOUBLE")

#The DuckDB type of every column of a delivery, read off its header. A %td or
#%d display format makes a column a date whatever its storage type says,
#because Stata stores a date as an integer and the format is what marks it.
column_types <- function(siab_file){
  header <- read.dta13(siab_file, convert.factors = FALSE, select.rows = c(1, 1))
  types  <- stata_storage_types[as.character(attr(header, "types"))]
  dates  <- grepl("^%(t|d)", attr(header, "formats"))
  types[dates] <- "DATE"
  if (anyNA(types)) {
    unknown <- attr(header, "types")[is.na(types)] |> unique() |> paste(collapse = ", ")
    stop("Unknown Stata storage type code(s) in ", basename(siab_file), ": ", unknown)
  }
  setNames(unname(types), names(rename_keys(header)))
}

#Create the `orig` table with those types, replacing whatever was there.
#dbWriteTable() would otherwise take the types from the first batch, and an R
#integer is 32 bits whatever the delivery declared.
create_orig_table <- function(con, types){
  columns <- c(types, pn_batch = "INTEGER")
  declaration <- paste0("  \"", names(columns), "\" ", unname(columns), collapse = ",\n")
  dbExecute(con, "DROP TABLE IF EXISTS orig")
  dbExecute(con, glue("CREATE TABLE orig (\n{declaration}\n)"))
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
  
  #The table is declared before the first batch, so every column keeps the
  #width the delivery gave it rather than the 32 bits an R integer arrives as.
  con <- siab_connect(dbfolder("siab.duckdb"))
  create_orig_table(con, column_types(siab_file))
  dbDisconnect(con, shutdown = TRUE)
  
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