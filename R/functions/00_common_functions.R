#====================================================================
#0. General functions 
#====================================================================

#We will need a negated  version of %in%
`%nin%` <- Negate(`%in%`)

#Make custom folder reference functions like here compatible with typical siab paths
folder_reference_factory <- function(target_folder){
  stopifnot("Please set the path to a target_folder" = !is.null(target_folder))
  function(...){
    if (!missing(..1)) {
      #Same test rprojroot uses internally, inlined so the factory does not
      #depend on another package's unexported functions
      absolute <- grepl("^[/\\\\~]|^[a-zA-Z]:[/\\\\]", ..1)
      if (all(absolute)) {
        return(file.path(...))
      }
      if (any(absolute)) {
        stop("Combination of absolute and relative paths not supported.",
             call. = FALSE)
      }
    }
    file.path(target_folder, ...)
  }
}

#Quick year from integer date
quick_year = function(dates) {
  quadrennia  <-  as.integer(unclass(dates) %/% 1461L)
  rr          <-  unclass(dates) %% 1461L
  rem_yrs     <-  (rr >= 365L) + (rr >= 730L) + (rr >= 1096L)
  return(1970L + 4L * quadrennia + rem_yrs)
}

#Round to Stata's float precision
#
#The reference prep generates its lookup values with plain `gen`, which gives a
#Stata float: four bytes, about seven decimal digits. classifications/*.csv holds
#the same values at full double precision, because build_statutory_tables.R reads
#the decimal literals out of the Stata source rather than the stored variable.
#Those extra digits are spurious: the reference never had them, and carrying them
#changes results where a value is compared for equality. The marginal-wage flag is
#the case that bites, where a spell earning exactly the threshold falls on the
#other side of `<=`. Round the lookup on read-in and the comparison agrees.
stata_float <- function(x) {
  readBin(writeBin(as.double(x), raw(), size = 4), "numeric",
          n = length(x), size = 4)
}

#Round to Stata's float precision inside the database
#
#The database companion of stata_float(), for a value DuckDB generates rather
#than one read from a csv. `gen` and `egen` make a float unless told otherwise,
#so a reference step that generates an intermediate has already dropped
#everything past about the seventh digit before it uses it. Where such an
#intermediate feeds a rounding, as in 02_grund154.do, carrying the full double
#through moves the result by a cent.
#
#Takes the expression as a SQL string and hands back a `sql()` object, so it is
#used inside mutate() like any other column expression.
sql_stata_float <- function(expression){
  sql(glue("CAST(CAST({expression} AS FLOAT) AS DOUBLE)"))
}

#Stata's greater-than, with its ordering of missing
#
#Stata stores a missing value as a number larger than any other, so `a > b` is
#true when a is missing and b is not, false when b is missing, and false when
#both are. dplyr propagates NA instead, which turns the first two cases into a
#missing result. Every port of a `replace ... if x > y` guard has to go through
#this, because the reference relies on the ordering: 10_wages_imputation.do
#leaves a spell whose assessment ceiling is unknown flagged as uncensored, and
#a case_when() that returns NA there disagrees on 15,738 rows of the test data.
#
#Takes both sides as SQL strings and hands back a `sql()` object, so it is used
#inside mutate() like any other column expression.
sql_stata_gt <- function(left, right){
  sql(glue("(CASE WHEN {left} IS NULL AND {right} IS NULL THEN FALSE ",
           "WHEN {left} IS NULL THEN TRUE ",
           "WHEN {right} IS NULL THEN FALSE ",
           "ELSE {left} > {right} END)"))
}

#Simple input validation for functions
validate_inputs <- function(predicates) {
  # Use lapply to iterate over predicates and stop on the first failure
  results <- lapply(names(predicates), function(error_msg) {
    if (!predicates[[error_msg]]) {
      stop(error_msg)
    }
  })
}


#====================================================================
#1. Database related functions
#====================================================================

#Open a DuckDB connection with this arm's settings
#
#DuckDB is given no memory budget of its own by default: it takes about 80
#percent of the machine and spills beyond that. `memory_limit` says how much it
#may hold, in DuckDB's own notation such as `4GB`, and `temp_directory` where it
#may spill. Both come from the environment, and unset they are no-ops, so a
#connection behaves as it did before this function existed.
#
#Every script in this arm connects through here, which is the counterpart of
#`open_store()` in the Python arm: one place configures a connection, rather
#than each dbConnect() call carrying its own settings or missing them.
siab_connect <- function(dbdir,
                         read_only      = FALSE,
                         memory_limit   = Sys.getenv("SIAB_DUCKDB_MEMORY_LIMIT"),
                         temp_directory = Sys.getenv("SIAB_DUCKDB_TEMP_DIR")){
  connection <- dbConnect(duckdb::duckdb(), dbdir = dbdir, read_only = read_only)

  c(memory_limit   = memory_limit,
    temp_directory = temp_directory) |>
    keep(nzchar) |>
    imap(\(value, setting) glue("SET {setting} = '{value}'")) |>
    walk(\(statement) dbExecute(connection, statement))

  connection
}

compute_and_overwrite <- function(query,target_table="data"){
  #Check whether there is no temp table
  if(dbExistsTable(con, "temp")){
    stop("Temporary table 'temp' allready exsists")
  }
  
  #Compute query
  query |>
    compute(name = "temp", temporary = FALSE)
  
  #Did the query compute
  if(!dbExistsTable(con, "temp")){
    stop("Temporary table could not be computed")
  }
  
  
  # Ensure the "data" table is dropped if it exists
  if (dbExistsTable(con, target_table)) {
    dbRemoveTable(con, target_table)
  }
  
  #Rename the query
  rename_sql <- glue('ALTER TABLE temp RENAME TO {target_table}') 
  dbExecute(con, rename_sql)

  #Every step rewrites the whole table, and the blocks the old copy held are
  #free the moment it is dropped. DuckDB reuses free blocks only across a
  #checkpoint, so without one the file grows by a full copy at every step and
  #the run's high-water mark is the sum of all of them. Measured over a
  #one-copy test run on 2026-09-21f: peak 1.462 GB and 0.958 GB left behind
  #without this line, 0.679 GB and 0.267 GB with it, at the same wall clock.
  dbExecute(con, "CHECKPOINT")
  invisible(NULL)
}

#Copy a finished store into a fresh file, leaving no free blocks behind
#
#DuckDB never shrinks a database file by itself, so a store carries every block
#any step of the run ever allocated. `CHECKPOINT` frees blocks for reuse and
#returns none of the file: what reclaims them is copying the live data into a
#new database, which `COPY FROM DATABASE` does in one statement. The copy is
#written beside the original and moved over it, so an interrupted compaction
#leaves the store it started from.
#
#The connection must be closed before this is called: it opens its own.
compact_store <- function(db_file){
  stopifnot("No such database file" = file.exists(db_file))

  before  <- file.size(db_file)
  compact <- paste0(db_file, ".compact")
  unlink(c(compact, paste0(compact, ".wal")))

  #Both databases are attached to an in-memory connection, the finished store
  #read-only: a read-only connection would pass that on to the file it is
  #copying into, and a read-write one would rewrite the store this is meant to
  #leave untouched until the move.
  connection <- siab_connect(":memory:")
  on.exit(dbDisconnect(connection, shutdown = TRUE), add = TRUE)
  dbExecute(connection, glue("ATTACH '{db_file}' AS finished (READ_ONLY)"))
  dbExecute(connection, glue("ATTACH '{compact}' AS compacted"))
  dbExecute(connection, "COPY FROM DATABASE finished TO compacted")
  dbExecute(connection, "DETACH compacted")
  dbExecute(connection, "DETACH finished")
  dbDisconnect(connection, shutdown = TRUE)
  on.exit()

  unlink(paste0(db_file, ".wal"))
  if (!file.rename(compact, db_file)) {
    unlink(c(compact, paste0(compact, ".wal")))
    stop("Could not move the compacted store over ", db_file)
  }

  after <- file.size(db_file)
  glue("Store compacted: {round(before / 1e9, 3)} GB -> {round(after / 1e9, 3)} GB") |>
    cat("\n")
  invisible(after)
}