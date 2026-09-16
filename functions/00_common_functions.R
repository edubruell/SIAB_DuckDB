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
  invisible(NULL)
}