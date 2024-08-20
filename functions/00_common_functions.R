#====================================================================
#0. General functions 
#====================================================================

#We will need a negated  version of %in%
`%nin%` <- Negate(`%in%`)

#Make custom folder reference functions like here compatible with typical siab paths
folder_reference_factory <- function(.target_folder){
  stopifnot("Please set the path to a .target_folder" = !is.null(.target_folder))
  function(...){
    if (!missing(..1)) {
      abs <- rprojroot:::is_absolute_path(..1)
      if (all(abs)) {
        return(path(...))
      }
      if (any(abs)) {
        stop("Combination of absolute and relative paths not supported.", 
             call. = FALSE)
      }
    }
    rprojroot:::path(.target_folder, ...)
  }
}

#Quick year from integer date
quick_year = function(dates) {
  quadrennia  <-  as.integer(unclass(dates) %/% 1461L)
  rr          <-  unclass(dates) %% 1461L
  rem_yrs     <-  (rr >= 365L) + (rr >= 730L) + (rr >= 1096L)
  return(1970L + 4L * quadrennia + rem_yrs)
}

#Simple input validation for functions
validate_inputs <- function(.predicates) {
  # Use lapply to iterate over predicates and stop on the first failure
  results <- lapply(names(.predicates), function(error_msg) {
    if (!.predicates[[error_msg]]) {
      stop(error_msg)
    }
  })
}


#====================================================================
#1. Database related functions
#====================================================================

compute_and_overwrite <- function(.query,.table="data"){
  #Check whether there is no temp table
  if(dbExistsTable(con, "temp")){
    stop("Temporary table 'temp' allready exsists")
  }
  
  #Compute query
  .query %>%
    compute(name = "temp", temporary = FALSE)
  
  #Did the query compute
  if(!dbExistsTable(con, "temp")){
    stop("Temporary table could not be computed")
  }
  
  
  # Ensure the "data" table is dropped if it exists
  if (dbExistsTable(con, .table)) {
    dbRemoveTable(con, .table)
  }
  
  #Rename the query
  rename_sql <- glue('ALTER TABLE temp RENAME TO {.table}') 
  dbExecute(con, rename_sql)
  invisible(NULL)
}