# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#   SIAB Preparation
#
# Drop every column that holds nothing but missing values
#
# Generates no variable and changes no value. It removes the columns that are
# missing on every row of the table and keeps the rest.
#
# Notes:
#   00_master_SIAB.do does this inline rather than in a numbered step, right
# after the source restriction and before it generates jahr and age:
#
#     * Remove all variables that contain only missings
#     foreach var of varlist _all {
#         capture assert missing(`var')
#         if !_rc {
#             drop `var'
#         }
#     }
#
# The position is what gives the step its point. Cutting the sources down to
# the employment history leaves behind the variables that only the benefit
# spells ever filled, and they would otherwise travel through the whole
# preparation as columns of nothing.
#
#   What counts as missing. Stata's `missing()` is true for a numeric missing
# and for the empty string, because the empty string is how a Stata string
# variable spells missing. DuckDB has both an empty string and SQL NULL, and
# they are different values. This port drops a column when every row of it is
# NULL, and it keeps a column of empty strings. The two notions therefore part
# company on exactly one case: a string column that is empty on every row is
# dropped by the reference and kept here. That is deliberate. An empty string
# in the database is a value someone wrote, and the read-in preserves the
# difference, so throwing the column away would discard information the
# reference never had in the first place.
#
#   The reference gates its optional do-files behind macros. `drop` is the same
# kind of switch: it is on by default, as the master's loop is, and setting it
# to FALSE leaves the column set alone.
#
#   The step runs on the table as it stands, so what it drops depends on where
# it is called. In the pipeline it is the first step after the source
# restriction, which is where the master runs it. It sees `year` and `age`,
# which the master generates just after the loop rather than just before it;
# neither can be missing throughout once begepi and gebjahr are there, so the
# column set is the same either way.
#
# Author(s): Eduard Bruell based on code by Wolfgang Dauth, Johann Eppelsheimer
#            and Heiko Stueber
#
# Version: 1.0
# Created: 2026-09-17
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

drop_empty_columns <- function(connection,
                               drop     = TRUE,
                               log_file = NULL){

  validate_inputs(c(
    "drop has to be TRUE or FALSE" =
      length(drop) == 1 && is.logical(drop) && !is.na(drop)
  ))

  # Remove old log file if it exists
  if (!is.null(log_file) && file.exists(log_file)) {
    file.remove(log_file)
  }

  # Clear existing log appenders
  log_appender(NULL, namespace = "empty_columns")

  # Initialize console logger
  log_appender(appender_console, namespace = "empty_columns")

  # Initialize file logger if log_file is specified
  if (!is.null(log_file)) {
    log_appender(appender_tee(file = log_file), namespace = "empty_columns")
  }

  if (!drop) {
    log_info("drop = FALSE, the column set is left as it is",
             namespace = "empty_columns")
    #Return the connection so we can pipe prepare functions
    return(connection)
  }

  columns <- colnames(tbl(connection, "data"))
  n_rows  <- tbl(connection, "data") |> count() |> pull(n)

  # With no rows nothing witnesses a value, so every column would qualify and
  # the step would empty the schema. An empty table is a broken pipeline, not a
  # table of empty columns, so say so and leave the columns alone.
  if (n_rows == 0L) {
    log_warn("The data table has no rows, so no column is dropped",
             namespace = "empty_columns")
    log_success("Empty columns dropped", namespace = "empty_columns")
    return(connection)
  }

  #One count of non-missing values per column. An empty string is counted,
  #only NULL is not.
  filled <- tbl(connection, "data") |>
    summarise(across(all_of(columns),
                     ~ sum(as.integer(!is.na(.x)), na.rm = TRUE))) |>
    collect()

  empty <- columns[as.integer(filled[1, columns]) == 0L]

  if (length(empty) == length(columns)) {
    stop("Every column of the data table is missing throughout, so the step ",
         "would leave no column at all", call. = FALSE)
  }

  if (length(empty) == 0L) {
    log_info("No column is missing throughout, all {length(columns)} are kept",
             namespace = "empty_columns")
    log_success("Empty columns dropped", namespace = "empty_columns")
    #Return the connection so we can pipe prepare functions
    return(connection)
  }

  tbl(connection, "data") |>
    select(-all_of(empty)) |>
    compute_and_overwrite()

  log_info("{length(empty)} of {length(columns)} columns are missing on all {n_rows} rows and are dropped: {paste(empty, collapse = ', ')}",
           namespace = "empty_columns")

  log_success("Empty columns dropped", namespace = "empty_columns")
  #Return the connection so we can pipe prepare functions
  return(connection)
}
