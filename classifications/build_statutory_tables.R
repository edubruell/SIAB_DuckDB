# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Build the three statutory lookup tables used by R/functions/04, 05 and 06.
#
# Writes:
#   - wa_ceiling.csv      year, east, limit_assess    contribution assessment
#                         ceiling ("Beitragsbemessungsgrenze"), EUR per day.
#   - limit_marginal.csv  year, east, limit_marginal  marginal part-time income
#                         threshold ("Geringfuegigkeitsgrenze"), EUR per day.
#   - cpi.csv             year, cpi                   consumer price index,
#                         base year 2015.
#
# Sources, parsed rather than transcribed:
#   - 06_wages_assessment_ceiling.do
#   - 07_wages_marginal.do
#   - 08_wages_deflation.do
# of the Stueber/Dauth/Eppelsheimer preparation. The script reads the `replace'
# statements out of those files and applies them in file order, the way Stata
# would, so the CSVs cannot drift from the reference by a typing slip.
#
# The three do-files are identical across all reference forks apart from a local
# `east' -> `east_ss' rename, so the published origin is the reference used here.
#
# The reference files themselves carry the FDZ-Arbeitshilfe values
# (http://doku.iab.de/fdz/Bemessungsgrenzen_de_en.xls). The CPI for 2023 and
# 2024 is not official: the Statistisches Bundesamt switched the base year to
# 2020, so 08_wages_deflation.do carries the 2015-base series forward with the
# published inflation rates (5.9% for 2023, 2.2% for 2024).
#
# Years 1975 to 1991 have no East/West split in the reference: the do-files set
# one value for the whole year. Both east rows therefore carry it, which is what
# the sequential `replace' produces in Stata too.
#
# Run this only when the reference changes; the CSVs it writes are committed.
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library("pacman")
p_load(dplyr, tidyr, readr, stringr, purrr, here)

# The reference do-files. Not redistributed with this repo, so the fallback
# points at the working folder this project keeps them in.
# make_fixtures.do reads the same variable.
stata_reference <- Sys.getenv(
  "SIAB_STATA_REFERENCE",
  here("local_context", "stata_reference", "origin_EastGermanWageStructure")
)

exchange_rate <- 1 / 1.95583   # EUR per DM, as set by $exch in the do-files

# ------------------------------------------------------------------------------
# Parse one `replace <variable> = <number>[*$exch] if <condition>' statement.
# Returns NULL for every other line.
# ------------------------------------------------------------------------------
parse_replace <- function(line, variable) {
  code <- str_remove(line, "//.*$") |> str_trim()

  pattern <- paste0(
    "^replace\\s+", variable, "\\s*=\\s*",     # the assignment
    "([0-9]*\\.?[0-9]+)",                      # the literal value
    "\\s*(\\*\\s*\\$exch)?",                   # optional DM conversion
    "\\s+if\\s+(.+)$"                          # the condition
  )
  if (!str_detect(code, pattern)) return(NULL)

  value <- as.numeric(str_match(code, pattern)[, 2])
  if (!is.na(str_match(code, pattern)[, 3])) value <- value * exchange_rate
  condition <- str_match(code, pattern)[, 4] |> str_trim()

  with_east <- str_match(
    condition,
    "^east\\s*==\\s*([01])\\s*&\\s*jahr\\s*==\\s*([0-9]{4})$"
  )
  if (!is.na(with_east[, 1])) {
    return(list(
      value = value,
      east  = as.integer(with_east[, 2]),
      year  = as.integer(with_east[, 3])
    ))
  }

  year_only <- str_match(condition, "^jahr\\s*==\\s*([0-9]{4})$")
  if (!is.na(year_only[, 1])) {
    return(list(value = value, east = NA_integer_, year = as.integer(year_only[, 2])))
  }

  stop("Unparsed condition in a `replace ", variable, "' statement: ", condition)
}

# ------------------------------------------------------------------------------
# Read a do-file and replay its `replace' statements onto a year x east grid, in
# file order, so that a later statement overwrites an earlier one exactly as
# Stata's sequential `replace' does. `rebase` is the one non-literal statement in
# the collection, the 1995 -> 2015 rebasing in 08_wages_deflation.do: it is given
# as the line to look for and the function to apply where that line stands.
# ------------------------------------------------------------------------------
build_table <- function(do_file, variable, value_name,
                        rebase_line = NULL, rebase_fun = NULL) {
  lines <- read_lines(file.path(stata_reference, do_file))
  parsed <- map(lines, parse_replace, variable = variable)

  if (all(map_lgl(parsed, is.null))) {
    stop("No `replace ", variable, "' statement in ", do_file)
  }

  rebase_at <- if (is.null(rebase_line)) NA_integer_ else which(str_detect(lines, fixed(rebase_line)))
  if (!is.null(rebase_line) && length(rebase_at) != 1) {
    stop("Expected exactly one rebasing statement matching `", rebase_line, "' in ", do_file)
  }

  years <- map_int(compact(parsed), "year")
  grid <- expand_grid(year = min(years):max(years), east = 0:1) |>
    mutate(value = NA_real_)

  for (i in seq_along(lines)) {
    if (!is.na(rebase_at) && i == rebase_at) {
      grid <- grid |> mutate(value = rebase_fun(value))
      next
    }
    s <- parsed[[i]]
    if (is.null(s)) next
    rows <- grid$year == s$year & (is.na(s$east) | grid$east == s$east)
    grid$value[rows] <- s$value
  }

  missing_years <- grid |> filter(is.na(value)) |> pull(year) |> unique()
  if (length(missing_years) > 0) {
    stop("No value for ", variable, " in: ", paste(missing_years, collapse = ", "))
  }

  grid |> rename(!!value_name := value)
}

# ------------------------------------------------------------------------------
# The three tables
# ------------------------------------------------------------------------------
wa_ceiling <- build_table(
  "06_wages_assessment_ceiling.do", "limit_assess", "limit_assess"
)

limit_marginal <- build_table(
  "07_wages_marginal.do", "limit_marginal", "limit_marginal"
)

# The CPI has no East/West split; the do-file's rebasing statement turns the
# 1975-1991 West series (base 1995) into the 2015 base the rest of it uses.
cpi <- build_table(
  "08_wages_deflation.do", "cpi", "cpi",
  rebase_line = "replace cpi = (cpi/89.0)*65.5",
  rebase_fun  = \(cpi) (cpi / 89.0) * 65.5
) |>
  filter(east == 0) |>
  select(year, cpi)

write_csv(wa_ceiling,     here("classifications", "wa_ceiling.csv"))
write_csv(limit_marginal, here("classifications", "limit_marginal.csv"))
write_csv(cpi,            here("classifications", "cpi.csv"))

cat(
  "wa_ceiling.csv    ", nrow(wa_ceiling), "rows,",
  min(wa_ceiling$year), "-", max(wa_ceiling$year), "\n",
  "limit_marginal.csv", nrow(limit_marginal), "rows,",
  min(limit_marginal$year), "-", max(limit_marginal$year), "\n",
  "cpi.csv           ", nrow(cpi), "rows,",
  min(cpi$year), "-", max(cpi$year), "\n"
)
