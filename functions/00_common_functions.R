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